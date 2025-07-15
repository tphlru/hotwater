from lightweight_charts import Chart
import pandas as pd
from datetime import datetime, timedelta
from tinkoff.invest import (
    AsyncClient,
    SubscriptionInterval,
)
from hotwater.back.levels import (
    MarketDataApp,
    LastPriceSubscription,
    CandlesSubscription,
    MarketDataStreamManager,
    MarketDataProcessor,
)
from hotwater.data.helpers import (
    ensure_timezone,
    GMT_3,
    standardize_datetime_column,
    quotation_to_float,
)


class LevelsAlerts(MarketDataApp):
    def __init__(self, chart: Chart, t_token: str, tickers: list, visible_ticker: str):
        self.tickers: list = tickers
        self.current_price: dict = {}
        self.show_ticker: str = visible_ticker
        super().__init__(t_token, chart)
        self._setup_subs()

    async def change_visible_ticker(self, ticker):
        if ticker not in self.tickers:
            raise KeyError(
                "При change_visible_ticker тикер должен быть в исходном списке"
            )
        self.show_ticker = ticker
        await self.stream_manager.unsubscribe("candles")
        self.add_subscription(
            CandlesSubscription(
                "candles", SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE
            ),
            [self.show_ticker],
            self.async_candles_handler,
        )
        await self.stream_manager.subscribe("candles")

    def update_candle_chart(self):
        line = self.historical_data[self.show_ticker].iloc[-2].copy()
        line["time"] = line["time"].strftime("%Y-%m-%dT%H:%M:%S")
        self.chart.update(line)

    def update_tick_chart(self, dt, price):
        formatted_dt = dt.strftime("%Y-%m-%dT%H:%M:%S")
        # не обновлять по тикам, если последняя свеча отстаёт более чем на 1 минуту
        last_candle_dt = self.last_dts.get("VTBR")
        if last_candle_dt is not None and (dt - last_candle_dt).total_seconds() > 60:
            print(
                f"[DEBUG] Пропуск update_from_tick: tick_dt={dt}, last_candle_dt={last_candle_dt}"
            )
            return
        tick = pd.Series({"time": formatted_dt, "price": price})
        self.chart.update_from_tick(tick)

    async def async_last_price_handler(self, marketdata):
        print("Last price update")
        figi = marketdata.last_price.figi
        ticker = self.subscriptions["last_price"][0].figi_to_ticker.get(figi)
        price = quotation_to_float(marketdata.last_price.price)
        dt = ensure_timezone(marketdata.last_price.time, GMT_3)
        self.current_price[ticker] = price
        print("\n".join(f"{k}: {v}" for k, v in self.current_price.items()))
        if ticker == self.show_ticker:
            self.update_tick_chart(dt, price)

    async def async_candles_handler(self, marketdata):
        candle = marketdata.candle
        dt = ensure_timezone(candle.time, GMT_3)
        ticker = self.subscriptions["candles"][0].figi_to_ticker[candle.figi]

        prices = {}
        for name in ("open", "high", "low", "close"):
            prices[name] = quotation_to_float(getattr(candle, name))
        prices["time"] = dt
        prices["volume"] = int(candle.volume)
        time_diff = dt - self.last_dts[ticker]

        new_candle = False
        if time_diff.total_seconds() == 60:  # 1 минута
            self.last_dts[ticker] = dt
            self.historical_data[ticker] = pd.concat(
                [self.historical_data[ticker], pd.DataFrame([prices])],
                ignore_index=True,
            )
            new_candle = True
            print(f"Свеча (неполная, предыдущая завершена) {ticker} в {dt}")
        elif time_diff.total_seconds() == 0:  # Та же, обновим
            self.historical_data[ticker].iloc[-1] = prices
            new_candle = False
            # print("Обновление (повторка)")
        elif time_diff.total_seconds() > 60:
            self.last_dts[ticker] = dt
            timeloss = round(time_diff.total_seconds() / 60)
            self.historical_data[ticker] = pd.concat(
                [self.historical_data[ticker], pd.DataFrame([prices])],
                ignore_index=True,
            )
            new_candle = True
            print(f"!!! Потеряно {timeloss} минут")

        if new_candle and ticker == self.show_ticker:
            self.update_candle_chart()

    def _setup_subs(self):
        # Добавляем подписку на последние цены
        self.add_subscription(
            LastPriceSubscription("last_price"),
            self.tickers,
            self.async_last_price_handler,
        )
        self.add_subscription(
            CandlesSubscription(
                "candles", SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE
            ),
            [self.show_ticker],
            self.async_candles_handler,
        )

    async def fresh_tail(self, ticker, df=None):
        if df is None:
            df = pd.DataFrame(
                columns=["time", "open", "high", "low", "close", "volume"]
            )
        if ticker not in self.tickers:
            print("Тикера нет в исходном списке, он будет добавлен.")
            self.tickers.append(ticker)
        if ticker not in self.last_dts:
            self.last_dts = await self.preload_data()

        df2 = self.historical_data[ticker] = standardize_datetime_column(
            self.historical_data[ticker], GMT_3
        )
        if df.empty:
            return df2
        df = standardize_datetime_column(df, GMT_3)
        last_dt = ensure_timezone(df.iloc[-1]["time"].to_pydatetime(), GMT_3)

        if datetime.now() - last_dt > timedelta(90):
            print(
                "В предоставленных данных нет данных за более чем 3 последних месяца! Они не будут использованы."
            )
            last_dt = datetime.now() - timedelta(90)
            return df2
        self.historical_data[ticker] = pd.concat(
            [df, df2[df2["time"] > last_dt]], ignore_index=True
        )
        return self.historical_data[ticker]

    async def run(self):
        print("Последние времена из исторических данных:")
        for ticker, dt in self.last_dts.items():
            print(f"  {ticker}: {dt}")

        client = AsyncClient(self.token)
        services = await client.__aenter__()
        try:
            self.stream_manager = MarketDataStreamManager(services, min_interval=5)

            # Добавляем все подписки в менеджер
            for subscription, tickers in self.subscriptions.values():
                await self.stream_manager.add_subscription(subscription, tickers)

            processor = MarketDataProcessor(
                self.stream_manager, self.signal_handler, self.data_handlers
            )

            await self.stream_manager.subscribe_all()
            await processor.process_market_data()
        finally:
            await client.__aexit__(None, None, None)

    def mod(self, chart: Chart):
        pass


async def main():
    from hotwater.data.secretconf import Secrets

    tickers = ["VTBR", "SBER", "AFLT"]
    show_ticker = "VTBR"
    chart = Chart(toolbox=True)
    control = LevelsAlerts(chart, Secrets.t_invest_token, tickers, show_ticker)
    for t in tickers:
        await control.fresh_tail(t)
    df = control.historical_data[show_ticker].copy()
    df["time"] = df["time"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    chart.set(df)
    chart.show()

    await control.run()


if __name__ == "__main__":
    import asyncio

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nПрограмма остановлена пользователем")
