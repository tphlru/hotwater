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
    def __init__(self, chart: Chart, t_token: str, tickers: list):
        self.tickers: list = tickers
        super().__init__(t_token, chart)
        self._setup_subs()

    async def async_last_price_handler(self, marketdata):
        print("Last price update")
        figi = marketdata.last_price.figi
        ticker = self.subscriptions["last_price"][0].figi_to_ticker.get(figi)
        if ticker == "VTBR":
            dt = ensure_timezone(marketdata.last_price.time, GMT_3).strftime(
                "%Y-%m-%dT%H:%M:%S"
            )

            price = quotation_to_float(marketdata.last_price.price)
            # не обновлять по тикам, если последняя свеча отстаёт более чем на 1 минуту
            last_candle_dt = self.last_dts.get("VTBR")
            tick_dt = ensure_timezone(marketdata.last_price.time, GMT_3)
            if (
                last_candle_dt is not None
                and (tick_dt - last_candle_dt).total_seconds() > 60
            ):
                print(
                    f"[DEBUG] Пропуск update_from_tick: tick_dt={tick_dt}, last_candle_dt={last_candle_dt}"
                )
                return
            tick = pd.Series({"time": dt, "price": price})
            print("[DEBUG] tick for update_from_tick:", tick)
            self.chart.update_from_tick(tick)

    async def async_candles_handler(self, marketdata):
        candle = marketdata.candle
        dt = ensure_timezone(candle.time, GMT_3)
        ticker = self.subscriptions["candles"][0].figi_to_ticker[candle.figi]

        prices = {}
        for name in ("open", "high", "low", "close"):
            prices[name] = quotation_to_float(getattr(candle, name))
        prices["datetime"] = dt
        prices["volume"] = int(candle.volume)
        time_diff = dt - self.last_dts[ticker]

        if time_diff.total_seconds() == 60:  # 1 минута
            self.last_dts[ticker] = dt
            self.historical_data[ticker] = pd.concat(
                [self.historical_data[ticker], pd.DataFrame([prices])],
                ignore_index=True,
            )
            if ticker == "VTBR":
                line = self.historical_data[ticker].iloc[-2].copy()
                if "datetime" in line:
                    dt_val = line["datetime"]
                    line["time"] = pd.to_datetime(dt_val).strftime("%Y-%m-%dT%H:%M:%S")
                    line = line.drop("datetime")
                # Переупорядочить поля для update
                line = line[["time", "open", "high", "low", "close", "volume"]]

                self.chart.update(line)
            print(f"Свеча (неполная, предыдущая завершена) {ticker} в {dt}")
        elif time_diff.total_seconds() == 0:  # Та же, обновим
            self.historical_data[ticker].iloc[-1] = prices
            # print("Обновление (повторка)")
        elif time_diff.total_seconds() > 60:
            self.last_dts[ticker] = dt
            timeloss = round(time_diff.total_seconds() / 60)
            self.historical_data[ticker] = pd.concat(
                [self.historical_data[ticker], pd.DataFrame([prices])],
                ignore_index=True,
            )
            if ticker == "VTBR":
                line = self.historical_data[ticker].iloc[-2]
                print(line)
                self.chart.update(line)
            print(f"!!! Потеряно {timeloss} минут")

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
            self.tickers,
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

        df2 = standardize_datetime_column(self.historical_data[ticker], GMT_3)
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
        df = self.historical_data["VTBR"].copy()
        df["time"] = pd.to_datetime(df["datetime"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
        df = df[["time", "open", "high", "low", "close", "volume"]]
        self.chart.set(df)
        self.chart.show()

        print("Последние времена из исторических данных:")
        for ticker, dt in self.last_dts.items():
            print(f"  {ticker}: {dt}")

        client = AsyncClient(self.token)
        services = await client.__aenter__()
        try:
            stream_manager = MarketDataStreamManager(services, min_interval=5)

            # Добавляем все подписки в менеджер
            for subscription, tickers in self.subscriptions.values():
                await stream_manager.add_subscription(subscription, tickers)

            processor = MarketDataProcessor(
                stream_manager, self.signal_handler, self.data_handlers
            )

            await stream_manager.subscribe_all()
            await processor.process_market_data()
        finally:
            await client.__aexit__(None, None, None)

    def levels_mod(self, chart: Chart):
        pass


async def main():
    from hotwater.data.secretconf import Secrets

    tickers = ["VTBR", "SBER", "AFLT"]
    chart = Chart(toolbox=True)
    control = LevelsAlerts(chart, Secrets.t_invest_token, tickers)
    for t in tickers:
        await control.fresh_tail(t)
    await control.run()


if __name__ == "__main__":
    import asyncio

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nПрограмма остановлена пользователем")
