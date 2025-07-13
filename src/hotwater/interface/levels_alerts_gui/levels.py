import asyncio
import os
import signal
from typing import List, Optional, Callable, Any, Dict, Union
from datetime import datetime, timedelta, timezone
from abc import ABC, abstractmethod

from tinkoff.invest import (
    AsyncClient,
    LastPriceInstrument,
    CandleInstrument,
    MarketDataRequest,
    SubscribeLastPriceRequest,
    SubscribeCandlesRequest,
    SubscriptionAction,
    SubscriptionInterval,
    CandleInterval,
)
from hotwater.data.secretconf import Secrets
from hotwater.data.helpers import get_figi_by_ticker
from hotwater.data.dataloader import load_data, save_configs, load_configs, DataConfig


class BaseSubscription(ABC):
    """Базовый класс для подписок на рыночные данные."""
    
    def __init__(self, name: str):
        self.name = name
        self.instruments = []
        self.figi_list = []
    
    @abstractmethod
    async def get_instruments(self, client: AsyncClient, tickers: List[str]) -> List[Any]:
        """Получает инструменты для подписки."""
        pass
    
    @abstractmethod
    def create_request(self, subscribe: bool = True) -> MarketDataRequest:
        """Создает запрос для подписки/отписки."""
        pass


class LastPriceSubscription(BaseSubscription):
    """Подписка на последние цены."""
    
    def __init__(self, name: str = "last_price"):
        super().__init__(name)
    
    async def get_instruments(self, client: AsyncClient, tickers: List[str]) -> List[LastPriceInstrument]:
        instruments = []
        figi_list = []
        for ticker in tickers:
            figi_data = await get_figi_by_ticker(ticker=ticker, client=client)
            instrument = LastPriceInstrument(instrument_id=figi_data["figi"])
            instruments.append(instrument)
            figi_list.append(figi_data["figi"])
        self.instruments = instruments
        self.figi_list = figi_list
        return instruments
    
    def create_request(self, subscribe: bool = True) -> MarketDataRequest:
        action = (
            SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE
            if subscribe else
            SubscriptionAction.SUBSCRIPTION_ACTION_UNSUBSCRIBE
        )
        
        return MarketDataRequest(
            subscribe_last_price_request=SubscribeLastPriceRequest(
                subscription_action=action,
                instruments=self.instruments,
            ),
        )


class CandlesSubscription(BaseSubscription):
    """Подписка на свечи."""
    
    def __init__(self, name: str = "candles", interval: SubscriptionInterval = SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE):
        super().__init__(name)
        self.interval = interval
    
    async def get_instruments(self, client: AsyncClient, tickers: List[str]) -> List[CandleInstrument]:
        instruments = []
        figi_list = []
        for ticker in tickers:
            figi_data = await get_figi_by_ticker(ticker=ticker, client=client)
            instrument = CandleInstrument(
                figi=figi_data["figi"],
                interval=self.interval
            )
            instruments.append(instrument)
            figi_list.append(figi_data["figi"])
        self.instruments = instruments
        self.figi_list = figi_list
        return instruments
    
    def create_request(self, subscribe: bool = True) -> MarketDataRequest:
        action = (
            SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE
            if subscribe else
            SubscriptionAction.SUBSCRIPTION_ACTION_UNSUBSCRIBE
        )
        
        return MarketDataRequest(
            subscribe_candles_request=SubscribeCandlesRequest(
                subscription_action=action,
                instruments=self.instruments,
            ),
        )


class MarketDataStreamManager:
    """Менеджер для работы с потоками рыночных данных."""
    
    def __init__(self, client: AsyncClient, min_interval: float = 1.0):
        self.client = client
        self.streams: Dict[str, asyncio.StreamReader] = {}
        self.subscriptions: Dict[str, BaseSubscription] = {}
        self.min_interval = min_interval
        self.last_processed = {}
    
    async def add_subscription(self, subscription: BaseSubscription, tickers: List[str]):
        """Добавляет подписку."""
        await subscription.get_instruments(self.client, tickers)
        self.subscriptions[subscription.name] = subscription
    
    async def create_request_iterator(self, subscription_name: str, subscribe: bool = True):
        """Создает итератор запросов для подписки/отписки."""
        if subscription_name not in self.subscriptions:
            raise ValueError(f"Подписка {subscription_name} не найдена")
        
        subscription = self.subscriptions[subscription_name]
        req = subscription.create_request(subscribe)
        yield req
        
        # Бесконечный цикл чтобы итератор не завершался
        while True:
            await asyncio.sleep(10)
    
    async def subscribe(self, subscription_name: str):
        """Подписывается на поток рыночных данных."""
        if subscription_name not in self.subscriptions:
            raise ValueError(f"Подписка {subscription_name} не найдена")
        
        self.streams[subscription_name] = self.client.market_data_stream.market_data_stream(
            self.create_request_iterator(subscription_name, subscribe=True)
        )
    
    async def subscribe_all(self):
        """Подписывается на все добавленные подписки."""
        for subscription_name in self.subscriptions:
            await self.subscribe(subscription_name)
    
    async def unsubscribe(self, subscription_name: str):
        """Отписывается от потока рыночных данных."""
        try:
            unsubscribe_stream = self.client.market_data_stream.market_data_stream(
                self.create_request_iterator(subscription_name, subscribe=False)
            )
            await unsubscribe_stream.__anext__()
            await unsubscribe_stream.aclose()
            print(f"Отписка от {subscription_name} отправлена успешно.")
        except Exception as e:
            print(f"Ошибка при отписке от {subscription_name}: {e}")
    
    async def unsubscribe_all(self):
        """Отписывается от всех подписок."""
        for subscription_name in list(self.subscriptions.keys()):
            await self.unsubscribe(subscription_name)
    
    async def close_stream(self, subscription_name: str):
        """Закрывает конкретный стрим."""
        if subscription_name in self.streams:
            try:
                await self.streams[subscription_name].aclose()
                del self.streams[subscription_name]
            except Exception as e:
                print(f"Ошибка при закрытии стрима {subscription_name}: {e}")
    
    async def close_all_streams(self):
        """Закрывает все стримы."""
        for subscription_name in list(self.streams.keys()):
            await self.close_stream(subscription_name)

    async def can_process(self, subscription_name: str, instrument_id: str) -> bool:
        """Проверяет, можно ли обрабатывать данные для инструмента."""
        key = f"{subscription_name}_{instrument_id}"
        current_time = asyncio.get_event_loop().time()
        last_time = self.last_processed.get(key, 0.0)
        if current_time - last_time >= self.min_interval:
            self.last_processed[key] = current_time
            return True
        return False
    
    def get_stream(self, subscription_name: str) -> Optional[asyncio.StreamReader]:
        """Получает стрим по имени подписки."""
        return self.streams.get(subscription_name)


class SignalHandler:    
    def __init__(self):
        self.shutdown_event = asyncio.Event()
        self._setup_signal_handlers()
    
    def _setup_signal_handlers(self):
        def signal_handler():
            print("\nПолучен сигнал остановки")
            self.shutdown_event.set()
        
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # Если event loop не запущен, создаем новый
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, signal_handler)
    
    def is_shutdown_requested(self) -> bool:
        """Проверяет, запрошена ли остановка."""
        return self.shutdown_event.is_set()


class MarketDataProcessor:
    """Обработчик данных."""
    
    def __init__(self, stream_manager: MarketDataStreamManager, signal_handler: SignalHandler, 
                 data_handlers: Dict[str, Callable]):
        self.stream_manager = stream_manager
        self.signal_handler = signal_handler
        self.data_handlers = data_handlers  # subscription_name -> handler
    
    async def process_market_data(self):
        """Обрабатывает потоки рыночных данных."""
        tasks = []
        
        # Создаем задачи для каждой подписки
        for subscription_name in self.stream_manager.subscriptions:
            if subscription_name in self.data_handlers:
                task = asyncio.create_task(
                    self._process_subscription(subscription_name)
                )
                tasks.append(task)
        
        try:
            # Ждем завершения всех задач или сигнала остановки
            await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            print("Получен сигнал отмены...")
        finally:
            await self._cleanup()
    
    async def _process_subscription(self, subscription_name: str):
        """Обрабатывает поток для конкретной подписки."""
        stream = self.stream_manager.get_stream(subscription_name)
        if not stream:
            print(f"Стрим для {subscription_name} не найден")
            return
        
        handler = self.data_handlers.get(subscription_name)
        if not handler:
            print(f"Обработчик для {subscription_name} не найден")
            return
        
        try:
            async for marketdata in stream:
                # Проверяем, нужно ли остановиться
                if self.signal_handler.is_shutdown_requested():
                    break
                
                # Определяем тип данных и обрабатываем
                await self._handle_market_data(subscription_name, marketdata, handler)
        
        except asyncio.CancelledError:
            print(f"Получен сигнал отмены для {subscription_name}...")
        except Exception as e:
            print(f"Ошибка при обработке {subscription_name}: {e}")
    
    async def _handle_market_data(self, subscription_name: str, marketdata, handler: Callable):
        """Обрабатывает отдельное сообщение с рыночными данными."""
        try:
            # Определяем ID инструмента в зависимости от типа данных
            instrument_id = None
            if hasattr(marketdata, 'last_price') and marketdata.last_price:
                instrument_id = marketdata.last_price.instrument_uid
            elif hasattr(marketdata, 'candle') and marketdata.candle:
                instrument_id = marketdata.candle.instrument_uid
            
            # Проверяем ограничение частоты
            if instrument_id and await self.stream_manager.can_process(subscription_name, instrument_id):
                if asyncio.iscoroutinefunction(handler):
                    await handler(marketdata)
                else:
                    handler(marketdata)
        
        except Exception as e:
            print(f"Ошибка в обработчике данных для {subscription_name}: {e}")
    
    async def _cleanup(self):
        print("Останавливаемся, отправляем отписку...")
        await self.stream_manager.close_all_streams()
        await self.stream_manager.unsubscribe_all()
        print("До свидания!")


class MarketDataApp:
    """Основное приложение для работы с рыночными данными."""
    
    def __init__(self, token: str, dataconf_path=None):
        self.token = token
        self.signal_handler = SignalHandler()
        self.dataconf_path = dataconf_path or "src/hotwater/interface/levels_alerts_gui/dataconfig.json"
        self.historical_datum = {} # ticker: df
        self.subscriptions = {}  # name: BaseSubscription
        self.data_handlers = {}  # name: handler
    
    def add_subscription(self, subscription: BaseSubscription, tickers: List[str], 
                        data_handler: Callable):
        """Добавляет подписку с обработчиком данных."""
        self.subscriptions[subscription.name] = (subscription, tickers)
        self.data_handlers[subscription.name] = data_handler
    
    def create_basic_dataonf(self, ticker):
        return DataConfig(
            ticker=ticker, 
            date_format="%Y-%m-%d %H:%M:%S", 
            data_path=f"/home/timur/Projects/hotwater/hotwater/summer/src/hotwater/data/{ticker}-1min.parquet",
            date_column="time",
            data_format="parquet",
            timeframe="1min",
            start_date=(datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d %H:%M:%S"),
            end_date="now",
            load_moex=False,
            load_tinkoff=True,
            topup=True,
            allow_resample=False
        )

    async def preload_data(self):
        # Собираем все уникальные тикеры из всех подписок
        all_tickers = set()
        for subscription, tickers in self.subscriptions.values():
            all_tickers.update(tickers)
        
        configs = []
        if os.path.exists(self.dataconf_path):
            configs = load_configs(self.dataconf_path)
        
        missing = [ticker for ticker in all_tickers if ticker not in [i.ticker for i in configs]]
        if missing:
            print(f"Следующие тикеры отсутствуют в конфигурации: {missing}")
            for t in missing:
                configs.append(self.create_basic_dataonf(t))
        
        for c in configs:
            self.historical_datum[c.ticker] = await load_data(c)
        save_configs(configs=configs, path=self.dataconf_path)

    
    async def run(self):
        """Запускает приложение."""
        await self.preload_data()
        print(self.historical_datum)
        
        async with AsyncClient(self.token) as client:
            stream_manager = MarketDataStreamManager(client, min_interval=10)
            
            # Добавляем все подписки в менеджер
            for subscription, tickers in self.subscriptions.values():
                await stream_manager.add_subscription(subscription, tickers)
            
            processor = MarketDataProcessor(stream_manager, self.signal_handler, self.data_handlers)
            
            await stream_manager.subscribe_all()
            await processor.process_market_data()


def main():
    TOKEN = Secrets.t_invest_token
    TICKERS = ["SBER", "VTBR"]
    
    async def async_last_price_handler(marketdata):
        print("Last price update")
        print(f"Асинхронно обрабатываем: {marketdata.last_price.price}")
    
    async def async_candles_handler(marketdata):
        print("Candle update")
        print(f"Свеча: {marketdata.candle}")
    
    app = MarketDataApp(TOKEN)
    
    # Добавляем подписку на последние цены
    last_price_sub = LastPriceSubscription("last_price")
    app.add_subscription(last_price_sub, TICKERS, async_last_price_handler)
    
    # Добавляем подписку на свечи
    candles_sub = CandlesSubscription("candles", SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE)
    app.add_subscription(candles_sub, TICKERS, async_candles_handler)
    
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        print("\nПрограмма остановлена пользователем")


if __name__ == "__main__":
    print("Запуск приложения для получения рыночных данных...")
    main()

