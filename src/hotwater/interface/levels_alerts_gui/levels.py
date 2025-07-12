import asyncio
import os
import signal
from typing import List, Optional, Callable, Any

from tinkoff.invest import (
    AsyncClient,
    LastPriceInstrument,
    MarketDataRequest,
    SubscribeLastPriceRequest,
    SubscriptionAction,
)
from hotwater.data.secretconf import Secrets
from hotwater.interface.levels_alerts_gui.helpers import get_figi_by_ticker
from async_throttle import Throttle


class MarketDataStreamManager:
    """Менеджер для работы с потоком рыночных данных."""
    
    def __init__(self, client: AsyncClient, tickers: List[str]):
        self.client = client
        self.tickers = tickers
        self.stream: Optional[asyncio.StreamReader] = None
        self.throttler = Throttle(1 * len(tickers), len(tickers), 5)
    
    async def get_instruments(self) -> List[LastPriceInstrument]:
        instruments = []
        for ticker in self.tickers:
            figi_data = await get_figi_by_ticker(ticker=ticker, client=self.client)
            instrument = LastPriceInstrument(instrument_id=figi_data["figi"])
            instruments.append(instrument)
        return instruments
    
    async def create_request_iterator(self, subscribe: bool = True):
        """Создает итератор запросов для подписки/отписки."""
        instruments = await self.get_instruments()
        
        action = (
            SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE
            if subscribe else
            SubscriptionAction.SUBSCRIPTION_ACTION_UNSUBSCRIBE
        )
        
        req = MarketDataRequest(
            subscribe_last_price_request=SubscribeLastPriceRequest(
                subscription_action=action,
                instruments=instruments,
            ),
        )
        yield req
        
        # Бесконечный цикл чтобы итератор не завершался
        while True:
            await asyncio.sleep(10)
    
    async def subscribe(self):
        """Подписывается на поток рыночных данных."""
        self.stream = self.client.market_data_stream.market_data_stream(
            self.create_request_iterator(subscribe=True)
        )
    
    async def unsubscribe(self):
        """Отписывается от потока рыночных данных."""
        try:
            unsubscribe_stream = self.client.market_data_stream.market_data_stream(
                self.create_request_iterator(subscribe=False)
            )
            await unsubscribe_stream.__anext__()
            await unsubscribe_stream.aclose()
            print("Отписка отправлена успешно.")
        except Exception as e:
            print(f"Ошибка при отписке: {e}")
    
    async def close_stream(self):
        """Закрывает текущий стрим."""
        if self.stream:
            try:
                await self.stream.aclose()
            except Exception as e:
                print(f"Ошибка при закрытии стрима: {e}")


class SignalHandler:    
    def __init__(self):
        self.shutdown_event = asyncio.Event()
        self._setup_signal_handlers()
    
    def _setup_signal_handlers(self):
        def signal_handler():
            print("\nПолучен сигнал остановки, начинаем мирное завершение...")
            self.shutdown_event.set()
        
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, signal_handler)
    
    def is_shutdown_requested(self) -> bool:
        """Проверяет, запрошена ли остановка."""
        return self.shutdown_event.is_set()


class MarketDataProcessor:
    """Обработчик данных."""
    
    def __init__(self, stream_manager: MarketDataStreamManager, signal_handler: SignalHandler, 
                 data_handler: Callable):
        self.stream_manager = stream_manager
        self.signal_handler = signal_handler
        self.data_handler = data_handler
    
    async def process_market_data(self):
        """Обрабатывает поток рыночных данных."""
        try:
            async for marketdata in self.stream_manager.stream:
                # Проверяем, нужно ли остановиться
                if self.signal_handler.is_shutdown_requested():
                    break
                
                # Обрабатываем данные с ограничением частоты
                async with self.stream_manager.throttler:
                    await self._handle_market_data(marketdata)
        
        except asyncio.CancelledError:
            print("Получен сигнал отмены...")
        finally:
            await self._cleanup()
    
    async def _handle_market_data(self, marketdata):
        """Обрабатывает отдельное сообщение с рыночными данными."""
        try:
            if asyncio.iscoroutinefunction(self.data_handler):
                await self.data_handler(marketdata)
            else:
                self.data_handler(marketdata)
        except Exception as e:
            print(f"Ошибка в обработчике данных: {e}")
    
    async def _cleanup(self):
        print("Останавливаемся, отправляем отписку...")
        await self.stream_manager.close_stream()
        await self.stream_manager.unsubscribe()
        print("До свидания!")


class MarketDataApp:
    """Основное приложение для работы с рыночными данными."""
    
    def __init__(self, token: str, tickers: List[str], 
                 data_handler: Callable):
        self.token = token
        self.tickers = tickers
        self.data_handler = data_handler
        self.signal_handler = SignalHandler()
    
    async def run(self):
        """Запускает приложение."""
        async with AsyncClient(self.token) as client:
            stream_manager = MarketDataStreamManager(client, self.tickers)
            processor = MarketDataProcessor(stream_manager, self.signal_handler, self.data_handler)
            
            await stream_manager.subscribe()
            await processor.process_market_data()


def main():
    TOKEN = Secrets.t_invest_token
    TICKERS = ["SBER", "VTBR"]
    
    
    async def async_custom_data_handler(marketdata):
        print(f"Асинхронно обрабатываем: {marketdata}")
    
    app = MarketDataApp(TOKEN, TICKERS, data_handler=async_custom_data_handler)
    
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        print("\nПрограмма остановлена пользователем")


if __name__ == "__main__":
    print("Запуск приложения для получения рыночных данных...")
    main()

