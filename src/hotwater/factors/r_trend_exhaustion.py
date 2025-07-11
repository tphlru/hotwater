import numpy as np
import pandas as pd
from typing import Tuple
from collections import deque
from hotwater.factors.smoothing import SmoothingFunctions


class USD_RTrendExhaustionCalculator:
    """
    Калькулятор upslidedown %R Trend Exhaustion индикатора

    Основан на комбинации быстрого и медленного Williams %R с определением
    зон перекупленности/перепроданности и разворотных сигналов.
    """

    def __init__(
        self,
        fast_length: int = 21,
        slow_length: int = 112,
        fast_smoothing: int = 7,
        slow_smoothing: int = 3,
        threshold: int = 20,
        smoothing_type: str = "ema",
        use_average: bool = False,
        average_ma: int = 3,
        alma_offset: float = 0.85,
        alma_sigma: float = 6.0,
    ):
        """
        Инициализация калькулятора

        Args:
            fast_length: Период быстрого %R
            slow_length: Период медленного %R
            fast_smoothing: Период сглаживания быстрого %R
            slow_smoothing: Период сглаживания медленного %R
            threshold: Порог для определения зон перекупленности/перепроданности
            smoothing_type: Тип сглаживания ('sma', 'ema', 'hma', 'alma')
            use_average: Использовать среднее значение вместо двух отдельных %R
            average_ma: Период сглаживания среднего значения
            alma_offset: Смещение для ALMA
            alma_sigma: Сигма для ALMA
        """
        self.fast_length = fast_length
        self.slow_length = slow_length
        self.fast_smoothing = fast_smoothing
        self.slow_smoothing = slow_smoothing
        self.threshold = threshold
        self.smoothing_type = smoothing_type.lower()
        self.use_average = use_average
        self.average_ma = average_ma
        self.alma_offset = alma_offset
        self.alma_sigma = alma_sigma

        # Буферы для реального времени
        self.reset_buffers()

    def reset_buffers(self):
        """Сброс буферов для реального времени"""
        max_length = max(self.fast_length, self.slow_length)
        max_smoothing = max(self.fast_smoothing, self.slow_smoothing, self.average_ma)
        buffer_size = max_length + max_smoothing + 10  # Запас для сглаживания

        self.price_buffer = deque(maxlen=buffer_size)
        self.high_buffer = deque(maxlen=buffer_size)
        self.low_buffer = deque(maxlen=buffer_size)

        # Состояние для отслеживания трендов
        self.was_overbought = False
        self.was_oversold = False
        self.prev_overbought = False
        self.prev_oversold = False

    def calculate_historical(
        self,
        df: pd.DataFrame,
        price_col: str = "close",
        high_col: str = "high",
        low_col: str = "low",
    ) -> pd.DataFrame:
        """
        Расчёт по историческим данным

        Args:
            df: DataFrame с OHLCV данными
            price_col: Название колонки с ценой закрытия
            high_col: Название колонки с максимальной ценой
            low_col: Название колонки с минимальной ценой

        Returns:
            DataFrame с результатами расчёта
        """
        if len(df) == 0:
            return pd.DataFrame()

        # Извлекаем данные
        prices = df[price_col].to_numpy()
        highs = df[high_col].to_numpy()
        lows = df[low_col].to_numpy()

        # Рассчитываем %R
        fast_r, slow_r, avg_r = self._calculate_williams_r(prices, highs, lows)

        # Определяем сигналы
        signals = self._calculate_signals(fast_r, slow_r, avg_r)

        # Формируем результат
        result = pd.DataFrame(index=df.index)

        if self.use_average:
            result["williams_r"] = avg_r
            result["fast_r"] = np.nan
            result["slow_r"] = np.nan
        else:
            result["fast_r"] = fast_r
            result["slow_r"] = slow_r
            result["williams_r"] = np.nan

        # Добавляем сигналы
        result["bull_start"] = signals["bull_start"]
        result["bear_start"] = signals["bear_start"]
        result["bull_break"] = signals["bull_break"]
        result["bear_break"] = signals["bear_break"]

        # Дополнительные состояния
        result["overbought"] = signals["overbought"]
        result["oversold"] = signals["oversold"]

        return result

    def update_realtime(
        self, price: float, high: float, low: float
    ) -> Tuple[float, float, dict]:
        """
        Обновление в реальном времени

        Args:
            price: Цена закрытия
            high: Максимальная цена
            low: Минимальная цена

        Returns:
            Кортеж (fast_r, slow_r, signals) или (avg_r, nan, signals)
        """
        # Добавляем новые данные в буферы
        self.price_buffer.append(price)
        self.high_buffer.append(high)
        self.low_buffer.append(low)

        # Проверяем, достаточно ли данных
        if len(self.price_buffer) < max(self.fast_length, self.slow_length):
            return np.nan, np.nan, {}

        # Конвертируем в массивы
        prices = np.array(self.price_buffer)
        highs = np.array(self.high_buffer)
        lows = np.array(self.low_buffer)

        # Рассчитываем %R для последних значений
        fast_r, slow_r, avg_r = self._calculate_williams_r(prices, highs, lows)

        # Получаем последние значения
        current_fast = fast_r[-1] if len(fast_r) > 0 else np.nan
        current_slow = slow_r[-1] if len(slow_r) > 0 else np.nan
        current_avg = avg_r[-1] if len(avg_r) > 0 else np.nan

        # Рассчитываем сигналы
        signals = self._calculate_signals_realtime(
            current_fast, current_slow, current_avg
        )

        if self.use_average:
            return current_avg, np.nan, signals
        else:
            return current_fast, current_slow, signals

    def _calculate_williams_r(
        self, prices: np.ndarray, highs: np.ndarray, lows: np.ndarray
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Расчёт Williams %R"""
        n = len(prices)

        # Инициализация массивов
        fast_r = np.full(n, np.nan)
        slow_r = np.full(n, np.nan)

        # Расчёт быстрого %R
        for i in range(self.fast_length - 1, n):
            window_high = np.max(highs[i - self.fast_length + 1 : i + 1])
            window_low = np.min(lows[i - self.fast_length + 1 : i + 1])

            if window_high != window_low:  # Избегаем деления на ноль
                fast_r[i] = 100 * (prices[i] - window_high) / (window_high - window_low)
            else:
                fast_r[i] = -50  # Нейтральное значение

        # Расчёт медленного %R
        for i in range(self.slow_length - 1, n):
            window_high = np.max(highs[i - self.slow_length + 1 : i + 1])
            window_low = np.min(lows[i - self.slow_length + 1 : i + 1])

            if window_high != window_low:  # Избегаем деления на ноль
                slow_r[i] = 100 * (prices[i] - window_high) / (window_high - window_low)
            else:
                slow_r[i] = -50  # Нейтральное значение

        # Применяем сглаживание
        if self.fast_smoothing > 1:
            fast_r = self._apply_smoothing(fast_r, self.fast_smoothing)

        if self.slow_smoothing > 1:
            slow_r = self._apply_smoothing(slow_r, self.slow_smoothing)

        # Расчёт среднего значения
        avg_r = np.full(n, np.nan)
        valid_mask = ~(np.isnan(fast_r) | np.isnan(slow_r))
        avg_r[valid_mask] = (fast_r[valid_mask] + slow_r[valid_mask]) / 2

        if self.average_ma > 1:
            avg_r = self._apply_smoothing(avg_r, self.average_ma)

        return fast_r, slow_r, avg_r

    def _apply_smoothing(self, values: np.ndarray, length: int) -> np.ndarray:
        return SmoothingFunctions.smooth(values, self.smoothing_type or "ema", length)

    def _calculate_signals(
        self, fast_r: np.ndarray, slow_r: np.ndarray, avg_r: np.ndarray
    ) -> dict:
        """Расчёт сигналов для исторических данных"""
        n = len(fast_r)

        # Инициализация массивов сигналов
        signals = {
            "bull_start": np.full(n, False),
            "bear_start": np.full(n, False),
            "bull_break": np.full(n, False),
            "bear_break": np.full(n, False),
            "overbought": np.full(n, False),
            "oversold": np.full(n, False),
        }

        # Выбираем данные для анализа
        if self.use_average:
            primary_data = avg_r
        else:
            primary_data = fast_r
            secondary_data = slow_r

        # Состояния для отслеживания
        was_overbought = False
        was_oversold = False

        for i in range(1, n):
            if np.isnan(primary_data[i]):
                continue

            if self.use_average:
                # Логика для среднего значения
                overbought = primary_data[i] >= -self.threshold
                oversold = primary_data[i] <= -100 + self.threshold
            else:
                # Логика для двух %R
                if np.isnan(secondary_data[i]):
                    continue

                overbought = (
                    primary_data[i] >= -self.threshold
                    and secondary_data[i] >= -self.threshold
                )
                oversold = (
                    primary_data[i] <= -100 + self.threshold
                    and secondary_data[i] <= -100 + self.threshold
                )

            # Сохраняем текущие состояния
            signals["overbought"][i] = overbought
            signals["oversold"][i] = oversold

            # Определяем сигналы
            if overbought and not was_overbought:
                signals["bear_start"][i] = True  # Начало медвежьего тренда
            elif not overbought and was_overbought:
                signals["bull_break"][i] = True  # Прорыв бычьего тренда

            if oversold and not was_oversold:
                signals["bull_start"][i] = True  # Начало бычьего тренда
            elif not oversold and was_oversold:
                signals["bear_break"][i] = True  # Прорыв медвежьего тренда

            # Обновляем состояния
            was_overbought = overbought
            was_oversold = oversold

        return signals

    def _calculate_signals_realtime(
        self, fast_r: float, slow_r: float, avg_r: float
    ) -> dict:
        """Расчёт сигналов в реальном времени"""
        signals = {
            "bull_start": False,
            "bear_start": False,
            "bull_break": False,
            "bear_break": False,
            "overbought": False,
            "oversold": False,
        }

        # Проверяем наличие данных
        if self.use_average:
            if np.isnan(avg_r):
                return signals
            current_overbought = avg_r >= -self.threshold
            current_oversold = avg_r <= -100 + self.threshold
        else:
            if np.isnan(fast_r) or np.isnan(slow_r):
                return signals
            current_overbought = fast_r >= -self.threshold and slow_r >= -self.threshold
            current_oversold = (
                fast_r <= -100 + self.threshold and slow_r <= -100 + self.threshold
            )

        # Сохраняем текущие состояния
        signals["overbought"] = current_overbought
        signals["oversold"] = current_oversold

        # Определяем сигналы на основе изменений состояний
        if current_overbought and not self.was_overbought:
            signals["bear_start"] = True
        elif not current_overbought and self.was_overbought:
            signals["bear_break"] = True

        if current_oversold and not self.was_oversold:
            signals["bull_start"] = True
        elif not current_oversold and self.was_oversold:
            signals["bull_break"] = True

        # Обновляем состояния
        self.was_overbought = current_overbought
        self.was_oversold = current_oversold

        return signals


# Пример использования
if __name__ == "__main__":
    # Создаём тестовые данные
    np.random.seed(42)
    dates = pd.date_range("2023-01-01", periods=500, freq="D")

    # Генерируем цены с трендом
    base_price = 100
    prices = []
    highs = []
    lows = []

    for i in range(len(dates)):
        # Добавляем тренд и шум
        trend = 0.02 * np.sin(i / 50) + 0.001 * i
        noise = np.random.normal(0, 0.01)
        close = base_price * (1 + trend + noise)

        # Генерируем high и low
        high = close * (1 + abs(np.random.normal(0, 0.005)))
        low = close * (1 - abs(np.random.normal(0, 0.005)))

        prices.append(close)
        highs.append(high)
        lows.append(low)

    # Создаём DataFrame
    df = pd.DataFrame({"date": dates, "high": highs, "low": lows, "close": prices})

    # Инициализируем калькулятор
    calculator = USD_RTrendExhaustionCalculator(
        fast_length=21,
        slow_length=112,
        fast_smoothing=7,
        slow_smoothing=3,
        threshold=20,
        smoothing_type="ema",
        use_average=False,
    )

    # Расчёт по историческим данным
    result = calculator.calculate_historical(df)

    print("Результаты расчёта:")
    print(result.tail(10))

    # Пример использования в реальном времени
    print("\nПример работы в реальном времени:")
    calculator.reset_buffers()

    # Заполняем буферы начальными данными
    for i in range(150):  # Минимум для slow_length
        fast_r, slow_r, signals = calculator.update_realtime(
            df.iloc[i]["close"], df.iloc[i]["high"], df.iloc[i]["low"]
        )

    # Проверяем последние несколько обновлений
    for i in range(150, min(155, len(df))):
        fast_r, slow_r, signals = calculator.update_realtime(
            df.iloc[i]["close"], df.iloc[i]["high"], df.iloc[i]["low"]
        )

        print(f"День {i}: Fast %R={fast_r:.2f}, Slow %R={slow_r:.2f}")
        if any(signals.values()):
            print(f"  Сигналы: {signals}")
