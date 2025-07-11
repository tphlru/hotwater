import numpy as np
import pandas as pd
from typing import Tuple

from hotwater.factors.smoothing import SmoothingFunctions


class FDICalculator:
    """
    Калькулятор Fractal Dimension Index (FDI) с поддержкой различных типов сглаживания
    и расчета угла наклона. Оптимизирован для работы с NumPy
    """

    def __init__(
        self,
        period: int = 30,
        smooth_enable: bool = True,
        smooth_type: str = "HMA",
        smooth_len: int = 14,
        alma_offset: float = 0.85,
        alma_sigma: float = 6.0,
        angle_len: int = 5,
    ):
        """
        Инициализация калькулятора FDI

        Args:
            period: Период для расчета FDI
            smooth_enable: Включить сглаживание
            smooth_type: Тип сглаживания ("SMA", "EMA", "HMA", "ALMA")
            smooth_len: Период сглаживания
            alma_offset: Смещение для ALMA
            alma_sigma: Сигма для ALMA
            angle_len: Период для расчета угла наклона
        """
        self.period = period
        self.smooth_enable = smooth_enable
        self.smooth_type = smooth_type
        self.smooth_len = smooth_len
        self.alma_offset = alma_offset
        self.alma_sigma = alma_sigma
        self.angle_len = angle_len

        # Буферы для реального времени
        self._price_buffer = np.full(period, np.nan)
        self._fdi_buffer = np.full(smooth_len, np.nan)
        self._smooth_buffer = np.full(angle_len, np.nan)
        self._buffer_pos = 0

    def _calculate_fdi_core(self, prices: np.ndarray) -> float:
        """
        Основной расчет FDI для одного окна данных

        Args:
            prices: Массив цен длиной period

        Returns:
            Значение FDI
        """
        if len(prices) < self.period:
            return np.nan

        # Убираем NaN значения
        valid_prices = prices[~np.isnan(prices)]
        if len(valid_prices) < self.period:
            return np.nan

        # Находим максимум и минимум
        hh = np.max(valid_prices)
        ll = np.min(valid_prices)

        # Защита от деления на ноль
        if np.isclose(hh, ll):
            return 1.5  # Возвращаем нейтральное значение

        # Расчет нормализованных значений
        normalized = (valid_prices - ll) / (hh - ll)

        # Расчет суммы длин
        length_sum = 0.0
        for i in range(len(normalized) - 1):
            d1 = normalized[i]
            d2 = normalized[i + 1]
            length_sum += np.sqrt((d1 - d2) ** 2 + (1 / self.period) ** 2)

        # Защита от логарифма от нуля или отрицательного числа
        if length_sum <= 0:
            return 1.5

        # Расчет FDI
        fdi = 1 + (np.log(length_sum) + np.log(2)) / np.log(2 * self.period)

        # Проверка на корректность результата
        if np.isnan(fdi) or np.isinf(fdi):
            return 1.5

        return fdi

    def _apply_smoothing(self, values: np.ndarray) -> np.ndarray:
        """Применение сглаживания"""
        if not self.smooth_enable:
            return values

        if self.smooth_type == "ALMA":
            return SmoothingFunctions.alma(
                values, self.smooth_len, self.alma_offset, self.alma_sigma
            )
        else:
            return SmoothingFunctions.smooth(values, self.smooth_type, self.smooth_len)

    def _calculate_angle(self, values: np.ndarray) -> np.ndarray:
        """Расчет угла наклона"""
        if len(values) < self.angle_len:
            return np.full(len(values), np.nan)

        result = np.full(len(values), np.nan)

        for i in range(self.angle_len, len(values)):
            if not np.isnan(values[i]) and not np.isnan(values[i - self.angle_len]):
                # Угол наклона через arctangent
                angle_rad = np.arctan(
                    (values[i] - values[i - self.angle_len]) / self.angle_len
                )
                result[i] = np.degrees(angle_rad)

        return result

    def calculate_historical(
        self, df: pd.DataFrame, price_col: str = "close"
    ) -> pd.DataFrame:
        """
        Расчет FDI для исторических данных

        Args:
            df: DataFrame с OHLCV данными
            price_col: Название колонки с ценами

        Returns:
            DataFrame с колонками: fdi, fdi_smooth, fdi_angle
        """
        if price_col not in df.columns:
            raise ValueError(f"Колонка '{price_col}' не найдена в DataFrame")

        prices = df[price_col].values
        n_points = len(prices)

        # Инициализация результирующих массивов
        fdi_values = np.full(n_points, np.nan)

        # Расчет FDI для каждой точки
        for i in range(self.period - 1, n_points):
            window = prices[i - self.period + 1 : i + 1]
            fdi_values[i] = self._calculate_fdi_core(window)

        # Применение сглаживания
        smooth_values = self._apply_smoothing(fdi_values)

        # Расчет угла наклона
        angle_values = self._calculate_angle(smooth_values)

        # Создание результирующего DataFrame
        result = pd.DataFrame(index=df.index)
        result["fdi"] = fdi_values
        result["fdi_smooth"] = smooth_values
        result["fdi_angle"] = angle_values

        return result

    def update_realtime(self, price: float) -> Tuple[float, float, float]:
        """
        Обновление для реального времени

        Args:
            price: Новая цена

        Returns:
            Tuple[fdi, fdi_smooth, angle]
        """
        # Обновление буфера цен
        self._price_buffer[self._buffer_pos % self.period] = price
        self._buffer_pos += 1

        # Расчет FDI если буфер заполнен
        if self._buffer_pos >= self.period:
            # Создаем правильно упорядоченный массив
            if self._buffer_pos == self.period:
                ordered_prices = self._price_buffer.copy()
            else:
                start_idx = self._buffer_pos % self.period
                ordered_prices = np.concatenate(
                    [self._price_buffer[start_idx:], self._price_buffer[:start_idx]]
                )

            current_fdi = self._calculate_fdi_core(ordered_prices)
        else:
            current_fdi = np.nan

        # Обновление буфера FDI для сглаживания
        if not np.isnan(current_fdi):
            self._fdi_buffer[1:] = self._fdi_buffer[:-1]
            self._fdi_buffer[0] = current_fdi

            # Расчет сглаженного значения
            if self.smooth_enable:
                valid_fdi = self._fdi_buffer[~np.isnan(self._fdi_buffer)]
                if len(valid_fdi) > 0:
                    if self.smooth_type == "ALMA":
                        current_smooth = SmoothingFunctions.alma(
                            valid_fdi,
                            min(len(valid_fdi), self.smooth_len),
                            self.alma_offset,
                            self.alma_sigma,
                        )[-1]
                    else:
                        current_smooth = SmoothingFunctions.smooth(
                            valid_fdi,
                            self.smooth_type,
                            min(len(valid_fdi), self.smooth_len),
                        )[-1]
                else:
                    current_smooth = np.nan
            else:
                current_smooth = current_fdi
        else:
            current_smooth = np.nan

        # Обновление буфера сглаженных значений для угла
        if not np.isnan(current_smooth):
            self._smooth_buffer[1:] = self._smooth_buffer[:-1]
            self._smooth_buffer[0] = current_smooth

            # Расчет угла
            if (
                len(self._smooth_buffer[~np.isnan(self._smooth_buffer)])
                >= self.angle_len
            ):
                current_angle = self._calculate_angle(self._smooth_buffer[::-1])[-1]
            else:
                current_angle = np.nan
        else:
            current_angle = np.nan

        return current_fdi, current_smooth, current_angle

    def reset_buffers(self):
        """Сброс буферов для реального времени"""
        self._price_buffer.fill(np.nan)
        self._fdi_buffer.fill(np.nan)
        self._smooth_buffer.fill(np.nan)
        self._buffer_pos = 0


if __name__ == "__main__":
    dates = pd.date_range("2023-01-01", periods=200, freq="D")
    prices = 100 + np.cumsum(np.random.randn(200) * 0.1)

    df = pd.DataFrame({"date": dates, "close": prices})
    df.set_index("date", inplace=True)

    fdi_calc = FDICalculator(
        period=30, smooth_enable=True, smooth_type="HMA", smooth_len=14, angle_len=5
    )

    # Расчет для исторических данных
    historical_result = fdi_calc.calculate_historical(df)
    print("Исторические данные (последние 10 значений):")
    print(historical_result.tail(10))

    # Пример использования в реальном времени
    print("\nПример реального времени:")
    fdi_calc.reset_buffers()

    # Заполнение буферов историческими данными
    for price in prices[-50:]:  # Используем последние 50 значений для инициализации
        fdi, smooth, angle = fdi_calc.update_realtime(price)
        print(
            f"Цена: {price:.2f}, FDI: {fdi:.4f}, Smooth: {smooth:.4f}, Angle: {angle:.2f}"
        )
