import numpy as np


class SmoothingFunctions:
    """
    Класс с функциями сглаживания для использования в различных индикаторах.
    Оптимизирован для работы с NumPy и подготовлен для Numba.
    """

    @staticmethod
    def sma(values: np.ndarray, length: int) -> np.ndarray:
        """
        Простое скользящее среднее

        Args:
            values: Массив значений
            length: Период сглаживания

        Returns:
            Массив сглаженных значений
        """
        if len(values) < length:
            return np.full(len(values), np.nan)

        result = np.full(len(values), np.nan)
        for i in range(length - 1, len(values)):
            window = values[i - length + 1 : i + 1]
            valid_values = window[~np.isnan(window)]
            if len(valid_values) > 0:
                result[i] = np.mean(valid_values)
        return result

    @staticmethod
    def ema(values: np.ndarray, length: int) -> np.ndarray:
        """
        Экспоненциальное скользящее среднее

        Args:
            values: Массив значений
            length: Период сглаживания

        Returns:
            Массив сглаженных значений
        """
        if len(values) < 1:
            return np.full(len(values), np.nan)

        alpha = 2.0 / (length + 1)
        result = np.full(len(values), np.nan)

        # Находим первое валидное значение
        first_valid = 0
        while first_valid < len(values) and np.isnan(values[first_valid]):
            first_valid += 1

        if first_valid >= len(values):
            return result

        result[first_valid] = values[first_valid]

        for i in range(first_valid + 1, len(values)):
            if not np.isnan(values[i]):
                result[i] = alpha * values[i] + (1 - alpha) * result[i - 1]
            else:
                result[i] = result[i - 1]

        return result

    @staticmethod
    def wma(values: np.ndarray, length: int) -> np.ndarray:
        """
        Взвешенное скользящее среднее

        Args:
            values: Массив значений
            length: Период сглаживания

        Returns:
            Массив сглаженных значений
        """
        if len(values) < length:
            return np.full(len(values), np.nan)

        weights = np.arange(1, length + 1, dtype=np.float64)
        weight_sum = np.sum(weights)

        result = np.full(len(values), np.nan)
        for i in range(length - 1, len(values)):
            window = values[i - length + 1 : i + 1]
            valid_mask = ~np.isnan(window)
            if np.sum(valid_mask) > 0:
                valid_values = window[valid_mask]
                valid_weights = weights[valid_mask]
                if len(valid_values) == length:
                    result[i] = np.sum(valid_values * valid_weights) / weight_sum
                else:
                    # Нормализуем веса для неполных окон
                    result[i] = np.sum(valid_values * valid_weights) / np.sum(
                        valid_weights
                    )

        return result

    @staticmethod
    def hma(values: np.ndarray, length: int) -> np.ndarray:
        """
        Hull Moving Average

        Args:
            values: Массив значений
            length: Период сглаживания

        Returns:
            Массив сглаженных значений
        """
        if len(values) < length:
            return np.full(len(values), np.nan)

        half_len = max(1, length // 2)
        sqrt_len = max(1, int(np.sqrt(length)))

        wma_half = SmoothingFunctions.wma(values, half_len)
        wma_full = SmoothingFunctions.wma(values, length)

        # 2 * WMA(n/2) - WMA(n)
        diff = 2 * wma_half - wma_full

        # WMA(sqrt(n)) от разности
        return SmoothingFunctions.wma(diff, sqrt_len)

    @staticmethod
    def rma(values: np.ndarray, length: int) -> np.ndarray:
        """Running Moving Average (RMA) - аналог EMA с alpha = 1/length"""
        if len(values) == 0:
            return np.array([])

        alpha = 1.0 / length
        result = np.full(len(values), np.nan)

        first_valid = np.where(~np.isnan(values))[0]
        if len(first_valid) == 0:
            return result

        result[first_valid[0]] = values[first_valid[0]]

        for i in range(first_valid[0] + 1, len(values)):
            if not np.isnan(values[i]):
                result[i] = alpha * values[i] + (1 - alpha) * result[i - 1]

        return result

    @staticmethod
    def alma(
        values: np.ndarray, length: int, offset: float = 0.85, sigma: float = 6.0
    ) -> np.ndarray:
        """
        Arnaud Legoux Moving Average

        Args:
            values: Массив значений
            length: Период сглаживания
            offset: Смещение (0-1)
            sigma: Параметр сглаживания

        Returns:
            Массив сглаженных значений
        """
        if len(values) < length:
            return np.full(len(values), np.nan)

        m = offset * (length - 1)
        s = length / sigma

        # Предварительный расчет весов
        weights = np.zeros(length)
        for i in range(length):
            weights[i] = np.exp(-((i - m) ** 2) / (2 * s**2))

        weights_sum = np.sum(weights)
        if weights_sum == 0:
            return np.full(len(values), np.nan)

        weights = weights / weights_sum

        result = np.full(len(values), np.nan)
        for i in range(length - 1, len(values)):
            window = values[i - length + 1 : i + 1]
            valid_mask = ~np.isnan(window)
            if np.sum(valid_mask) > 0:
                valid_values = window[valid_mask]
                valid_weights = weights[valid_mask]
                if len(valid_values) == length:
                    result[i] = np.sum(valid_values * valid_weights)
                else:
                    # Нормализуем веса для неполных окон
                    valid_weights_sum = np.sum(valid_weights)
                    if valid_weights_sum > 0:
                        result[i] = (
                            np.sum(valid_values * valid_weights) / valid_weights_sum
                        )

        return result

    @staticmethod
    def smooth(values: np.ndarray, method: str, length: int, **kwargs) -> np.ndarray:
        """
        Универсальная функция сглаживания

        Args:
            values: Массив значений
            method: Тип сглаживания ("SMA", "EMA", "WMA", "HMA", "ALMA")
            length: Период сглаживания
            **kwargs: Дополнительные параметры для ALMA

        Returns:
            Массив сглаженных значений
        """
        method = method.upper()

        if method == "SMA":
            return SmoothingFunctions.sma(values, length)
        elif method == "EMA":
            return SmoothingFunctions.ema(values, length)
        elif method == "WMA":
            return SmoothingFunctions.wma(values, length)
        elif method == "HMA":
            return SmoothingFunctions.hma(values, length)
        elif method == "ALMA":
            offset = kwargs.get("offset", 0.85)
            sigma = kwargs.get("sigma", 6.0)
            return SmoothingFunctions.alma(values, length, offset, sigma)
        else:
            raise ValueError(f"Неизвестный метод сглаживания: {method}")


def example_smoothing_usage():
    """Пример использования функций сглаживания"""

    # Создание тестовых данных
    np.random.seed(42)
    data = 100 + np.cumsum(np.random.randn(100) * 0.5)

    # Применение различных типов сглаживания
    sma_result = SmoothingFunctions.sma(data, 10)
    ema_result = SmoothingFunctions.ema(data, 10)
    wma_result = SmoothingFunctions.wma(data, 10)
    hma_result = SmoothingFunctions.hma(data, 10)
    alma_result = SmoothingFunctions.alma(data, 10, offset=0.85, sigma=6.0)

    # Использование универсальной функции
    universal_sma = SmoothingFunctions.smooth(data, "SMA", 10)
    universal_ema = SmoothingFunctions.smooth(data, "EMA", 10)
    universal_alma = SmoothingFunctions.smooth(data, "ALMA", 10, offset=0.9, sigma=5.0)

    print("Пример использования функций сглаживания:")
    print(f"Исходное значение: {data[-1]:.2f}")
    print(f"SMA(10): {sma_result[-1]:.2f}")
    print(f"EMA(10): {ema_result[-1]:.2f}")
    print(f"WMA(10): {wma_result[-1]:.2f}")
    print(f"HMA(10): {hma_result[-1]:.2f}")
    print(f"ALMA(10): {alma_result[-1]:.2f}")
    print(f"Universal SMA: {universal_sma[-1]:.2f}")
    print(f"Universal EMA: {universal_ema[-1]:.2f}")
    print(f"Universal ALMA: {universal_alma[-1]:.2f}")
