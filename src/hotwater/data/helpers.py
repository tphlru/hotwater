from tinkoff.invest.utils import quotation_to_decimal
import re
from typing import Union, Optional
from datetime import timedelta, timezone

GMT_3 = timezone(timedelta(hours=3), name='GMT+3')

class Timeframe:
    """
    Универсальный класс для работы с таймфреймами/интервалами.
    
    Стандарт таймфреймов:
    - Строковый формат: "N[unit]" где N - число, unit - единица измерения
    - Поддерживаемые единицы: s (секунды), min (минуты), h (часы), d (дни), w (недели), m (месяцы), y (годы)
    - Примеры: "30s", "1min", "5min", "15min", "1h", "4h", "1d", "1w", "1m", "1y"
    """
    
    # Регулярное выражение для парсинга таймфреймов
    TIMEFRAME_PATTERN = re.compile(r'^(\d+(?:\.\d+)?)\s*(s|sec|second|seconds|min|minute|minutes|h|hour|hours|d|day|days|w|week|weeks|m|month|months|y|year|years)$', re.IGNORECASE)
    
    # Маппинг единиц измерения в секунды
    UNIT_TO_SECONDS = {
        's': 1, 'sec': 1, 'second': 1, 'seconds': 1,
        'min': 60, 'minute': 60, 'minutes': 60,
        'h': 3600, 'hour': 3600, 'hours': 3600,
        'd': 86400, 'day': 86400, 'days': 86400,
        'w': 604800, 'week': 604800, 'weeks': 604800,
        'm': 2592000, 'month': 2592000, 'months': 2592000,  # ~30 дней
        'y': 31536000, 'year': 31536000, 'years': 31536000,  # ~365 дней
    }
    
    # Стандартные таймфреймы для быстрого доступа
    STANDARD_TIMEFRAMES = {
        '1s': '1s', '5s': '5s', '10s': '10s', '15s': '15s', '30s': '30s',
        '1min': '1min', '5min': '5min', '10min': '10min', '15min': '15min', 
        '30min': '30min', '45min': '45min',
        '1h': '1h', '2h': '2h', '3h': '3h', '4h': '4h', '6h': '6h', '8h': '8h', '12h': '12h',
        '1d': '1d', '1w': '1w', '1m': '1m', '1y': '1y'
    }
    
    def __init__(self, value: Union[str, int, float, timedelta, 'Timeframe']):
        """
        Инициализация таймфрейма.
        
        Args:
            value: Таймфрейм в любом поддерживаемом формате:
                - str: "1min", "5min", "1h", "1d" и т.д.
                - int/float: количество минут
                - timedelta: объект timedelta
                - Timeframe: копирование существующего объекта
        """
        if isinstance(value, Timeframe):
            self._seconds = value._seconds
            self._original_string = value._original_string
        else:
            self._seconds = self._parse_to_seconds(value)
            self._original_string = self._normalize_string(value)
    
    def _parse_to_seconds(self, value: Union[str, int, float, timedelta]) -> int:
        """Парсит значение в секунды."""
        if isinstance(value, str):
            return self._parse_string_to_seconds(value)
        elif isinstance(value, (int, float)):
            return int(value * 60)  # Интерпретируем как минуты
        elif isinstance(value, timedelta):
            return int(value.total_seconds())
        else:
            raise ValueError(f"Неподдерживаемый тип для таймфрейма: {type(value)}")
    
    def _parse_string_to_seconds(self, timeframe_str: str) -> int:
        """Парсит строку таймфрейма в секунды."""
        # Проверяем стандартные таймфреймы
        if timeframe_str in self.STANDARD_TIMEFRAMES:
            timeframe_str = self.STANDARD_TIMEFRAMES[timeframe_str]
        
        # Парсим с помощью регулярного выражения
        match = self.TIMEFRAME_PATTERN.match(timeframe_str.strip())
        if not match:
            raise ValueError(f"Неверный формат таймфрейма: {timeframe_str}")
        
        number = float(match.group(1))
        unit = match.group(2).lower()
        
        # Нормализуем единицу измерения
        for unit_key, seconds_per_unit in self.UNIT_TO_SECONDS.items():
            if unit.startswith(unit_key):
                return int(number * seconds_per_unit)
        
        raise ValueError(f"Неизвестная единица измерения: {unit}")
    
    def _normalize_string(self, value: Union[str, int, float, timedelta]) -> str:
        """Нормализует значение в стандартную строку."""
        if isinstance(value, str):
            # Проверяем стандартные таймфреймы
            if value in self.STANDARD_TIMEFRAMES:
                return self.STANDARD_TIMEFRAMES[value]
            
            # Парсим и пересоздаем
            seconds = self._parse_string_to_seconds(value)
            return self._seconds_to_string(seconds)
        elif isinstance(value, (int, float)):
            return self._seconds_to_string(int(value * 60))
        elif isinstance(value, timedelta):
            return self._seconds_to_string(int(value.total_seconds()))
        else:
            return self._seconds_to_string(self._seconds)
    
    def _seconds_to_string(self, seconds: int) -> str:
        """Конвертирует секунды в читаемую строку."""
        if seconds < 60:
            return f"{seconds}s"
        elif seconds < 3600:
            minutes = seconds // 60
            return f"{minutes}min"
        elif seconds < 86400:
            hours = seconds // 3600
            return f"{hours}h"
        elif seconds < 604800:
            days = seconds // 86400
            return f"{days}d"
        elif seconds < 2592000:
            weeks = seconds // 604800
            return f"{weeks}w"
        elif seconds < 31536000:
            months = seconds // 2592000
            return f"{months}m"
        else:
            years = seconds // 31536000
            return f"{years}y"
    
    @property
    def seconds(self) -> int:
        """Возвращает таймфрейм в секундах."""
        return self._seconds
    
    @property
    def minutes(self) -> float:
        """Возвращает таймфрейм в минутах."""
        return self._seconds / 60
    
    @property
    def hours(self) -> float:
        """Возвращает таймфрейм в часах."""
        return self._seconds / 3600
    
    @property
    def days(self) -> float:
        """Возвращает таймфрейм в днях."""
        return self._seconds / 86400
    
    @property
    def timedelta(self) -> timedelta:
        """Возвращает таймфрейм как timedelta."""
        return timedelta(seconds=self._seconds)
    
    def to_tinkoff_interval(self):
        """Конвертирует в формат CandleInterval для Tinkoff API."""
        try:
            from tinkoff.invest import CandleInterval
        except ImportError:
            raise ImportError("Tinkoff Invest API not available")
        
        # Маппинг стандартных таймфреймов
        tinkoff_mapping = {
            60: CandleInterval.CANDLE_INTERVAL_1_MIN,      # 1min
            300: CandleInterval.CANDLE_INTERVAL_5_MIN,     # 5min
            900: CandleInterval.CANDLE_INTERVAL_15_MIN,    # 15min
            3600: CandleInterval.CANDLE_INTERVAL_HOUR,     # 1h
            14400: CandleInterval.CANDLE_INTERVAL_4_HOUR,  # 4h
            86400: CandleInterval.CANDLE_INTERVAL_DAY,     # 1d
            604800: CandleInterval.CANDLE_INTERVAL_WEEK,   # 1w
            2592000: CandleInterval.CANDLE_INTERVAL_MONTH, # 1m
        }
        
        return tinkoff_mapping.get(self._seconds, CandleInterval.CANDLE_INTERVAL_1_MIN)
    
    def to_moex_period(self) -> int:
        """Конвертирует в период для MOEX API."""
        # MOEX поддерживает только определенные периоды
        moex_periods = [1, 5, 10, 15, 30, 60]  # в минутах
        
        minutes = self.minutes
        if minutes in moex_periods:
            return int(minutes)
        else:
            # Возвращаем ближайший поддерживаемый период
            closest = min(moex_periods, key=lambda x: abs(x - minutes))
            return closest
    
    def to_pandas_freq(self) -> str:
        """Конвертирует в формат частоты pandas для resample."""
        if self._seconds < 60:
            return f"{self._seconds}S"
        elif self._seconds < 3600:
            minutes = self._seconds // 60
            return f"{minutes}T"
        elif self._seconds < 86400:
            hours = self._seconds // 3600
            return f"{hours}H"
        elif self._seconds < 604800:
            days = self._seconds // 86400
            return f"{days}D"
        elif self._seconds < 2592000:
            weeks = self._seconds // 604800
            return f"{weeks}W"
        elif self._seconds < 31536000:
            months = self._seconds // 2592000
            return f"{months}M"
        else:
            years = self._seconds // 31536000
            return f"{years}Y"
    
    def is_supported_by_tinkoff(self) -> bool:
        """
        Проверяет, поддерживается ли таймфрейм Tinkoff API напрямую или через ресемплинг.
        Tinkoff поддерживает интервалы: 1, 5, 15 минут, 1 час, 4 часа, 1 день, 1 неделя, 1 месяц.
        Ресемплинг возможен, если таймфрейм кратен 1 минуте.
        """
        supported_seconds = [60, 300, 900, 3600, 14400, 86400, 604800, 2592000]
        if self._seconds in supported_seconds:
            return True
        # Кратность 1 минуте
        return self._seconds % 60 == 0 and self._seconds >= 60

    def is_supported_by_moex(self) -> bool:
        """
        Проверяет, поддерживается ли таймфрейм MOEX напрямую или через ресемплинг.
        MOEX поддерживает интервалы: 1, 5, 10, 15, 30, 60 минут.
        Ресемплинг возможен, если таймфрейм кратен 1 минуте.
        """
        supported_minutes = [1, 5, 10, 15, 30, 60]
        if self.minutes in supported_minutes:
            return True
        # Кратность 1 минуте
        return self._seconds % 60 == 0 and self._seconds >= 60
    
    def __str__(self) -> str:
        return self._original_string
    
    def __repr__(self) -> str:
        return f"Timeframe('{self._original_string}')"
    
    def __eq__(self, other) -> bool:
        if isinstance(other, Timeframe):
            return self._seconds == other._seconds
        elif isinstance(other, str):
            return self._original_string == other
        elif isinstance(other, (int, float)):
            return self.minutes == other
        else:
            return False
    
    def __hash__(self) -> int:
        return hash(self._seconds)
    
    def __add__(self, other: Union['Timeframe', timedelta]) -> 'Timeframe':
        if isinstance(other, Timeframe):
            return Timeframe(timedelta(seconds=self._seconds + other._seconds))
        elif isinstance(other, timedelta):
            return Timeframe(timedelta(seconds=self._seconds + other.total_seconds()))
        else:
            raise TypeError(f"Нельзя сложить Timeframe с {type(other)}")
    
    def __sub__(self, other: Union['Timeframe', timedelta]) -> 'Timeframe':
        if isinstance(other, Timeframe):
            return Timeframe(timedelta(seconds=max(0, self._seconds - other._seconds)))
        elif isinstance(other, timedelta):
            return Timeframe(timedelta(seconds=max(0, self._seconds - other.total_seconds())))
        else:
            raise TypeError(f"Нельзя вычесть {type(other)} из Timeframe")
    
    @classmethod
    def from_seconds(cls, seconds: int) -> 'Timeframe':
        """Создает Timeframe из секунд."""
        return cls(timedelta(seconds=seconds))
    
    @classmethod
    def from_minutes(cls, minutes: Union[int, float]) -> 'Timeframe':
        """Создает Timeframe из минут."""
        return cls(timedelta(minutes=minutes))
    
    @classmethod
    def from_hours(cls, hours: Union[int, float]) -> 'Timeframe':
        """Создает Timeframe из часов."""
        return cls(timedelta(hours=hours))
    
    @classmethod
    def from_days(cls, days: Union[int, float]) -> 'Timeframe':
        """Создает Timeframe из дней."""
        return cls(timedelta(days=days))


# Удаляем устаревшие функции (timeframe_to_tinkoff_interval и др.)


async def get_figi_by_ticker(ticker: str, client):
    methods = ["shares", "bonds", "etfs", "currencies", "futures"]
    for method in methods:
        response = await getattr(client.instruments, method)()
        for item in response.instruments:
            if item.ticker == ticker:
                return {
                    "figi": item.figi,
                    "uid": item.uid,
                    "name": item.name,
                    "class_code": item.class_code,
                    "type": method,
                    "min_price_increment": quotation_to_decimal(item.min_price_increment),
                    "lot": item.lot,
                    "currency": item.currency,
                    "exchange": item.exchange,
                }
    return None


def quotation_to_float(quotation) -> float:
    """Преобразует Quotation в float."""
    if not hasattr(quotation, 'units') or not hasattr(quotation, 'nano'):
        return float(quotation)
    return quotation.units + quotation.nano / 1_000_000_000 