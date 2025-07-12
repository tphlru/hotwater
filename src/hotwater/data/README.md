# Модуль Data

Модуль для загрузки, управления и обработки финансовых данных из различных источников (Tinkoff Invest API, MOEX).

## Основные компоненты

### 1. Timeframe - Универсальный стандарт таймфреймов

Класс `Timeframe` предоставляет единый стандарт для работы с временными интервалами во всем проекте.

#### Формат таймфреймов
- **Строковый формат**: `"N[unit]"` где N - число, unit - единица измерения
- **Поддерживаемые единицы**: 
  - `s` - секунды (например, "30s")
  - `min` - минуты (например, "1min", "5min", "15min", "30min")
  - `h` - часы (например, "1h", "4h")
  - `d` - дни (например, "1d")
  - `w` - недели (например, "1w")
  - `m` - месяцы (например, "1m")
  - `y` - годы (например, "1y")

#### Примеры использования
```python
from hotwater.data.helpers import Timeframe

# Создание таймфреймов
tf_1min = Timeframe("1min")
tf_5min = Timeframe("5min")
tf_1h = Timeframe("1h")
tf_custom = Timeframe("37min")  # Нестандартный интервал

# Конвертация в различные форматы
tinkoff_interval = tf_1min.to_tinkoff_interval()  # CandleInterval.CANDLE_INTERVAL_1_MIN
moex_period = tf_5min.to_moex_period()           # 5
pandas_freq = tf_1h.to_pandas_freq()             # "1H"

# Проверка поддержки
tf_1min.is_supported_by_tinkoff()  # True
tf_1min.is_supported_by_moex()     # True
tf_custom.is_supported_by_tinkoff() # False (но поддерживается через ресемплинг)
```

#### Поддерживаемые таймфреймы

**Tinkoff Invest API:**
- Напрямую: 1min, 5min, 15min, 1h, 4h, 1d, 1w, 1m
- Через ресемплинг: любой таймфрейм, кратный 1 минуте

**MOEX:**
- Напрямую: 1min, 5min, 10min, 15min, 30min, 1h
- Через ресемплинг: любой таймфрейм, кратный 1 минуте

### 2. DataConfig - Конфигурация данных

Класс для настройки загрузки данных для конкретного инструмента.

#### Поля конфигурации
```python
@dataclass
class DataConfig:
    ticker: str                    # Тикер инструмента (например, "SBER", "VTBR")
    date_format: str              # Формат даты для парсинга
    data_path: str                # Путь к файлу данных
    date_column: str = "datetime" # Название колонки с датой
    data_format: str = "csv"      # Формат файла ("csv" или "parquet")
    timeframe: Optional[Timeframe] = None  # Таймфрейм данных
    start_date: Optional[str] = None       # Дата начала загрузки
    end_date: Optional[str] = None         # Дата окончания загрузки
    load_moex: bool = False       # Загружать с MOEX
    load_tinkoff: bool = False    # Загружать с Tinkoff
    topup: bool = False           # Дозагрузка новых данных
    allow_resample: bool = False  # Разрешить ресемплинг
```

### 3. Функции загрузки данных

#### download_tinkoff_history()
Загружает исторические данные с Tinkoff Invest API.

```python
async def download_tinkoff_history(
    ticker: str,
    start_dt: datetime,
    end_dt: datetime,
    token: str,
    save_path: str,
    file_format: str = "csv",
    timeframe: Timeframe = Timeframe("1min"),
    show_progress: bool = False,
    topup: bool = False,
    resample_target: Optional[Timeframe] = None,
) -> bool
```

**Особенности:**
- Автоматическое ограничение периода до 3 месяцев
- Поддержка дозагрузки с нахлестом
- Автоматический ресемплинг при необходимости
- Сохранение в CSV или Parquet формате

#### download_moex_history()
Загружает исторические данные с MOEX.

```python
def download_moex_history(
    secid: str,
    period: Timeframe,
    start_dt: datetime,
    username: str,
    password: str,
    save_path: str,
    file_format: str = "parquet",
    skip_conditions: Optional[list] = None,
    skip_exceptions: Optional[dict] = None,
    show_progress: bool = False,
    topup: bool = False,
    resample_target: Optional[Timeframe] = None,
) -> bool
```

**Особенности:**
- Поддержка различных типов данных (candles, tradestats)
- Автоматическая чанковая загрузка для больших периодов
- Обработка исключений и пропусков
- Поддержка индексов и обычных инструментов

### 4. Основные функции

#### load_configs()
Загружает конфигурации из JSON файла.

```python
def load_configs(path: str = DATACONFIG_PATH) -> List[DataConfig]
```

#### load_data()
Основная функция для загрузки данных по конфигурации.

```python
def load_data(dataconfig: DataConfig) -> pd.DataFrame
```

**Возможности:**
- Автоматическая загрузка данных при отсутствии файла
- Дозагрузка новых данных при topup=True
- Автоматическое определение таймфрейма из данных
- Обработка timezone-aware и timezone-naive дат

#### try_download_data()
Запускает загрузку данных по конфигурации.

```python
def try_download_data(dataconfig: DataConfig)
```

## Конфигурационный файл

Файл `dataconfig.json` содержит конфигурации для всех инструментов.

### Пример конфигурации
```json
[
  {
    "ticker": "SBER",
    "date_format": "%Y-%m-%d %H:%M:%S",
    "data_path": "/path/to/SBER-1min.csv",
    "date_column": "time",
    "data_format": "csv",
    "timeframe": "1min",
    "start_date": "2025-04-01 00:00:00",
    "end_date": "2025-07-01 23:59:59",
    "load_moex": false,
    "load_tinkoff": true,
    "topup": true,
    "allow_resample": true
  }
]
```

### Поля конфигурации

| Поле | Тип | Описание |
|------|-----|----------|
| `ticker` | string | Тикер инструмента |
| `date_format` | string | Формат даты (например, "%Y-%m-%d %H:%M:%S") |
| `data_path` | string | Полный путь к файлу данных |
| `date_column` | string | Название колонки с датой в файле |
| `data_format` | string | Формат файла ("csv" или "parquet") |
| `timeframe` | string | Таймфрейм в строковом формате |
| `start_date` | string | Дата начала загрузки |
| `end_date` | string | Дата окончания загрузки |
| `load_moex` | boolean | Загружать с MOEX |
| `load_tinkoff` | boolean | Загружать с Tinkoff |
| `topup` | boolean | Дозагрузка новых данных |
| `allow_resample` | boolean | Разрешить ресемплинг |

## Использование

### Базовое использование
```python
from hotwater.data import load_configs, load_data

# Загружаем конфигурации
configs = load_configs()

# Загружаем данные для первого инструмента
df = load_data(configs[0])
print(f"Загружено {len(df)} строк данных")
print(f"Период: {df['datetime'].min()} - {df['datetime'].max()}")
```

### Создание собственной конфигурации
```python
from hotwater.data import DataConfig, load_data
from hotwater.data.helpers import Timeframe

# Создаем конфигурацию
config = DataConfig(
    ticker="GAZP",
    date_format="%Y-%m-%d %H:%M:%S",
    data_path="./data/GAZP-5min.csv",
    timeframe=Timeframe("5min"),
    start_date="2025-01-01 00:00:00",
    end_date="2025-01-31 23:59:59",
    load_tinkoff=True,
    topup=False,
    allow_resample=True
)

# Загружаем данные
df = load_data(config)
```

### Работа с таймфреймами
```python
from hotwater.data.helpers import Timeframe

# Создание таймфреймов
tf_1min = Timeframe("1min")
tf_30min = Timeframe("30min")
tf_1h = Timeframe("1h")

# Проверка поддержки
print(f"1min поддерживается Tinkoff: {tf_1min.is_supported_by_tinkoff()}")
print(f"30min поддерживается MOEX: {tf_30min.is_supported_by_moex()}")

# Конвертация
print(f"1h в минутах: {tf_1h.minutes}")
print(f"1h в секундах: {tf_1h.seconds}")
print(f"1h как timedelta: {tf_1h.timedelta}")
```

## Структура данных

Все функции возвращают DataFrame со следующими колонками:

| Колонка | Тип | Описание |
|---------|-----|----------|
| `datetime` | datetime | Время свечи |
| `open` | float | Цена открытия |
| `close` | float | Цена закрытия |
| `high` | float | Максимальная цена |
| `low` | float | Минимальная цена |
| `volume` | int | Объем торгов |

## Особенности и ограничения

### Tinkoff Invest API
- Ограничение: максимум 3 месяца за один запрос
- Поддерживаемые таймфреймы: 1min, 5min, 15min, 1h, 4h, 1d, 1w, 1m
- Ресемплинг: любой таймфрейм, кратный 1 минуте
- Требуется токен доступа

### MOEX
- Поддерживаемые таймфреймы: 1min, 5min, 10min, 15min, 30min, 1h
- Ресемплинг: любой таймфрейм, кратный 1 минуте
- Требуется авторизация (логин/пароль)
- Ограничения по количеству запросов

### Общие особенности
- Автоматическая обработка timezone-aware дат
- Поддержка дозагрузки с нахлестом для устранения пропусков
- Автоматическое удаление дубликатов
- Округление цен до 2 знаков после запятой
- Фильтрация строк без торговой активности

## Файлы модуля

- `dataloader.py` - Основные функции загрузки данных
- `helpers.py` - Вспомогательные функции и класс Timeframe
- `secretconf.py` - Конфигурация секретов (токены, логины)
- `dataconfig.json` - Конфигурации инструментов
- `README.md` - Документация модуля 