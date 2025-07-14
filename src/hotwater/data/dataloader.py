import pandas as pd
from dataclasses import dataclass, asdict, field
from typing import List, Optional
import json
import os
import asyncio

from datetime import datetime, timedelta, timezone
from moexalgo import Ticker, session, Index
from tqdm import tqdm
import logging

from hotwater.data.secretconf import Secrets
from hotwater.data.helpers import get_figi_by_ticker, quotation_to_float, Timeframe

try:
    from tinkoff.invest import AsyncClient, CandleInterval
except ImportError:
    AsyncClient = None
    CandleInterval = None


DATACONFIG_PATH = "./src/hotwater/data/dataconfig.json"


logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s",
)

# Отключаем логгинг ниже ERROR для библиотек
logging.getLogger("moexalgo").setLevel(logging.ERROR)
logging.getLogger("httpx").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)


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
) -> bool:
    """
    Скачивает исторические данные с Tinkoff Invest API для одного тикера, сохраняет в указанный файл.
    topup: если True — дозаписывает новые данные в конец файла (дозагрузка), с overlap-нахлестом для удаления пропусков.
    show_progress: если True — использовать tqdm для прогресса загрузки.
    """
    if not AsyncClient or not CandleInterval:
        logging.error("Tinkoff Invest API not available. Install tinkoff-investments package.")
        return False
    
    logging.info(
        f"Запуск загрузки Tinkoff {ticker}, период {timeframe}, с {start_dt} по {end_dt} в файл {save_path} ({file_format})"
    )
    
    # Проверяем ограничение в 3 месяца
    if (end_dt - start_dt).days > 90:
        logging.warning(f"Период загрузки превышает 3 месяца. Обрезаем старые данные, end_date в приоритете. Ограничиваем до 3 месяцев до {end_dt}")
        start_dt = end_dt - timedelta(days=90)
    
    now = datetime.now(timezone.utc)
    df_exist = None
    overlap_td = None
    
    # ДОЗАГРУЗКА
    if topup and os.path.exists(save_path) and os.path.getsize(save_path) > 0:
        if file_format == "parquet":
            df_exist = pd.read_parquet(save_path)
        elif file_format == "csv":
            df_exist = pd.read_csv(save_path, parse_dates=["time"])
        else:
            df_exist = None
        if df_exist is not None and not df_exist.empty:
            # Определим последнюю дату
            last_time = pd.to_datetime(df_exist["time"].iloc[-1])
            
            # Нахлест для удаления пропусков
            overlap_td = timedelta(days=1)
            start_dt = last_time - overlap_td
            logging.info(f"Дозагрузка с {start_dt} (нахлест {overlap_td})")
        else:
            logging.info(f"Файл существует, но пуст. Скачиваем с {start_dt}")
    else:
        df_exist = None
    
    # Загружаем данные через Tinkoff API
    df = pd.DataFrame()
    
    async with AsyncClient(token) as client:
        # Получаем FIGI по тикеру
        instrument_info = await get_figi_by_ticker(ticker, client)
        if not instrument_info:
            logging.error(f"Не удалось найти инструмент с тикером {ticker}")
            return False
        
        figi = instrument_info["figi"]
        logging.info(f"Найден FIGI для {ticker}: {figi}")
        
        # Преобразуем timeframe в формат Tinkoff
        interval = timeframe.to_tinkoff_interval()
        
        # Загружаем исторические данные
        try:
            candle_generator = client.get_all_candles(
                instrument_id=figi,
                from_=start_dt.replace(tzinfo=timezone.utc),
                to=end_dt.replace(tzinfo=timezone.utc),
                interval=interval,
            )
            
            candle_data = []
            async for candle in candle_generator:
                candle_data.append({
                    "time": candle.time,
                    "open": quotation_to_float(candle.open),
                    "close": quotation_to_float(candle.close),
                    "high": quotation_to_float(candle.high),
                    "low": quotation_to_float(candle.low),
                    "volume": candle.volume,
                })
            
            if not candle_data:
                logging.info(f"Нет данных для {ticker} период {timeframe}")
                return False
            
            df = pd.DataFrame(candle_data)
            df["time"] = pd.to_datetime(df["time"])
            
        except Exception as e:
            logging.error(f"Ошибка при загрузке данных для {ticker}: {e}")
            return False
    
    if df.empty:
        logging.info(f"Нет данных для {ticker} период {timeframe}")
        return False
    
    # Стандартизируем колонки
    df = df[["time", "open", "close", "high", "low", "volume"]].copy()
    
    # === объединение и удаление дублей ===
    if topup and df_exist is not None and not df_exist.empty:
        # Только новые строки, которые позже самой свежей строки в исходном файле
        last_time_exist = pd.to_datetime(df_exist["time"].max())
        # Оставляем только строки, которые новее последней уже сохранённой
        df_new = df[df["time"] > last_time_exist]
        if df_new.empty:
            logging.info("Новых данных нет, файл актуален.")
            return True
        df_all = pd.concat([df_exist, df_new], ignore_index=True)
        df_all.drop_duplicates(subset=["time"], keep="last", inplace=True)
        df_all = df_all.sort_values("time")
        df_all.reset_index(drop=True, inplace=True)
        df = df_all
    
    # === Ресэмплинг ===
    if resample_target and timeframe != resample_target:
        # Ресемплинг только из минимального поддерживаемого (1min)
        if timeframe.seconds == 60:
            df = (
                df.resample(
                    resample_target.to_pandas_freq(),
                    on="time",
                    origin="start",
                    label="right",
                    closed="right",
                )
                .agg(
                    {
                        "open": "first",
                        "close": "last",
                        "high": "max",
                        "low": "min",
                        "volume": "sum",
                    }
                )
                .reset_index()
            )
            # Удаляем строки, где нет сделок (volume == 0 или все OHLC NaN)
            df = df[
                ~(
                    (df[["open", "close", "high", "low"]].isna().all(axis=1))
                    & (df["volume"] == 0)
                )
            ]
            # Округляем до 2 знаков после запятой
            for col in ["open", "close", "high", "low"]:
                df[col] = df[col].round(2)
    
    # Сохраняем
    if file_format == "parquet":
        df.to_parquet(save_path, index=False, engine="pyarrow", compression="snappy")
    elif file_format == "csv":
        df.to_csv(save_path, index=False, encoding="utf-8")
    
    logging.info(
        f"Данные Tinkoff для {ticker} период {timeframe} сохранены в {save_path} ({len(df)} строк)"
    )
    return True


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
) -> bool:
    """
    Скачивает исторические данные с MOEX для одного тикера и одного периода, сохраняет в указанный файл.
    topup: если True — дозаписывает новые данные в конец файла (дозагрузка), с overlap-нахлестом для удаления пропусков.
    show_progress: если True — использовать tqdm для прогресса загрузки чанков.
    """
    logging.info(
        f"Запуск загрузки {secid}, период {period}, с {start_dt} в файл {save_path} ({file_format})"
    )

    # Авторизация
    if not session.authorize(username, password):
        logging.error(f"Авторизация не удалась для пользователя {username}")
        return False

    # Проверка на исключения/пропуски
    if skip_conditions:
        for cond in skip_conditions:
            if (cond[0] == secid or cond[0] is None) and (
                cond[1] == period or cond[1] is None
            ):
                logging.warning(
                    f"Пропуск: {(skip_exceptions.get((secid, period)) if skip_exceptions else None)}"
                )
                return False
    if skip_exceptions and (secid, period) in skip_exceptions:
        logging.warning(f"Пропуск: {skip_exceptions[(secid, period)]}")
        return False

    now = datetime.now()
    df_exist = None
    overlap_td = None

    # === ДОЗАГРУЗКА ===
    if topup and os.path.exists(save_path) and os.path.getsize(save_path) > 0:
        if file_format == "parquet":
            df_exist = pd.read_parquet(save_path)
        elif file_format == "csv":
            df_exist = pd.read_csv(save_path, parse_dates=["time"])
        else:
            df_exist = None
        if df_exist is not None and not df_exist.empty:
            # Определим последнюю дату
            last_time = pd.to_datetime(df_exist["time"].iloc[-1])

            # Получим 1 полный чанк (примерно)
            # Для period=1 это 10000 минут максимум, обычно это примерно неделя данных
            if period.minutes == 5:
                ticker = Ticker(secid)
                if not isinstance(ticker, Index):
                    df_temp = ticker.tradestats(start=last_time.date(), end=now.date())
                else:
                    df_temp = ticker.candles(
                        start=last_time, end=now, period=period.to_moex_period(), use_dataframe=True
                    )
                if not df_temp.empty:
                    df_temp["time_end"] = pd.to_datetime(
                        df_temp["tradedate"].astype(str)
                        + " "
                        + df_temp["tradetime"].astype(str)
                    )
                    df_temp["time"] = pd.to_datetime(df_temp["time_end"]) - timedelta(
                        minutes=5
                    )
                    first_time = df_temp["time"].iloc[0]
                    last_temp_time = df_temp["time"].iloc[-1]
                    chunk_time_span = last_temp_time - first_time
                else:
                    chunk_time_span = timedelta(minutes=period.minutes)
            else:
                df_temp = Ticker(secid).candles(
                    start=last_time, end=now, period=period.to_moex_period(), use_dataframe=True
                )
                if not df_temp.empty and "begin" in df_temp and "end" in df_temp:
                    first_time = pd.to_datetime(df_temp.iloc[0]["begin"])
                    last_temp_time = pd.to_datetime(df_temp.iloc[-1]["end"])
                    chunk_time_span = last_temp_time - first_time
                else:
                    chunk_time_span = timedelta(minutes=period.minutes)

            overlap_td = chunk_time_span * 0.5

            start_dt = last_time - overlap_td
            logging.info(f"Дозагрузка с {start_dt} (нахлест {overlap_td})")
        else:
            logging.info(f"Файл существует, но пуст. Скачиваем с {start_dt}")
    else:
        df_exist = None

    # Загрузка новых данных
    df = pd.DataFrame()
    if period.minutes == 5:
        chunk_start = start_dt.date()
        progress = None
        ticker = Ticker(secid)
        if not isinstance(ticker, Index):
            df_first = ticker.tradestats(start=chunk_start, end=now.date())
        else:
            df_first = ticker.candles(
                start=chunk_start, end=now, period=period.to_moex_period(), use_dataframe=True
            )
        if df_first.empty:
            logging.info(f"Нет данных для {secid} период {period}")
            return False
        df_first["time_end"] = pd.to_datetime(
            df_first["tradedate"].astype(str) + " " + df_first["tradetime"].astype(str)
        )
        df_first["time"] = pd.to_datetime(df_first["time_end"]) - timedelta(minutes=5)
        first_time = df_first["time"].iloc[0]
        last_time = df_first["time"].iloc[-1]
        chunk_time_span = last_time - first_time
        total_time = now - datetime.combine(chunk_start, datetime.min.time())
        estimated_chunks = (
            max(1, int(total_time / chunk_time_span))
            if chunk_time_span > timedelta(0)
            else 1
        )
        estimated_chunks *= 1.32  # + выходные
        if show_progress:
            progress = tqdm(
                total=round(estimated_chunks), desc=f"{secid}-{period} загрузка"
            )
        df = pd.concat([df, df_first], ignore_index=True)
        chunk_start = df_first["time_end"].iloc[-1].to_pydatetime().date()
        if progress:
            progress.update(1)
        while True:
            ticker = Ticker(secid)
            if not isinstance(ticker, Index):
                df_load = ticker.tradestats(start=chunk_start, end=now.date())
            else:
                # Для индексов используем candles, т.к. tradestats не поддерживается
                df_load = ticker.candles(
                    start=chunk_start, end=now, period=period.to_moex_period(), use_dataframe=True
                )
            if df_load.empty:
                break
            df_load["time_end"] = pd.to_datetime(
                df_load["tradedate"].astype(str)
                + " "
                + df_load["tradetime"].astype(str)
            )
            df_load["time"] = pd.to_datetime(df_load["time_end"]) - timedelta(minutes=5)
            chunk_start = df_load["time_end"].iloc[-1].to_pydatetime().date()
            df = pd.concat([df, df_load], ignore_index=True)
            if progress:
                progress.update(1)
            if len(df_load) < 10000:
                break
        if progress:
            progress.n = progress.total
            progress.refresh()
            progress.close()
        if len(df) > 0:
            df.rename(
                columns={
                    "pr_open": "open",
                    "pr_close": "close",
                    "pr_high": "high",
                    "pr_low": "low",
                    "vol": "volume",
                },
                inplace=True,
            )
    else:
        chunk_start = start_dt
        progress = None
        df_first = Ticker(secid).candles(
            start=chunk_start, end=now, period=period.to_moex_period(), use_dataframe=True
        )
        if df_first.empty:
            logging.info(f"Нет данных для {secid} период {period}")
            return False
        first_time = pd.to_datetime(df_first.iloc[0]["begin"])
        last_time = pd.to_datetime(df_first.iloc[-1]["end"])
        chunk_time_span = last_time - first_time
        total_time = now - chunk_start
        estimated_chunks = (
            max(1, int(total_time / chunk_time_span))
            if chunk_time_span > timedelta(0)
            else 1
        )
        estimated_chunks *= 1.32  # + выходные
        if show_progress:
            progress = tqdm(
                total=round(estimated_chunks), desc=f"{secid}-{period} загрузка"
            )
        df = pd.concat([df, df_first], ignore_index=True)
        if progress:
            progress.update(1)
        # Новый цикл дозагрузки
        while True:
            # Определяем максимальное время в уже загруженных данных
            max_time = (
                pd.to_datetime(df["end"].max())
                if "end" in df.columns
                else pd.to_datetime(df["time"].max())
            )
            next_start = max_time + timedelta(minutes=period.minutes)
            if next_start > now:
                break
            df_ohlcv = Ticker(secid).candles(
                start=next_start, end=now, period=period.to_moex_period(), use_dataframe=True
            )
            if df_ohlcv.empty:
                break
            df = pd.concat([df, df_ohlcv], ignore_index=True)
            if progress:
                progress.update(1)
        if show_progress and progress is not None:
            progress.n = progress.total
            progress.refresh()
            progress.close()
        if not df.empty and "begin" in df.columns:
            df["time"] = df["begin"]

    if df.empty:
        logging.info(f"Нет данных для {secid} период {period}")
        return False

    # Стандартизируем колонки
    if period.minutes == 5:
        df = df[
            [
                "time",
                "open",
                "close",
                "high",
                "low",
                "volume",
                "pr_vwap",
                "pr_change",
                "trades",
                "trades_b",
                "trades_s",
                "vol_b",
                "vol_s",
                "disb",
                "time_end",
            ]
        ].copy()
    else:
        if secid == "IMOEX":
            df = df[["time", "open", "close", "high", "low", "value"]].copy()
        else:
            df = df[["time", "open", "close", "high", "low", "volume"]].copy()

    # === объединение и удаление дублей ===
    if topup and df_exist is not None and not df_exist.empty:
        # Только новые строки, которые позже самой свежей строки в исходном файле
        last_time_exist = pd.to_datetime(df_exist["time"].max())
        # Оставляем только строки, которые новее последней уже сохранённой
        df_new = df[df["time"] > last_time_exist]
        if df_new.empty:
            logging.info("Новых данных нет, файл актуален.")
            return True
        df_all = pd.concat([df_exist, df_new], ignore_index=True)
        df_all.drop_duplicates(subset=["time"], keep="last", inplace=True)
        df_all = df_all.sort_values("time")
        df_all.reset_index(drop=True, inplace=True)
        df = df_all

    # === Ресэмплинг === (из 1 в любой)
    if resample_target and period != resample_target and period.minutes == 1:
        df = (
            df.resample(
                resample_target.to_pandas_freq(),
                on="time",
                origin="start",
                label="right",
                closed="right",
            )
            .agg(
                {
                    "open": "first",
                    "close": "last",
                    "high": "max",
                    "low": "min",
                    "volume": "sum",
                }
            )
            .reset_index()
        )
        # Удаляем строки, где нет сделок (volume == 0 или все OHLC NaN)
        df = df[
            ~(
                (df[["open", "close", "high", "low"]].isna().all(axis=1))
                & (df["volume"] == 0)
            )
        ]
        # Округляем до 2 знаков после запятой
        for col in ["open", "close", "high", "low"]:
            df[col] = df[col].round(2)

    # Сохраняем
    if file_format == "parquet":
        df.to_parquet(save_path, index=False, engine="pyarrow", compression="snappy")
    elif file_format == "csv":
        df.to_csv(save_path, index=False, encoding="utf-8")

    logging.info(
        f"Данные для {secid} период {period} сохранены в {save_path} ({len(df)} строк)"
    )
    return True


@dataclass
class DataConfig:
    ticker: str
    date_format: str
    data_path: str
    date_column: str = "datetime"
    data_format: str = "csv"
    timeframe: Optional[Timeframe] = field(default=None)
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    load_moex: bool = False
    load_tinkoff: bool = False
    topup: bool = False
    allow_resample: bool = False

    def __post_init__(self):
        if self.timeframe is not None and not isinstance(self.timeframe, Timeframe):
            self.timeframe = Timeframe(self.timeframe)

    def get_start_datetime(self) -> datetime:
        """Convert start_date to datetime, handling 'now' special value"""
        if not self.start_date:
            raise ValueError(f"Не указана start_date для {self.ticker}")
        
        if self.start_date.lower() == "now":
            return datetime.now()
        
        return datetime.strptime(self.start_date, self.date_format)

    def get_end_datetime(self) -> datetime:
        """Convert end_date to datetime, handling 'now' special value"""
        if not self.end_date:
            raise ValueError(f"Не указана end_date для {self.ticker}")
        
        if self.end_date.lower() == "now":
            return datetime.now()
        
        return datetime.strptime(self.end_date, self.date_format)

    def update_dates(self, df: pd.DataFrame):
        if not df.empty:
            self.start_date = df[self.date_column].min().strftime(self.date_format)
            # Не обновляем end_date если он равен "now" и включена дозагрузка
            if not (self.end_date and self.end_date.lower() == "now" and self.topup):
                self.end_date = df[self.date_column].max().strftime(self.date_format)

    def update_tf(self, df):
        if self.timeframe:
            return
        df = df.sort_values(self.date_column)
        deltas = df[self.date_column].diff().dropna()
        if not deltas.empty:
            most_common_delta = deltas.mode()[0]
            self.timeframe = Timeframe(most_common_delta)
            return
        raise ValueError(
            f"Не могу определить timeframe для {self.ticker}. "
            "Проверь данные в файле или укажите timeframe вручную."
        )


def load_configs(path: str = DATACONFIG_PATH) -> List[DataConfig]:
    with open(path, "r") as f:
        configs = json.load(f)
    return [DataConfig(**cfg) for cfg in configs]


def save_single(updated_config: DataConfig, config_path: str = DATACONFIG_PATH):
    configs = load_configs(config_path)
    for i, cfg in enumerate(configs):
        if cfg.ticker == updated_config.ticker:
            configs[i] = updated_config
            break
    else:
        # raise ValueError(f" {updated_config.ticker} not found.")
        return
    save_configs(configs, config_path)


def save_configs(configs: List[DataConfig], path: str):
    with open(path, "w") as f:
        # Преобразуем конфигурации в словари, сериализуя Timeframe в строки
        config_dicts = []
        for cfg in configs:
            cfg_dict = asdict(cfg)
            # Преобразуем Timeframe в строку для JSON сериализации
            if cfg_dict.get('timeframe') is not None:
                cfg_dict['timeframe'] = str(cfg_dict['timeframe'])
            config_dicts.append(cfg_dict)
        json.dump(config_dicts, f, indent=2)


async def load_data(dataconfig: DataConfig) -> pd.DataFrame:
    downloaded = False
    match dataconfig.data_format:
        case "parquet":
            if (
                not os.path.exists(dataconfig.data_path) and (dataconfig.load_moex or dataconfig.load_tinkoff)
            ) or dataconfig.topup:
                await try_download_data(dataconfig)

            df = pd.read_parquet(
                dataconfig.data_path,
                engine="pyarrow",
            )
        case "csv":
            if (
                not os.path.exists(dataconfig.data_path) and (dataconfig.load_moex or dataconfig.load_tinkoff)
            ) or dataconfig.topup:
                await try_download_data(dataconfig)

            df = pd.read_csv(
                dataconfig.data_path,
                parse_dates=[dataconfig.date_column if downloaded else "time"],
                date_format=dataconfig.date_format,
            )
        case _:
            raise ValueError(f"Unsupported source type: {dataconfig.data_format}")

    if not df.empty:
        df = df.rename(columns={dataconfig.date_column: "datetime"})
        
        # Check if datetime column contains timezone information
        sample_dt = str(df["datetime"].iloc[0]) if not df["datetime"].empty else ""
        has_timezone = "+" in sample_dt or sample_dt.endswith("Z") or "UTC" in sample_dt
        
        if has_timezone:
            # Use automatic parsing for timezone-aware datetimes
            df["datetime"] = pd.to_datetime(df["datetime"])
            # Переводим в локальное время (МСК, +3)
            df["datetime"] = df["datetime"].dt.tz_convert("Europe/Moscow") if df["datetime"].dt.tz is not None else df["datetime"]
            # Если tz_convert не сработал (например, tz-naive), просто добавим 3 часа
            if df["datetime"].dt.tz is None:
                df["datetime"] = df["datetime"] + pd.Timedelta(hours=3)
            # Убираем временную зону
            df["datetime"] = df["datetime"].dt.tz_localize(None)
        else:
            # Use specified format for timezone-naive datetimes
            df["datetime"] = pd.to_datetime(df["datetime"], format=dataconfig.date_format)
        
        old_dt_col = dataconfig.date_column
        dataconfig.date_column = "datetime"
        dataconfig.update_tf(df)
        dataconfig.update_dates(df)
        dataconfig.date_column = old_dt_col
        save_single(dataconfig)
        return df

    raise ValueError(f"Данных в файле {dataconfig.data_path} нет. ")


async def try_download_data(dataconfig: DataConfig):
    if dataconfig.load_moex:
        assert dataconfig.load_moex, "Загрузка данных с MOEX не включена."
        if not dataconfig.timeframe:
            raise ValueError(
                f"Не указан timeframe для {dataconfig.ticker}. "
                "Укажите его в конфигурации (Это необходимо при load_moex=True)."
            )
        if not dataconfig.start_date:
            raise ValueError(
                f"Не указана start_date для {dataconfig.ticker}. "
                "Укажите её в конфигурации (Это необходимо при load_moex=True)."
            )

        tf = dataconfig.timeframe
        resample = False
        if not tf.is_supported_by_moex():
            if not dataconfig.allow_resample:
                raise ValueError(
                    f"Неподдерживаемый timeframe {dataconfig.timeframe} для {dataconfig.ticker}. "
                    "Укажите 1min, 5min, 10min, 15min, 30min, 1h или разрешите ресэмплинг (allow_resample=True)."
                )
            resample = True
        
        start_dt = dataconfig.get_start_datetime()
        
        download_moex_history(
            secid=dataconfig.ticker,
            period=Timeframe("1min") if resample else tf,
            start_dt=start_dt,
            username=Secrets.moex_username,
            password=Secrets.moew_password,
            save_path=dataconfig.data_path,
            file_format=dataconfig.data_format,
            show_progress=True,
            skip_conditions=[("IMOEX", 5)],  # например, пропустить IMOEX 5 минут
            skip_exceptions={("GAZP", 60): "Нет данных для GAZP 60 минут"},
            topup=dataconfig.topup,
            resample_target=tf if resample else None,
        )
        
        # Если это не дозагрузка и end_date было "now", обновляем конфигурацию
        if not dataconfig.topup and dataconfig.end_date and dataconfig.end_date.lower() == "now":
            # Загружаем данные для получения фактической end_date
            df = await load_data(dataconfig)
            if not df.empty:
                dataconfig.end_date = df["datetime"].max().strftime(dataconfig.date_format)
                save_single(dataconfig)
    
    elif dataconfig.load_tinkoff:
        if not AsyncClient or not CandleInterval:
            raise ImportError("Tinkoff Invest API not available. Install tinkoff-investments package.")
        
        if not dataconfig.timeframe:
            raise ValueError(
                f"Не указан timeframe для {dataconfig.ticker}. "
                "Укажите его в конфигурации (Это необходимо при load_tinkoff=True)."
            )
        if not dataconfig.start_date:
            raise ValueError(
                f"Не указана start_date для {dataconfig.ticker}. "
                "Укажите её в конфигурации (Это необходимо при load_tinkoff=True)."
            )
        if not dataconfig.end_date:
            raise ValueError(
                f"Не указана end_date для {dataconfig.ticker}. "
                "Укажите её в конфигурации (Это необходимо при load_tinkoff=True)."
            )

        tf = dataconfig.timeframe
        resample = False
        if isinstance(tf, str):
            if tf.endswith("h"):
                h = tf[:-1]
                if h.isdigit():
                    tf = h * 60
            elif tf.endswith("min"):
                m = tf[:-3]
                if m.isdigit():
                    tf = int(m)

        # Проверяем ограничения Tinkoff API
        if tf not in [1, 5, 15, 60, 1440]:  # 1min, 5min, 15min, 1h, 1d
            if not dataconfig.allow_resample:
                raise ValueError(
                    f"Неподдерживаемый timeframe {dataconfig.timeframe} для Tinkoff {dataconfig.ticker}. "
                    "Укажите 1min, 5min, 15min, 1h или 1d или разрешите ресэмплинг (allow_resample=True)."
                )
            resample = True
        
        start_dt = dataconfig.get_start_datetime()
        end_dt = dataconfig.get_end_datetime()
        
        # Запускаем асинхронную загрузку напрямую
        await download_tinkoff_history(
            ticker=dataconfig.ticker,
            start_dt=start_dt,
            end_dt=end_dt,
            token=Secrets.t_invest_token,
            save_path=dataconfig.data_path,
            file_format=dataconfig.data_format,
            timeframe=dataconfig.timeframe,
            show_progress=True,
            topup=dataconfig.topup,
            resample_target=int(tf.minutes) if resample else None,
        )
        
        # Если это не дозагрузка и end_date было "now", обновляем конфигурацию
        if not dataconfig.topup and dataconfig.end_date and dataconfig.end_date.lower() == "now":
            # Загружаем данные для получения фактической end_date
            df = await load_data(dataconfig)
            if not df.empty:
                dataconfig.end_date = df["datetime"].max().strftime(dataconfig.date_format)
                save_single(dataconfig)
    else:
        print(f"Загрузка данных для {dataconfig.ticker} не требуется.")


if __name__ == "__main__":
    async def main():
        configs = load_configs()
        df = await load_data(configs[0])
        print(df.head())
    
    asyncio.run(main())
