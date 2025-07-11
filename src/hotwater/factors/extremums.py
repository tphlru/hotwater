from tqdm import tqdm
import pandas as pd


def first_level_extremums(df):
    df["maxmin_1"] = 0  # 1 - максимум, -1 - минимум, 0 - не экстремум

    bars = df[df["high"] != df["low"]].copy()

    bars_indices = bars.index.to_list()

    # Начальные значения для maxmin_1
    first_idx = bars.index.to_list()[0]
    first_bar = bars.loc[first_idx]
    trend = "bull" if first_bar["open"] < first_bar["close"] else "bear"
    df.at[first_idx, "maxmin_1"] = 1 if trend == "bull" else -1
    last_extreme_idx = first_idx
    prev_high = first_bar["high"]
    prev_low = first_bar["low"]

    # Цикл анализа трендов
    for idx in tqdm(bars_indices[1:]):
        cur_bar = bars.loc[idx]
        cur_high = cur_bar["high"]
        cur_low = cur_bar["low"]

        if trend == "bull":
            if cur_high > prev_high:
                # Обновляем максимум
                df.at[last_extreme_idx, "maxmin_1"] = 0
                df.at[idx, "maxmin_1"] = 1
                last_extreme_idx = idx
                prev_high = cur_high
            else:
                # Разворот на медвежий
                df.at[idx, "maxmin_1"] = -1
                trend = "bear"
                last_extreme_idx = idx
                prev_low = cur_low
        else:  # trend == "bear"
            if cur_low < prev_low:
                # Обновляем минимум
                df.at[last_extreme_idx, "maxmin_1"] = 0
                df.at[idx, "maxmin_1"] = -1
                last_extreme_idx = idx
                prev_low = cur_low
            else:
                # Разворот на бычий
                df.at[idx, "maxmin_1"] = 1
                trend = "bull"
                last_extreme_idx = idx
                prev_high = cur_high

    return df


def next_level_extremum(df, prev_column_name="maxmin_1", next_column_name="maxmin_2"):
    """
    Функция для поиска экстремумов следующего уровня.
    Принимает DataFrame с экстремумами предыдущего уровня и возвращает DataFrame с экстремумами следующего уровня.
    """
    maxmins = df[df[prev_column_name] != 0].copy()
    # Оставляем только нужные столбцы, нужная цена в price
    maxmins["price"] = maxmins.apply(
        lambda row: row["high"] if row[prev_column_name] == 1 else row["low"], axis=1
    )
    maxmins = maxmins.loc[:, maxmins.columns.intersection(["price", prev_column_name])]
    maxmins["old_index"] = maxmins.index
    maxmins.reset_index(drop=True, inplace=True)  # Сбрасываем индекс для удобства
    maxmins[next_column_name] = 0

    # Первый экстремум наследует значение maxmin_1
    maxmins.iloc[0, maxmins.columns.get_loc(next_column_name)] = maxmins.iloc[0][
        prev_column_name
    ]
    trend = "bull" if maxmins.iloc[0][prev_column_name] == 1 else "bear"
    prev = maxmins.index[0]

    for ind in tqdm(maxmins.index[1:]):
        if trend == "bull":
            if maxmins.loc[ind][prev_column_name] != 1:
                continue  # Пропускаем, если это не максимум
            if maxmins.loc[ind]["price"] > maxmins.loc[prev]["price"]:
                maxmins.loc[ind, next_column_name] = 1
                maxmins.loc[prev, next_column_name] = 0
                prev = ind
            else:
                maxmins.loc[ind - 1, next_column_name] = -1
                prev = ind - 1
                trend = "bear"
        else:  # trend == "bear"
            if maxmins.loc[ind][prev_column_name] != -1:
                continue  # Пропускаем, если это не минимум
            if maxmins.loc[ind]["price"] < maxmins.loc[prev]["price"]:
                maxmins.loc[ind, next_column_name] = -1
                maxmins.loc[prev, next_column_name] = 0
                prev = ind
            else:
                maxmins.loc[ind - 1, next_column_name] = 1
                prev = ind - 1
                trend = "bull"

    maxmins.index = maxmins["old_index"]  # Возвращаем старый индекс
    df[next_column_name] = 0
    df.loc[maxmins.index, next_column_name] = maxmins[next_column_name]
    return df


def get_trends_info(df, trends_col="maxmin_2"):
    # Найдём индексы всех экстремумов
    # Исключим нули - теперь будет последовательность экстремумов
    extremes = df[df[trends_col] != 0].copy()
    extremes = extremes.reset_index()
    # Список для хранения трендов
    trends = []

    for i in tqdm(range(len(extremes) - 1)):
        row_start = extremes.iloc[i]
        row_end = extremes.iloc[i + 1]

        # Определяем направление тренда
        if row_start[trends_col] == -1 and row_end[trends_col] == 1:
            kind = 1  # Бычий тренд
            begin_price = df.loc[row_start["index"], "low"]
            end_price = df.loc[row_end["index"], "high"]
        elif row_start[trends_col] == 1 and row_end[trends_col] == -1:
            kind = -1  # Медвежий тренд
            begin_price = df.loc[row_start["index"], "high"]
            end_price = df.loc[row_end["index"], "low"]
        else:
            print(
                f"!!! Не удалось определить тренд между индексами {row_start['index']} и {row_end['index']}"
            )
            continue  # Пропускаем невалидные пары

        # Временные метки
        begin_time = df.loc[row_start["index"], "datetime"]
        end_time = df.loc[row_end["index"], "datetime"]
        # Длина тренда как количество баров внутри тренда
        length = len(df.loc[row_start["index"] : row_end["index"]])

        trends.append(
            {
                "begin": begin_time,
                "end": end_time,
                "begin_price": begin_price,
                "end_price": end_price,
                "kind": kind,
                "delta": (end_price - begin_price) / begin_price * 100,
                "len": length,
                "speed": (
                    round(
                        abs(
                            round(abs((end_price - begin_price) / begin_price) * 100, 2)
                            / length
                        ),
                        4,
                    )
                    if begin_price and length
                    else None
                ),  # Скорость изменения в % за минуту
            }
        )

    return pd.DataFrame(trends)
