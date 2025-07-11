import numpy as np
import pandas as pd
from scipy.ndimage import gaussian_filter


def prepare_volume_stats(df):
    stats = []
    for day, group in df.groupby(df.index.date):
        group = group.copy()

        # Новый способ расчёта силы знака с учётом теней
        def sign_strength(row):
            sign = np.sign(row["close"] - row["open"])
            lower_shadow = min(row["open"], row["close"]) - row["low"]
            upper_shadow = row["high"] - max(row["open"], row["close"])
            if lower_shadow > 0 and upper_shadow > 0:
                if lower_shadow < upper_shadow:
                    ratio = lower_shadow / upper_shadow
                    shadow_sign = 1
                else:
                    ratio = upper_shadow / lower_shadow
                    shadow_sign = -1
                return sign + shadow_sign * ratio
            else:
                return sign

        group["signed_volume"] = group.apply(
            lambda row: row["volume"] * sign_strength(row), axis=1
        )
        group["cumulative_volume"] = group["signed_volume"].cumsum()
        daily_high = group["high"].max()
        daily_low = group["low"].min()
        high_time = group[group["high"] == daily_high].index[0]
        low_time = group[group["low"] == daily_low].index[0]
        volume_at_high = group.loc[high_time, "cumulative_volume"]
        volume_at_low = group.loc[low_time, "cumulative_volume"]
        high_minutes = high_time.hour * 60 + high_time.minute
        low_minutes = low_time.hour * 60 + low_time.minute
        minutes_since_open_high = max(high_minutes - 600, 1)
        minutes_since_open_low = max(low_minutes - 600, 1)
        volume_rate_high = volume_at_high / minutes_since_open_high
        volume_rate_low = volume_at_low / minutes_since_open_low
        stats.append(
            {
                "date": day,
                "high_time_minutes": high_minutes,
                "low_time_minutes": low_minutes,
                "volume_rate_high": volume_rate_high,
                "volume_rate_low": volume_rate_low,
                "month": high_time.month,
                "weekday": high_time.weekday(),
            }
        )
    return pd.DataFrame(stats)


def get_3d_hist(
    data, value_column, time_column, n_time_bins=24, n_volume_bins=20, smooth_sigma=None
):
    # Используем log10(abs(value)), фильтруем нули и NaN
    values = data[value_column]
    mask = values != 0
    log_values = np.log10(np.abs(values[mask]))
    times = data[time_column][mask]
    # Удаляем NaN и бесконечности
    valid = (
        (~np.isnan(log_values))
        & (~np.isinf(log_values))
        & (~np.isnan(times))
        & (~np.isinf(times))
    )
    log_values = log_values[valid]
    times = times[valid]
    if len(log_values) == 0 or len(times) == 0:
        raise ValueError("No valid data for histogram (all values are zero or invalid)")
    time_bins = np.linspace(times.min(), times.max(), n_time_bins)
    volume_bins = np.linspace(log_values.min(), log_values.max(), n_volume_bins)
    H, xedges, yedges = np.histogram2d(times, log_values, bins=[time_bins, volume_bins])
    H = H / H.sum()  # вероятность
    if smooth_sigma is not None:
        H = gaussian_filter(H, sigma=smooth_sigma)
        H = H / H.sum()  # нормализация после сглаживания
    x_centers = (xedges[:-1] + xedges[1:]) / 2
    y_centers = (yedges[:-1] + yedges[1:]) / 2
    X, Y = np.meshgrid(x_centers, y_centers)
    return X, Y, H.T


def get_probability_from_surface(X, Y, Z, log_vol, minutes):
    x_idx = (np.abs(X[0] - minutes)).argmin()
    y_idx = (np.abs(Y[:, 0] - log_vol)).argmin()
    return Z[y_idx, x_idx]


def calculate_probabilities(df: pd.DataFrame, X, Y, Z) -> pd.DataFrame:
    """Calculate probabilities for each row in the DataFrame using signed volume with shadow ratio influence."""

    def get_prob(row):
        minutes = row.name.hour * 60 + row.name.minute
        # Основной знак по open-close
        sign = np.sign(row["close"] - row["open"])
        # Рассчитываем тени
        lower_shadow = min(row["open"], row["close"]) - row["low"]
        upper_shadow = row["high"] - max(row["open"], row["close"])
        # Если обе тени положительные и не равны нулю
        if lower_shadow > 0 and upper_shadow > 0:
            if lower_shadow < upper_shadow:
                ratio = lower_shadow / upper_shadow
                shadow_sign = 1
            else:
                ratio = upper_shadow / lower_shadow
                shadow_sign = -1
            # Итоговый множитель: 1+ratio или -1-ratio
            sign_strength = sign + shadow_sign * ratio
        else:
            sign_strength = sign
        vol = row["volume"] * sign_strength
        log_vol = np.log10(abs(vol)) if vol != 0 else 0
        return get_probability_from_surface(X, Y, Z, log_vol, minutes)

    result = df.apply(lambda row: get_prob(row), axis=1)
    return pd.DataFrame({"value": result}, index=df.index)
