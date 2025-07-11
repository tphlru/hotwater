import pandas as pd
from lightweight_charts import Chart  # type: ignore

from hotwater.data import load_configs, load_data
from hotwater.factors.fdi import FDICalculator
from hotwater.factors.r_trend_exhaustion import USD_RTrendExhaustionCalculator
# from factors.extremums import (
#     first_level_extremums,
#     next_level_extremum,
#     get_trends_info,
# )
# from probability import prepare_volume_stats, get_3d_hist, calculate_probabilities  # type: ignore


def prepare_data() -> pd.DataFrame:
    config = load_configs()[0]
    df = load_data(config)
    df = df.tail(len(df) // 4)
    df = df.set_index(pd.to_datetime(df["datetime"]))
    df = df.drop(columns=["datetime"])
    return df


def prepare_chart(df: pd.DataFrame) -> Chart:
    chart = Chart(inner_height=1, toolbox=True)
    chart.legend(visible=True)
    return chart


def find_periods_by_query(df: pd.DataFrame, query: str):
    """
    Возвращает список кортежей (start, end) для непрерывных последовательностей,
    где условие query выполняется (query — строка для DataFrame.query)
    """
    mask = df.query(query)
    mask_index = mask.index
    periods = []
    start = None
    prev_idx = None
    for idx in df.index:
        val = idx in mask_index
        if val and start is None:
            start = idx
        elif not val and start is not None:
            periods.append((start, prev_idx))
            start = None
        prev_idx = idx
    if start is not None:
        periods.append((start, prev_idx))
    return periods


def fdi_zones(fdi: pd.DataFrame, chart: Chart):
    periods1 = find_periods_by_query(fdi, "fdi_smooth > 0.1")  # 1.55
    for start, end in periods1:
        chart.vertical_span(start, end, color="rgba(255, 136, 0, 0.20)")

    periods2 = find_periods_by_query(fdi, "fdi_smooth < -0.08")
    for start, end in periods2:
        chart.vertical_span(start, end, color="rgba(0, 68, 255, 0.20)")


def r_markers(df: pd.DataFrame, chart: Chart):
    marker_count = 0
    max_markers = 50
    for idx, row in reversed(list(df.iterrows())):
        if marker_count >= max_markers:
            break
        if row.get("bull_break") is True:
            chart.marker(idx, text="bull_break", pane_index=2, shape="arrow_down")
            marker_count += 1
            if marker_count >= max_markers:
                break
        if row.get("bear_break") is True:
            chart.marker(idx, text="bear_break", pane_index=2, shape="arrow_up")
            marker_count += 1
            if marker_count >= max_markers:
                break
    print(f"Total markers placed: {marker_count}")


def main():
    df = prepare_data()
    print(df)
    chart = prepare_chart(df)
    chart.set(df)

    fdi_calc = FDICalculator(
        period=30, smooth_enable=True, smooth_type="HMA", smooth_len=14, angle_len=5
    )

    fdi = fdi_calc.calculate_historical(df)
    fdi["fdi_angle"] = fdi["fdi_angle"] / 2.5
    fdi["fdi_smooth"] = fdi["fdi_smooth"] - 1.45
    fdi["fdi"] = fdi["fdi"] - 1.45

    chart.create_histogram(
        "fdi_angle",
        color="auto",
        up_color="#00ff00",
        down_color="#ff0000",
        pane_index=1,
    ).set(fdi)
    chart.create_line("fdi", color="#00b7ff", pane_index=1).set(fdi)
    chart.create_line("fdi_smooth", color="#FFFFFF", pane_index=1).set(fdi)
    fdi_zones(fdi, chart)

    usdrte_calc = USD_RTrendExhaustionCalculator(
        fast_length=21,
        slow_length=112,
        fast_smoothing=7,
        slow_smoothing=3,
        threshold=20,
        smoothing_type="ema",
        use_average=False,
    )
    usdrte_df = usdrte_calc.calculate_historical(df)
    chart.create_line("fast_r", color="#007b8b", pane_index=2).set(usdrte_df)
    chart.create_line("slow_r", color="#a52c00", pane_index=2).set(usdrte_df)

    r_markers(usdrte_df, chart)

    chart.show(block=True)


if __name__ == "__main__":
    main()
