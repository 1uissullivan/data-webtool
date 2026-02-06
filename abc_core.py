"""
ABC Analysis Core Functions
Implements ABC distribution and daily volume analysis.
"""

from typing import Tuple, Dict, Optional
import pandas as pd
import numpy as np


def run_abc(
    df: pd.DataFrame,
    sku_col: str,
    value_col: str,
    a_cut: float = 0.80,
    b_cut: float = 0.95,
    debug: bool = False
) -> Tuple[pd.DataFrame, Dict]:
    """
    Run ABC analysis on SKU data.

    Logic:
    - Group by SKU and COUNT all rows (lines) per SKU
    - Sort SKUs by line count (descending)
    - Calculate incremental SKU count (1, 2, 3, ...)
    - Calculate SKU % as (SKU count / Total SKUs)
    - Calculate cumulative line % as (cumulative lines / total lines)

    Important: This uses COUNT of rows, not DISTINCT COUNT.
    """

    sku_totals = df.groupby(sku_col).size().reset_index(name='Lines')
    sku_totals.rename(columns={sku_col: 'SKU'}, inplace=True)

    if debug:
        print(f"\n=== ABC Calculation Debug ===")
        print(f"Input rows: {len(df)}")
        print(f"Unique SKUs: {len(sku_totals)}")
        print(f"Total lines (sum of row counts): {sku_totals['Lines'].sum()}")

    sku_totals = sku_totals.sort_values('Lines', ascending=False).reset_index(drop=True)

    total_skus = len(sku_totals)
    total_lines = sku_totals['Lines'].sum()

    sku_totals['SKU_Count'] = np.arange(1, total_skus + 1)
    sku_totals['SKU_Pct'] = (sku_totals['SKU_Count'] / total_skus)

    sku_totals['Lines_Cum'] = sku_totals['Lines'].cumsum()
    sku_totals['Lines_Cum_Pct'] = sku_totals['Lines_Cum'] / total_lines

    sku_totals['share'] = sku_totals['Lines'] / total_lines

    sku_totals['ABC_Category'] = 'C'
    sku_totals.loc[sku_totals['Lines_Cum_Pct'] <= a_cut, 'ABC_Category'] = 'A'
    sku_totals.loc[
        (sku_totals['Lines_Cum_Pct'] > a_cut) & (sku_totals['Lines_Cum_Pct'] <= b_cut),
        'ABC_Category'
    ] = 'B'

    a_skus = (sku_totals['ABC_Category'] == 'A').sum()
    b_skus = (sku_totals['ABC_Category'] == 'B').sum()
    c_skus = (sku_totals['ABC_Category'] == 'C').sum()

    a_lines = sku_totals[sku_totals['ABC_Category'] == 'A']['Lines'].sum()
    b_lines = sku_totals[sku_totals['ABC_Category'] == 'B']['Lines'].sum()
    c_lines = sku_totals[sku_totals['ABC_Category'] == 'C']['Lines'].sum()

    summary = {
        'total_skus': total_skus,
        'total_value': total_lines,
        'A_skus': a_skus,
        'B_skus': b_skus,
        'C_skus': c_skus,
        'A_value': a_lines,
        'B_value': b_lines,
        'C_value': c_lines,
        'A_sku_share': a_skus / total_skus if total_skus > 0 else 0,
        'B_sku_share': b_skus / total_skus if total_skus > 0 else 0,
        'C_sku_share': c_skus / total_skus if total_skus > 0 else 0,
        'A_value_share': a_lines / total_lines if total_lines > 0 else 0,
        'B_value_share': b_lines / total_lines if total_lines > 0 else 0,
        'C_value_share': c_lines / total_lines if total_lines > 0 else 0,
    }

    abc_df = sku_totals[[
        'SKU', 'Lines', 'SKU_Count', 'SKU_Pct',
        'Lines_Cum', 'Lines_Cum_Pct', 'share', 'ABC_Category'
    ]].copy()

    abc_df = abc_df.rename(columns={
        'Lines_Cum_Pct': 'cum_share',
    })

    return abc_df, summary


def _parse_dates_robust(series: pd.Series) -> pd.Series:
    """
    Robust date parser with deterministic handling for DD/MM/YYYY and common variants.

    Strategy:
    1) If numeric: treat as Excel serial date (best-effort).
    2) Try DD/MM/YYYY HH:MM:SS then DD/MM/YYYY (dayfirst implied).
    3) Fallback: dayfirst=True generic parse.
    4) Final fallback: generic parse.
    """
    s = series.copy()

    # If already datetime
    if pd.api.types.is_datetime64_any_dtype(s):
        return s

    # Numeric Excel serial dates (common in exports)
    if pd.api.types.is_numeric_dtype(s):
        # Excel's day 0 is 1899-12-30 in pandas convention for origin='1899-12-30'
        dt = pd.to_datetime(s, unit='D', origin='1899-12-30', errors='coerce')
        return dt

    s_str = s.astype(str).str.strip()
    s_str = s_str.replace({"": np.nan, "None": np.nan, "nan": np.nan})

    # Explicit EU formats first (fast + deterministic)
    dt = pd.to_datetime(s_str, errors='coerce', format="%d/%m/%Y %H:%M:%S")
    if dt.isna().mean() > 0.5:
        dt2 = pd.to_datetime(s_str, errors='coerce', format="%d/%m/%Y")
        # choose better of the two
        if dt2.isna().sum() < dt.isna().sum():
            dt = dt2

    # Generic dayfirst parse (helps for "d/m/yy", "dd-mm-yyyy", etc.)
    if dt.isna().sum() > 0:
        dt3 = pd.to_datetime(s_str, errors='coerce', dayfirst=True)
        if dt3.isna().sum() < dt.isna().sum():
            dt = dt3

    # Last resort
    if dt.isna().all():
        dt = pd.to_datetime(s_str, errors='coerce')

    return dt


def run_lines_per_day(
    df: pd.DataFrame,
    date_col: str,
    qty_col: Optional[str] = None,
    percentiles: Tuple[float, ...] = (0.50, 0.80, 1.00)
) -> Tuple[pd.DataFrame, Dict]:
    """
    Calculate daily line counts and optionally item quantities.

    Key fixes:
    - Deterministic date parsing for DD/MM/YYYY inputs (prevents NaT drops and day/month swaps).
    - Normalize to date (no time component) before grouping.
    - Output Date as YYYY-MM-DD strings for UI consistency.
    """
    # Parse date once, robustly
    dt = _parse_dates_robust(df[date_col])

    # Drop invalid dates
    mask_valid = dt.notna()
    work_dt = dt[mask_valid]

    # Normalize to daily granularity to avoid time-based fragmentation
    day = work_dt.dt.normalize()

    # Lines per day (count rows)
    daily_lines = day.value_counts().sort_index()
    daily_df = daily_lines.rename_axis("Date").reset_index(name="Lines")

    # Quantity per day (optional)
    if qty_col and qty_col in df.columns:
        qty = pd.to_numeric(df.loc[mask_valid, qty_col], errors='coerce')
        qty_df = pd.DataFrame({"Date": day, "Quantity": qty}).dropna(subset=["Quantity"])
        daily_qty = qty_df.groupby("Date", as_index=False)["Quantity"].sum()
        daily_df = daily_df.merge(daily_qty, on="Date", how="left")

    # Sort
    daily_df = daily_df.sort_values("Date").reset_index(drop=True)

    # Summary
    summary: Dict = {
        'total_days': int(len(daily_df)),
        'peak_lines_value': int(daily_df['Lines'].max()) if not daily_df.empty else 0,
        'peak_lines_date': daily_df.loc[daily_df['Lines'].idxmax(), 'Date'] if not daily_df.empty else None,
    }

    if not daily_df.empty:
        for pct in percentiles:
            summary[f'lines_p{int(pct*100)}'] = float(daily_df['Lines'].quantile(pct))

    if 'Quantity' in daily_df.columns:
        q = daily_df['Quantity']
        summary['peak_qty_value'] = float(q.max()) if not daily_df.empty else 0
        summary['peak_qty_date'] = daily_df.loc[q.idxmax(), 'Date'] if not daily_df.empty else None
        if not daily_df.empty:
            for pct in percentiles:
                summary[f'qty_p{int(pct*100)}'] = float(q.quantile(pct))

    # Output Date formatted as YYYY-MM-DD strings (for tables/UI)
    daily_df["Date"] = daily_df["Date"].dt.strftime("%Y-%m-%d")

    return daily_df, summary
