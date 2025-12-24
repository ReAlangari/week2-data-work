import pandas as pd
import re 

# ============================================================================
# Exercise 7: Implement enforce_schema function from day 1
# ============================================================================

def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Enforce correct dtypes on orders DataFrame.

    - Converts IDs to string
    - Converts amount to Float64 (invalid values become NA)
    - Converts quantity to Int64 (invalid values become NA)

    Args:
        df: Raw orders DataFrame

    Returns:
        DataFrame with enforced dtypes
    """    
    return df.assign(
        order_id=df["order_id"].astype("string"),
        user_id=df["user_id"].astype("string"),
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64"),
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"),
    )

# ============================================================================
# Exercise 5: Implement missingness_report form day 2
# ============================================================================

def missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    """Create a report of missing values per column.

    Returns:
        DataFrame with n_missing and p_missing columns, sorted by p_missing desc
    """
    n = len(df)
    return (
        df.isna().sum()
        .rename("n_missing")  # FIXED: Should be string "n_missing", not variable n_mssing
        .to_frame()
        .assign(p_missing=lambda t: t["n_missing"] / n)
        .sort_values("p_missing", ascending=False)  # FIXED: Should be string "p_missing"
    )

# ============================================================================
# Exercise 7: Implement normalize_text form day 2
# ============================================================================

_ws = re.compile(r"\s+")

def normalize_text(s: pd.Series) -> pd.Series:
    """Normalize text: strip, casefold, collapse whitespace.

    Args:
        s: Series of text values

    Returns:
        Normalized series
    """
    return (
         s.astype("string")
        .str.strip()
        .str.casefold()
        .str.replace(_ws, " ", regex=True)
    )

# ============================================================================
# Exercise 8: Implement apply_mapping form day 2
# ============================================================================

def apply_mapping(s: pd.Series, mapping: dict) -> pd.Series:
    """Apply value mapping, keeping unmapped values unchanged.

    Args:
        s: Series of values
        mapping: Dict mapping old values to new values

    Returns:
        Series with mapped values
    """
    return s.map(lambda x: mapping.get(x, x)) 

# ============================================================================
# Exercise 6: Implement add_missing_flags form day 2
# ============================================================================

def add_missing_flags(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Add boolean columns indicating missing values.

    Args:
        df: Input DataFrame
        cols: Columns to create flags for

    Returns:
        DataFrame with new columns like 'amount__isna' (True/False)
    """
    out = df.copy()
    for c in cols:
        out[f"{c}__isna"] = out[c].isna()
    return out

# ============================================================================
# DAY 3 
# ============================================================================

def parse_datetime(
    df: pd.DataFrame,
    col: str,
    *,
    utc: bool = True,
) -> pd.DataFrame:
    """Parse a timestamp column safely.
    
    - invalid strings become NA (errors="coerce")
    - utc=True gives timezone-aware timestamps (recommended default)
    """
    dt = pd.to_datetime(df[col], errors="coerce", utc=utc)
    return df.assign(**{col: dt})

def add_time_parts(df: pd.DataFrame, ts_col: str) -> pd.DataFrame:
    """Add common time grouping keys (month, day-of-week, hour, etc.)."""
    ts = df[ts_col]
    return df.assign(
        date=ts.dt.date,
        year=ts.dt.year,
        month=ts.dt.to_period("M").astype("string"),
        dow=ts.dt.day_name(),
        hour=ts.dt.hour,
    )


def iqr_bounds(s: pd.Series, k: float = 1.5) -> tuple[float, float]:
    """Return (lo, hi) IQR bounds for outlier flagging."""
    x = s.dropna()
    q1 = x.quantile(0.25)
    q3 = x.quantile(0.75)
    iqr = q3 - q1
    return float(q1 - k * iqr), float(q3 + k * iqr)

def winsorize(s: pd.Series, lo: float = 0.01, hi: float = 0.99) -> pd.Series:
    """Cap values to [p_lo, p_hi] (helpful for visualization, not deletion)."""
    x = s.dropna()
    a, b = x.quantile(lo), x.quantile(hi)
    return s.clip(lower=a, upper=b)


def add_outlier_flag(df: pd.DataFrame, col: str, *, k: float = 1.5) -> pd.DataFrame:
    """Add boolean column indicating outliers using IQR method.
    
    Args:
        df: Input DataFrame
        col: Column to check for outliers
        k: IQR multiplier (default 1.5)
    
    Returns:
        DataFrame with new column {col}__is_outlier
    """
    lo, hi = iqr_bounds(df[col], k=k)
    return df.assign(**{f"{col}__is_outlier": (df[col] < lo) | (df[col] > hi)})