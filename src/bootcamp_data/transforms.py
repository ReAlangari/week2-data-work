import pandas as pd
import re  # ADD THIS IMPORT

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