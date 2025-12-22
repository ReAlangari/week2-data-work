import pandas as pd

# ============================================================================
# Exercise 1: Implement require_columns from day 2
# ============================================================================

def require_columns(df: pd.DataFrame, cols: list[str]) -> None:
    """Check that all required columns exist.

    Args:
        df: DataFrame to check
        cols: List of required column names

    Raises:
        AssertionError: If any column is missing
    """
    missing = [x for x in cols if x not in df.columns]
    assert not missing, f"Missing columns: {missing}"

# ============================================================================
# Exercise 2: Implement assert_non_empty from day 2
# ============================================================================

def assert_non_empty(df: pd.DataFrame, name: str = "df") -> None:
    """Assert that DataFrame has at least one row.

    Args:
        df: DataFrame to check
        name: Name to use in error message

    Raises:
        AssertionError: If DataFrame is empty
    """
    assert len(df) > 0, f"{name} has 0 rows"

# ============================================================================
# Exercise 3: Implement assert_unique_key from day 2
# ============================================================================

def assert_unique_key(df: pd.DataFrame, key: str, *, allow_na: bool = False) -> None:
    """Assert that a column is unique (no duplicates).

    Args:
        df: DataFrame to check
        key: Column name to check for uniqueness
        allow_na: If False, also check that no NA values exist

    Raises:
        AssertionError: If column has duplicates or NAs (when not allowed)
    """
    if not allow_na:
        assert df[key].notna().all(), f"{key} contains NA"
    
    dup = df[key].duplicated(keep=False) & df[key].notna()
    assert not dup.any(), f"{key} not unique; {dup.sum()} duplicate rows"

# ============================================================================
# Exercise 4: Implement assert_in_range from day 2
# ============================================================================

def assert_in_range(s: pd.Series, *, lo=None, hi=None, name: str = "value") -> None:
    """Assert that series values are within range (ignoring NA).

    Args:
        s: Series to check
        lo: Minimum allowed value (None = no minimum)
        hi: Maximum allowed value (None = no maximum)
        name: Name to use in error message

    Raises:
        AssertionError: If any value is outside range
    """
    x = s.dropna()
    
    if lo is not None:
        assert (x >= lo).all(), f"{name} below {lo}"
    
    if hi is not None:
        assert (x <= hi).all(), f"{name} above {hi}"