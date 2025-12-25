from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path


import re
@dataclass(frozen=True)
class ETLConfig:
    root: Path
    raw_orders: Path
    raw_users: Path
    out_orders_clean: Path
    out_users: Path
    out_analytics: Path
    run_meta: Path
import pandas as pd
from bootcamp_data.io import read_orders_csv, read_users_csv

def load_inputs(cfg: ETLConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    orders = read_orders_csv(cfg.raw_orders)
    users = read_users_csv(cfg.raw_users)
    return orders, users


def require_columns(df: pd.DataFrame, columns: list[str]) -> None:
    """Check that required columns exist."""
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

def assert_non_empty(df: pd.DataFrame, name: str) -> None:
    """Check that DataFrame is not empty."""
    if len(df) == 0:
        raise ValueError(f"{name} is empty")

def assert_unique_key(df: pd.DataFrame, column: str) -> None:
    """Check that column values are unique."""
    if df[column].duplicated().any():
        raise ValueError(f"Column {column} has duplicate values")

def safe_left_join(left: pd.DataFrame, right: pd.DataFrame, on: str, validate: str = None) -> pd.DataFrame:
    """Perform a left join with optional validation."""
    return pd.merge(left, right, on=on, how="left", validate=validate)
def transform(orders_raw: pd.DataFrame, users: pd.DataFrame) -> pd.DataFrame:
    """Transform raw orders and users into analytics table."""
   
    require_columns(orders_raw, ["order_id", "user_id", "amount", "quantity", "created_at", "status"])
    require_columns(users, ["user_id", "country", "signup_date"])
    assert_non_empty(orders_raw, "orders_raw")
    assert_non_empty(users, "users")
    assert_unique_key(users, "user_id")
    

    status_map = {"paid": "paid", "refund": "refund", "refunded": "refund"}
    
    orders = (
        orders_raw
        .pipe(enforce_schema) 
        .assign(
            status_clean=lambda d: apply_mapping(
                normalize_text(d["status"]),  
                status_map
            )
        )
        .pipe(add_missing_flags, cols=["amount", "quantity"])  
    )
    
 
    users_clean = users.assign(user_id=users["user_id"].astype("string"))
    
    
    orders = (
        orders
        .pipe(parse_datetime, col="created_at", utc=True) 
        .pipe(add_time_parts, ts_col="created_at") 
    )
    
 
    analytics = safe_left_join(
        orders,
        users_clean,
        on="user_id",
        validate="many_to_one"
    )
    
    analytics = analytics.assign(
        amount_winsor=winsorize(analytics["amount"]) 
    )
    analytics = add_outlier_flag(analytics, "amount", k=1.5)  
    

    return analytics


def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Enforce correct dtypes on orders DataFrame."""
    return df.assign(
        order_id=df["order_id"].astype("string"),
        user_id=df["user_id"].astype("string"),
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64"),
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"),
    )

def normalize_text(s: pd.Series) -> pd.Series:
    """Normalize text: strip, casefold, collapse whitespace."""
    _ws = re.compile(r"\s+")
    return (
        s.astype("string")
        .str.strip()
        .str.casefold()
        .str.replace(_ws, " ", regex=True)
    )

def apply_mapping(s: pd.Series, mapping: dict) -> pd.Series:
    """Apply value mapping, keeping unmapped values unchanged."""
    return s.map(lambda x: mapping.get(x, x))

def add_missing_flags(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Add boolean columns indicating missing values."""
    out = df.copy()
    for c in cols:
        out[f"{c}__isna"] = out[c].isna()
    return out

def parse_datetime(df: pd.DataFrame, col: str, *, utc: bool = True) -> pd.DataFrame:
    """Parse a timestamp column safely."""
    dt = pd.to_datetime(df[col], errors="coerce", utc=utc)
    return df.assign(**{col: dt})

def add_time_parts(df: pd.DataFrame, ts_col: str) -> pd.DataFrame:
    """Add common time grouping keys."""
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
    """Cap values to [p_lo, p_hi]."""
    x = s.dropna()
    a, b = x.quantile(lo), x.quantile(hi)
    return s.clip(lower=a, upper=b)

def add_outlier_flag(df: pd.DataFrame, col: str, *, k: float = 1.5) -> pd.DataFrame:
    """Add boolean column indicating outliers using IQR method."""
    lo, hi = iqr_bounds(df[col], k=k)
    return df.assign(**{f"{col}__is_outlier": (df[col] < lo) | (df[col] > hi)})

def load_outputs(*, analytics: pd.DataFrame, users: pd.DataFrame, cfg: ETLConfig) -> None:
    """Write processed artifacts to data/processed/."""
  
    analytics.to_parquet(cfg.out_analytics, index=False)
    users.to_parquet(cfg.out_users, index=False)
    
    
    user_side_cols = [c for c in users.columns if c != "user_id"]
    cols_to_drop = [c for c in user_side_cols if c in analytics.columns]
    orders_clean = analytics.drop(columns=cols_to_drop, errors="ignore")
    orders_clean.to_parquet(cfg.out_orders_clean, index=False)
def write_run_meta(cfg: ETLConfig, orders_raw, users, analytics) -> None:
    """Write run metadata JSON."""
    import json
    
    meta = {
        "rows_in_orders_raw": len(orders_raw),
        "rows_in_users": len(users),
        "rows_out_analytics": len(analytics),
        "missing_created_at": int(analytics["created_at"].isna().sum()) if "created_at" in analytics.columns else 0,
        "country_match_rate": round(1.0 - float(analytics["country"].isna().mean()), 3) if "country" in analytics.columns else 0.0,
        "config": {
            "raw_orders": str(cfg.raw_orders),
            "raw_users": str(cfg.raw_users),
            "out_analytics": str(cfg.out_analytics)
        }
    }
    
    with open(cfg.run_meta, 'w') as f:
        json.dump(meta, f, indent=2)

def run_etl(cfg: ETLConfig) -> None:
    """Orchestrate the full ETL pipeline."""
    import logging
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    
    log.info("Starting ETL...")
    
    # 1. Extract
    orders, users = load_inputs(cfg)
    log.info(f"Loaded {len(orders)} orders, {len(users)} users")
    
    # 2. Transform
    analytics = transform(orders, users)
    log.info(f"Transformed to {len(analytics)} analytics rows")
    
    # 3. Load
    load_outputs(analytics=analytics, users=users, cfg=cfg)
    write_run_meta(cfg, orders_raw=orders, users=users, analytics=analytics)
    
    log.info("ETL completed successfully")