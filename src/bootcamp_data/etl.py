from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import pandas as pd
import json
import logging

from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.quality import (
    require_columns,
    assert_non_empty,
    assert_unique_key,
)
from bootcamp_data.transforms import (
    enforce_schema,
    add_missing_flags,
    normalize_text,
    apply_mapping,
    parse_datetime,
    add_time_parts,
    winsorize,
    add_outlier_flag,
)
from bootcamp_data.joins import safe_left_join

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ETLConfig:
    root: Path
    raw_orders: Path
    raw_users: Path
    out_orders_clean: Path
    out_users: Path
    out_analytics: Path
    run_meta: Path



def load_inputs(cfg: ETLConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    orders_raw = read_orders_csv(cfg.raw_orders)
    users_raw = read_users_csv(cfg.raw_users)
    return orders_raw, users_raw


def transform(orders_raw: pd.DataFrame, users: pd.DataFrame) -> pd.DataFrame:
    require_columns(orders_raw, ["order_id", "user_id", "amount", "quantity", "created_at", "status"])
    require_columns(users, ["user_id", "country", "signup_date"])
    assert_non_empty(orders_raw, "orders_raw")
    assert_non_empty(users, "users")
    assert_unique_key(users, "user_id")

    status_map = {"paid": "paid", "refund": "refund", "refunded": "refund"}

    orders = (
        orders_raw.pipe(enforce_schema)
        .assign(
            status_clean=lambda d: apply_mapping(normalize_text(d["status"]), status_map)
        )
        .pipe(add_missing_flags, cols=["amount", "quantity"])
        .pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
    )

    analytics = safe_left_join(orders, users, on="user_id", validate="many_to_one")

    analytics["amount_winsor"] = winsorize(analytics["amount"])
    analytics = add_outlier_flag(analytics, "amount")

    return analytics



def load_outputs(*, analytics: pd.DataFrame, users: pd.DataFrame, cfg: ETLConfig) -> None:
    write_parquet(users, cfg.out_users)
    write_parquet(analytics, cfg.out_analytics)

    user_side_cols = [c for c in users.columns if c != "user_id"]
    cols_to_drop = [c for c in user_side_cols if c in analytics.columns] + [
        c for c in analytics.columns if c.endswith("_user")
    ]
    orders_clean = analytics.drop(columns=cols_to_drop, errors="ignore")
    write_parquet(orders_clean, cfg.out_orders_clean)


def write_run_meta(cfg: ETLConfig, *, orders_raw: pd.DataFrame, users: pd.DataFrame, analytics: pd.DataFrame) -> None:
    missing_created_at = int(analytics["created_at"].isna().sum()) if "created_at" in analytics.columns else None
    country_match_rate = 1.0 - float(analytics["country"].isna().mean()) if "country" in analytics.columns else None

    meta = {
        "rows_in_orders_raw": int(len(orders_raw)),
        "rows_in_users": int(len(users)),
        "rows_in_analytics": int(len(analytics)),
        "missing_created_at": missing_created_at,
        "country_match_rate": country_match_rate,
        "paths": {
            "raw_orders": str(cfg.raw_orders),
            "raw_users": str(cfg.raw_users),
            "orders_clean": str(cfg.out_orders_clean),
            "users": str(cfg.out_users),
            "analytics": str(cfg.out_analytics),
            "run_meta": str(cfg.run_meta),
        },
    }

    with open(cfg.run_meta, "w") as f:
        json.dump(meta, f, indent=2)



def run_etl(cfg: ETLConfig) -> None:
    log.info("Loading raw inputs...")
    orders_raw, users_raw = load_inputs(cfg)

    log.info("Transforming data...")
    analytics = transform(orders_raw, users_raw)

    log.info("Writing outputs...")
    load_outputs(analytics=analytics, users=users_raw, cfg=cfg)

    log.info("Writing run metadata...")
    write_run_meta(cfg, orders_raw=orders_raw, users=users_raw, analytics=analytics)

    log.info("ETL run complete.")