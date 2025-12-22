from pathlib import Path
import sys
import logging

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.quality import require_columns, assert_non_empty, assert_in_range
from bootcamp_data.transforms import enforce_schema, missingness_report, normalize_text, apply_mapping, add_missing_flags

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
log = logging.getLogger(__name__)

def main():
    p = make_paths(ROOT)
    
    log.info("Loading raw inputs")
    orders_raw = read_orders_csv(p.raw / "orders.csv")
    users = read_users_csv(p.raw / "users.csv")
    
    log.info(f"Rows: orders_raw={len(orders_raw)}, users={len(users)}")
    
    # Basic checks
    require_columns(orders_raw, ["order_id", "user_id", "amount", "quantity", "created_at", "status"])
    require_columns(users, ["user_id", "country", "signup_date"])
    assert_non_empty(orders_raw, "orders_raw")
    assert_non_empty(users, "users")
    
    # Enforce schema
    orders = enforce_schema(orders_raw)
    
    # Create and save missingness report
    rep = missingness_report(orders)
    reports_dir = ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    rep_path = reports_dir / "missingness_orders.csv"
    rep.to_csv(rep_path, index=True)
    log.info(f"Wrote missingness report: {rep_path}")
    
    # Text normalization
    status_norm = normalize_text(orders["status"])
    mapping = {"paid": "paid", "refund": "refund", "refunded": "refund"}
    status_clean = apply_mapping(status_norm, mapping)
    
    # Add missing flags and status_clean column
    orders_clean = orders.copy()
    orders_clean["status_clean"] = status_clean
    orders_clean = add_missing_flags(orders_clean, ["amount", "quantity"])
    
    # Add validation checks
    assert_in_range(orders_clean["amount"], lo=0, name="amount")
    assert_in_range(orders_clean["quantity"], lo=0, name="quantity")
    

    p.processed.mkdir(parents=True, exist_ok=True)
    

    write_parquet(orders_clean, p.processed / "orders_clean.parquet")
    write_parquet(users, p.processed / "users.parquet")
    log.info(f"Wrote processed outputs to: {p.processed}")

if __name__ == "__main__":
    main()