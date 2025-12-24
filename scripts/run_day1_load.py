import sys
from pathlib import Path
from datetime import datetime, timezone
import logging
import json


ROOT = Path(__file__).resolve().parents[1] 
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.transforms import enforce_schema

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")


def main() -> None:
    p = make_paths(ROOT)

    orders = read_orders_csv(p.raw / "orders.csv")
    users = read_users_csv(p.raw / "users.csv")

    orders = enforce_schema(orders)

    out_orders = p.processed / "orders.parquet"
    out_users = p.processed / "users.parquet"
    write_parquet(orders, out_orders)
    write_parquet(users, out_users)

    meta = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "rows": {"orders": int(len(orders)), "users": int(len(users))},
        "outputs": {"orders": str(out_orders), "users": str(out_users)},
    }
    meta_path = p.processed / "_run_meta_day1.json"
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    log.info("Day 1 pipeline completed successfully")
    log.info("Orders Parquet: %s", out_orders)
    log.info("Users Parquet: %s", out_users)


if __name__ == "__main__":
    main()