from pathlib import Path
import sys
import json
from datetime import datetime, timezone


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet

def main():
    p = make_paths(ROOT)
    
    orders = read_orders_csv(p.raw / "orders.csv")
    users = read_users_csv(p.raw / "users.csv")
    
    write_parquet(orders, p.processed / "orders.parquet")
    write_parquet(users, p.processed / "users.parquet")
    


if __name__ == "__main__":
    main()