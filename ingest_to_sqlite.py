"""
Ingest synthetic e-commerce CSVs into a local SQLite database.

Reads from current directory:
- customers.csv
- products.csv
- orders.csv
- order_items.csv
- categories.csv

Creates/overwrites ecommerce.db with proper schema and constraints, then
loads all rows from the CSVs.
"""

from __future__ import annotations

import csv
import sqlite3
from pathlib import Path
from typing import Iterable, Dict, Any

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "ecommerce.db"

CSV_FILES = {
    "categories": BASE_DIR / "categories.csv",
    "products": BASE_DIR / "products.csv",
    "customers": BASE_DIR / "customers.csv",
    "orders": BASE_DIR / "orders.csv",
    "order_items": BASE_DIR / "order_items.csv",
}


def read_csv(path: Path) -> Iterable[Dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def ensure_files_exist():
    missing = [str(p) for p in CSV_FILES.values() if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Missing CSV files: {', '.join(missing)}")


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.executescript(
        """
        DROP TABLE IF EXISTS order_items;
        DROP TABLE IF EXISTS orders;
        DROP TABLE IF EXISTS products;
        DROP TABLE IF EXISTS customers;
        DROP TABLE IF EXISTS categories;

        CREATE TABLE categories (
            category_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT
        );

        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY,
            category_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            price REAL NOT NULL,
            stock INTEGER NOT NULL,
            FOREIGN KEY (category_id) REFERENCES categories(category_id)
        );

        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT NOT NULL,
            signup_date TEXT NOT NULL
        );

        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            order_date TEXT NOT NULL,
            status TEXT NOT NULL,
            total REAL NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );

        CREATE TABLE order_items (
            order_item_id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            line_total REAL NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(order_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        );
        """
    )


def insert_data(conn: sqlite3.Connection) -> None:
    categories_rows = [
        {
            "category_id": int(r["category_id"]),
            "name": r["name"],
            "description": r.get("description") or None,
        }
        for r in read_csv(CSV_FILES["categories"])
    ]

    products_rows = [
        {
            "product_id": int(r["product_id"]),
            "category_id": int(r["category_id"]),
            "name": r["name"],
            "price": float(r["price"]),
            "stock": int(r["stock"]),
        }
        for r in read_csv(CSV_FILES["products"])
    ]

    customers_rows = [
        {
            "customer_id": int(r["customer_id"]),
            "first_name": r["first_name"],
            "last_name": r["last_name"],
            "email": r["email"],
            "signup_date": r["signup_date"],
        }
        for r in read_csv(CSV_FILES["customers"])
    ]

    orders_rows = [
        {
            "order_id": int(r["order_id"]),
            "customer_id": int(r["customer_id"]),
            "order_date": r["order_date"],
            "status": r["status"],
            "total": float(r["total"]),
        }
        for r in read_csv(CSV_FILES["orders"])
    ]

    order_items_rows = [
        {
            "order_item_id": int(r["order_item_id"]),
            "order_id": int(r["order_id"]),
            "product_id": int(r["product_id"]),
            "quantity": int(r["quantity"]),
            "unit_price": float(r["unit_price"]),
            "line_total": float(r["line_total"]),
        }
        for r in read_csv(CSV_FILES["order_items"])
    ]

    with conn:
        conn.executemany(
            "INSERT INTO categories (category_id, name, description) VALUES (:category_id, :name, :description);",
            categories_rows,
        )
        conn.executemany(
            "INSERT INTO products (product_id, category_id, name, price, stock) VALUES (:product_id, :category_id, :name, :price, :stock);",
            products_rows,
        )
        conn.executemany(
            "INSERT INTO customers (customer_id, first_name, last_name, email, signup_date) VALUES (:customer_id, :first_name, :last_name, :email, :signup_date);",
            customers_rows,
        )
        conn.executemany(
            "INSERT INTO orders (order_id, customer_id, order_date, status, total) VALUES (:order_id, :customer_id, :order_date, :status, :total);",
            orders_rows,
        )
        conn.executemany(
            "INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, line_total) VALUES (:order_item_id, :order_id, :product_id, :quantity, :unit_price, :line_total);",
            order_items_rows,
        )


def main():
    ensure_files_exist()
    if DB_PATH.exists():
        DB_PATH.unlink()

    with sqlite3.connect(DB_PATH) as conn:
        init_db(conn)
        insert_data(conn)

    print(f"Success: populated {DB_PATH}")


if __name__ == "__main__":
    main()
