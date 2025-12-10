"""
Analyze top customers by total spend from the SQLite ecommerce database.
Connects to C:/Users/HP/ecommerce.db, joins customers, orders, and order_items,
computes total spend per customer, and prints a sorted table.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path("C:/Users/HP/ecommerce.db")

QUERY = """
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.email,
    COALESCE(SUM(oi.line_total), 0) AS total_spend
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.customer_id
LEFT JOIN order_items oi ON oi.order_id = o.order_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email
ORDER BY total_spend DESC, customer_name ASC;
"""


def fetch_top_customers(db_path: Path):
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found at {db_path}")
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(QUERY)
        return cur.fetchall()


def format_table(rows):
    headers = ["Customer ID", "Name", "Email", "Total Spend"]
    data = [
        [
            row["customer_id"],
            row["customer_name"],
            row["email"],
            f"${row['total_spend']:.2f}",
        ]
        for row in rows
    ]

    col_widths = [
        max(len(str(item)) for item in [header] + [r[i] for r in data])
        for i, header in enumerate(headers)
    ]

    def fmt_row(items):
        return " | ".join(str(item).ljust(col_widths[i]) for i, item in enumerate(items))

    divider = "-+-".join("-" * w for w in col_widths)
    lines = [fmt_row(headers), divider]
    for row in data:
        lines.append(fmt_row(row))
    return "\n".join(lines)


def main():
    rows = fetch_top_customers(DB_PATH)
    if not rows:
        print("No data found.")
        return
    table = format_table(rows)
    print("Top Customers by Total Spend:\n")
    print(table)


if __name__ == "__main__":
    main()
