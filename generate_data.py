"""
Generate synthetic e-commerce CSV datasets with consistent relationships.

Creates:
- customers.csv
- products.csv
- orders.csv
- order_items.csv
- categories.csv

Each file gets 20 rows with IDs that link correctly across tables.
"""

from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path


OUTPUT_DIR = Path('.')
ROW_COUNT = 20
RANDOM_SEED = 42


def generate_categories():
    categories = []
    for i in range(1, ROW_COUNT + 1):
        categories.append(
            {
                'category_id': i,
                'name': f'Category {i}',
                'description': f'Description for category {i}',
            }
        )
    return categories


def generate_products(categories):
    products = []
    for i in range(1, ROW_COUNT + 1):
        category_id = categories[(i - 1) % len(categories)]['category_id']
        price = round(random.uniform(10, 200), 2)
        products.append(
            {
                'product_id': i,
                'category_id': category_id,
                'name': f'Product {i}',
                'price': price,
                'stock': random.randint(5, 50),
            }
        )
    return products


def generate_customers():
    customers = []
    for i in range(1, ROW_COUNT + 1):
        customers.append(
            {
                'customer_id': i,
                'first_name': f'First{i}',
                'last_name': f'Last{i}',
                'email': f'user{i}@example.com',
                'signup_date': (datetime(2023, 1, 1) + timedelta(days=i)).date(),
            }
        )
    return customers


def generate_orders(customers):
    orders = []
    base_date = datetime(2023, 2, 1)
    for i in range(1, ROW_COUNT + 1):
        customer_id = random.choice(customers)['customer_id']
        orders.append(
            {
                'order_id': i,
                'customer_id': customer_id,
                'order_date': (base_date + timedelta(days=i)).date(),
                'status': random.choice(['pending', 'shipped', 'delivered']),
                'total': 0,
            }
        )
    return orders


def generate_order_items(orders, products):
    order_items = []
    for order in orders:
        product = random.choice(products)
        quantity = random.randint(1, 3)
        line_total = round(product['price'] * quantity, 2)
        order_items.append(
            {
                'order_item_id': len(order_items) + 1,
                'order_id': order['order_id'],
                'product_id': product['product_id'],
                'quantity': quantity,
                'unit_price': product['price'],
                'line_total': line_total,
            }
        )
        order['total'] = line_total
    return order_items


def write_csv(filename, fieldnames, rows):
    path = OUTPUT_DIR / filename
    with path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    random.seed(RANDOM_SEED)

    categories = generate_categories()
    products = generate_products(categories)
    customers = generate_customers()
    orders = generate_orders(customers)
    order_items = generate_order_items(orders, products)

    write_csv(
        'categories.csv',
        ['category_id', 'name', 'description'],
        categories,
    )
    write_csv(
        'products.csv',
        ['product_id', 'category_id', 'name', 'price', 'stock'],
        products,
    )
    write_csv(
        'customers.csv',
        ['customer_id', 'first_name', 'last_name', 'email', 'signup_date'],
        customers,
    )
    write_csv(
        'orders.csv',
        ['order_id', 'customer_id', 'order_date', 'status', 'total'],
        orders,
    )
    write_csv(
        'order_items.csv',
        ['order_item_id', 'order_id', 'product_id', 'quantity', 'unit_price', 'line_total'],
        order_items,
    )

    print('CSV files generated in', OUTPUT_DIR.resolve())


if __name__ == '__main__':
    main()
