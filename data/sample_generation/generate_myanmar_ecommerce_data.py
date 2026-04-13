from __future__ import annotations

import argparse
import csv
import random
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.config import get_settings
from src.utils.date_utils import date_range
from src.utils.logger import get_logger


logger = get_logger(__name__)
fake = Faker()
random.seed(42)
Faker.seed(42)

REGION_CITY_MAP = {
    "Yangon Region": "Yangon",
    "Mandalay Region": "Mandalay",
    "Naypyidaw Union Territory": "Naypyidaw",
    "Shan State": "Taunggyi",
    "Mon State": "Mawlamyine",
    "Bago Region": "Bago",
    "Ayeyarwady Region": "Pathein",
}

CATEGORIES = {
    "Electronics": ["Android Phone", "Power Bank", "Bluetooth Earbuds", "Rice Cooker"],
    "Fashion": ["Longyi", "Office Shirt", "Women Handbag", "Sneakers"],
    "Home": ["LED Bulb", "Water Purifier", "Standing Fan", "Blanket Set"],
    "Beauty": ["Thanaka Pack", "Face Wash", "Shampoo", "Body Lotion"],
    "Groceries": ["Premium Rice", "Cooking Oil", "Instant Noodles", "Coffee Mix"],
}

PAYMENT_METHODS = ["cash_on_delivery", "KBZPay", "Wave Pay", "AYA Pay", "bank_transfer"]
DELIVERY_PROVIDERS = ["Royal Express", "Myanmar Deli", "Grab Local Partner", "City Rider Network"]
SELLER_TYPES = ["marketplace_seller", "brand_store", "wholesale_partner"]


@dataclass
class EntityCounts:
    customers: int
    sellers: int
    products: int
    orders: int


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_csv(path: Path, rows: list[dict]) -> None:
    ensure_directory(path.parent)
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as file_handle:
        writer = csv.DictWriter(file_handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def generate_customers(count: int) -> list[dict]:
    customers: list[dict] = []
    regions = list(REGION_CITY_MAP.keys())
    for index in range(1, count + 1):
        region = random.choice(regions)
        city = REGION_CITY_MAP[region]
        customers.append(
            {
                "customer_id": f"CUST{index:07d}",
                "customer_name": fake.name(),
                "phone_number": f"09{random.randint(100000000, 999999999)}",
                "email": fake.email(),
                "city": city,
                "region": region,
                "township": f"{city} Township",
                "segment": random.choice(["new", "returning", "vip"]),
                "created_at": fake.date_time_between(start_date="-2y", end_date="now").isoformat(),
            }
        )
    return customers


def generate_sellers(count: int) -> list[dict]:
    sellers: list[dict] = []
    regions = list(REGION_CITY_MAP.keys())
    for index in range(1, count + 1):
        region = random.choice(regions)
        sellers.append(
            {
                "seller_id": f"SELL{index:05d}",
                "seller_name": f"{fake.company()} Myanmar Store",
                "city": REGION_CITY_MAP[region],
                "region": region,
                "seller_type": random.choice(SELLER_TYPES),
                "rating": round(random.uniform(3.5, 5.0), 2),
                "created_at": fake.date_time_between(start_date="-3y", end_date="-30d").isoformat(),
            }
        )
    return sellers


def generate_products(count: int, sellers: list[dict]) -> list[dict]:
    products: list[dict] = []
    seller_ids = [seller["seller_id"] for seller in sellers]
    for index in range(1, count + 1):
        category = random.choice(list(CATEGORIES.keys()))
        products.append(
            {
                "product_id": f"PROD{index:06d}",
                "seller_id": random.choice(seller_ids),
                "product_name": random.choice(CATEGORIES[category]),
                "category": category,
                "brand": random.choice(["Shwe", "Ayar", "Moe", "Golden Lotus", "Mingalar"]),
                "unit_price_mmk": random.randrange(2500, 350000, 500),
                "is_active": random.choice([True, True, True, False]),
                "created_at": fake.date_time_between(start_date="-2y", end_date="-7d").isoformat(),
            }
        )
    return products


def generate_orders_for_day(
    batch_date: str,
    counts: EntityCounts,
    customers: list[dict],
    products: list[dict],
) -> tuple[list[dict], list[dict], list[dict], list[dict], list[dict]]:
    customer_lookup = {item["customer_id"]: item for item in customers}
    customer_ids = list(customer_lookup.keys())
    product_lookup = {item["product_id"]: item for item in products}
    product_ids = list(product_lookup.keys())

    orders: list[dict] = []
    order_items: list[dict] = []
    payments: list[dict] = []
    deliveries: list[dict] = []
    inventory_snapshots: list[dict] = []

    day_start = datetime.strptime(batch_date, "%Y-%m-%d")

    for inv_index, product in enumerate(products, start=1):
        available = random.randint(0, 300)
        status = "out_of_stock" if available == 0 else ("low_stock" if available < 25 else "in_stock")
        inventory_snapshots.append(
            {
                "inventory_snapshot_id": f"INV{batch_date.replace('-', '')}{inv_index:06d}",
                "product_id": product["product_id"],
                "seller_id": product["seller_id"],
                "snapshot_date": batch_date,
                "available_quantity": available,
                "reserved_quantity": random.randint(0, 20),
                "inventory_status": status,
                "reorder_threshold": random.choice([10, 15, 20, 30]),
            }
        )

    for order_index in range(1, counts.orders + 1):
        order_id = f"ORD{batch_date.replace('-', '')}{order_index:07d}"
        customer_id = random.choice(customer_ids)
        customer = customer_lookup[customer_id]
        order_timestamp = day_start + timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )
        status = random.choices(
            ["placed", "confirmed", "shipped", "delivered", "cancelled"],
            weights=[5, 10, 15, 60, 10],
            k=1,
        )[0]
        item_count = random.randint(1, 4)
        selected_products = random.sample(product_ids, item_count)
        subtotal = 0
        total_quantity = 0

        for line_number, product_id in enumerate(selected_products, start=1):
            unit_price = int(product_lookup[product_id]["unit_price_mmk"])
            quantity = random.randint(1, 3)
            gross_line_amount = unit_price * quantity
            discount_amount = int(gross_line_amount * random.choice([0, 0.03, 0.05, 0.1]))
            net_line_amount = gross_line_amount - discount_amount
            order_items.append(
                {
                    "order_item_id": f"ITEM{uuid.uuid4().hex[:16].upper()}",
                    "order_id": order_id,
                    "product_id": product_id,
                    "seller_id": product_lookup[product_id]["seller_id"],
                    "line_number": line_number,
                    "quantity": quantity,
                    "unit_price_mmk": unit_price,
                    "gross_line_amount_mmk": gross_line_amount,
                    "discount_amount_mmk": discount_amount,
                    "net_line_amount_mmk": net_line_amount,
                }
            )
            subtotal += net_line_amount
            total_quantity += quantity

        shipping_fee = random.choice([1500, 2000, 2500, 3000, 3500])
        order_total = subtotal + shipping_fee
        payment_method = random.choice(PAYMENT_METHODS)

        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_timestamp": order_timestamp.isoformat(),
                "order_status": status,
                "city": customer["city"],
                "region": customer["region"],
                "payment_method": payment_method,
                "shipping_fee_mmk": shipping_fee,
                "subtotal_amount_mmk": subtotal,
                "order_total_mmk": order_total,
                "currency": "MMK",
                "item_count": item_count,
                "total_quantity": total_quantity,
                "batch_date": batch_date,
            }
        )

        payment_amount = order_total if status != "cancelled" else 1
        payments.append(
            {
                "payment_id": f"PAY{uuid.uuid4().hex[:14].upper()}",
                "order_id": order_id,
                "customer_id": customer_id,
                "payment_method": payment_method,
                "payment_status": "captured" if status != "cancelled" else "refunded",
                "payment_amount_mmk": payment_amount,
                "paid_at": (order_timestamp + timedelta(minutes=random.randint(1, 180))).isoformat(),
                "batch_date": batch_date,
            }
        )

        expected_days = random.randint(1, 6)
        actual_days = expected_days + random.choice([-1, 0, 0, 1, 2])
        deliveries.append(
            {
                "delivery_id": f"DLV{uuid.uuid4().hex[:14].upper()}",
                "order_id": order_id,
                "delivery_provider": random.choice(DELIVERY_PROVIDERS),
                "region": customer["region"],
                "city": customer["city"],
                "promised_delivery_date": (day_start + timedelta(days=expected_days)).date().isoformat(),
                "actual_delivery_date": (day_start + timedelta(days=max(actual_days, 0))).date().isoformat(),
                "delivery_status": "delivered" if status == "delivered" else random.choice(["pending", "in_transit", "failed_attempt"]),
                "delivery_fee_mmk": shipping_fee,
                "batch_date": batch_date,
            }
        )

    return orders, order_items, payments, deliveries, inventory_snapshots


def generate_dataset(batch_start_date: str, days: int, row_scale: int) -> None:
    settings = get_settings()
    counts = EntityCounts(
        customers=max(2000, row_scale // 2),
        sellers=max(120, row_scale // 40),
        products=max(800, row_scale // 4),
        orders=row_scale,
    )

    customers = generate_customers(counts.customers)
    sellers = generate_sellers(counts.sellers)
    products = generate_products(counts.products, sellers)

    write_csv(settings.raw_data_dir / "master" / "customers.csv", customers)
    write_csv(settings.raw_data_dir / "master" / "sellers.csv", sellers)
    write_csv(settings.raw_data_dir / "master" / "products.csv", products)

    for batch_date in date_range(batch_start_date, days):
        batch_dir = settings.raw_data_dir / "batches" / f"batch_date={batch_date}"
        orders, order_items, payments, deliveries, inventory_snapshots = generate_orders_for_day(
            batch_date=batch_date,
            counts=counts,
            customers=customers,
            products=products,
        )
        write_csv(batch_dir / "orders.csv", orders)
        write_csv(batch_dir / "order_items.csv", order_items)
        write_csv(batch_dir / "payments.csv", payments)
        write_csv(batch_dir / "deliveries.csv", deliveries)
        write_csv(batch_dir / "inventory_snapshots.csv", inventory_snapshots)
        logger.info("Generated synthetic raw batch for %s with %s orders", batch_date, len(orders))


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Myanmar e-commerce synthetic datasets.")
    parser.add_argument("--batch-start-date", default=get_settings().default_batch_date)
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--row-scale", type=int, default=get_settings().default_row_scale)
    args = parser.parse_args()
    generate_dataset(args.batch_start_date, args.days, args.row_scale)


if __name__ == "__main__":
    main()
