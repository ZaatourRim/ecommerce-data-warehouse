import csv, random
from datetime import datetime, timedelta

NUM_ROWS = 10000
CATEGORIES = ["electronics", "fashion", "home", "books"]
STATUSES = ["pending", "shipped", "delivered", "cancelled"]

def main(today):
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2025, 11, 1)
    OUTPUT_PATH = f"data/raw/orders_{today}.csv"
    with open (OUTPUT_PATH, mode="w", newline="") as f:
        writer = csv.writer(f)
        header = [
            "order_id",
            "order_timestamp",
            "customer_id",
            "product_id",
            "category",
            "price",
            "quantity",
            "status",
        ]
        writer.writerow(header)
        total_days = (end_date - start_date).days

        for order_id in range(1, NUM_ROWS + 1):
            # order_timestamp
            offset_days = random.randint(0, total_days)
            offset_seconds = random.randint(0, 24 * 60 * 60 - 1)
            order_date = start_date + timedelta(days=offset_days, seconds=offset_seconds)

            order_timestamp = order_date.strftime("%Y-%m-%d %H:%M:%S")
            # customer_id
            customer_id = random.randint(1, 5000)
            # product_id + category
            category = random.choice(CATEGORIES)
            product_id = random.randint(1, 300)
            # price
            if category == "electronics":
                price = random.uniform(50, 1000)
            elif category == "fashion":
                price = random.uniform(10, 400)
            elif category == "home":
                price = random.uniform(40, 3000)
            else:
                price = random.uniform(6, 70)
            price = round(price, 2)
            # quantity 
            quantity = random.randint(1, 10)
            # status
            r = random.random()
            if r < 0.75:
                status = "delivered"
            elif r < 0.90:
                status = "shipped"
            elif r < 0.98:
                status = "pending"
            else:
                status = "cancelled"
            
            # build row
            row = [
                order_id,
                order_timestamp,
                customer_id,
                product_id,
                category,
                price,
                quantity,
                status,
            ]
            writer.writerow(row)
    
if __name__ == "__main__":
    main()



