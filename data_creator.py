from faker import Faker
import faker_commerce
import pandas as pd
import datetime
import random

fake = Faker()
fake.add_provider(faker_commerce.Provider)
date_format = "%Y-%m-%d %H:%M:%S"


def generate_start_data():
    update_date = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime(
        date_format
    )

    pd.DataFrame(
        [
            {
                "id": 1,
                "name": "Ethan Fuerst",
                "created_at": update_date,
            }
        ]
    ).to_csv("data/customers_source.csv", index=False)

    pd.DataFrame(
        [
            {
                "id": 1,
                "customer_id": 1,
                "created_at": update_date,
            }
        ]
    ).to_csv("data/orders_source.csv", index=False)

    # add payments?

    pd.DataFrame(
        [
            {
                "id": 1,
                "order_id": 1,
                "product_id": 1,
                "created_at": update_date,
            }
        ]
    ).to_csv("data/order_items_source.csv", index=False)

    pd.DataFrame(
        [
            {
                "id": 1,
                "seller_id": 1,
                "cost": round((random.random() + 0.01) * 750, 2),
                "name": fake.ecommerce_name(),
                "created_at": update_date,
            }
        ]
    ).to_csv("data/products_source.csv", index=False)

    pd.DataFrame(
        [
            {
                "id": 1,
                "name": fake.company(),
                "created_at": update_date,
            }
        ]
    ).to_csv("data/sellers_source.csv", index=False)

    pd.DataFrame(
        [
            {
                "id": 1,
                "created_at": update_date,
            }
        ]
    ).to_csv("data/updates.csv", index=False)


def get_most_recent_record(df):
    return df.sort_values("id", ascending=False).iloc[0]


def add_records():
    NUM_NEW_CUSTOMERS = 8
    NUM_NEW_ORDERS = 12
    NUM_NEW_SELLERS = 5
    NUM_NEW_PRODUCTS = 10

    update_date = datetime.datetime.strptime(
        pd.read_csv("data/updates.csv")
        .sort_values("created_at", ascending=False)
        .iloc[0]["created_at"],
        date_format,
    ) + datetime.timedelta(days=1)
    print(update_date)

    exist_customers = pd.read_csv("data/customers_source.csv")
    most_recent_customer = get_most_recent_record(exist_customers)
    mr_cust_id = most_recent_customer["id"]
    new_customers = pd.DataFrame(
        [
            {"id": x, "name": fake.name(), "created_at": update_date}
            for x in range(mr_cust_id + 1, mr_cust_id + NUM_NEW_CUSTOMERS + 1)
        ]
    )

    exist_orders = pd.read_csv("data/orders_source.csv")
    most_recent_order = get_most_recent_record(exist_orders)
    mr_order_id = most_recent_order["id"]
    # new orders from existing customers
    new_orders_exist_cust = pd.DataFrame(
        [
            {
                "id": x,
                "customer_id": random.choice(exist_customers["id"].values),
                "created_at": update_date,
            }
            for x in range(mr_order_id + 1, mr_order_id + mr_cust_id + 1)
        ]
    )
    # create new orders from new customers
    # every customer gets one order then random chance for more
    new_orders_new_cust = pd.concat(
        [
            pd.DataFrame(
                [
                    {
                        "id": order_id,
                        "customer_id": customer_id,
                        "created_at": update_date,
                    }
                    for order_id, customer_id in zip(
                        range(
                            mr_order_id + mr_cust_id + 1,
                            mr_order_id + NUM_NEW_CUSTOMERS + mr_cust_id + 2,
                        ),
                        range(mr_cust_id + 1, mr_cust_id + NUM_NEW_CUSTOMERS + 2),
                    )
                ]
            ),
            pd.DataFrame(
                [
                    {
                        "id": x,
                        "customer_id": random.choice(new_customers["id"].values),
                        "created_at": update_date,
                    }
                    for x in range(
                        mr_order_id + NUM_NEW_CUSTOMERS + mr_cust_id + 2,
                        mr_order_id
                        + NUM_NEW_CUSTOMERS
                        + mr_cust_id
                        + NUM_NEW_ORDERS
                        + 2,
                    )
                ]
            ),
        ]
    )
    new_orders = pd.concat([new_orders_exist_cust, new_orders_new_cust])

    # add payments?

    exist_sellers = pd.read_csv("data/sellers_source.csv")
    most_recent_seller = get_most_recent_record(exist_sellers)
    mr_seller_id = most_recent_seller["id"]
    new_sellers = pd.DataFrame(
        [
            {
                "id": x,
                "name": fake.company(),
                "created_at": update_date,
            }
            for x in range(mr_seller_id + 1, mr_seller_id + NUM_NEW_SELLERS + 1)
        ]
    )

    exist_products = pd.read_csv("data/products_source.csv")
    most_recent_product = get_most_recent_record(exist_products)
    mr_product_id = most_recent_product["id"]
    # create new products from new sellers
    new_prods_new_sellers = pd.DataFrame(
        [
            {
                "id": x,
                "seller_id": seller_id,
                "cost": round((random.random() + 0.01) * 750, 2),
                "name": fake.ecommerce_name(),
                "created_at": update_date,
            }
            for x, seller_id in zip(
                range(mr_product_id + 1, mr_product_id + NUM_NEW_SELLERS + 2),
                range(mr_seller_id + 1, mr_seller_id + NUM_NEW_SELLERS + 2),
            )
        ]
    )
    # create new products from existing sellers
    new_prods_exist_sellers = pd.DataFrame(
        [
            {
                "id": x,
                "seller_id": random.choice(exist_sellers["id"].values),
                "cost": round((random.random() + 0.01) * 750, 2),
                "name": fake.ecommerce_name(),
                "created_at": update_date,
            }
            for x in range(
                mr_product_id + NUM_NEW_SELLERS + 2,
                mr_product_id + NUM_NEW_SELLERS + NUM_NEW_PRODUCTS + 2,
            )
        ]
    )
    new_products = pd.concat([new_prods_new_sellers, new_prods_exist_sellers])
    # return new_products

    # new order items for new orders with new products
    most_recent_order_item = get_most_recent_record(
        pd.read_csv("data/order_items_source.csv")
    )
    mr_order_item_id = most_recent_order_item["id"]
    new_order_items_new_prods = pd.DataFrame(
        [
            {
                "id": x,
                "order_id": random.choice(new_orders["id"].values),
                "product_id": random.choice(new_products["id"].values),
                "created_at": update_date,
            }
            for x in range(mr_order_item_id + 1, mr_order_item_id + 1 + len(new_orders))
        ]
    )
    # new order items for new orders with old products
    new_order_items_old_prods = pd.DataFrame(
        [
            {
                "id": x,
                "order_id": random.choice(new_orders["id"].values),
                "product_id": random.choice(exist_products["id"].values),
                "created_at": update_date,
            }
            for x in range(
                mr_order_item_id + 1 + len(new_orders),
                mr_order_item_id + 1 + len(new_orders) + (len(new_orders) // 2),
            )
        ]
    )
    new_order_items = pd.concat([new_order_items_new_prods, new_order_items_old_prods])

    new_update_record = pd.DataFrame(
        [
            {
                "id": len(pd.read_csv("data/updates.csv")) + 1,
                "created_at": update_date,
            }
        ]
    )

    new_customers.to_csv(
        "data/customers_source.csv", index=False, mode="a", header=False
    )
    new_orders.to_csv("data/orders_source.csv", index=False, mode="a", header=False)
    new_sellers.to_csv("data/sellers_source.csv", index=False, mode="a", header=False)
    new_products.to_csv("data/products_source.csv", index=False, mode="a", header=False)
    new_order_items.to_csv(
        "data/order_items_source.csv", index=False, mode="a", header=False
    )
    new_update_record.to_csv("data/updates.csv", index=False, mode="a", header=False)


generate_start_data()
for i in range(11):
    add_records()
