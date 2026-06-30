import sys
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import loader


def load_customer_data():
    try:
        print('Started processing customer records')
        # Call the API
        user_url = 'https://dummyjson.com/users'
        c_response = requests.get(user_url).json()

        customer_df = pd.json_normalize(c_response['users'])
        

        # filter record
        customer_df = customer_df[['id', 'firstName',
                'lastName', 'age', 'gender', 'email', 'phone', 'username', 'company.address.city',
                'company.address.state', 'company.address.stateCode', 'company.address.country']]
        
        # Rename Columns
        col_names = ['id', 'first_name', 'last_name', 'age', 'gender', 'email',
                    'phone', 'username', 'city', 'state', 'state_code', 'country']
        customer_df.columns = col_names

        customer_df['loading_datetime'] = datetime.now()

        print(customer_df.head(2))

        loader.write_to_db(data_df=customer_df, table_name='customer_details_raw'
                        ,mode='replace',schema='bronze_layer')
        print('Successfully saved customer record to DB')

    except Exception as err:
        print(f'Failed loading customer record with Error : {err}')
        raise




def load_cart_sales():
    try:
        print('Started processing cart records')
        # Call the API
        url = 'https://dummyjson.com/carts'
        response = requests.get(url).json()

        # Normalize nested structure into flat DataFrame
        df = pd.json_normalize(response['carts'])

        product_df = pd.json_normalize(
            response['carts'],
            record_path='products',
            meta=['id', 'userId', 'total'],
            meta_prefix='cart_'
        )
        product_df['loading_datetime'] = datetime.now()

        product_df.drop('thumbnail', axis=1, inplace=True)

        loader.write_to_db(data_df=product_df, table_name='carts_record_raw'
                        ,mode='replace',schema='bronze_layer')
        print('Successfully saved cart record to DB')
    except Exception as err:
        print(f'Failed loading cart record with Error : {err}')
        raise


def load_bronze():
    """Load already flattened bronze tables"""

    customers_query = "SELECT * FROM bronze_layer.customer_details_raw"
    carts_query = "SELECT * FROM bronze_layer.carts_record_raw"

    customers_df = loader.read_df(query=customers_query)
    carts_df = loader.read_df(carts_query)

    return customers_df, carts_df


def transform_to_silver():
    customers_df, carts_df = load_bronze()
    customers_clean = customers_df.copy()

    customers_clean = customers_clean.drop_duplicates(subset=["id"])

    customers_clean = customers_clean.rename(columns={
        "id": "userId"})


    customers_clean["first_name"] = customers_clean["first_name"].fillna("")
    customers_clean["last_name"] = customers_clean["last_name"].fillna("")

    print(customers_clean.head(3))

    carts_clean = carts_df.copy()

    carts_clean = carts_clean.drop_duplicates()

    carts_clean = carts_clean.rename(columns={"cart_id": "cart_id",
        "cart_userId": "userId",
        "cart_total": "cart_total",
        "id": "product_id",
        "title": "product_name",
        "price": "product_price",
        "quantity": "product_quantity",
        "total": "product_total"})

    # Handle nulls
    carts_clean = carts_clean.dropna(subset=["userId", "product_name"])

    # Fix datatypes
    numeric_cols = ["product_price", "product_quantity", "product_total", "cart_total"]
    for col in numeric_cols:
        if col in carts_clean.columns:
            carts_clean[col] = pd.to_numeric(carts_clean[col], errors="coerce")


    silver_df = carts_clean.merge(customers_clean,on="userId",how="left")

    silver_df["customer_name"] = (silver_df["first_name"] + " " + silver_df["last_name"]).str.strip()


    silver_df["unit_price"] = (silver_df["product_total"] /
        silver_df["product_quantity"].replace(0, pd.NA))

    silver_df = silver_df.drop_duplicates()

    silver_df = silver_df[["cart_id", "userId", "customer_name",
            "product_id", "product_name",
            "product_quantity", "product_price", "product_total",
            "cart_total","email", "phone", "city", "state", "country"]]
    
    loader.write_to_db(data_df=silver_df,table_name='cart_user_enriched'
                       ,mode='replace',schema='silver_layer')
    print('done')

def load_gold_data():
    try:
        print("Started processing gold data")

        query = """
        SELECT *
        FROM silver_layer.cart_user_enriched
        """

        df = loader.read_df(query=query)

        print("Successfully loaded silver data")

        # Ensure numeric columns
        numeric_cols = [
            "product_quantity",
            "product_price",
            "product_total",
            "cart_total"
        ]

        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Remove invalid rows
        df = df.dropna(subset=["product_total"])

        # ====================================
        # Gold Table 1 - Sales by Country
        # ====================================

        sales_country = (
            df.groupby("country", as_index=False)
              .agg(
                  total_sales=("product_total", "sum"),
                  total_quantity=("product_quantity", "sum"),
                  total_orders=("cart_id", "nunique")
              )
              .sort_values("total_sales", ascending=False)
        )

        loader.write_to_db(
            data_df=sales_country,
            table_name="sales_by_country",
            mode="replace",
            schema="gold_layer"
        )

        # ====================================
        # Gold Table 2 - Sales by Product
        # ====================================

        sales_product = (
            df.groupby(["product_id", "product_name"], as_index=False)
              .agg(
                  total_sales=("product_total", "sum"),
                  quantity_sold=("product_quantity", "sum"),
                  total_orders=("cart_id", "nunique")
              )
              .sort_values("total_sales", ascending=False)
        )

        loader.write_to_db(
            data_df=sales_product,
            table_name="sales_by_product",
            mode="replace",
            schema="gold_layer"
        )

        # ====================================
        # Gold Table 3 - Top Customers
        # ====================================

        customer_sales = (
            df.groupby(
                ["userId", "customer_name", "country"],
                as_index=False
            )
            .agg(
                total_spent=("product_total", "sum"),
                total_orders=("cart_id", "nunique")
            )
            .sort_values("total_spent", ascending=False)
        )

        loader.write_to_db(
            data_df=customer_sales,
            table_name="customer_sales_summary",
            mode="replace",
            schema="gold_layer"
        )

        # ====================================
        # Gold Table 4 - State Summary
        # ====================================

        state_summary = (
            df.groupby(["country", "state"], as_index=False)
              .agg(
                  customers=("userId", "nunique"),
                  sales=("product_total", "sum")
              )
              .sort_values("sales", ascending=False)
        )

        loader.write_to_db(
            data_df=state_summary,
            table_name="state_sales_summary",
            mode="replace",
            schema="gold_layer"
        )

        print("Successfully processed gold layer")

    except Exception as err:
        print(f"Failed processing gold layer with Error: {err}")
        raise

if __name__ == '__main__':
    run_module = sys.argv[1]
    if run_module == 'cust':
        load_customer_data()
    elif run_module == 'cart':
        load_cart_sales()
    elif run_module == 'enrich':
        transform_to_silver()
    elif run_module == 'summary':
        load_gold_data()
