# Import all necessary libraries
import requests                     # For making HTTP requests (Shopify & Razorpay APIs)
import pandas as pd                 # For data manipulation and DataFrames
import mysql.connector              # For connecting to MySQL database
from datetime import datetime, timedelta, timezone
import json                         # For working with JSON

def get_mysql_connection():
    return mysql.connector.connect(
        host='holistique-middleware.c9wdjmzy25ra.ap-south-1.rds.amazonaws.com',
        user='Chandan',
        password='Chandan@#4321',
        database='Holistique'
    )

def fetch_automate_credentials(brand=None):
    conn = get_mysql_connection()
    cursor = conn.cursor(dictionary=True)
    if brand:
        cursor.execute("SELECT * FROM automate_credentials WHERE brand=%s LIMIT 1", (brand,))
    else:
        cursor.execute("SELECT * FROM automate_credentials LIMIT 1")
    creds = cursor.fetchone()
    cursor.close()
    conn.close()
    if not creds:
        raise Exception(f"No credentials found in automate_credentials table for brand '{brand}'!")
    return creds

def to_shopify_iso(dt_str):
    # dt_str like "2025-07-01 00:00:00" (assume UTC for first run)
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")

# 2. Fetch latest 60 Shopify orders using the GraphQL API
def fetch_shopify_orders_paginated(SHOPIFY_GRAPHQL_URL, SHOPIFY_ACCESS_TOKEN, from_dt, to_dt, after_cursor=None, max_records=300):
    orders = []
    fetched_count = 0
    cursor = after_cursor
    latest_created_at = from_dt
    has_next_page = True

    while has_next_page and fetched_count < max_records:
        query = """
        query ($search: String, $after: String) {
        orders(first: 30, sortKey: CREATED_AT, reverse: false, query: $search, after: $after) {
            pageInfo { hasNextPage endCursor }
            edges { node {
                id
                name
                createdAt
                sourceName
                paymentGatewayNames
                totalShippingPriceSet { shopMoney { amount } }
                currentTotalPriceSet { presentmentMoney { amount } }
                transactions(first: 50) { id kind status gateway paymentId }
                customer { id displayName email phone }
                billingAddress { name phone address1 address2 city province zip country }
                shippingAddress { name phone address1 address2 city province zip country latitude longitude }
                lineItems(first: 200) {
                edges { node {
                    name sku variant { id } currentQuantity
                    originalTotalSet { presentmentMoney { amount } }
                    originalUnitPriceSet { presentmentMoney { amount } }
                    discountAllocations { allocatedAmountSet { shopMoney { amount } } }
                    totalDiscountSet { presentmentMoney { amount } }
                    product { onlineStoreUrl variants(first: 50) { edges { node { id title compareAtPrice } } } }
                    taxLines { title priceSet { presentmentMoney { amount } } }
                }}
                }
                discountApplications(first: 80) {
                edges { node {
                    __typename
                    ... on DiscountCodeApplication { code }
                    ... on ManualDiscountApplication { title }
                    ... on AutomaticDiscountApplication { title }
                    ... on ScriptDiscountApplication { title }
                }}
                }
            }}
        }
        }
        """

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN
        }
        from_iso = to_shopify_iso(from_dt)
        to_iso   = to_shopify_iso(to_dt)

        variables = {
            "search": f"status:any created_at:>={from_iso} created_at:<{to_iso}",
            "after": cursor
        }
        response = requests.post(SHOPIFY_GRAPHQL_URL, headers=headers, json={"query": query, "variables": variables})
        if response.status_code != 200:
            raise Exception(f"Shopify API error: {response.text}")
        data = response.json()

        shopify_orders = data.get("data", {}).get("orders", {})
        edges = shopify_orders.get("edges", [])
        if not edges:
            print("No Shopify edges returned. Debug:")
            print(" search =", variables["search"])
            print(" pageInfo =", shopify_orders.get("pageInfo"))
            print(json.dumps(data, indent=2)[:1000])
            break
        for edge in edges:
            node = edge["node"]
            txs = node.get("transactions", []) or []
            primary_tx = next(
                (t for t in txs if str(t.get("status", "")).lower() in ("success", "succeeded", "captured", "completed")),
                (txs[0] if txs else None)
            )
            transaction_gid = primary_tx.get("id") if primary_tx else None
            payment_id = primary_tx.get("paymentId") if primary_tx else None
            shipping_pincode = node.get("shippingAddress", {}).get("zip") if node.get("shippingAddress") else None
            orders.append({
                "order_name": node.get("name"),
                "order_id_gid": node.get("id"),
                "created_at": node.get("createdAt", "").replace("T", " ").replace("Z", ""),
                "shipping_pincode": shipping_pincode,
                "source_name": node.get("sourceName"),
                "status": None,  
                "shipping_amount": float(node.get("totalShippingPriceSet", {}).get("shopMoney", {}).get("amount", 0)),
                "total_amount": float(node.get("currentTotalPriceSet", {}).get("presentmentMoney", {}).get("amount", 0)),
                "payment_gateway_names": json.dumps(node.get("paymentGatewayNames", [])),
                "customer_json": json.dumps(node.get("customer", {})),
                "billing_address_json": json.dumps(node.get("billingAddress", {})),
                "shipping_address_json": json.dumps(node.get("shippingAddress", {})),
                "transactions_json": json.dumps(txs),
                "transaction_gid": transaction_gid,
                "payment_id": payment_id,
                "lineitems_json": json.dumps([item['node'] for item in node.get("lineItems", {}).get("edges", [])]),
                "discountapplications_json": json.dumps([item['node'] for item in node.get("discountApplications", {}).get("edges", [])]),
                "brand": None
            })
        fetched_count += len(edges)
        page_info = shopify_orders.get("pageInfo", {})
        has_next_page = page_info.get("hasNextPage", False)
        cursor = page_info.get("endCursor")
        latest_created_at = edges[-1]['node']['createdAt'] if edges else latest_created_at
        if fetched_count >= max_records:
            break

    return pd.DataFrame(orders), latest_created_at, cursor

# 3. Fetch Razorpay payments 
def fetch_razorpay_payments_incremental(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET, from_unix, to_unix, max_records=300):
    url = "https://api.razorpay.com/v1/payments"
    payments = []
    count = 100
    skip = 0
    fetched = 0
    latest_created_at = from_unix

    while fetched < max_records:
        params = {
            "from": from_unix,
            "to": to_unix,
            "count": count,
            "skip": skip
        }
        resp = requests.get(url, params=params, auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET))
        if resp.status_code != 200:
            raise Exception(f"Razorpay API error: {resp.text}")
        data = resp.json()
        batch = data.get("items", [])
        if not batch:
            break
        payments.extend(batch)
        fetched += len(batch)
        batch_times = [p.get("created_at", from_unix) for p in batch]
        if batch_times:
            latest_created_at = max(latest_created_at, max(batch_times))
        if len(batch) < count or fetched >= max_records:
            break
        skip += count
    return pd.json_normalize(payments), latest_created_at

# 3b. Flatten Razorpay DataFrame using flat keys
def flatten_razorpay_df(df):
    new_cols = {
        'error_code': lambda r: r.get('error_code'),
        'error_description': lambda r: r.get('error_description'),
        'error_source': lambda r: r.get('error_source'),
        'error_step': lambda r: r.get('error_step'),
        'error_reason': lambda r: r.get('error_reason'),

        # All notes.* fields are already flat
        'notes.cancelUrl': lambda r: r.get('notes.cancelUrl'),
        'notes.domain': lambda r: r.get('notes.domain'),
        'notes.domains_list': lambda r: r.get('notes.domains_list'),
        'notes.gid': lambda r: r.get('notes.gid'),
        'notes.is_magic_x': lambda r: r.get('notes.is_magic_x'),
        'notes.mode': lambda r: r.get('notes.mode'),
        'notes.referer_url': lambda r: r.get('notes.referer_url'),
        'notes.shopify_order_id': lambda r: r.get('notes.shopify_order_id'),
        'notes.type': lambda r: r.get('notes.type'),

        # acquirer_data.* fields are already flat
        'acquirer_data.rrn': lambda r: r.get('acquirer_data.rrn'),
        'acquirer_data.upi_transaction_id': lambda r: r.get('acquirer_data.upi_transaction_id'),
        'acquirer_data.bank_transaction_id': lambda r: r.get('acquirer_data.bank_transaction_id'),
        'acquirer_data.auth_code': lambda r: r.get('acquirer_data.auth_code'),
        'acquirer_data.arn': lambda r: r.get('acquirer_data.arn'),
        'acquirer_data.transaction_id': lambda r: r.get('acquirer_data.transaction_id'),
        'acquirer_data.authentication_reference_number': lambda r: r.get('acquirer_data.authentication_reference_number'),

        # upi.* fields
        'upi.payer_account_type': lambda r: r.get('upi.payer_account_type'),
        'upi.vpa': lambda r: r.get('upi.vpa'),

        # card.* fields
        'card.id': lambda r: r.get('card.id'),
        'card.entity': lambda r: r.get('card.entity'),
        'card.name': lambda r: r.get('card.name'),
        'card.last4': lambda r: r.get('card.last4'),
        'card.network': lambda r: r.get('card.network'),
        'card.type': lambda r: r.get('card.type'),
        'card.issuer': lambda r: r.get('card.issuer'),
        'card.international': lambda r: r.get('card.international'),
        'card.emi': lambda r: r.get('card.emi'),
        'card.sub_type': lambda r: r.get('card.sub_type'),
        'card.token_iin': lambda r: r.get('card.token_iin'),

        'token_id': lambda r: r.get('token_id'),
    }
    # Add/replace columns by applying each lambda function
    for col, func in new_cols.items():
        df[col] = df.apply(func, axis=1)
    return df

# 4. Create necessary MySQL tables if they don't exist already
def create_tables():
    # SQL statement to create Shopify table
    create_shopify = """
    CREATE TABLE IF NOT EXISTS shopify_orders (
        id INT AUTO_INCREMENT PRIMARY KEY,
        order_name VARCHAR(50),
        order_id_gid VARCHAR(100),
        created_at DATETIME,
        shipping_pincode VARCHAR(20),
        source_name VARCHAR(100),
        status VARCHAR(50),
        shipping_amount DECIMAL(12,2),
        total_amount DECIMAL(12,2),
        payment_gateway_names JSON,
        customer_json JSON,
        billing_address_json JSON,
        shipping_address_json JSON,
        transactions_json JSON,
        transaction_gid VARCHAR(100),
        payment_id VARCHAR(100), 
        lineitems_json JSON,
        discountapplications_json JSON,
        brand VARCHAR(50) 
    );
    """
    # SQL statement to create Razorpay table
    create_razorpay = """
    CREATE TABLE IF NOT EXISTS razorpay_payments (
        id VARCHAR(50) PRIMARY KEY,
        entity VARCHAR(50),
        amount DECIMAL(12,2),
        currency VARCHAR(10),
        status VARCHAR(30),
        order_id VARCHAR(50),
        invoice_id VARCHAR(50),
        international BOOLEAN,
        method VARCHAR(20),
        amount_refunded DECIMAL(12,2),
        refund_status VARCHAR(30),
        captured BOOLEAN,
        description TEXT,
        card_id VARCHAR(50),
        bank VARCHAR(50),
        wallet VARCHAR(30),
        vpa VARCHAR(100),
        email VARCHAR(100),
        contact VARCHAR(50),
        fee DECIMAL(12,2),
        tax DECIMAL(12,2),
        error_code VARCHAR(50),
        error_description TEXT,
        error_source VARCHAR(50),
        error_step VARCHAR(50),
        error_reason VARCHAR(100),
        created_at DATETIME,
        `notes.cancelUrl` TEXT,
        `notes.domain` VARCHAR(100),
        `notes.domains_list` TEXT,
        `notes.gid` VARCHAR(100),
        `notes.is_magic_x` BOOLEAN,
        `notes.mode` VARCHAR(50),
        `notes.referer_url` TEXT,
        `notes.shopify_order_id` VARCHAR(100),
        `notes.type` VARCHAR(50),
        `acquirer_data.rrn` VARCHAR(100),
        `acquirer_data.upi_transaction_id` VARCHAR(100),
        `upi.payer_account_type` VARCHAR(50),
        `upi.vpa` VARCHAR(100),
        `acquirer_data.bank_transaction_id` VARCHAR(100),
        `card.id` VARCHAR(100),
        `card.entity` VARCHAR(100),
        `card.name` VARCHAR(100),
        `card.last4` VARCHAR(10),
        `card.network` VARCHAR(50),
        `card.type` VARCHAR(50),
        `card.issuer` VARCHAR(50),
        `card.international` BOOLEAN,
        `card.emi` BOOLEAN,
        `card.sub_type` VARCHAR(50),
        `acquirer_data.auth_code` VARCHAR(100),
        `acquirer_data.arn` VARCHAR(100),
        `acquirer_data.transaction_id` VARCHAR(100),
        token_id VARCHAR(100),
        `card.token_iin` VARCHAR(20),
        `acquirer_data.authentication_reference_number` VARCHAR(100),
        brand VARCHAR(50)
    );
    """
    # Connect to MySQL and create tables
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute(create_shopify)
    cursor.execute(create_razorpay)
    conn.commit()
    cursor.close()
    conn.close()

# 5. Upload Shopify orders DataFrame to MySQL
def upload_shopify(df):
    conn = get_mysql_connection()
    cursor = conn.cursor()
    insert_query = """
    INSERT INTO shopify_orders
    (order_name, order_id_gid, created_at, shipping_pincode, source_name, status, shipping_amount, total_amount,
     payment_gateway_names, customer_json, billing_address_json, shipping_address_json,
     transactions_json, transaction_gid, payment_id, lineitems_json, discountapplications_json, brand)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      order_name=VALUES(order_name),
      status=VALUES(status),
      transaction_gid=VALUES(transaction_gid),
      payment_id=VALUES(payment_id),
      brand=VALUES(brand)
    """
    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            row["order_name"], row["order_id_gid"], row["created_at"], row["shipping_pincode"], row["source_name"],
            row["status"], row["shipping_amount"], row["total_amount"], row["payment_gateway_names"],
            row["customer_json"], row["billing_address_json"], row["shipping_address_json"],
            row["transactions_json"], row["transaction_gid"], row["payment_id"],
            row["lineitems_json"], row["discountapplications_json"], row["brand"]
        ))
    conn.commit()
    cursor.close()
    conn.close()

# 6. Upload Razorpay payments DataFrame to MySQL 
def upload_razorpay(df):
    df = df.where(pd.notnull(df), None)
    conn = get_mysql_connection()
    cursor = conn.cursor()
    insert_query = """
    REPLACE INTO razorpay_payments
    (id, entity, amount, currency, status, order_id, invoice_id, international, method,
    amount_refunded, refund_status, captured, description, card_id, bank, wallet, vpa,
    email, contact, fee, tax, error_code, error_description, error_source, error_step, error_reason, created_at,
    `notes.cancelUrl`, `notes.domain`, `notes.domains_list`, `notes.gid`, `notes.is_magic_x`, `notes.mode`, `notes.referer_url`, `notes.shopify_order_id`, `notes.type`,
    `acquirer_data.rrn`, `acquirer_data.upi_transaction_id`, `upi.payer_account_type`, `upi.vpa`, `acquirer_data.bank_transaction_id`,
    `card.id`, `card.entity`, `card.name`, `card.last4`, `card.network`, `card.type`, `card.issuer`, `card.international`, `card.emi`, `card.sub_type`,
    `acquirer_data.auth_code`, `acquirer_data.arn`, `acquirer_data.transaction_id`, token_id, `card.token_iin`, `acquirer_data.authentication_reference_number`, brand)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s)
    """
    for _, row in df.iterrows():
        row_dict = row.to_dict()
        for k, v in row_dict.items():
            if isinstance(v, float) and pd.isna(v):
                row_dict[k] = None
        values = (
            row_dict.get("id"), row_dict.get("entity"), row_dict.get("amount", 0)/100 if row_dict.get("amount") is not None else None, row_dict.get("currency"),
            row_dict.get("status"), row_dict.get("order_id"), row_dict.get("invoice_id"), int(bool(row_dict.get("international"))) if row_dict.get("international") is not None else None,
            row_dict.get("method"), row_dict.get("amount_refunded", 0)/100 if row_dict.get("amount_refunded") is not None else None,
            row_dict.get("refund_status"), int(bool(row_dict.get("captured"))) if row_dict.get("captured") is not None else None,
            row_dict.get("description"), row_dict.get("card_id"), row_dict.get("bank"), row_dict.get("wallet"), row_dict.get("vpa"),
            row_dict.get("email"), row_dict.get("contact"), row_dict.get("fee", 0)/100 if row_dict.get("fee") is not None else None, row_dict.get("tax", 0)/100 if row_dict.get("tax") is not None else None,
            row_dict.get("error_code"), row_dict.get("error_description"), row_dict.get("error_source"), row_dict.get("error_step"), row_dict.get("error_reason"),
            datetime.fromtimestamp(row_dict.get("created_at", 0)) if row_dict.get("created_at") else None,
            row_dict.get("notes.cancelUrl"), row_dict.get("notes.domain"), row_dict.get("notes.domains_list"), row_dict.get("notes.gid"), row_dict.get("notes.is_magic_x"),
            row_dict.get("notes.mode"), row_dict.get("notes.referer_url"), row_dict.get("notes.shopify_order_id"), row_dict.get("notes.type"),
            row_dict.get("acquirer_data.rrn"), row_dict.get("acquirer_data.upi_transaction_id"),
            row_dict.get("upi.payer_account_type"), row_dict.get("upi.vpa"), row_dict.get("acquirer_data.bank_transaction_id"),
            row_dict.get("card.id"), row_dict.get("card.entity"), row_dict.get("card.name"), row_dict.get("card.last4"),
            row_dict.get("card.network"), row_dict.get("card.type"), row_dict.get("card.issuer"), row_dict.get("card.international"),
            row_dict.get("card.emi"), 
            row_dict.get("card.sub_type"),
            row_dict.get("acquirer_data.auth_code"), row_dict.get("acquirer_data.arn"), row_dict.get("acquirer_data.transaction_id"),
            row_dict.get("token_id"), row_dict.get("card.token_iin"), row_dict.get("acquirer_data.authentication_reference_number"), row["brand"]
        )
        if insert_query.count('%s') != len(values):
            print("ERROR: Number of placeholders and values do not match!")
            print(values)
            raise Exception("Mismatch between placeholders and values")
        cursor.execute(insert_query, values)
    conn.commit()
    cursor.close()
    conn.close()

def create_etl_state_table():
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS etl_state (
        id INT AUTO_INCREMENT PRIMARY KEY,
        source VARCHAR(30),
        brand VARCHAR(50),
        last_fetched_at DATETIME,
        last_cursor VARCHAR(255),
        last_fetched_unix BIGINT,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uq_etl_state (source, brand)
    );
    """)
    conn.commit()
    cursor.close()
    conn.close()

def get_etl_state(source, brand):
    conn = get_mysql_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM etl_state WHERE source=%s AND brand=%s", (source, brand))
    state = cursor.fetchone()
    cursor.close()
    conn.close()
    return state

def set_etl_state(source, brand, last_fetched_at=None, last_cursor=None, last_fetched_unix=None):
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO etl_state (source, brand, last_fetched_at, last_cursor, last_fetched_unix)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          last_fetched_at=VALUES(last_fetched_at),
          last_cursor=VALUES(last_cursor),
          last_fetched_unix=VALUES(last_fetched_unix)
    """, (source, brand, last_fetched_at, last_cursor, last_fetched_unix))
    conn.commit()
    cursor.close()
    conn.close()

# =========================== MAIN SCRIPT ===========================
if __name__ == "__main__":
    create_tables()
    create_etl_state_table()  
    Brands = ["TFS"]
    for brand in Brands:
        print(f"\n===== Processing for {brand} =====")
        creds = fetch_automate_credentials(brand)
        SHOPIFY_GRAPHQL_URL = creds['shopify_graphql_url']
        SHOPIFY_ACCESS_TOKEN = creds['shopify_access_token']
        RAZORPAY_KEY_ID = creds['razorpay_key_id']
        RAZORPAY_KEY_SECRET = creds['razorpay_key_secret']

        # Shopify
        shopify_state = get_etl_state('shopify', brand)
        if not shopify_state:
            set_etl_state('shopify', brand, last_fetched_at="2025-07-01T00:00:00Z", last_cursor=None)
            shopify_state = get_etl_state('shopify', brand)
        from_dt = (shopify_state['last_fetched_at'].strftime("%Y-%m-%d %H:%M:%S")
                if shopify_state and shopify_state.get('last_fetched_at')
                else "2025-07-01 00:00:00")
        after_cursor = shopify_state['last_cursor'] if shopify_state else None
        to_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        print("Fetching Shopify orders...")
        shopify_df, latest_created_at, latest_cursor = fetch_shopify_orders_paginated(
            SHOPIFY_GRAPHQL_URL, SHOPIFY_ACCESS_TOKEN, from_dt, to_dt, after_cursor=after_cursor, max_records=300
        )
        shopify_df["brand"] = brand
        print(f"Fetched {len(shopify_df)} Shopify records.")

        if not shopify_df.empty:
            print("Uploading Shopify data to MySQL...")
            upload_shopify(shopify_df)
            set_etl_state('shopify', brand, last_fetched_at=latest_created_at, last_cursor=latest_cursor)
        else:
            print("No new Shopify records.")

        # Razorpay 
        razorpay_state = get_etl_state('razorpay', brand)
        from_unix = (razorpay_state['last_fetched_unix']
                     if razorpay_state and razorpay_state.get('last_fetched_unix')
                     else int(datetime(2025,7,1).timestamp()))
        to_unix = int(datetime.now(timezone.utc).timestamp())
        print("Fetching Razorpay payments...")
        razorpay_df, latest_created_unix = fetch_razorpay_payments_incremental(
            RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET, from_unix, to_unix, max_records=300
        )
        print(f"Fetched {len(razorpay_df)} Razorpay payments.")

        if not razorpay_df.empty:
            print("Flattening Razorpay data for DB upload...")
            razorpay_df = flatten_razorpay_df(razorpay_df)
            razorpay_df["brand"] = brand
            print("Uploading Razorpay data to MySQL...")
            upload_razorpay(razorpay_df)
            set_etl_state('razorpay', brand, last_fetched_unix=latest_created_unix)
        else:
            print("No new Razorpay records.")

        print(f"All data uploaded for {brand}!\n")
