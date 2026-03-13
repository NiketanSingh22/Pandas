"""
Pandas Window Function Practice
Clean version for GitHub
"""

import pandas as pd


# -------------------------
# DATA
# -------------------------

orders = pd.DataFrame({
    "order_id": [1,2,3,4,5,6,7,8,9,10],
    "customer_id": [101,102,101,103,104,102,101,103,104,101],
    "product": ["Laptop","Mouse","Keyboard","Laptop","Mouse",
                "Keyboard","Mouse","Laptop","Mouse","Laptop"],
    "category": ["Electronics","Accessories","Accessories","Electronics",
                 "Accessories","Accessories","Accessories","Electronics",
                 "Accessories","Electronics"],
    "amount": [70000,500,1500,65000,450,1600,600,62000,550,72000],
    "date": pd.to_datetime([
        "2024-01-01","2024-01-02","2024-01-03","2024-01-04","2024-01-05",
        "2024-01-06","2024-01-07","2024-01-08","2024-01-09","2024-01-10"
    ])
})

customers = pd.DataFrame({
    "customer_id": [101,102,103,104,105],
    "name": ["Rahul","Amit","Neha","Riya","Karan"],
    "city": ["Pune","Mumbai","Delhi","Pune","Delhi"]
})

payments = pd.DataFrame({
    "order_id": [1,1,2,4,6,7,10],
    "mode": ["UPI","Card","Cash","UPI","Card","UPI","UPI"]
})


# -------------------------
# Q1 Top 2 orders per customer
# -------------------------

q1 = (
    orders
    .sort_values(["customer_id", "amount"], ascending=[True, False])
    .groupby("customer_id")
    .head(2)
)


# -------------------------
# Q2 Running total per customer
# -------------------------

q2 = (
    orders
    .sort_values("date")
    .groupby("customer_id")["amount"]
    .cumsum()
)


# -------------------------
# Q3 Customers using >1 payment mode
# -------------------------

df = orders.merge(payments, on="order_id", how="inner")

x = df.groupby("customer_id")["mode"].nunique()

q3 = x[x > 1]


# -------------------------
# Q4 amount > avg per customer
# -------------------------

orders["amount_avg"] = (
    orders.groupby("customer_id")["amount"]
    .transform("mean")
)

q4 = orders[orders["amount"] > orders["amount_avg"]]


# -------------------------
# Q5 2nd highest per customer
# -------------------------

orders["rnk"] = (
    orders.groupby("customer_id")["amount"]
    .rank(method="dense", ascending=False)
)

q5 = orders[orders["rnk"] == 2]


# -------------------------
# Q6 last order highest
# -------------------------

df1 = orders.sort_values(
    ["customer_id", "date"],
    ascending=[True, False]
)

df1["rn"] = df1.groupby("customer_id").cumcount() + 1

last_orders = df1[df1["rn"] == 1]

q6 = last_orders[
    last_orders["amount"] == last_orders["amount"].max()
]


# -------------------------
# Q7 lag difference
# -------------------------

orders = orders.sort_values(
    ["customer_id", "date"]
)

orders["lag"] = (
    orders.groupby("customer_id")["amount"]
    .shift(1)
)

orders["diff"] = orders["amount"] - orders["lag"]


# -------------------------
# Q8 consecutive days
# -------------------------

orders["prev_date"] = (
    orders.groupby("customer_id")["date"]
    .shift(1)
)

orders["date_diff"] = (
    orders["date"] - orders["prev_date"]
).dt.days

q8 = orders[orders["date_diff"] == 1]


# -------------------------
# Q9 percentage of total
# -------------------------

orders["sum_amt"] = (
    orders.groupby("customer_id")["amount"]
    .transform("sum")
)

orders["pct"] = (
    orders["amount"] /
    orders["sum_amt"]
)


# -------------------------
# Q10 latest > first
# -------------------------

orders = orders.sort_values(
    ["customer_id", "date"]
)

orders["first_amt"] = (
    orders.groupby("customer_id")["amount"]
    .transform("first")
)

orders["last_amt"] = (
    orders.groupby("customer_id")["amount"]
    .transform("last")
)

q10 = (
    orders.loc[
        orders["last_amt"] > orders["first_amt"],
        "customer_id"
    ]
    .drop_duplicates()
)


# -------------------------
# Q11 running diff > 50000
# -------------------------

orders["running_total"] = (
    orders.groupby("customer_id")["amount"]
    .cumsum()
)

orders["prev_running"] = (
    orders.groupby("customer_id")["running_total"]
    .shift(1)
)

orders["run_diff"] = (
    orders["running_total"] -
    orders["prev_running"]
)

q11 = orders[orders["run_diff"] > 50000]


# -------------------------
# Q12 moving avg > overall avg
# -------------------------

orders["moving_avg_3"] = (
    orders.groupby("customer_id")["amount"]
    .rolling(3)
    .mean()
    .reset_index(level=0, drop=True)
)

orders["overall_avg"] = (
    orders.groupby("customer_id")["amount"]
    .transform("mean")
)

q12 = (
    orders.loc[
        orders["moving_avg_3"] >
        orders["overall_avg"],
        "customer_id"
    ]
    .drop_duplicates()
)


print("All queries executed")