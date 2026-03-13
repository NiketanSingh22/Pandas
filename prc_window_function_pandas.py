prc_window_function_pandas


import pandas as pd

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

Q1 — Top 2 orders per customer by amount (window function)

Expected:
    orders.sort_values(
    ["customer_id", "amount"],
    ascending=[True, False]
).groupby("customer_id").head(2)

Q2 — Running total of amount per customer ordered by date
SUM(amount)
OVER(
PARTITION BY customer
ORDER BY date
)

orders.sort_values("date")\
      .groupby("customer_id")["amount"]\
      .cumsum()
(window + sort + groupby + cumsum)


Q3 - Find customers who used more than one payment mode
df = orders.merge(payments, on="order_id", how="inner")
x = df.groupby("customer_id")["mode"].nunique()
x[x > 1]

Q4 Find orders where amount > average amount of that customer
select order_id
(select order_id ,avg() over(partition by customer_id order by amount)as average
from orders
) as t
where amount > average

df["amount_avg"] = df.groupby("customer_id")["amount"].transform(mean)
df[df["amount"] > df["amount_avg"]]

Q3 - Find 2nd highest order per customer
df["rnk"] = df.groupby("customer_id")["amount"].rank(method = "dense" , ascending = False)
df[df["rnk"] == 2]

Q4 - Find customers whose last order amount is highest

df1 = orders.sort_values(
    ["customer_id", "date"],
    ascending=[True, False]
)

df1["rn"] = df1.groupby("customer_id").cumcount() + 1

last_orders = df1[df1["rn"] == 1]

result = last_orders[
    last_orders["amount"] == last_orders["amount"].max()
]

result

Q5 - Find gap between current order amount and previous order amount per customer
orders["lag"] = (
    orders.sort_values(
        ["customer_id", "date"],
        ascending=[True, True]
    )
    .groupby("customer_id")["amount"]
    .shift(1)
)

orders["diff"] = orders["amount"] - orders["lag"]

Q6 - Find customers who ordered on consecutive days
orders["prev_date"] = (
    orders.sort_values(["customer_id", "date"])
          .groupby("customer_id")["date"]
          .shift(1)
)

orders["diff"] = (
    orders["date"] - orders["prev_date"]
).dt.days

result = orders[orders["diff"] == 1]

result

Q7 - Find percentage contribution of each order in its customer total
select amount / (select sum() over(partition by customer_id) from orders)
from orders 
group by customer_id


Q8 - Find the second highest order amount for each customer using pandas window logic.

Conditions:

Must return full row

Must work per customer

Do not modify dataset

orders["rnk"] = (
    orders
    .groupby("customer_id")["amount"]
    .rank(method="dense", ascending=False)
)

orders[orders["rnk"] == 2]


Q9 - For each customer, find percentage of each order amount out of customer's total amount.

orders["sum"] = orders.groupby("customer_id")["amount"].transform("sum")
orders["result"] = order["amount"] / orders["sum"]


Q10 - Find customers whose latest order amount is greater than their first order amount.
orders = orders.sort_values(["customer_id", "date"])

orders["first_amt"] = (
    orders.groupby("customer_id")["amount"]
    .transform("first")
)

orders["last_amt"] = (
    orders.groupby("customer_id")["amount"]
    .transform("last")
)

orders.loc[
    orders["last_amt"] > orders["first_amt"],
    "customer_id"
].drop_duplicates()

Q11-Find orders where running total per customer exceeds previous running total by more than 50000.
orders = orders.sort_values(["customer_id" ,"date"])
orders["running_total"] = orders.groupby("customer_id")["amount"].cumsum()
orders["prev_running_total"] = orders.groupby("customer_id")["running_total"].shift(1)
orders["diff"] = (orders["running_total"] - orders["prev_running_total"])
orders[orders["diff"] > 50000]

Q13 - Find customers whose 3 order moving average is greater than their overall average.

orders = orders.sort_values(["customer_id", "date"])

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

orders.loc[
    orders["moving_avg_3"] > orders["overall_avg"],
    "customer_id"
].drop_duplicates()
