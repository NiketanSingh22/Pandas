"""
PANDAS BASIC FUNCTIONS REVISION
This file covers:

1. Filtering
2. New Columns
3. Sorting
4. GroupBy
5. HAVING logic
6. Top N
7. Joins
8. Relationship types
9. Revenue duplication problem
10. Missing rows after join
11. Distinct count
12. Duplication check before join
"""

import pandas as pd
import numpy as np


# -----------------------------
# 1. FILTERING (SQL WHERE)
# -----------------------------

df[df["price"] > 100]

df[(df["city"] == "Pune") & (df["amount"] > 500)]

df[df["city"].isin(["Pune", "Delhi"])]

df[df["date"].between("2024-01-01", "2024-01-10")]

# Rules:
# == not =
# & for AND
# | for OR
# () required


# -----------------------------
# 2. NEW COLUMNS
# -----------------------------

df["total"] = df["price"] * df["qty"]

# CASE WHEN equivalent

df["size"] = np.where(df["amount"] > 50000, "High", "Low")

# Multiple conditions

df["type"] = np.select(
    [df["amount"] > 50000, df["amount"] > 1000],
    ["High", "Medium"],
    default="Low"
)


# -----------------------------
# 3. SORTING
# -----------------------------

df.sort_values("amount")

df.sort_values("amount", ascending=False)

df.sort_values(["city", "amount"], ascending=[True, False])


# -----------------------------
# 4. GROUPBY BASICS
# -----------------------------

df.groupby("customer")["amount"].sum()

df.groupby("city")["qty"].sum()

df.groupby(["city", "category"])["amount"].sum()

# Multiple aggregation

df.groupby("city").agg({
    "amount": "sum",
    "qty": "mean"
})


# -----------------------------
# 5. HAVING LOGIC
# -----------------------------

# SQL:
# GROUP BY + HAVING

# Pandas:
# groupby -> agg -> filter

x = df.groupby("customer")["amount"].sum()

x[x > 10000]

# Alternative

df.groupby("customer")["amount"].sum()[lambda x: x > 10000]


# -----------------------------
# 6. TOP N
# -----------------------------

df.groupby("customer")["amount"].sum() \
    .sort_values(ascending=False) \
    .head(2)

# NOT .limit()


# -----------------------------
# 7. JOINS
# -----------------------------

# Inner
pd.merge(a, b, on="id", how="inner")

# Left
pd.merge(a, b, on="id", how="left")

# Right
pd.merge(a, b, on="id", how="right")

# Outer
pd.merge(a, b, on="id", how="outer")

# Different keys
pd.merge(a, b, left_on="id1", right_on="id2")


# -----------------------------
# 8. RELATIONSHIP TYPES
# -----------------------------

"""
1-1  -> safe
N-1  -> safe
1-N  -> duplicates possible
N-N  -> explosion (danger)
"""


# -----------------------------
# 9. REVENUE DUPLICATION RULE
# -----------------------------

# orders -> payments = one to many

# WRONG
# sum after join

# CORRECT
# sum from orders table
# or deduplicate

df.drop_duplicates("order_id")


# -----------------------------
# 10. FIND MISSING ROWS AFTER JOIN
# -----------------------------

df = pd.merge(orders, payments, on="order_id", how="left")

df[df["payment_mode"].isna()]


# -----------------------------
# 11. DISTINCT COUNT
# -----------------------------

df.groupby("customer")["payment_mode"].nunique()


# -----------------------------
# 12. CHECK DUPLICATION BEFORE JOIN
# -----------------------------

df["id"].duplicated().sum()

df.groupby("id").size()