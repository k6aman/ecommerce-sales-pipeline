# Databricks notebook source
display(dbutils.fs.ls("/Volumes/workspace/ecommerce/raw_files"))

# Reasoning: dbutils.fs.ls lists files in a path. If you see the 4 CSVs, Spark can read them.

# COMMAND ----------

# Cell 1: Imports and settings

from pyspark.sql import functions as F

CATALOG      = "workspace"
SCHEMA       = "ecommerce"
RAW_PATH     = f"/Volumes/{CATALOG}/{SCHEMA}/raw_files"
FILE_NAME    = "E-commerce_sales_data_01-10-2026.csv"
FILE_DATE    = "2026-10-01"                                   # the day this file belongs to
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.sales_clean_2026_10_01"

# Reasoning: Put all names and paths in variables at the top. Then you change one line instead of hunting through the notebook

# COMMAND ----------

 # Cell 2: Read the raw CSV

raw_df = (spark.read
          .option("header", True)          # first row = column names
          .option("inferSchema", False)    # read EVERYTHING as string on purpose
          .csv(f"{RAW_PATH}/{FILE_NAME}"))

raw_df.printSchema()
print("Rows:", raw_df.count())
display(raw_df.limit(20))

# COMMAND ----------

### Cell 3: Missing values per column

# One column first

# raw_df.filter(F.col("qty").isNull()).count()

#Read it left to right:
#- raw_df: your data
#- .filter(...): keep only the rows that match the condition
#- F.col("qty").isNull(): the condition is "qty is empty"
#- .count(): count how many rows are left


# Every column
for c in raw_df.columns:
    missing = raw_df.filter(F.col(c).isNull()).count()
    print(c, "→", missing)

#Read it left to right:

#- for c in raw_df.columns: goes through each column name, one by one.
#- The middle line counts the empty rows for that column.
#- print shows the column name and its count.

# Reasoning: Loop through each column, keep only the rows where that column is null, and count them. This shows how many values are missing in each column.


# COMMAND ----------

# Cell 4: Placeholder text that's really "missing"

for c in raw_df.columns:
    fake = raw_df.filter(F.trim(F.col(c)).isin("N/A", "NA", "null", "NULL")).count()
    print(c, "→", fake)

# Reading it left to right

#- for c in raw_df.columns: goes through each column, one by one.
#- F.col(c) takes the values in that column.
#- F.trim(...) removes extra spaces from the start and end, so " N/A " becomes "N/A". Without it, values with spaces would slip through.
#- .isin("N/A", "NA", "null", "NULL") asks "is the value one of these words?" and gets yes or no.
#- .filter(...) keeps only the "yes" rows.
#- .count() counts them.
#- print(c, "→", fake) shows the result.

# COMMAND ----------

# Cell 5: Exact duplicate rows

total = raw_df.count()
distinct = raw_df.dropDuplicates().count()
print(f"Total rows: {total} | Distinct rows: {distinct} | Duplicates: {total - distinct}")

# COMMAND ----------

# Cell 6: What's inside qty?

display(raw_df.groupBy("qty").count().orderBy(F.desc("count")))

# Reading it left to right

#- raw_df.groupBy("qty") puts rows with the same qty value into one group. All the 1s go together, all the 2s go together, and so on.
#- .count() counts how many rows are in each group.
#- .orderBy(F.desc("count")) sorts the result with the biggest count at the top. desc means descending, from high to low.
#- display(...) shows the result as a table.

# COMMAND ----------

# Cell 7 A: Date formats

display(raw_df.groupBy(F.substring("order_dt", 1, 10).alias("date_part")).count())

# Reading it left to right:
#- F.substring("order_dt", 1, 10) takes only the first 10 characters, starting at character 1. So 01-10-2026 00:27 becomes   01-10-2026, and the time is dropped.
#- .alias("date_part") names that new column date_part.
#- .groupBy(...).count() groups the same date parts together and counts them


# COMMAND ----------

# Cell 7 B: messy text

display(raw_df.groupBy("location").count().orderBy("location"))

# Reading it left to right:
# - .groupBy("location") groups rows with the same city text.
#- .count() counts each group.
#- .orderBy("location") sorts by city name A→Z, so similar spellings appear next to each other.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality report: file 01-10-2026 (5000 rows)
# MAGIC
# MAGIC | Problem | Column | Rows affected | Fixed in |
# MAGIC |---|---|---|---|
# MAGIC | Real nulls | ord_id 25 · order_dt 47 · cust_fname 73 · cust_lname 89 · location 90 · product_info 56 · qty 105 | 485 | C2 |
# MAGIC | Fake missing text (N/A, null) | order_dt 22 · cust_fname 26 · location 32 | 80 | C2 |
# MAGIC | Exact duplicate rows | all | 150 | C1 |
# MAGIC | qty with spaces, decimals (1.0), words (one, three), negatives, zero | qty | ~30 different values | C3 + C4 |
# MAGIC | 3 date formats + impossible dates (30/02, 31/02) + wrong years (2062, 2025) | order_dt | 38 impossible + 45 wrong year | C3 + C4 |
# MAGIC | Same city in different case and spacing | location | 62 values for ~16 cities | C4 |

# COMMAND ----------

# MAGIC %md
# MAGIC # **Data Cleaning**

# COMMAND ----------

# C1: Remove duplicate rows

rows_before = raw_df.count()      # rows_before = raw_df.count(): count all the rows before cleaning (5,000)
df = raw_df.dropDuplicates()      # if two or more rows match in all 7 columns, keep one and remove the rest

print(f"C1 · Removed {rows_before - df.count()} duplicate rows → {df.count()} rows left")

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1: Remove duplicate rows
# MAGIC Sometimes the same order is exported twice, so two rows match in **every column**.
# MAGIC If we keep both, Power BI counts that sale twice and revenue looks too high.
# MAGIC `dropDuplicates()` keeps one copy and removes the rest.

# COMMAND ----------

# C2a: Replace/Remove "NA", "N/A", "null", "NULL" values

df = df.replace(["NA", "N/A", "null", "NULL"], None)

# To check/verify it

for c in df.columns:
    print(c, df.filter(F.col(c).isNull()).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2a: Turn fake "missing" text into real nulls
# MAGIC Works like **Find & Replace in Excel**: `NA`, `N/A`, `null`, `NULL` become real nulls,
# MAGIC so one null check catches every missing value.

# COMMAND ----------

#C2b: Delete rows that are missing an important value

CRITICAL_COLS = ["ord_id", "order_dt", "product_info"]

rows_before = df.count()
df = df.dropna(subset=CRITICAL_COLS)
print(f"C2b · Dropped {rows_before - df.count()} rows missing a critical field → {df.count()} rows left")

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2b: Drop rows missing a critical field
# MAGIC Without `ord_id`, `order_dt` or `product_info`, a row is useless.
# MAGIC We can't guess these without making up fake data, so the row is deleted.

# COMMAND ----------

#C2c: Fill the empty values that are left

df = df.fillna({
    "cust_fname": "Unknown",
    "cust_lname": "",
    "location":   "Unknown, Unknown",
    "qty":        "1",
})

# To check it

for c in df.columns:
    print(c, df.filter(F.col(c).isNull()).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2c: Fill the remaining empty values
# MAGIC These rows are still real sales, so we keep them and fill the gaps with safe defaults:
# MAGIC names/location → `Unknown`, qty → `1` (the most common quantity).

# COMMAND ----------

# C3: Convert types safely 

df = (df
    # order_dt: try each known format, keep the first one that works
    .withColumn("order_dt", F.coalesce(
        F.expr("try_to_timestamp(trim(order_dt), 'dd/MM/yyyy HH:mm')"),      # 01/10/2026 14:35
        F.expr("try_to_timestamp(trim(order_dt), 'yyyy-MM-dd HH:mm:ss')"),   # 2026-10-01 14:35:20
        F.expr("try_to_timestamp(trim(order_dt), 'dd-MM-yyyy HH:mm')"),      # 01-10-2026 14:35
    ))
    # qty: " 3 " / "3.0" → 3 ;  "two" → null
    .withColumn("qty", F.expr("try_cast(trim(qty) AS DOUBLE)").cast("int"))
)

df.printSchema()

# COMMAND ----------

# C3 check: what failed to convert?

print("order_dt that failed to parse:", df.filter(F.col("order_dt").isNull()).count())
print("qty that failed to convert:   ", df.filter(F.col("qty").isNull()).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### C3: Fix data types
# MAGIC Like **VALUE() / DATEVALUE() in Excel**: `order_dt` text → timestamp (tries 3 date formats),
# MAGIC `qty` text → integer (" 3 " / "3.0" → 3). Bad values like `31/02` or `two` become null
# MAGIC instead of crashing the notebook. C4 removes them.

# COMMAND ----------



# COMMAND ----------

