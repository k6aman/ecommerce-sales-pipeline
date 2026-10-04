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

# C4a: Remove dates that couldn't be parsed

rows_before = df.count()   
# Counts how many rows the table has right now (4,706 for file 01) and saves that number in rows_before.

df = df.filter(F.col("order_dt").isNotNull())
# Keeps only the rows where order_dt has a value and drops the rows where it's empty (null). In step C3, impossible dates like 31/02/2026 couldn't be turned into real dates, so they became null. This line removes them.

print(f"C4a · Removed {rows_before - df.count()} rows with an invalid date → {df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### C4a: Remove dates that couldn't be parsed
# MAGIC Impossible dates like `31/02/2026` became null in C3. The real date can't be recovered, so the row is removed.

# COMMAND ----------

# C4b: Keep only orders from this file's day

rows_before = df.count()
# Saves the current row count (4,670 after C4a).

df = df.filter(F.to_date("order_dt") == F.lit(FILE_DATE).cast("date"))
# Keeps only the orders from this file's day. It has three parts:
# - F.to_date("order_dt") takes just the date from the timestamp and drops the time, so 2026-10-01 14:35 becomes 2026-10-01.
# - F.lit(FILE_DATE) turns your FILE_DATE setting (e.g. "2026-10-01") into a value Spark can compare against.
# - .cast("date") changes that text into a real date, so both sides of == are dates.

# Rows whose date matches the file's day stay. Rows with typo years like 01/10/2062 or 01/10/2025 are removed.

print(f"C4b · Removed {rows_before - df.count()} rows with a date outside {FILE_DATE} → {df.count()}")
# Prints how many rows were removed and how many are left. For file 01 you should see Removed 44 … → 4626.

# COMMAND ----------

# MAGIC %md
# MAGIC ### C4b: Keep only orders from this file's day
# MAGIC Each file is one day's export. Typo years like `2062` or `2025` don't match `FILE_DATE`, so those rows are removed.

# COMMAND ----------

# C4c: Remove qty values that weren't numbers

rows_before = df.count()
# Saves the current row count (4,626 after C4b).

df = df.filter(F.col("qty").isNotNull())
# Keeps only the rows where qty has a value and removes the rows where it's empty (null).

print(f"C4c · Removed {rows_before - df.count()} rows with non-numeric qty → {df.count()}")
# Prints how many rows were removed and how many are left. For file 01 you should see Removed 18 … → 4608.

# COMMAND ----------

# MAGIC %md
# MAGIC ### C4c: Remove qty values that weren't numbers
# MAGIC Words like `two` became null in C3. We never guess a value, so the row is removed.

# COMMAND ----------

# C4d: Fix negative qty, remove zero qty

print("Negative qty rows fixed:", df.filter(F.col("qty") < 0).count())
# Counts the rows where qty is below 0 and prints that number. This only reports and doesn't change anything. For file 01 you should see 56.

rows_before = df.count()
# Saves the current row count (4,608 after C4c).

# Step 1: turn negative numbers into positive ones
df = df.withColumn("qty", F.abs("qty"))    # F.abs removes the minus sign, so -2 becomes 2 and 5 stays 5.

# Step 2: keep only rows where qty is more than 0
df = df.filter(F.col("qty") > 0)

print(f"C4d · Removed {rows_before - df.count()} rows with qty = 0 → {df.count()}")
# Prints how many rows were removed. For file 01 you should see Removed 27 … → 4581.

# COMMAND ----------

# MAGIC %md
# MAGIC ### C4d: Fix negative qty, remove zero qty
# MAGIC This feed has no returns, so `-2` is a typo and `abs()` turns it into `2`. A qty of `0` isn't a sale, so the row is removed.

# COMMAND ----------

# C4e: Fix messy text (spaces and mixed case)

df = df.withColumn("cust_fname", F.initcap(F.trim("cust_fname")))
df = df.withColumn("cust_lname", F.initcap(F.trim("cust_lname")))
df = df.withColumn("location",   F.initcap(F.trim("location")))

# Each line uses two functions, and the inner one runs first:

# 1. F.trim removes extra spaces at the start and end of the text, so "  surat  " becomes "surat".
# 2. F.initcap makes the first letter of each word a capital and the rest small, so "SURAT, GUJARAT" becomes "Surat, Gujarat".

display(df.groupBy("location").count().orderBy("location"))
#Shows each distinct location with how many orders it has, sorted A to Z. This is just a check, and you should see exactly 16 locations (15 cities plus Unknown, Unknown).

# COMMAND ----------

# MAGIC %md
# MAGIC ### C4e: Fix messy text
# MAGIC Like **TRIM() + PROPER() in Excel**: `SURAT, GUJARAT` and `  surat, gujarat ` both become `Surat, Gujarat`, so Power BI shows one bar per city.