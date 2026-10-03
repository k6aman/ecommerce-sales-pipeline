# Databricks notebook source
# Copy of the Databricks notebook Ecommerce_Project/01_clean_transform_day1
# P-1028: read the raw 01-10-2026 CSV and profile it before cleaning.

# COMMAND ----------

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

# Reasoning: Loop through each column, keep only the rows where that column is null, and count them. This shows how many
# values are missing in each column.

# COMMAND ----------

# Cell 4: Placeholder text that's really "missing"
for c in raw_df.columns:
    fake = raw_df.filter(F.trim(F.col(c)).isin("N/A", "NA", "null", "NULL")).count()
    print(c, "→", fake)

# Reading it left to right
#- for c in raw_df.columns: goes through each column, one by one.
#- F.col(c) takes the values in that column.
#- F.trim(...) removes extra spaces from the start and end, so " N/A " becomes "N/A". Without it, values with spaces would
#  slip through.
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
#- raw_df.groupBy("qty") puts rows with the same qty value into one group. All the 1s go together, all the 2s go together, and
#  so on.
#- .count() counts how many rows are in each group.
#- .orderBy(F.desc("count")) sorts the result with the biggest count at the top. desc means descending, from high to low.
#- display(...) shows the result as a table.

# COMMAND ----------

# Cell 7 A: Date formats

display(raw_df.groupBy(F.substring("order_dt", 1, 10).alias("date_part")).count())

# Reading it left to right:
#- F.substring("order_dt", 1, 10) takes only the first 10 characters, starting at character 1. So 01-10-2026 00:27 becomes
#  01-10-2026, and the time is dropped.
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
