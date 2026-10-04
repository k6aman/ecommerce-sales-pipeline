# Databricks notebook source
# Cell 1: Find the new raw file

from pyspark.sql import functions as F

# Part 1: Where to look
CATALOG  = "workspace"
SCHEMA   = "ecommerce"
RAW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_files"
# This is the address of the raw folder, the same as in notebook 01.

# Part 2: Start with "nothing found"
FILE_NAME = None
# None means empty, no file yet. We use it later to check whether anything was found.

# Part 3: Check each file
for f in dbutils.fs.ls(RAW_PATH):                                  # take each file one by one
    dd, mm, yyyy = f.name[-14:-4].split("-")                       # read the date from its name
    table = f"{CATALOG}.{SCHEMA}.sales_clean_{yyyy}_{mm}_{dd}"     # its table name
    if not spark.catalog.tableExists(table):                       # table missing? → new file!
        FILE_NAME    = f.name
        FILE_DATE    = f"{yyyy}-{mm}-{dd}"
        TARGET_TABLE = table
        break                                                       # found it → stop looking

#How the date is read from the name:
#E-commerce_sales_data_02-10-2026.csv
#                      02-10-2026        ← f.name[-14:-4]  (cut out the date)
#                      "02" "10" "2026"  ← .split("-")     (break at each "-")
#                       dd   mm   yyyy
#These pieces are then used to build:
#- table name: sales_clean_2026_10_02
#- file date: 2026-10-02

# Part 4: Nothing new? Stop.
if FILE_NAME is None:
    dbutils.notebook.exit("No new file today")

print("Processing:", FILE_NAME, "→", TARGET_TABLE)
# - If FILE_NAME is still None, no new file was found, so the notebook stops quietly. This isn't an error.
#- Otherwise it prints which file it's going to clean.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Find the new raw file
# MAGIC Picks the first raw file without a clean table and sets FILE_NAME, FILE_DATE and TARGET_TABLE. Every cell below uses these, so the cleaning code is the same as notebook 01. If there's no new file, the notebook stops.

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

# COMMAND ----------

# MAGIC %md
# MAGIC # **Data Transformation**
# MAGIC

# COMMAND ----------

# T1: Rename columns

df = df.withColumnsRenamed({
    "ord_id":       "Order_ID",
    "order_dt":     "Order_Timestamp",
    "cust_fname":   "First_Name",
    "cust_lname":   "Last_Name",
    "location":     "Location",
    "product_info": "Product_Info",
    "qty":          "Quantity",
})
print(df.columns)

# "withColumnsRenamed" renames several columns in one go. It takes a dictionary where each pair is written "old name": "new name".
# df = ... saves the result back into df.

# COMMAND ----------

# MAGIC %md
# MAGIC ### T1: Rename columns
# MAGIC Short technical names (`ord_id`, `qty`) become readable business names (`Order_ID`, `Quantity`), so every later step and Power BI uses clear names.

# COMMAND ----------

# T2: Split Location and Product_Info

loc_parts  = F.split(F.col("Location"), ",")        # "Surat, Gujarat"
# This cuts Location at the comma, so "Surat, Gujarat" becomes ["Surat", " Gujarat"] (an array, which is a list of pieces).
#- Nothing changes in df yet. loc_parts is just a recipe that gets used below.

prod_parts = F.split(F.col("Product_Info"), r"\|")    # "Electronics|Wireless Earbuds|1299"
# This cuts Product_Info at the |, so "Electronics|Wireless Earbuds|1299" becomes ["Electronics", "Wireless Earbuds", "1299"].
#- split treats the pattern as a regular expression (regex), and in regex | means "OR". The \ makes it an ordinary pipe character, and the r in front keeps Python from changing the backslash.

df = (df
      .withColumn("City",       F.trim(loc_parts.getItem(0)))
      .withColumn("State",      F.trim(loc_parts.getItem(1)))
# withColumn adds a new column.
#- .getItem(0) takes the 1st piece and .getItem(1) takes the 2nd. Counting starts at 0.
#- F.trim removes extra spaces, so " Gujarat" becomes "Gujarat".

      .withColumn("Category",   F.trim(prod_parts.getItem(0)))
      .withColumn("Product",    F.trim(prod_parts.getItem(1)))
      .withColumn("Unit_Price", F.trim(prod_parts.getItem(2)).cast("double"))      
# These lines work the same way on the product pieces: 1st → Category, 2nd → Product, 3rd → Unit_Price.
#- .cast("double") turns the text "1299" into the number 1299.0, so you can multiply it later when you calculate Revenue in T4.
      
      .drop("Location", "Product_Info"))
# This removes the two original combined columns, which aren't needed any more.

display(df.limit(10))
#- This shows the first 10 rows so you can check the new columns.

# COMMAND ----------

# MAGIC %md
# MAGIC ### T2: Split combined columns
# MAGIC One cell should hold one value. `Location` becomes City + State, and `Product_Info` becomes Category + Product + Unit_Price, so we can filter by State and chart revenue by Category.

# COMMAND ----------

# T3: Merge First_Name + Last_Name → Customer_Name

df = (df
      .withColumn("Customer_Name", F.trim(F.concat_ws(" ", "First_Name", "Last_Name")))
      .drop("First_Name", "Last_Name"))
#1. df = (df ...): takes the DataFrame, changes it, and saves the result back into df. The brackets let the code run over several lines.
#2. .withColumn("Customer_Name", ...): adds a new column called Customer_Name.
#3. F.concat_ws(" ", "First_Name", "Last_Name"): joins the first and last name with a space between them, so "Rahul" and "Shah" become "Rahul Shah". If one part is null it skips that part instead of making the whole name null.
#4. F.trim(...): removes extra spaces at the start or end. For example, "Rahul " becomes "Rahul" when the last name was empty.
#5. .drop("First_Name", "Last_Name"): deletes the two old columns, since Customer_Name now holds both.



# COMMAND ----------

# MAGIC %md
# MAGIC ### T3: Merge name columns
# MAGIC `First_Name` + `Last_Name` become one `Customer_Name` column, so reports use a single customer field.

# COMMAND ----------

# T4: Calculated column: Revenue

df = df.withColumn("Revenue", F.round(F.col("Quantity") * F.col("Unit_Price"), 2))
#1. df = df...: changes the DataFrame and saves the result back into df.
#2. .withColumn("Revenue", ...): adds a new column called Revenue.
#3. F.col("Quantity") * F.col("Unit_Price"): multiplies the two columns for every row. For example, 3 items × ₹499.50 = ₹1498.50.
#4. F.round(..., 2): keeps only 2 decimal places (rupees and paise), so you don't get long decimals like 1498.4999999.

display(df.agg(F.count("*").alias("rows"), F.round(F.sum("Revenue"), 0).alias("revenue")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### T4: Revenue
# MAGIC Revenue = Quantity × Unit_Price, worked out once here so every Power BI report uses the same number.

# COMMAND ----------

# T5: Split the timestamp into date parts

df = (df
      .withColumn("Order_Date",    F.to_date("Order_Timestamp"))
      .withColumn("Order_Day",     F.dayofmonth("Order_Timestamp"))
      .withColumn("Order_Month",   F.month("Order_Timestamp"))
      .withColumn("Order_Year",    F.year("Order_Timestamp"))
      .withColumn("Order_Weekday", F.date_format("Order_Timestamp", "EEEE"))   # e.g. Thursday
      .withColumn("Order_Hour",    F.hour("Order_Timestamp")))
#Every line takes one part of Order_Timestamp and puts it in its own new column.

#Example timestamp: 2026-10-01 14:35:20

#┌────────────────────────────┬───────────────────────────────────────────────────────────┬────────────┐
#           Line            │                       What it does                        │   Result   │
#├────────────────────────────┼───────────────────────────────────────────────────────────┼────────────┤
#│ F.to_date(...)             │ keeps only the date and drops the time                    │ 2026-10-01 │
#├────────────────────────────┼───────────────────────────────────────────────────────────┼────────────┤
#│ F.dayofmonth(...)          │ day number in the month                                   │ 1          │
#├────────────────────────────┼───────────────────────────────────────────────────────────┼────────────┤
#│ F.month(...)               │ month number                                              │ 10         │
#├────────────────────────────┼───────────────────────────────────────────────────────────┼────────────┤
#│ F.year(...)                │ year                                                      │ 2026       │
#├────────────────────────────┼───────────────────────────────────────────────────────────┼────────────┤
#│ F.date_format(..., "EEEE") │ full day name ("EEEE" means the full name, like Thursday) │ Thursday   │
#├────────────────────────────┼───────────────────────────────────────────────────────────┼────────────┤
#│ F.hour(...)                │ hour of the day, 0–23                                     │ 14         │
#└────────────────────────────┴───────────────────────────────────────────────────────────┴────────────┘

display(df.select("Order_Timestamp", "Order_Date", "Order_Day", "Order_Month",
                  "Order_Year", "Order_Weekday", "Order_Hour").limit(10))
#select(...): picks only these columns.
#- .limit(10): takes just the first 10 rows.
#- display(...): shows them as a table, so you can check the split worked.

# COMMAND ----------

# MAGIC %md
# MAGIC ### T5: Date parts
# MAGIC Day, Month, Year, Weekday and Hour taken from `Order_Timestamp`. They're used for slicers, daily trends and the peak-hour question.

# COMMAND ----------

# T6: Add lineage columns and reorder

df = (df
      .withColumn("Source_File",  F.lit(FILE_NAME))
      .withColumn("Processed_At", F.current_timestamp()))
#- withColumn("Source_File", ...) adds a new column called Source_File.
#- F.lit(FILE_NAME) puts the same value (the file name) in every row. lit means "literal", a fixed value.
#- withColumn("Processed_At", F.current_timestamp()) adds a column holding the date and time the code ran.
#- Why: if a number looks wrong later, these two columns tell you which file the row came from and when it was processed.

FINAL_COLUMNS = ["Order_ID", "Customer_Name", "Order_Timestamp", "Order_Date", "Order_Day", "Order_Month", "Order_Year", "Order_Weekday", "Order_Hour", "City", "State", "Category", "Product", "Quantity", "Unit_Price", "Revenue", "Source_File", "Processed_At"]
# This is a plain Python list of column names in the order you want them. The groups are: order info → time → place → product → numbers → lineage.

clean_df = df.select(FINAL_COLUMNS)
#- select keeps only these columns, in this exact order, and saves the result as a new DataFrame, clean_df.

clean_df.printSchema()
#- This prints each column's name and data type so you can confirm everything looks right before saving.

# COMMAND ----------

# MAGIC %md
# MAGIC ### T6: Lineage columns + final column order
# MAGIC Adds Source_File and Processed_At so every row can be traced back to its file and run time, then puts the columns in a fixed order (order → time → place → product → numbers → lineage).

# COMMAND ----------

# Save as a Delta table

(clean_df.write
# - Start saving clean_df. .write means "I want to save this DataFrame somewhere."
#- The outer ( ) brackets just let you split the code across several lines so it's easier to read.
    .format("delta")
# Save it in Delta format, Databricks' standard table format.
#- Delta is like a normal data file plus a history log. Every save is recorded, so you can look at older versions later.
    .mode("overwrite")
# - If the table already exists, replace it instead of adding rows on top.
#- Why: if you run this cell twice by mistake, you still get the same table, not double the data.
    .option("overwriteSchema", "true")
# - This lets the save go through even if the columns have changed, for example after you add or rename one.
#- Without it, Spark would throw an error saying the columns don't match the existing table.
    .saveAsTable(TARGET_TABLE))
#- Save it as a named table: workspace.ecommerce.sales_clean_2026_10_01.
#- After this, the table shows up in Catalog → workspace → ecommerce, and SQL and Power BI can find it.

print(f"Saved: {TARGET_TABLE}")
# - This prints a confirmation message so you know the save finished.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save as Delta table
# MAGIC Saves clean_df as workspace.ecommerce.sales_clean_2026_10_01. overwrite replaces the table on every run, so re-running never doubles the data.

# COMMAND ----------

# Add the "Combined table" cell

# Part 1: The name of the big table
ALL_TABLE = f"{CATALOG}.{SCHEMA}.sales_clean_all"
#This saves the full name workspace.ecommerce.sales_clean_all in a variable, so we don't have to type it again.

# Part 2: Create the big table, first time only
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {ALL_TABLE}
    AS SELECT * FROM {CATALOG}.{SCHEMA}.sales_clean_2026_10_01
""")
#- spark.sql(...) runs SQL from Python.
#- CREATE TABLE IF NOT EXISTS means: if the table isn't there yet, create it. If it's already there, do nothing.
#- AS SELECT * FROM ..._01 fills it with all the Day-1 rows.

# Part 3: Add today's rows
(clean_df.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"Source_File = '{FILE_NAME}'")
    .saveAsTable(ALL_TABLE))
#- clean_df.write saves today's clean data.
#- .format("delta") uses Delta format, the same as before.
#- .mode("overwrite") together with .option("replaceWhere", ...) is the important part. Read the two together:
# Overwrite only the rows where Source_File = today's file name.
#  - Rows from other days are not touched.
#  - If today's rows were already there (a re-run), they're removed and written again.
# - If they weren't there, they're simply added.
#- .saveAsTable(ALL_TABLE) writes into sales_clean_all.

print(f"Added {clean_df.count()} rows from {FILE_NAME} to {ALL_TABLE}")
# This shows how many rows were added and from which file.

# Why not plain "overwrite"? It would erase the whole table and keep only today's file, so you'd lose the other days.
# Why not "append"? If you run it twice, today's rows would be added twice, and Power BI would show double sales.
# "replaceWhere" avoids both problems.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Combined table: sales_clean_all
# MAGIC Adds today's rows to sales_clean_all, the one table Power BI uses. The first run creates it from the Day-1 table. replaceWhere replaces only this file's rows, so re-running never doubles the data.

# COMMAND ----------

# Add a Verify cell at the end

# Verify: rows and revenue per file in the combined table

display(spark.sql(f"""
    SELECT Source_File, COUNT(*) AS rows, ROUND(SUM(Revenue), 0) AS revenue
    FROM {ALL_TABLE}
    GROUP BY Source_File
    ORDER BY Source_File
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify
# MAGIC One row per file with its row count and revenue. Each file should appear once.