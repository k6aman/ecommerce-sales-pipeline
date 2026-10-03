# E-commerce Sales Pipeline: Workflow

Databricks → Delta Lake → Power BI

```
STAGE 1 · LOCAL                     generate_data.py → 4 raw CSVs (5,000 × 7)
        │
STAGE 2 · DATABRICKS SETUP          workspace.ecommerce (schema)
        │                             └─ raw_files (volume) ← upload 4 CSVs
        │                           Ecommerce_Project/ (workspace folder + notebooks)
        │
STAGE 3 · DAY-1 NOTEBOOK            read file 01 → profile
        │   (manual)                CLEAN     C1 duplicates → C2 missing → C3 types → C4 invalid
        │                           TRANSFORM T1 rename → T2 split → T3 merge → T4 Revenue
        │                                     → T5 date parts → T6 reorder + lineage
        │                           → Delta: sales_clean_2026_10_01
        │
STAGE 4 · DAILY JOB 09:00 IST       02_daily_pipeline (same code as a function)
        │                           skip processed → clean 02, 03, 04 in one run
        │                           → sales_clean_2026_10_02 / _03 / _04
        │                           → rebuild sales_clean_all  + pipeline_audit_log
        │
STAGE 5 · POWER BI                  SQL Warehouse + token → Import → Sales table
                                    → DAX measures → 3-page dashboard (7 questions)
```

## Stages and tickets

| Stage | Tickets |
|---|---|
| 1 · Local | P-1026 (#67) |
| 2 · Databricks setup | P-1027 (#68) |
| 3 · Day-1 notebook | P-1028 – P-1034 (#69–#75) |
| 4 · Daily job | P-1035 – P-1036 (#76–#77) |
| 5 · Power BI | P-1037 – P-1039 (#78–#80) |
| Wrap-up | P-1040 (#81): README + public repo |

## Data shape

- **Raw (7 columns):** `ord_id, order_dt, cust_fname, cust_lname, location, product_info, qty`
- **Clean (18 columns):** `Order_ID, Customer_Name, Order_Timestamp, Order_Date, Order_Day, Order_Month, Order_Year, Order_Weekday, Order_Hour, City, State, Category, Product, Quantity, Unit_Price, Revenue, Source_File, Processed_At`
- **Expected totals:** 18,333 rows, revenue 37,442,489
