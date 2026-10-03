"""
Generate 4 messy e-commerce sales CSV files (5,000 rows x 7 columns each)
for practising data cleaning + transformation in Databricks (PySpark).

Every file has DIFFERENT values but the SAME kinds of mistakes:
duplicates, missing values, wrong data types, invalid values, messy text.

Run:     python generate_data.py
Output:  data/raw/E-commerce_sales_data_<DD-MM-YYYY>.csv
"""
import csv
import os
import random

OUTPUT_DIR = os.path.join("data", "raw")
FILE_DATES = ["01-10-2026", "02-10-2026", "03-10-2026", "04-10-2026"]
ROWS_PER_FILE = 5000
DUPLICATE_ROWS = 150          # exact copies added to every file

FIRST_NAMES = [
    "Aarav", "Vivaan", "Aditya", "Vihaan", "Arjun", "Sai", "Reyansh", "Ayaan", "Krishna", "Ishaan",
    "Shaurya", "Atharv", "Advik", "Pranav", "Rudra", "Kabir", "Dhruv", "Aryan", "Rohan", "Karan",
    "Rahul", "Amit", "Vikas", "Nikhil", "Manish", "Suresh", "Rajesh", "Deepak", "Harsh", "Yash",
    "Jay", "Parth", "Dev", "Kunal", "Tushar", "Mihir", "Neel", "Om", "Varun", "Gaurav",
    "Aanya", "Diya", "Saanvi", "Ananya", "Aadhya", "Pari", "Anika", "Navya", "Myra", "Sara",
    "Riya", "Priya", "Neha", "Pooja", "Sneha", "Kavya", "Isha", "Nisha", "Megha", "Shreya",
    "Tanvi", "Khushi", "Janvi", "Hetal", "Krupa", "Dhwani", "Bhavna", "Komal", "Payal", "Ritu",
    "Swati", "Divya", "Anjali", "Simran", "Nidhi", "Kriti", "Aditi", "Roshni", "Mansi", "Palak",
]
LAST_NAMES = [
    "Sharma", "Verma", "Gupta", "Patel", "Shah", "Mehta", "Desai", "Joshi", "Kumar", "Singh",
    "Reddy", "Nair", "Iyer", "Rao", "Das", "Bose", "Ghosh", "Mukherjee", "Chopra", "Kapoor",
    "Malhotra", "Agarwal", "Bansal", "Jain", "Trivedi", "Pandya", "Parmar", "Chauhan", "Rathod", "Solanki",
    "Kalavadia", "Modi", "Thakkar", "Bhatt", "Vyas", "Pillai", "Menon", "Kulkarni", "Patil", "Pawar",
    "Yadav", "Mishra", "Tiwari", "Dubey", "Saxena", "Srivastava", "Chaudhary", "Sinha", "Khanna", "Arora",
]
CITIES = [
    ("Mumbai", "Maharashtra"), ("Pune", "Maharashtra"), ("Nagpur", "Maharashtra"),
    ("Delhi", "Delhi"), ("Bengaluru", "Karnataka"), ("Mysuru", "Karnataka"),
    ("Hyderabad", "Telangana"), ("Chennai", "Tamil Nadu"), ("Coimbatore", "Tamil Nadu"),
    ("Kolkata", "West Bengal"), ("Ahmedabad", "Gujarat"), ("Surat", "Gujarat"),
    ("Vadodara", "Gujarat"), ("Jaipur", "Rajasthan"), ("Lucknow", "Uttar Pradesh"),
]
CITY_WEIGHTS = [14, 8, 3, 13, 12, 3, 9, 8, 3, 7, 7, 6, 3, 5, 4]

PRODUCTS = {
    "Electronics": [("Wireless Earbuds", 1299), ("Smart Watch", 2499), ("Power Bank", 899),
                    ("Bluetooth Speaker", 1799), ("Laptop Stand", 749)],
    "Fashion": [("Cotton Kurta", 799), ("Denim Jeans", 1199), ("Running Shoes", 2199),
                ("Leather Wallet", 499), ("Sunglasses", 999)],
    "Home And Kitchen": [("Non Stick Pan", 1099), ("Steel Water Bottle", 399), ("Bedsheet Set", 1499),
                         ("Wall Clock", 649), ("Storage Box", 299)],
    "Beauty": [("Face Wash", 249), ("Hair Serum", 549), ("Perfume", 1299),
               ("Lipstick", 399), ("Sunscreen", 449)],
    "Sports": [("Yoga Mat", 699), ("Dumbbell Set", 1899), ("Cricket Bat", 1599),
               ("Football", 799), ("Skipping Rope", 199)],
}
# Hour-of-day weights (0-23): quiet at night, peaks at lunch and late evening
HOUR_WEIGHTS = [1, 1, 1, 1, 1, 1, 2, 3, 4, 5, 6, 8, 10, 10, 8, 7, 7, 8, 9, 11, 13, 13, 9, 4]
QTY_VALUES = [1, 2, 3, 4, 5, 6]
QTY_WEIGHTS = [50, 25, 12, 7, 4, 2]
NUMBER_WORDS = {1: "one", 2: "two", 3: "three", 4: "four", 5: "five", 6: "six"}


def build_customer_pool():
    """Same pool for every file, so some customers order on more than one day."""
    rng = random.Random(42)
    combos = set()
    while len(combos) < 20300:
        city = rng.choices(CITIES, weights=CITY_WEIGHTS)[0]
        combos.add((rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES), city[0], city[1]))
    combos = sorted(combos)
    rng.shuffle(combos)
    return combos[:300], combos[300:]          # 300 loyal + 20,000 regular customers


def messy_case(rng, text):
    """Randomly lower-case, upper-case or pad text with spaces (invalid formatting)."""
    r = rng.random()
    if r < 0.20:
        return text.lower()
    if r < 0.30:
        return text.upper()
    if r < 0.40:
        return "  " + text + " "
    return text


def make_row(rng, seq, day, month, year, loyal, regular, cat_weights):
    # --- clean values ---------------------------------------------------
    first, last, city, state = rng.choice(loyal) if rng.random() < 0.25 else rng.choice(regular)
    category = rng.choices(list(PRODUCTS), weights=cat_weights)[0]
    product, price = rng.choice(PRODUCTS[category])
    qty = rng.choices(QTY_VALUES, weights=QTY_WEIGHTS)[0]
    hour = rng.choices(range(24), weights=HOUR_WEIGHTS)[0]
    minute = rng.randint(0, 59)
    second = rng.randint(0, 59)

    # --- ord_id: 0.5% missing -------------------------------------------
    ord_id = f"ORD-{year}{month:02d}{day:02d}-{seq:04d}"
    if rng.random() < 0.005:
        ord_id = ""

    # --- order_dt: mixed formats + missing + invalid --------------------
    r = rng.random()
    if r < 0.010:
        order_dt = ""                                                   # missing
    elif r < 0.015:
        order_dt = "N/A"                                                # placeholder text
    elif r < 0.020:
        order_dt = f"{rng.choice([30, 31])}/02/{year} {hour:02d}:{minute:02d}"   # impossible date
    elif r < 0.030:
        wrong_year = rng.choice([2062, 2025])                           # typo year = wrong day
        order_dt = f"{day:02d}/{month:02d}/{wrong_year} {hour:02d}:{minute:02d}"
    else:
        f = rng.random()
        if f < 0.50:
            order_dt = f"{day:02d}/{month:02d}/{year} {hour:02d}:{minute:02d}"
        elif f < 0.85:
            order_dt = f"{year}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}"
        else:
            order_dt = f"{day:02d}-{month:02d}-{year} {hour:02d}:{minute:02d}"

    # --- names: missing + messy case ------------------------------------
    r = rng.random()
    cust_fname = "" if r < 0.015 else "null" if r < 0.020 else messy_case(rng, first)
    cust_lname = "" if rng.random() < 0.020 else messy_case(rng, last)

    # --- location "City, State": missing + messy case -------------------
    r = rng.random()
    location = "" if r < 0.015 else "N/A" if r < 0.020 else messy_case(rng, f"{city}, {state}")

    # --- product_info "Category|Product|Price": 1% missing --------------
    product_info = "" if rng.random() < 0.010 else f"{category}|{product}|{price}"

    # --- qty: text type, missing, words, negative, zero ------------------
    r = rng.random()
    if r < 0.020:
        qty_txt = ""                                    # missing
    elif r < 0.025:
        qty_txt = NUMBER_WORDS[qty]                     # word instead of number
    elif r < 0.035:
        qty_txt = str(-qty)                             # negative (sign typo)
    elif r < 0.042:
        qty_txt = "0"                                   # zero quantity
    elif r < 0.092:
        qty_txt = f"{qty}.0"                            # decimal text
    elif r < 0.142:
        qty_txt = f" {qty} "                            # padded with spaces
    else:
        qty_txt = str(qty)

    return [ord_id, order_dt, cust_fname, cust_lname, location, product_info, qty_txt]


def generate_file(file_date, loyal, regular):
    day, month, year = (int(x) for x in file_date.split("-"))
    rng = random.Random(year * 10000 + month * 100 + day)          # fixed seed per file
    cat_weights = [rng.uniform(0.6, 1.4) * w for w in [30, 25, 18, 15, 12]]

    unique_rows = ROWS_PER_FILE - DUPLICATE_ROWS
    rows = [make_row(rng, i + 1, day, month, year, loyal, regular, cat_weights)
            for i in range(unique_rows)]
    rows += [list(r) for r in rng.sample(rows, DUPLICATE_ROWS)]    # exact duplicates
    rng.shuffle(rows)

    path = os.path.join(OUTPUT_DIR, f"E-commerce_sales_data_{file_date}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["ord_id", "order_dt", "cust_fname", "cust_lname",
                         "location", "product_info", "qty"])
        writer.writerows(rows)
    print(f"Created {path}  ({len(rows)} rows)")


if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    loyal_customers, regular_customers = build_customer_pool()
    for d in FILE_DATES:
        generate_file(d, loyal_customers, regular_customers)
