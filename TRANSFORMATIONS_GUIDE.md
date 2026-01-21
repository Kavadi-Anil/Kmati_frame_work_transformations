# PySpark Transformation Library - Complete Guide

A comprehensive guide to all 17 transformations in the Databricks Medallion Pipeline Framework with detailed examples.

---

## Table of Contents

1. [trim_strings](#1-trim_strings)
2. [regex_clean](#2-regex_clean)
3. [parse_numeric](#3-parse_numeric)
4. [parse_date](#4-parse_date)
5. [normalize_status](#5-normalize_status)
6. [null_handling](#6-null_handling)
7. [duplicate_handling](#7-duplicate_handling)
8. [rename_columns](#8-rename_columns)
9. [standardize_column_case](#9-standardize_column_case)
10. [fill_missing](#10-fill_missing)
11. [surrogate_key](#11-surrogate_key)
12. [outlier_capping](#12-outlier_capping)
13. [fuzzy_normalize](#13-fuzzy_normalize)
14. [flatten_json](#14-flatten_json)
15. [schema_validation](#15-schema_validation)
16. [json_key_default](#16-json_key_default)
17. [array_size_validation](#17-array_size_validation)

---

## 1. trim_strings

### Purpose
Removes leading and trailing whitespace from string columns.

### PySpark Code
```python
def trim_strings(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    for c in cfg.get("columns", []):
        if c in df.columns:
            df = df.withColumn(c, F.trim(F.col(c)))
    return df
```

### YAML Configuration
```yaml
trim_strings: { enabled: true, columns: [first_name, last_name, email] }
```

### Sample Input
| first_name | last_name | email |
|------------|-----------|-------|
| "   John   " | "  Doe  " | " john@email.com " |
| "  Alice" | "Smith   " | "alice@email.com  " |
| "Bob" | "   Wilson" | "   bob@email.com" |

### Sample Output
| first_name | last_name | email |
|------------|-----------|-------|
| "John" | "Doe" | "john@email.com" |
| "Alice" | "Smith" | "alice@email.com" |
| "Bob" | "Wilson" | "bob@email.com" |

### Key Points
- ✅ Removes spaces from both ends
- ✅ Does not affect spaces in the middle
- ✅ Only processes columns that exist in DataFrame
- ⚠️ Row count: **Unchanged**

---

## 2. regex_clean

### Purpose
Cleans data using regular expressions - removes unwanted characters from numbers, standardizes emails.

### PySpark Code
```python
def regex_clean(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    for c in cfg.get("number_columns", []):
        name = c["name"]
        if name in df.columns:
            expr_col = F.regexp_replace(F.col(name), c["pattern_remove"], "")
            if c.get("trim"):
                expr_col = F.trim(expr_col)
            df = df.withColumn(c["out"], expr_col)

    for c in cfg.get("email_columns", []):
        name = c["name"]
        if name in df.columns:
            expr_col = F.col(name)
            if c.get("trim"):
                expr_col = F.trim(expr_col)
            if c.get("lowercase"):
                expr_col = F.lower(expr_col)
            df = df.withColumn(c["out"], expr_col)

    return df
```

### YAML Configuration
```yaml
regex_clean: { enabled: true,
  number_columns: [
    { name: phone_raw, out: phone_clean, pattern_remove: "[^0-9]", trim: true },
    { name: balance_raw, out: balance_clean, pattern_remove: "[^0-9\\.\\-]", trim: true }
  ],
  email_columns: [
    { name: email_raw, out: email, trim: true, lowercase: true }
  ]
}
```

### Sample Input
| phone_raw | balance_raw | email_raw |
|-----------|-------------|-----------|
| "(555) 123-4567" | "$1,234.56" | "  JOHN@GMAIL.COM  " |
| "+1-800-555-0199" | "-$500.00 USD" | "Alice@Yahoo.COM" |
| "555.867.5309" | "10,000.99" | "  BOB@email.COM " |

### Sample Output
| phone_raw | balance_raw | email_raw | phone_clean | balance_clean | email |
|-----------|-------------|-----------|-------------|---------------|-------|
| "(555) 123-4567" | "$1,234.56" | "  JOHN@GMAIL.COM  " | "5551234567" | "1234.56" | "john@gmail.com" |
| "+1-800-555-0199" | "-$500.00 USD" | "Alice@Yahoo.COM" | "18005550199" | "-500.00" | "alice@yahoo.com" |
| "555.867.5309" | "10,000.99" | "  BOB@email.COM " | "5558675309" | "10000.99" | "bob@email.com" |

### Key Points
- ✅ Creates new output columns (keeps original)
- ✅ Phone: Removes all non-numeric characters
- ✅ Balance: Keeps numbers, decimal, and minus sign
- ✅ Email: Trims whitespace + converts to lowercase
- ⚠️ Row count: **Unchanged**

---

## 3. parse_numeric

### Purpose
Converts string columns to numeric types (int, double, float) after cleaning non-numeric characters.

### PySpark Code
```python
def parse_numeric(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    for c in cfg.get("columns", []):
        name = c["name"]
        if name in df.columns:
            expr_col = F.regexp_replace(
                F.col(name).cast("string"),
                r"[^0-9\.\-]",
                ""
            )
            df = df.withColumn(c["out"], expr_col.cast(c["cast_type"]))
    return df
```

### YAML Configuration
```yaml
parse_numeric: { enabled: true,
  columns: [
    { name: balance_clean, out: balance, cast_type: double },
    { name: age_clean, out: age, cast_type: int },
    { name: quantity_raw, out: quantity, cast_type: long }
  ]
}
```

### Sample Input
| balance_clean | age_clean | quantity_raw |
|---------------|-----------|--------------|
| "1234.56" | "25" | "100" |
| "-500.00" | "30 years" | "50 units" |
| "10000.99" | "45" | "200" |

### Sample Output
| balance_clean | age_clean | quantity_raw | balance | age | quantity |
|---------------|-----------|--------------|---------|-----|----------|
| "1234.56" | "25" | "100" | 1234.56 | 25 | 100 |
| "-500.00" | "30 years" | "50 units" | -500.0 | 30 | 50 |
| "10000.99" | "45" | "200" | 10000.99 | 45 | 200 |

### Supported Cast Types
| cast_type | Description | Example |
|-----------|-------------|---------|
| `int` | Integer (32-bit) | 123 |
| `long` | Long integer (64-bit) | 9999999999 |
| `float` | Single precision decimal | 123.45 |
| `double` | Double precision decimal | 123.456789 |

### Key Points
- ✅ Removes non-numeric characters before casting
- ✅ Handles negative numbers (keeps minus sign)
- ✅ Creates new typed column
- ⚠️ Row count: **Unchanged**

---

## 4. parse_date

### Purpose
Parses date strings into proper date format, trying multiple formats until one works.

### PySpark Code
```python
def parse_date(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    for c in cfg.get("columns", []):
        name = c["name"]
        if name in df.columns:
            parsed = None
            for fmt in c.get("formats", []):
                if any(t in fmt for t in ["HH", "hh", "mm", "ss"]):
                    attempt = F.to_date(F.to_timestamp(F.col(name), fmt))
                else:
                    attempt = F.to_date(F.col(name), fmt)
                
                parsed = attempt if parsed is None else F.coalesce(parsed, attempt)
            
            df = df.withColumn(c["out"], parsed)
    return df
```

### YAML Configuration
```yaml
parse_date: { enabled: true,
  columns: [
    { name: date_raw, out: date_clean,
      formats: ["yyyy-MM-dd", "dd/MM/yyyy", "MM-dd-yyyy", "yyyy-MM-dd HH:mm:ss"] }
  ]
}
```

### Sample Input
| date_raw |
|----------|
| "2024-01-15" |
| "25/12/2023" |
| "07-04-2024" |
| "2024-06-20 14:30:00" |
| "invalid_date" |

### Sample Output
| date_raw | date_clean |
|----------|------------|
| "2024-01-15" | 2024-01-15 |
| "25/12/2023" | 2023-12-25 |
| "07-04-2024" | 2024-07-04 |
| "2024-06-20 14:30:00" | 2024-06-20 |
| "invalid_date" | NULL |

### Common Date Formats
| Format | Example |
|--------|---------|
| `yyyy-MM-dd` | 2024-01-15 |
| `dd/MM/yyyy` | 15/01/2024 |
| `MM-dd-yyyy` | 01-15-2024 |
| `yyyy-MM-dd HH:mm:ss` | 2024-01-15 14:30:00 |
| `dd/MM/yyyy HH:mm:ss` | 15/01/2024 14:30:00 |

### Key Points
- ✅ Tries multiple formats in order
- ✅ Uses COALESCE to pick first successful parse
- ✅ Handles datetime by extracting date portion
- ⚠️ Returns NULL if no format matches
- ⚠️ Row count: **Unchanged**

---

## 5. normalize_status

### Purpose
Maps various status/category values to standardized values using a lookup mapping. Works for any categorical data standardization (status, currency, country, etc.).

### PySpark Code
```python
def normalize_status(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    for c in cfg.get("columns", []):
        name = c["name"]
        if name in df.columns:
            inp = F.lower(F.trim(F.col(name)))  # Trims + Lowercases
            expr = None
            for raw, clean in c.get("mapping", {}).items():
                expr = (
                    F.when(inp == raw.lower(), F.lit(clean))
                    if expr is None
                    else expr.when(inp == raw.lower(), F.lit(clean))
                )
            df = df.withColumn(c["out"], expr.otherwise(inp))
    return df
```

### YAML Configuration
```yaml
# Example 1: Status normalization
normalize_status: { enabled: true,
  columns: [{ name: status_raw, out: status,
    mapping: { 
      "a": "Active", 
      "active": "Active", 
      "c": "Closed", 
      "closed": "Closed",
      "i": "Inactive",
      "inactive": "Inactive"
    } 
  }]
}

# Example 2: Currency normalization
normalize_status: { enabled: true,
  columns: [{ name: currency_raw, out: currency,
    mapping: { 
      "usd": "USD", 
      "us dollar": "USD", 
      "us dollars": "USD",
      "eur": "EUR",
      "euro": "EUR"
    } 
  }]
}
```

### Sample Input (Status)
| status_raw |
|------------|
| "a" |
| "Active" |
| "ACTIVE" |
| "  active  " |
| "c" |
| "Closed" |
| "unknown" |

### Sample Output (Status)
| status_raw | status |
|------------|--------|
| "a" | "Active" |
| "Active" | "Active" |
| "ACTIVE" | "Active" |
| "  active  " | "Active" |
| "c" | "Closed" |
| "Closed" | "Closed" |
| "unknown" | "unknown" |

### Sample Input (Currency)
| currency_raw |
|--------------|
| "USD" |
| " Us Dollar" |
| " Us Dollar " |
| "USD" |
| " usd " |
| "EUR" |

### Sample Output (Currency)
| currency_raw | currency |
|--------------|----------|
| "USD" | "USD" |
| " Us Dollar" | "USD" |
| " Us Dollar " | "USD" |
| "USD" | "USD" |
| " usd " | "USD" |
| "EUR" | "EUR" |

### Processing Steps
```
INPUT: " Us Dollar "
         │
         ▼
    F.trim() → "Us Dollar"
         │
         ▼
    F.lower() → "us dollar"
         │
         ▼
    mapping["us dollar"] → "USD"
         │
         ▼
OUTPUT: "USD"
```

### Key Points
- ✅ Case-insensitive matching (converts to lowercase)
- ✅ Trims whitespace before matching
- ✅ Unmapped values pass through as-is (lowercased)
- ✅ Works for any categorical data (status, currency, country, etc.)
- ⚠️ Row count: **Unchanged**

---

## 6. null_handling

### Purpose
Drops rows where specified columns have NULL values.

### PySpark Code
```python
def null_handling(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if cfg.get("enabled", False):
        df = df.dropna(subset=cfg.get("drop_if_null", []))
    return df
```

### YAML Configuration
```yaml
null_handling: { enabled: true, drop_if_null: [customer_id, email] }
```

### Sample Input
| customer_id | name | email |
|-------------|------|-------|
| "C001" | "John" | "john@email.com" |
| NULL | "Alice" | "alice@email.com" |
| "C003" | "Bob" | NULL |
| "C004" | "Eve" | "eve@email.com" |
| NULL | "Tom" | NULL |

### Sample Output
| customer_id | name | email |
|-------------|------|-------|
| "C001" | "John" | "john@email.com" |
| "C004" | "Eve" | "eve@email.com" |

### Rows Dropped
- Row 2: `customer_id` is NULL
- Row 3: `email` is NULL
- Row 5: Both `customer_id` and `email` are NULL

### Key Points
- ✅ Drops rows if ANY specified column is NULL
- ✅ Multiple columns can be specified
- ⚠️ Row count: **DECREASES** ⬇️

---

## 7. duplicate_handling

### Purpose
Removes duplicate rows based on specified unique key columns. Keeps the first occurrence.

### PySpark Code
```python
def duplicate_handling(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if cfg.get("enabled", False):
        df = df.dropDuplicates(cfg.get("unique_keys", []))
    return df
```

### YAML Configuration
```yaml
duplicate_handling: { enabled: true, unique_keys: [customer_id] }
```

### Sample Input
| customer_id | name | updated_at |
|-------------|------|------------|
| "C001" | "John" | "2024-01-01" |
| "C001" | "John Smith" | "2024-01-15" |
| "C002" | "Alice" | "2024-01-01" |
| "C002" | "Alice Wong" | "2024-01-20" |
| "C003" | "Bob" | "2024-01-01" |

### Sample Output
| customer_id | name | updated_at |
|-------------|------|------------|
| "C001" | "John" | "2024-01-01" |
| "C002" | "Alice" | "2024-01-01" |
| "C003" | "Bob" | "2024-01-01" |

### Key Points
- ✅ Keeps first occurrence of duplicate
- ✅ Can use multiple columns as composite key
- ⚠️ Row count: **DECREASES** ⬇️

### Multi-Column Example
```yaml
duplicate_handling: { enabled: true, unique_keys: [customer_id, product_id] }
```

---

## 8. rename_columns

### Purpose
Renames columns from old names to new names.

### PySpark Code
```python
def rename_columns(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    for old, new in cfg.get("mapping", {}).items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    return df
```

### YAML Configuration
```yaml
rename_columns: { enabled: true,
  mapping: { 
    cust_id: customer_id, 
    amt: amount, 
    txn_dt: transaction_date,
    prod_nm: product_name
  }
}
```

### Sample Input
| cust_id | amt | txn_dt | prod_nm |
|---------|-----|--------|---------|
| "C001" | 100.0 | "2024-01-15" | "Laptop" |
| "C002" | 250.5 | "2024-01-16" | "Phone" |

### Sample Output
| customer_id | amount | transaction_date | product_name |
|-------------|--------|------------------|--------------|
| "C001" | 100.0 | "2024-01-15" | "Laptop" |
| "C002" | 250.5 | "2024-01-16" | "Phone" |

### Key Points
- ✅ Only renames columns that exist
- ✅ Preserves data and data types
- ⚠️ Row count: **Unchanged**

---

## 9. standardize_column_case

### Purpose
Converts all column names to snake_case or lowercase format.

### PySpark Code
```python
def standardize_column_case(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    def to_snake(name: str) -> str:
        n = name.lower()
        n = re.sub(r"[^\w]+", "_", n)
        n = re.sub(r"_+", "_", n)
        return n.strip("_")

    for c in df.columns:
        new = to_snake(c) if cfg.get("format") == "snake_case" else c.lower()
        if new != c:
            df = df.withColumnRenamed(c, new)

    return df
```

### YAML Configuration
```yaml
standardize_column_case: { enabled: true, format: snake_case }
```

### Sample Input Columns
```
FirstName, Last Name, Email-Address, Phone Number, Created_At, userID
```

### Sample Output Columns
```
first_name, last_name, email_address, phone_number, created_at, userid
```

### Transformation Rules
| Original | snake_case Output |
|----------|-------------------|
| `FirstName` | `first_name` |
| `Last Name` | `last_name` |
| `Email-Address` | `email_address` |
| `Phone Number` | `phone_number` |
| `Created_At` | `created_at` |
| `userID` | `userid` |
| `CUSTOMER__ID` | `customer_id` |

### Key Points
- ✅ Converts to lowercase
- ✅ Replaces spaces and special chars with underscore
- ✅ Removes duplicate underscores
- ✅ Strips leading/trailing underscores
- ⚠️ Row count: **Unchanged**

---

## 10. fill_missing

### Purpose
Fills NULL values in specified columns with default values.

### PySpark Code
```python
def fill_missing(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    for c in cfg.get("columns", []):
        name = c["name"]
        if name in df.columns:
            df = df.withColumn(
                name,
                F.when(F.col(name).isNull(), F.lit(c["value"]))
                 .otherwise(F.col(name))
            )
    return df
```

### YAML Configuration
```yaml
fill_missing: { enabled: true,
  columns: [
    { name: country, value: "Unknown" },
    { name: age, value: 0 },
    { name: is_active, value: false },
    { name: score, value: 0.0 }
  ]
}
```

### Sample Input
| name | country | age | is_active | score |
|------|---------|-----|-----------|-------|
| "John" | "USA" | 30 | true | 85.5 |
| "Alice" | NULL | NULL | NULL | NULL |
| "Bob" | "UK" | 25 | NULL | NULL |
| "Eve" | NULL | 35 | true | 92.0 |

### Sample Output
| name | country | age | is_active | score |
|------|---------|-----|-----------|-------|
| "John" | "USA" | 30 | true | 85.5 |
| "Alice" | "Unknown" | 0 | false | 0.0 |
| "Bob" | "UK" | 25 | false | 0.0 |
| "Eve" | "Unknown" | 35 | true | 92.0 |

### Key Points
- ✅ Only fills NULL values (not empty strings)
- ✅ Preserves existing non-null values
- ✅ Can use any data type for default value
- ⚠️ Row count: **Unchanged**

---

## 11. surrogate_key

### Purpose
Generates a unique UUID (Universally Unique Identifier) for each row as a surrogate key.

### PySpark Code
```python
def surrogate_key(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    col_name = cfg.get("column", "surrogate_key")
    df = df.withColumn(col_name, F.expr("uuid()"))
    return df
```

### YAML Configuration
```yaml
surrogate_key: { enabled: true, column: customer_sk, method: uuid }
```

### Sample Input
| customer_id | name | email |
|-------------|------|-------|
| "C001" | "John" | "john@email.com" |
| "C002" | "Alice" | "alice@email.com" |
| "C003" | "Bob" | "bob@email.com" |

### Sample Output
| customer_id | name | email | customer_sk |
|-------------|------|-------|-------------|
| "C001" | "John" | "john@email.com" | "550e8400-e29b-41d4-a716-446655440000" |
| "C002" | "Alice" | "alice@email.com" | "6ba7b810-9dad-11d1-80b4-00c04fd430c8" |
| "C003" | "Bob" | "bob@email.com" | "f47ac10b-58cc-4372-a567-0e02b2c3d479" |

### Key Points
- ✅ Generates globally unique identifier
- ✅ UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
- ✅ New UUID generated for each row on every run
- ⚠️ Row count: **Unchanged**

---

## 12. outlier_capping

### Purpose
Caps numeric values within specified minimum and maximum bounds. Values below min are set to min, values above max are set to max.

### PySpark Code
```python
def outlier_capping(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    for c in cfg.get("columns", []):
        name = c["name"]
        if name in df.columns:
            df = df.withColumn(
                name,
                F.when(F.col(name) < c["min"], c["min"])
                 .when(F.col(name) > c["max"], c["max"])
                 .otherwise(F.col(name))
            )
    return df
```

### YAML Configuration
```yaml
outlier_capping: { enabled: true,
  columns: [
    { name: balance, min: 0, max: 100000 },
    { name: age, min: 0, max: 120 },
    { name: score, min: 0, max: 100 }
  ]
}
```

### Sample Input
| customer_id | balance | age | score |
|-------------|---------|-----|-------|
| "C001" | 5000 | 25 | 85 |
| "C002" | -500 | -5 | 150 |
| "C003" | 150000 | 200 | -10 |
| "C004" | 75000 | 45 | 50 |

### Sample Output
| customer_id | balance | age | score |
|-------------|---------|-----|-------|
| "C001" | 5000 | 25 | 85 |
| "C002" | 0 | 0 | 100 |
| "C003" | 100000 | 120 | 0 |
| "C004" | 75000 | 45 | 50 |

### What Changed
| Row | Column | Original | Capped | Reason |
|-----|--------|----------|--------|--------|
| C002 | balance | -500 | 0 | Below min (0) |
| C002 | age | -5 | 0 | Below min (0) |
| C002 | score | 150 | 100 | Above max (100) |
| C003 | balance | 150000 | 100000 | Above max (100000) |
| C003 | age | 200 | 120 | Above max (120) |
| C003 | score | -10 | 0 | Below min (0) |

### Key Points
- ✅ Values within range are unchanged
- ✅ Values below min → set to min
- ✅ Values above max → set to max
- ✅ NULL values remain NULL
- ⚠️ Row count: **Unchanged**

---

## 13. fuzzy_normalize

### Purpose
Normalizes text data by removing punctuation, collapsing multiple spaces, and converting to lowercase. Useful for text matching and deduplication.

### PySpark Code
```python
def fuzzy_normalize(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    rules = cfg.get("rules", {})

    for c in cfg.get("columns", []):
        name = c["name"]
        if name not in df.columns:
            continue

        expr = F.col(name)

        if rules.get("remove_punctuation"):
            expr = F.regexp_replace(expr, r"[^\w\s]", "")
        if rules.get("collapse_spaces"):
            expr = F.regexp_replace(expr, r"\s+", " ")
        if rules.get("lowercase"):
            expr = F.lower(expr)

        df = df.withColumn(c["out"], F.trim(expr))

    return df
```

### YAML Configuration
```yaml
fuzzy_normalize: { enabled: true,
  rules: { 
    remove_punctuation: true, 
    collapse_spaces: true, 
    lowercase: true 
  },
  columns: [
    { name: company_name, out: company_clean },
    { name: address, out: address_clean }
  ]
}
```

### Sample Input
| company_name | address |
|--------------|---------|
| "Apple,  Inc." | "123   Main St.,  Suite #100" |
| "GOOGLE   LLC!!!" | "456  Oak   Ave.  Floor 5" |
| "Microsoft   Corporation." | "789 Pine    Rd.,   Building A" |

### Sample Output
| company_name | address | company_clean | address_clean |
|--------------|---------|---------------|---------------|
| "Apple,  Inc." | "123   Main St.,  Suite #100" | "apple inc" | "123 main st suite 100" |
| "GOOGLE   LLC!!!" | "456  Oak   Ave.  Floor 5" | "google llc" | "456 oak ave floor 5" |
| "Microsoft   Corporation." | "789 Pine    Rd.,   Building A" | "microsoft corporation" | "789 pine rd building a" |

### Processing Steps
```
INPUT: "Apple,  Inc."
         │
         ▼
    remove_punctuation → "Apple  Inc"
         │
         ▼
    collapse_spaces → "Apple Inc"
         │
         ▼
    lowercase → "apple inc"
         │
         ▼
    trim → "apple inc"
         │
         ▼
OUTPUT: "apple inc"
```

### Key Points
- ✅ Removes punctuation (keeps letters, numbers, spaces)
- ✅ Collapses multiple spaces into single space
- ✅ Converts to lowercase
- ✅ Trims leading/trailing whitespace
- ✅ Creates new output column
- ⚠️ Row count: **Unchanged**

---

## 14. flatten_json

### Purpose
Flattens nested JSON structures (StructType) into separate columns and explodes arrays (ArrayType) into separate rows.

### PySpark Code
```python
def flatten_json(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    def _flatten(dframe: DataFrame) -> DataFrame:
        complex_fields = [
            (f.name, f.dataType)
            for f in dframe.schema.fields
            if isinstance(f.dataType, (StructType, ArrayType))
        ]

        while complex_fields:
            col_name, dtype = complex_fields.pop(0)

            if isinstance(dtype, StructType):
                expanded = [
                    F.col(f"{col_name}.{f.name}").alias(f"{col_name}_{f.name}")
                    for f in dtype.fields
                ]
                dframe = dframe.select("*", *expanded).drop(col_name)

            elif isinstance(dtype, ArrayType):
                dframe = dframe.withColumn(col_name, F.explode_outer(col_name))

            complex_fields = [
                (f.name, f.dataType)
                for f in dframe.schema.fields
                if isinstance(f.dataType, (StructType, ArrayType))
            ]

        return dframe

    return _flatten(df)
```

### YAML Configuration
```yaml
flatten_json: { enabled: true }
```

### Example 1: StructType (Nested Object)

#### Sample Input
| id | customer |
|----|----------|
| 1 | {"name": "John", "email": "john@email.com", "age": 30} |
| 2 | {"name": "Alice", "email": "alice@email.com", "age": 25} |

#### Sample Output
| id | customer_name | customer_email | customer_age |
|----|---------------|----------------|--------------|
| 1 | "John" | "john@email.com" | 30 |
| 2 | "Alice" | "alice@email.com" | 25 |

### Example 2: ArrayType (List)

#### Sample Input
| id | tags |
|----|------|
| 1 | ["python", "spark", "data"] |
| 2 | ["java", "scala"] |

#### Sample Output
| id | tags |
|----|------|
| 1 | "python" |
| 1 | "spark" |
| 1 | "data" |
| 2 | "java" |
| 2 | "scala" |

### Example 3: Combined Nested Structure

#### Sample Input
| order_id | customer | items |
|----------|----------|-------|
| 101 | {"name": "John", "city": "NYC"} | [{"product": "Laptop", "price": 1000}, {"product": "Mouse", "price": 25}] |

#### Sample Output
| order_id | customer_name | customer_city | items_product | items_price |
|----------|---------------|---------------|---------------|-------------|
| 101 | "John" | "NYC" | "Laptop" | 1000 |
| 101 | "John" | "NYC" | "Mouse" | 25 |

### Key Points
- ✅ StructType → Expands to columns (naming: `parent_child`)
- ✅ ArrayType → Explodes to rows
- ✅ Recursively flattens nested structures
- ✅ Uses `explode_outer` to preserve NULLs
- ⚠️ Row count: **MAY INCREASE** ⬆️ (when arrays are exploded)

---

## 15. schema_validation

### Purpose
Validates that required columns exist in the DataFrame. Raises an error and stops the pipeline if any required columns are missing.

### PySpark Code
```python
def schema_validation(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    required = cfg.get("required_columns", [])
    missing = [c for c in required if c not in df.columns]

    if missing:
        raise ValueError(f"❌ Missing required columns: {missing}")

    return df
```

### YAML Configuration
```yaml
schema_validation: { required_columns: [customer_id, email, created_at, status] }
```

### Example 1: Validation Passes

#### DataFrame Columns
```
customer_id, email, created_at, status, name, phone
```

#### Result
✅ Validation passes - all required columns exist

### Example 2: Validation Fails

#### DataFrame Columns
```
customer_id, email, name, phone
```

#### Required Columns
```
customer_id, email, created_at, status
```

#### Result
```
❌ Error: Missing required columns: ['created_at', 'status']
```
Pipeline stops execution.

### Key Points
- ✅ Checks for required columns before processing
- ✅ Prevents silent failures from missing data
- ❌ Raises error if columns are missing
- ⚠️ Row count: **Unchanged** (or error)

---

## 16. json_key_default

### Purpose
Adds missing columns with default values. Useful when JSON data might have missing keys.

### PySpark Code
```python
def json_key_default(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    for col_cfg in cfg.get("columns", []):
        name = col_cfg["name"]
        default = col_cfg.get("default")

        if name not in df.columns:
            df = df.withColumn(name, F.lit(default))

    return df
```

### YAML Configuration
```yaml
json_key_default: { columns: [
  { name: country, default: "Unknown" },
  { name: verified, default: false },
  { name: score, default: 0.0 },
  { name: category, default: "General" }
]}
```

### Sample Input
DataFrame has columns: `customer_id`, `name`, `email`

| customer_id | name | email |
|-------------|------|-------|
| "C001" | "John" | "john@email.com" |
| "C002" | "Alice" | "alice@email.com" |

### Sample Output
DataFrame now has additional columns with defaults:

| customer_id | name | email | country | verified | score | category |
|-------------|------|-------|---------|----------|-------|----------|
| "C001" | "John" | "john@email.com" | "Unknown" | false | 0.0 | "General" |
| "C002" | "Alice" | "alice@email.com" | "Unknown" | false | 0.0 | "General" |

### Key Points
- ✅ Only adds columns that don't exist
- ✅ Existing columns are not modified
- ✅ All rows get the same default value
- ⚠️ Row count: **Unchanged**

---

## 17. array_size_validation

### Purpose
Replaces empty arrays with NULL. Useful before explode operations to avoid unexpected results with empty arrays.

### PySpark Code
```python
def array_size_validation(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    for col_name in cfg.get("columns", []):
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.when(F.size(F.col(col_name)) == 0, None)
                 .otherwise(F.col(col_name))
            )
    return df
```

### YAML Configuration
```yaml
array_size_validation: { columns: [tags, categories, items] }
```

### Sample Input
| id | tags | categories | items |
|----|------|------------|-------|
| 1 | ["a", "b", "c"] | [] | ["item1"] |
| 2 | [] | ["cat1", "cat2"] | [] |
| 3 | ["x"] | ["cat3"] | ["item2", "item3"] |
| 4 | NULL | [] | NULL |

### Sample Output
| id | tags | categories | items |
|----|------|------------|-------|
| 1 | ["a", "b", "c"] | NULL | ["item1"] |
| 2 | NULL | ["cat1", "cat2"] | NULL |
| 3 | ["x"] | ["cat3"] | ["item2", "item3"] |
| 4 | NULL | NULL | NULL |

### What Changed
- Row 1: `categories` [] → NULL
- Row 2: `tags` [] → NULL, `items` [] → NULL
- Row 4: `categories` [] → NULL

### Key Points
- ✅ Empty arrays `[]` become NULL
- ✅ Non-empty arrays are unchanged
- ✅ Already NULL values stay NULL
- ✅ Useful before `explode` operations
- ⚠️ Row count: **Unchanged**

---

## Summary Table

| # | Transformation | Purpose | Row Count Impact |
|---|----------------|---------|------------------|
| 1 | `trim_strings` | Remove whitespace from strings | Unchanged |
| 2 | `regex_clean` | Clean with regex patterns | Unchanged |
| 3 | `parse_numeric` | Convert strings to numbers | Unchanged |
| 4 | `parse_date` | Parse date strings | Unchanged |
| 5 | `normalize_status` | Standardize categorical values | Unchanged |
| 6 | `null_handling` | Drop rows with NULL values | **⬇️ Decreases** |
| 7 | `duplicate_handling` | Remove duplicate rows | **⬇️ Decreases** |
| 8 | `rename_columns` | Rename column names | Unchanged |
| 9 | `standardize_column_case` | Convert to snake_case | Unchanged |
| 10 | `fill_missing` | Fill NULL with defaults | Unchanged |
| 11 | `surrogate_key` | Generate UUID keys | Unchanged |
| 12 | `outlier_capping` | Cap values in range | Unchanged |
| 13 | `fuzzy_normalize` | Normalize text for matching | Unchanged |
| 14 | `flatten_json` | Flatten nested JSON | **⬆️ May Increase** |
| 15 | `schema_validation` | Validate required columns | Unchanged (or Error) |
| 16 | `json_key_default` | Add missing columns | Unchanged |
| 17 | `array_size_validation` | Empty arrays → NULL | Unchanged |

---

## Execution Order

Transformations are executed based on the `order` field in YAML:

```yaml
transformations:
  - { order: 0, trim_strings: { ... } }      # Executes 1st
  - { order: 1, regex_clean: { ... } }       # Executes 2nd
  - { order: 2, parse_numeric: { ... } }     # Executes 3rd
  - { order: 3, null_handling: { ... } }     # Executes 4th
  # ... and so on
```

### Recommended Order
1. **Data Cleaning** (trim, regex_clean)
2. **Type Conversion** (parse_numeric, parse_date)
3. **Value Standardization** (normalize_status)
4. **Data Quality** (null_handling, duplicate_handling)
5. **Column Operations** (rename, standardize_case)
6. **Fill Defaults** (fill_missing)
7. **Add Keys** (surrogate_key)
8. **Final Adjustments** (outlier_capping)

---

## Best Practices

1. **Always trim strings first** - Clean whitespace before other operations
2. **Parse types before filtering** - Convert to proper types before null/duplicate handling
3. **Standardize before deduplication** - Normalize values to catch more duplicates
4. **Add surrogate keys last** - Generate keys after all data cleaning
5. **Use schema_validation early** - Fail fast if required data is missing
6. **Test with sample data** - Verify transformations work as expected

---

## Author
Databricks Medallion Pipeline Framework
