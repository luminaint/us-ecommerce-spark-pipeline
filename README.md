# US E-Commerce Analytics Pipeline with PySpark on Google Cloud

**How does a large online marketplace turn raw order data into answers about sales, sellers, delivery and payments, at a scale of 1 million orders?**

This project builds an end-to-end big data pipeline on **Google Cloud Dataproc**. It loads seven raw CSV files from HDFS, cleans them, joins them into one analysis table, and answers business questions with PySpark. It then tunes Spark for the cluster and serves the results as Parquet files, a Hive table and CSV.

| Dataset at a glance | Value |
|---|---|
| Orders | **1,000,000** |
| Time range | **2019-01-01 to 2025-12-31** |
| Products | **2,000** across **7 categories** |
| Sellers | **500** |
| Geography | **United States**, built around 10 major regional hubs |
| Source files used | **7 CSVs** (orders, order items, payments, reviews, products, sellers, geolocation) |

> Headline result numbers (revenue, delivery time and so on) are filled in from the notebook output. See [Key findings](#key-findings).

---

## The business problem

An online marketplace collects data in many separate tables: orders, items, payments, reviews, products and sellers. Each table on its own says very little. The business needs them cleaned and joined so it can see which products and sellers drive revenue, how fast orders arrive, how customers pay, and when people shop. At 1 million orders, this is too much data to handle comfortably on a laptop, so the work runs on a Spark cluster in the cloud.

**Main question:** Where does the marketplace's revenue come from, how well are orders being delivered and reviewed, and how can the data pipeline be made fast and reusable for reporting?

## Business questions

| Question | Why a business cares | Notebook section |
|---|---|---|
| How many orders are delivered vs canceled? | Shows how much demand is lost before delivery | Module 1 |
| How do customers pay? | Guides which payment methods to support and promote | Modules 1–2 |
| Which products bring in the most revenue and sell the most units? | Tells merchandising and inventory where to focus | Modules 1, 3 |
| How long does delivery take, and which orders take longest? | Delivery speed drives satisfaction and repeat purchases | Module 1 |
| Which sellers bring in the most revenue, and how are they reviewed? | Identifies sellers to reward and sellers who need support | Module 3 |
| Which products have unstable prices or many competing sellers? | Flags pricing problems and crowded products | Module 3 |
| What is the value of a typical order? | Baseline for promotions and free-shipping thresholds | Modules 2–3 |
| When do people shop (hour of day, weekday vs weekend)? | Times marketing, staffing and site maintenance | Module 3 |
| What mix of small, medium and large products is sold? | Affects shipping costs and warehouse planning | Module 2 |
| How should the data be stored so analysts and BI tools can reuse it? | Avoids redoing the cleaning for every report | Modules 4–5 |

## Dataset overview

The data is the [E-Commerce U.S Dataset (2019–2025, 1 Million rows)](https://www.kaggle.com/datasets/limjeongeun/synthetic-u-s-e-commerce-dataset-1m-orders) on Kaggle. It is **fully synthetic**: it was generated with Python, Faker and rule-based business logic, and it contains no real customer or business data.

| File | What it holds | Key |
|---|---|---|
| `orders.csv` | Order status and timestamps (purchase, approval, carrier, delivery, estimated delivery) | `order_id` |
| `order_items.csv` | One row per item: product, seller, price, freight, discount rate | `order_id` + `order_item_id` |
| `order_payments.csv` | Payment type, installments and amount (one order can have several payments) | `order_id` |
| `order_reviews.csv` | Review score (1–5), comment and dates | `review_id`, `order_id` |
| `products.csv` | Category, name, brand, weight, size, cost and base price | `product_id` |
| `sellers.csv` | Seller company, contact and location | `seller_id` |
| `geolocation.csv` | ZIP prefix with latitude, longitude, city and state | `zip_code_prefix` |

All files are stored in HDFS under `/data/us-ecom/`.

**Things to keep in mind:**
- The original Kaggle dataset has an eighth file, `customers.csv`, which is **not used** in this project. So there is no customer location, age or segment, and no way to link one person's repeat orders.
- In `orders.csv`, `customer_id` is a one-time ID per order. "Per customer" metrics in the notebook are really per order, so they are not used for retention conclusions.
- `products.csv` and `order_items.csv` both have a `price` column. The product one (base retail price) is renamed to `base_price`, and all revenue figures use `order_items.price` (the final selling price).
- Geolocation is joined on the **seller's** ZIP code, because there is no customer ZIP. It is de-duplicated to one row per ZIP so orders are not double-counted.

## Tools and technologies

| Tool | What it did |
|---|---|
| **Google Cloud Dataproc** | Managed Spark/Hadoop cluster where the notebook runs |
| **HDFS** | Stores the raw CSVs and the processed Parquet output on the cluster |
| **PySpark (DataFrame API, Spark SQL)** | Loading, cleaning, joins, aggregations and window functions |
| **Spark MLlib `Imputer`** | Fills missing payment values with the mean or median |
| **Hive metastore** | Registers the cleaned data as tables that can be queried with SQL |
| **Google Cloud Storage** | Optional export location for Parquet output outside the cluster |
| **Jupyter Notebook** | Development environment on the cluster |

### Google Cloud setup

| Component | Configuration |
|---|---|
| Master node | `n2-standard-4` (4 vCPUs, 16 GB RAM, 32 GB disk) |
| Worker nodes | 2 × `n2-standard-4` (4 vCPUs, 16 GB RAM, 64 GB disk each) |
| Total worker capacity | 8 vCPUs, about 32 GB RAM |
| Storage | `pd-balanced` persistent disks (no SSDs, so reducing disk I/O matters) |
| Disabled features | Autoscaling, Dataproc Metastore, advanced execution layer and optimizations |
| Networking | Internal IP enabled |

## How the analysis worked

The pipeline follows five stages, one per module.

| Step | What was done | Why | Tools / methods | Output |
|---|---|---|---|---|
| **1. Ingest and explore** | Loaded the 7 CSVs from HDFS with schema inference; checked row counts, nulls and duplicate `order_id`s; looked at order status, payment type, top products by revenue and delivery time | Understand the data before changing it | `spark.read.csv`, `groupBy`, `datediff` | Data profile and first answers |
| **2. Clean and transform** | Dropped rows missing key IDs; filled or imputed missing values; converted timestamps to dates; relabeled payment types; removed duplicate orders; filtered price outliers at the 1st and 99th percentiles; grouped products into Small, Medium and Large by weight | Make the data consistent and trustworthy | `na.drop`, `fillna`, `Imputer`, `approxQuantile`, `when` | Cleaned tables, saved as Parquet in `/data/us-ecom-proc/` and registered in Hive as `cleaned_products` |
| **3. Integrate and aggregate** | Joined orders → items → products → sellers → geolocation → reviews → payments into one table; repeated it with **broadcast joins** for the small tables; built seller, product and order-value metrics; ranked products within each seller; added delivered/canceled flags, order revenue (price + freight), hour of day and weekday/weekend | Answer the business questions from one consistent table | Joins, `broadcast`, `cache`, aggregations, `Window` + `rank` | Analysis table saved as Parquet in `/data/us-ecom-processed/` |
| **4. Optimize performance** | Tuned Spark for the 2-worker cluster: executor memory and cores, 64 shuffle partitions, adaptive query execution, a 20 MB broadcast threshold and file split sizes; compared broadcast, sort-merge, bucket (repartition) and skew join strategies | Make the pipeline faster and cheaper on limited hardware | `SparkSession.builder.config`, join strategies | Tuned configuration |
| **5. Serve the data** | Wrote the final table as Parquet in HDFS, as a Hive table (`full_order_detail`) and as CSV; optional Parquet export to Google Cloud Storage | Let analysts, SQL users and BI tools use the data without rerunning the pipeline | `write.parquet`, `saveAsTable`, `write.csv` | Parquet, Hive and CSV outputs |

**Techniques used:** data profiling, missing-value handling and imputation, percentile-based outlier filtering, multi-table joins, broadcast joins, caching, window functions, feature engineering, Spark performance tuning, and multi-format data serving.

## Key findings

> **To fill in:** run `US_Ecommerce_Spark_Combined.ipynb` on the cluster and replace each `[ ]` with the value from the notebook output. Nothing below should be filled in from memory.

### 1. Order outcomes
- Orders delivered: `[ ]` (`[ ]`%)
- Orders canceled: `[ ]` (`[ ]`%)

**What it means:** _[write after run]_

### 2. How customers pay
- Most common payment type: `[ ]` (`[ ]`% of payments)
- Full ranking: `[ ]`

**What it means:** _[write after run]_

### 3. Top products
- Top product by revenue: `[ ]` with `$[ ]`
- Top product by units sold: `[ ]`

**What it means:** _[write after run]_

### 4. Delivery time
- Typical delivery time (purchase to delivery): `[ ]` days
- Longest delivery: `[ ]` days

**What it means:** _[write after run]_

### 5. Seller performance
- Top seller revenue: `$[ ]`
- Average review score range across sellers: `[ ]` to `[ ]`

**What it means:** _[write after run]_

### 6. Order value
- Average order value: `$[ ]`
- Price outlier cut-offs (1st / 99th percentile): `$[ ]` / `$[ ]`

### 7. When people shop
- Peak hour of day: `[ ]`
- Weekday vs weekend share of orders: `[ ]`% / `[ ]`%

### 8. Product size mix
- Small / Medium / Large share: `[ ]`% / `[ ]`% / `[ ]`%

## Recommendations

**Business recommendations.** These are **proposed actions** based on the questions above. The "Based on" column should be updated with the finding once the numbers are in. None of them has been implemented or measured.

| Recommendation | Based on | What to measure |
|---|---|---|
| Review the sellers and products with the most canceled orders, and fix stock or listing problems first | Finding 1 (order outcomes) | Cancellation rate by seller and product |
| Promote the payment methods customers already prefer at checkout, and check that less common methods aren't causing drop-off | Finding 2 (payment mix) | Payment-type share; checkout completion |
| Keep the top-revenue products well stocked and feature them in campaigns | Finding 3 (top products) | Stock-outs; revenue share of top products |
| Set a delivery-time target and work with sellers whose orders arrive late | Finding 4 (delivery time) | Average and 90th-percentile delivery days |
| Reward high-revenue, well-reviewed sellers, and give support plans to low-rated ones | Finding 5 (seller performance) | Seller review score; seller revenue |
| Set free-shipping or bundle thresholds just above the average order value | Finding 6 (order value) | Average order value |
| Schedule promotions for peak shopping hours and site maintenance for quiet hours | Finding 7 (shopping times) | Orders and conversion by hour |
| Plan shipping rates and warehouse space around the product size mix | Finding 8 (size mix) | Freight cost per order |

**Engineering recommendations.** These are standard Spark practices applied in this pipeline:

| Recommendation | Why |
|---|---|
| Store processed data as **Parquet** instead of CSV | Columnar and compressed, so it reads much faster and keeps the schema |
| **Broadcast** small tables (sellers, de-duplicated geolocation) in joins | Avoids shuffling the large orders table across the network |
| **Cache** only tables that are reused, and avoid heavy caching on disk-based clusters | Saves recomputation without filling up `pd-balanced` disks |
| Size shuffle partitions to the cluster (about 64 for 8 cores) and turn on adaptive query execution | Fewer tiny tasks and better handling of skewed data |
| Register outputs as **Hive tables** | Analysts and BI tools can query with SQL without rerunning the pipeline |

## Limitations and next steps

**What this analysis cannot tell us:**
- **Customer behavior.** Without `customers.csv` there is no customer location, segment or repeat-purchase link, so retention, lifetime value and customer segmentation are out of scope.
- **Real-world truth.** The data is synthetic. Patterns reflect the generator's rules (for example, review scores are affected by delays and price level), not a real business.
- **Multiple payments and reviews per order.** The joined table has one row per item × payment × review, so revenue totals in Module 3 should be checked against an order-level total to avoid double counting.

**Next steps:**
- Add `customers.csv` back to enable repeat-purchase, segment and customer-location analysis.
- Analyze delivery delay (actual vs estimated delivery date) against review score.
- Use the `discount_rate` and `cost` columns for margin and discount-effectiveness analysis.
- Connect the Hive table to a BI tool (for example Looker Studio or Power BI) for a dashboard.
- Automate the pipeline with Airflow (Cloud Composer) on a schedule.

## How to run the project

**Prerequisites**
- A Google Cloud project with a Dataproc cluster (see [Google Cloud setup](#google-cloud-setup)) with Jupyter enabled
- Spark 3.x with Hive support

**Data**
1. Download the dataset from [Kaggle](https://www.kaggle.com/datasets/limjeongeun/synthetic-u-s-e-commerce-dataset-1m-orders).
2. Upload the 7 CSV files to HDFS:
   ```bash
   hadoop fs -mkdir -p /data/us-ecom/
   hadoop fs -put orders.csv order_items.csv order_payments.csv order_reviews.csv products.csv sellers.csv geolocation.csv /data/us-ecom/
   ```

**Run it**
1. Open `US_Ecommerce_Spark_Combined.ipynb` in Jupyter on the cluster.
2. Run all cells from top to bottom.
3. Optional: put your own bucket name in the Google Cloud Storage cell in Module 5 and uncomment it.

**Output**

| Location | Contents |
|---|---|
| `/data/us-ecom-proc/` | Cleaned order details and products (Parquet) |
| `/data/us-ecom-processed/` | Full joined analysis table (Parquet) |
| `/data/us-ecom-serving/parquet` and `/csv/` | Served copies in Parquet and CSV |
| Hive tables | `cleaned_products`, `full_order_detail` |

## Project structure

```text
spark/
├── README.md                              # this file
├── US_Ecommerce_Spark_Combined.ipynb      # combined notebook (Modules 1–5)
└── Resource/                              # original course notebooks and PDFs (Olist version)
    ├── Module 1 - Data Ingestion and Exploration.ipynb
    ├── Module 2 - Data Cleaning and Transformation.ipynb
    ├── Module 3 - Data Integration and Aggregation.ipynb
    ├── Module 4 - Property Optmization.ipynb
    ├── Module 5 - Data Serving.ipynb
    └── Projects.pdf
```

## Final summary

This project takes a 1-million-order marketplace dataset from raw CSV files in HDFS to clean, joined, query-ready tables on a Google Cloud Dataproc cluster. It answers core business questions about order outcomes, payments, top products, delivery speed, seller performance and shopping times. It also shows how to tune Spark for a small cluster and serve the results in formats that analysts and BI tools can use directly. _[Add one or two sentences on the most important finding once the notebook has been run.]_
