
# Hotel Booking Analytics Platform | Lakeflow Spark Declarative Pipelines

![Architecture Overview](images/cover_image.png)

An automated, end-to-end Data Engineering pipeline built on **Databricks** using **Lakeflow Spark Declarative Pipelines (SQL)** and **Unity Catalog Volumes**. This project transforms a raw, messy hotel bookings CSV export into trusted, business-ready Gold summary tables and an interactive 3-page dashboard using the **Medallion Architecture**.

---

## 🏨 Business Background & Problem Statement
A hotel group operating **10 properties across 7 Indian cities** (Goa, Jaipur, Udaipur, Kochi, Shimla, Mumbai, and Bengaluru) needed a reliable, automated reporting pipeline. Guests book rooms (Standard, Deluxe, Suite, Family Room) across multiple channels (Website, Mobile App, Travel Agent, Walk In).

The raw CSV export (`1,316 rows`, `20 columns`) suffered from common real-world data issues:
* **Formatting inconsistencies:** Extra leading/trailing spaces and mixed capitalization in `city`, `room_type`, `booking_channel`, and `meal_plan`.
* **Missing values:** Blanks in `city`, `booking_channel`, `guest_id`, `nights`, and `total_amount`.
* **Duplicate transactions:** Repeated `booking_id` entries inflating booking counts and revenue.
* **Invalid records:** Impossible stay dates (`check_out_date <= check_in_date`) and negative room rates.
* **Untrusted revenue metrics:** Mixing cancelled stays with completed stays in financial totals.

---

## 📂 Repository Structure

```text
├── bronze/
│   └── my_transformation.sql    # Streaming ingestion from Unity Catalog Volume
├── silver/
│   └── my_transformation.sql    # Data cleaning, deduplication & quarantine expectations
├── gold/
│   └── my_transformation.sql    # 5 business summary materialized views & reconciliation check
├── images/
│   └── cover_image.png          # End-to-end Medallion Architecture diagram
└── README.md
```

---

## 🏗️ Medallion Architecture & Layer Responsibilities

| Layer | Table Type | Purpose & Transformations | Output |
| :--- | :--- | :--- | :--- |
| **Bronze** | Streaming Table | Ingests raw `hotel_bookings_raw.csv` incrementally from a Unity Catalog Volume with source file name and load timestamp metadata. | `bronze_hotel_bookings` (1,316 rows) |
| **Silver** | Materialized Views | Deduplicates by `booking_id`, standardizes text (`TRIM`, `INITCAP`), imputes blanks (`COALESCE`, `DATEDIFF`), and enforces quality expectations (`ON VIOLATION DROP ROW`) to quarantine bad records. | `silver_bookings_clean` + `silver_bookings_rejected` |
| **Gold** | Materialized Views | Aggregates clean data to answer business requirements (BR-01 to BR-06) and isolates net revenue to `Completed` stays only. | 5 Gold summary tables |
| **Dashboard** | Databricks AI/BI | Visualizes monthly trends, hotel/city performance, room type ratings, and channel cancellation rates. | 1 interactive 3-page dashboard |

---

## 🔧 Data Cleaning Rules & Key Business Formulas

### Silver Cleaning Logic
* **Text Standardization:** Applied `TRIM()` and `INITCAP()` across categorical columns so identical groups do not split in reports.
* **Blank Enrichment:** Used `COALESCE()` and `DATEDIFF(check_out_date, check_in_date)` to fill missing `nights`, and recalculated missing `total_amount` using `nights × room_rate`.
* **Cancelled Bookings:** Retained in `silver_bookings_clean` to accurately measure channel cancellation rates, while excluded from revenue and room nights in Gold.
* **Blank Ratings:** Left as `NULL` rather than replacing with `0` so unrated or cancelled stays do not artificially lower average guest satisfaction scores.

### Core Business Formulas
* **Stay Duration:** `nights = DATEDIFF(check_out_date, check_in_date)`
* **Gross Amount:** `total_amount = nights × room_rate`
* **Net Revenue:** `total_amount − discount_amount` *(Completed bookings only)*
* **Room Nights:** `SUM(nights)` *(Completed bookings only)*
* **Average Daily Rate (ADR):** `net_revenue ÷ room_nights`

---

## 📊 Business Requirements Addressed
* **BR-01 (Monthly Trends):** Tracks total bookings, cancelled bookings, realized room nights, and net revenue by stay month.
* **BR-02 (Hotel Performance):** Compares bookings, room nights, net revenue, and ADR across regions, cities, and individual hotels.
* **BR-03 (Room Type Performance):** Evaluates room class demand, room nights, net revenue, and average guest ratings.
* **BR-04 (Channel Analysis):** Measures booking volume, cancellation counts, cancellation rates, and net revenue by booking channel.
* **BR-05 & BR-06 (Data Quality & Reconciliation):** Proves zero data loss by verifying that `Bronze Rows = Silver Clean + Silver Rejected + Duplicates Removed`.

---

## 🚀 Setup & Execution Guide
1. Upload `hotel_bookings_raw.csv` into your Unity Catalog Volume:
   ```text
   /Volumes/<catalog>/<schema>/<volume>/hotel_bookings_raw.csv
   ```
2. Create a new **Lakeflow Spark Declarative Pipeline** in Databricks and attach the SQL files from `bronze/`, `silver/`, and `gold/`.
3. Configure your target Unity Catalog and schema, then click **Start** to run the pipeline end-to-end.
4. Inspect the `gold_data_quality` table to verify that the reconciliation check passes with zero missing rows.
