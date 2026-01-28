# KNMI Extreme Weather Pipeline (MVP)

This project implements a scalable Spark-based pipeline to identify heatwaves and coldwaves in the Netherlands (Station: De Bilt) using historical 10-minute interval sensor data.

## 🛠 Architectural Choices & Logic

### 1. Medallion Architecture
The pipeline is structured into three distinct layers to ensure data lineage and reliability:
* **Bronze:** Raw data ingestion. Data is stored in its original form with added ingestion metadata (timestamps).
* **Silver:** Data cleaning and standardization. This layer downsamples the 10-minute readings into daily aggregates (Min/Max temperatures) and filters specifically for the De Bilt location (`260_T_a`).
* **Gold:** Business logic layer. This calculates the actual "waves" and prepares the final report.

### 2. Gaps and Islands Pattern
To identify "consecutive days," I utilized the **Gaps and Islands** pattern via Spark Window functions (`row_number` and `date_sub`). 
* **Scalability:** This approach is far superior to iterative Python loops as it allows Spark to distribute the calculation across a cluster, making the solution "horizontally scalable" as required.
* **Logic:** By subtracting a sequence number from the date, consecutive days fall into the same "island" group, which can then be aggregated.

### 3. Column Mapping & Genericity
The `calculate_extreme_weather` function was designed to be polymorphic. It handles both heatwaves and coldwaves by swapping thresholds and operators. For simplicity and code reuse, generic names are used internally and map to the requirement as follows:

| Internal Column | Requirement Mapping (Heatwave) | Requirement Mapping (Coldwave) |
| :--- | :--- | :--- |
| `from_date` | From date | From date |
| `to_date` | To date (inc.) | To date (inc.) |
| `duration_days` | Duration (in days) | Duration (in days) |
| `intense_days` | Number of tropical days | Number of severe frost days |
| `peak_temperature` | Max temperature | Min temperature |

---

## 🚧 Known Limitations & "Corners Cut"
In the interest of time and delivering a functional MVP within the requested window, several "production" features were simplified:

* **Mode Overwrite:** I used `.mode("overwrite")` for all layers. In a true production environment, I would implement incremental loading (Append/Merge) to avoid re-calculating the entire 20-year history every run.
* **Simplified DQ Gate:** Currently, data quality is handled by a basic filter (Range: -20°C to 40°C). This is a "fail-safe" but doesn't account for more nuanced sensor errors.
* **Independence of Processes:** The pipeline currently runs as a single flow. In a mature environment, Bronze, Silver, and Gold would be decoupled into independent scripts or tasks managed by an orchestrator like Airflow.

---

## 📈 Production Roadmap (Next Steps)

If this application were to be moved to a production-ready state, the following improvements would be prioritized:

### 1. Advanced Data Quality (Z-Score)
Instead of static range filters, I would implement **Z-Score based outlier detection**. This would compare incoming 10-minute readings against a rolling mean/standard deviation to identify sensor drift or "ghost" readings that fall within the -20/40 range but are statistically impossible.

### 2. Performance: Partitioning & Auto-Optimize
* **Partitioning:** I would partition the Delta tables by `year` to enable partition pruning, ensuring Spark only reads the data relevant to the calculation period.
* **Auto-Optimize:** For high-frequency 10-minute data, I would enable Delta's `autoOptimize` and `optimizedWrite` to prevent the "small files problem" and maintain high read speeds.

### 3. Transition to Streaming
Since meteorological data is often a continuous stream, I would transition the Bronze layer to **Structured Streaming**. Using `Trigger.AvailableNow`, the pipeline could process new data arriving in the landing zone incrementally, drastically reducing compute costs.

### 4. Test Coverage
While core logic is covered, I would expand unit tests to include:
* **Continuity Gaps:** How the pipeline handles a missing day in the source data.
* **Schema Evolution:** Ensuring the pipeline doesn't break if KNMI adds new columns in the future.
* **Timezone Logic:** Explicitly handling UTC to CET/CEST conversions for the De Bilt station.