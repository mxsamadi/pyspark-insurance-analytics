# Insurance Claims Analytics Using PySpark

## Project Overview

An end-to-end insurance data processing and analytics pipeline built using PySpark and Spark SQL. The project demonstrates practical Apache Spark capabilities through insurance policy and claims analysis.

## Dataset

The project uses 100,000 synthetic insurance policies, with intentionally introduced missing values and duplicate records to demonstrate data quality management.

## Key Features

- Data ingestion using Spark DataFrames
- Missing value handling and duplicate removal
- Feature engineering
- Data aggregation and transformation
- Spark SQL queries
- DataFrame joins
- Window functions and ranking
- Parquet data export

## Business Metrics

- Total premiums
- Total claims
- Total losses
- Claim frequency
- Claim severity
- Loss ratio
- Top policies by claim amount

## Data Quality Results

| Metric | Result |
|---|---:|
| Original records | 100,200 |
| Cleaned records | 100,000 |
| Duplicates removed | 200 |
| Missing industry values before cleaning | 502 |
| Missing industry values after cleaning | 0 |

## Technologies

- Python 3.12
- Apache Spark
- PySpark 4.0.1
- Spark SQL
- Java 17
- Pandas
- NumPy
- Apache Parquet

## How to Run

Install the required dependencies:

`python -m pip install -r requirements.txt`

Generate the synthetic dataset:

`python generate_data.py`

Run the analytics pipeline:

`python analysis.py`

The processed datasets and analytical summaries are exported to the output directory.

## Project Limitations

The dataset is synthetic and does not represent actual insurance risk. Spark runs in local mode rather than on a multi-node cluster. The objective is to demonstrate practical PySpark skills rather than benchmark distributed computing performance.