import os
import sys

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark
spark = (
    SparkSession.builder
    .appName("InsuranceAnalytics")
    .master("local[2]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# 1. Load dataset
df = spark.read.csv(
    "data/insurance_data.csv",
    header=True,
    inferSchema=True
)

# 2. Explore data
print("Original record count:", df.count())

df.printSchema()
df.show(5)

# 3. Check missing values
print("Missing industry values:")
df.filter(F.col("industry").isNull()).count()

print(df.filter(F.col("industry").isNull()).count())

# 4. Remove duplicate records
df = df.dropDuplicates(["policy_id"])

# 5. Handle missing values
df = df.fillna({"industry": "Unknown"})

# 6. Feature engineering
df = df.withColumn(
    "loss_ratio",
    F.col("claim_amount") / F.col("premium")
)

df = df.withColumn(
    "has_claim",
    F.when(F.col("claim_count") > 0, 1).otherwise(0)
)

# 7. Validate cleaned dataset
print("Cleaned record count:", df.count())

print(
    "Remaining missing industry values:",
    df.filter(F.col("industry").isNull()).count()
)

df.show(5)

# 8. Insurance analytics by state
state_summary = df.groupBy("state").agg(
    F.count("policy_id").alias("total_policies"),
    F.sum("premium").alias("total_premium"),
    F.sum("claim_count").alias("total_claims"),
    F.sum("claim_amount").alias("total_losses")
)

state_summary = (
    state_summary
    .withColumn(
        "claim_frequency",
        F.round(F.col("total_claims") / F.col("total_policies"), 4)
    )
    .withColumn(
        "claim_severity",
        F.round(F.try_divide("total_losses", "total_claims"), 2)
    )
    .withColumn(
        "loss_ratio",
        F.round(F.col("total_losses") / F.col("total_premium"), 4)
    )
)

print("\nInsurance Performance by State:")
state_summary.orderBy(F.desc("loss_ratio")).show()

# 9. Register DataFrame as a SQL temporary view
df.createOrReplaceTempView("insurance")

# Analyze insurance performance by industry
industry_summary = spark.sql("""
    SELECT
        industry,
        COUNT(*) AS total_policies,
        ROUND(SUM(premium), 2) AS total_premium,
        SUM(claim_count) AS total_claims,
        ROUND(SUM(claim_amount), 2) AS total_losses,
        ROUND(SUM(claim_count) / COUNT(*), 4) AS claim_frequency,
        ROUND(TRY_DIVIDE(SUM(claim_amount), SUM(claim_count)), 2)
            AS claim_severity,
        ROUND(SUM(claim_amount) / SUM(premium), 4) AS loss_ratio
    FROM insurance
    GROUP BY industry
    ORDER BY loss_ratio DESC
""")

print("\nInsurance Performance by Industry:")

industry_summary.select(
    "industry",
    "total_policies",
    F.format_number("total_premium", 2).alias("total_premium"),
    "total_claims",
    F.format_number("total_losses", 2).alias("total_losses"),
    F.concat(
        F.format_number(F.col("claim_frequency") * 100, 2),
        F.lit("%")
    ).alias("claim_frequency"),
    F.format_number("claim_severity", 2).alias("claim_severity"),
    F.concat(
        F.format_number(F.col("loss_ratio") * 100, 2),
        F.lit("%")
    ).alias("loss_ratio")
).show(truncate=False)

# 10. Create state-to-region mapping
regions = spark.sql("""
    SELECT * FROM VALUES
        ('MD', 'Northeast'),
        ('VA', 'South'),
        ('NY', 'Northeast'),
        ('CA', 'West'),
        ('TX', 'South'),
        ('FL', 'South')
    AS regions(state, region)
""")

# Join insurance data with region mapping
df = df.join(regions, on="state", how="left")

print("\nJoined Dataset:")
df.show(5)

# Aggregate premiums and losses by region
region_summary = df.groupBy("region").agg(
    F.count("policy_id").alias("total_policies"),
    F.round(F.sum("premium"), 2).alias("total_premium"),
    F.round(F.sum("claim_amount"), 2).alias("total_losses")
)

print("\nInsurance Performance by Region:")
region_summary.show()

# 11. Window functions
from pyspark.sql.window import Window

window_spec = Window.partitionBy("state").orderBy(
    F.desc("claim_amount"),
    F.asc("policy_id")
)

ranked_df = df.withColumn(
    "claim_rank",
    F.row_number().over(window_spec)
)

# Identify top 3 policies by claim amount in each state
top_claims = ranked_df.filter(F.col("claim_rank") <= 3)

print("\nTop 3 Policies by Claim Amount in Each State:")

top_claims.select(
    "state",
    "policy_id",
    "industry",
    "claim_amount",
    "claim_rank"
).orderBy("state", "claim_rank").show(18)

# 12. Export processed data and analytical results

df.write.mode("overwrite").parquet(
    "output/cleaned_insurance_data"
)

state_summary.write.mode("overwrite").parquet(
    "output/state_summary"
)

industry_summary.write.mode("overwrite").parquet(
    "output/industry_summary"
)

region_summary.write.mode("overwrite").parquet(
    "output/region_summary"
)

print("\nAll datasets exported successfully.")

spark.stop()