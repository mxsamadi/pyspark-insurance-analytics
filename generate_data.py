import numpy as np
import pandas as pd
from pathlib import Path

# Reproducibility
rng = np.random.default_rng(42)

n = 100_000

# Generate insurance policy data
df = pd.DataFrame({
    "policy_id": np.arange(1, n + 1),
    "state": rng.choice(
        ["MD", "VA", "NY", "CA", "TX", "FL"], n
    ),
    "industry": rng.choice(
        ["Retail", "Construction", "Manufacturing",
         "Technology", "Healthcare"], n
    ),
    "premium": rng.uniform(1000, 25000, n).round(2),
    "claim_count": rng.poisson(0.3, n),
    "policy_year": rng.choice(
        [2022, 2023, 2024, 2025], n
    )
})

# Generate total claim amounts
df["claim_amount"] = (
    rng.gamma(2, 3000, n) * df["claim_count"]
).round(2)

# Introduce missing values for data cleaning
df.loc[rng.choice(n, 500, replace=False), "industry"] = None

# Introduce duplicate records
df = pd.concat([df, df.iloc[:200]], ignore_index=True)

# Save dataset
Path("data").mkdir(exist_ok=True)

df.to_csv("data/insurance_data.csv", index=False)

print("Dataset generated successfully.")
print("Total records:", len(df))
print(df.head())
