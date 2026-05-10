import pandas as pd

# Load classified data
df = pd.read_csv("data/gold/dim_narrative_classified_openai.csv")

# Stratified sampling — proportional to actual distribution
sampled_df = df.groupby('risk_tier', group_keys=False).apply(
    lambda x: x.sample(n=min(len(x), max(1, round(30 * len(x) / len(df)))), random_state=42)
)

# Add blank column for your manual review
sampled_df['your_verdict'] = ''

# Save
sampled_df.to_csv("data/gold/audit_sample_30.csv", index=False)

print(f"Total sampled: {len(sampled_df)}")
print(sampled_df['risk_tier'].value_counts())