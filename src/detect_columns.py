import pandas as pd

# Load only the header row
df = pd.read_csv('data/bronze/ASRS_DBOnline.csv', skiprows=1, nrows=0)

print("SEARCH RESULTS:")
print("-" * 30)

# Loop through and find the indexes
for i, col in enumerate(df.columns):
    # We check for the technical keywords we need
    if any(word in col for word in ["Component", "Problem", "ACN", "Narrative", "Model"]):
        print(f"Index {i}: {col}")

print("-" * 30)