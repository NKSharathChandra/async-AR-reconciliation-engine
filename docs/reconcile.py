import os
import numpy as np
import pandas as pd

# -----------------------------------------
# Recompute balances using the ERP formula and compare
# against the provided "Customer Balance". Flag rows
# beyond tolerance and provide a likely root cause hint.
# -----------------------------------------

IN_PATH  = r"D:\AI Models\ar-audit\data\erp_export.csv"
OUT_PATH = r"D:\AI Models\ar-audit\reports\mismatches_rule_based.csv"

os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
df = pd.read_csv(IN_PATH)

# Recompute expected balance
expected = (
    (df["Invoice Total"] - df["Invoice applied amount"]) * df["Invoice exchange rate"]
    - (df["Payment Total"] - df["Payment applied amount"]) * df["Payment exchange rate"]
    - (df["Credit Total"]  - df["Credit applied amount"])  * df["Credit exchange rate"]
    + (df["Adjustment Total"] - df["Adjustment applied amount"]) * df["Adjustment exchange rate"]
)

df["Expected Balance"] = np.round(expected, 2)
df["Delta"] = np.round(df["Customer Balance"] - df["Expected Balance"], 2)
df["Abs Delta"] = df["Delta"].abs()

# Heuristic: guess which component is likely wrong
def likely_cause(row):
    # Component contributions (signed)
    inv = (row["Invoice Total"] - row["Invoice applied amount"]) * row["Invoice exchange rate"]
    pay = (row["Payment Total"] - row["Payment applied amount"]) * row["Payment exchange rate"]
    cred = (row["Credit Total"] - row["Credit applied amount"]) * row["Credit exchange rate"]
    adj  = (row["Adjustment Total"] - row["Adjustment applied amount"]) * row["Adjustment exchange rate"]

    # Simple rules:
    # - If Delta has same sign as invoice impact and |invoice| dominates, suspect invoice.
    comps = {
        "Invoice": inv,
        "Payments": -pay,
        "Credits": -cred,
        "Adjustments": adj
    }
    # Pick the component with largest absolute magnitude (driver)
    driver = max(comps.items(), key=lambda kv: abs(kv[1]))[0]

    # Additional quick checks
    if row["Invoice applied amount"] > row["Invoice Total"] + 0.01:
        return "Invoice applied exceeds total"
    if row["Payment applied amount"] > row["Payment Total"] + 0.01:
        return "Payment applied exceeds total"
    if row["Credit applied amount"] > row["Credit Total"] + 0.01:
        return "Credit applied exceeds total"
    if row["Adjustment applied amount"] > row["Adjustment Total"] + 0.01:
        return "Adjustment applied exceeds total"

    return f"Check {driver}/exchange rate/rounding"

# Tolerance: 0.01 is “exact”; use 0.05 to ignore penny rounding
TOL = 0.05
bad = df[df["Abs Delta"] > TOL].copy()
bad["Likely Cause"] = bad.apply(likely_cause, axis=1)
bad = bad.sort_values("Abs Delta", ascending=False)

bad.to_csv(OUT_PATH, index=False)
print(f"✅ Rule-based mismatches → {OUT_PATH} (rows: {len(bad)})")
