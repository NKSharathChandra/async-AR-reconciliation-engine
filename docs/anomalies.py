import os
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

# -----------------------------------------
# Train a simple linear regression to learn the balance from
# components (it should match the ERP formula).
# Rows with large residuals are likely integration mistakes.
# -----------------------------------------

IN_PATH  = r"D:\AI Models\ar-audit\data\erp_export.csv"
OUT_PATH = r"D:\AI Models\ar-audit\reports\anomalies_ml.csv"

os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
df = pd.read_csv(IN_PATH)

# Feature set (all components except the reported Customer Balance)
X = df[[
    "Invoice Total","Invoice applied amount","Invoice exchange rate",
    "Payment Total","Payment applied amount","Payment exchange rate",
    "Credit Total","Credit applied amount","Credit exchange rate",
    "Adjustment Total","Adjustment applied amount","Adjustment exchange rate"
]].copy()

y_reported = df["Customer Balance"].values

# Train the model (no split needed here; we use residuals as anomaly signal)
model = LinearRegression()
model.fit(X, y_reported)

y_pred = model.predict(X)
resid = y_reported - y_pred
abs_resid = np.abs(resid)

df_out = df.copy()
df_out["ML Predicted Balance"] = np.round(y_pred, 2)
df_out["Residual"] = np.round(resid, 2)
df_out["Abs Residual"] = np.round(abs_resid, 2)

# Threshold: mean + 2.5*std (robust cut), or >= 5.00 absolute
thr = max(abs_resid.mean() + 2.5 * abs_resid.std(), 5.00)
anomalies = df_out[df_out["Abs Residual"] >= thr].copy()
anomalies = anomalies.sort_values("Abs Residual", ascending=False)

anomalies.to_csv(OUT_PATH, index=False)
print(f"✅ ML anomalies → {OUT_PATH} (rows: {len(anomalies)})")
print(f"   Residual threshold used: {thr:.2f}")
