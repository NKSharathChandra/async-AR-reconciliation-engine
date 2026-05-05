import os
import numpy as np
import pandas as pd

# -----------------------------------------
# Generate synthetic AR export for 1,000 customers.
# Includes a small fraction of rows with intentionally corrupted
# balances / components to simulate integration errors.
# -----------------------------------------

OUT_PATH = r"D:\AI Models\ar-audit\data\erp_export.csv"
os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

rng = np.random.default_rng(42)
n = 1000

def rand_money(low, high, size):
    # Create positive financial amounts with 2-decimal precision
    vals = rng.uniform(low, high, size)
    return np.round(vals, 2)

# Base entities
customer_ids = [f"CUST-{i:04d}" for i in range(1, n + 1)]
customer_names = [f"Customer {i:04d}" for i in range(1, n + 1)]

# Components per customer
invoice_total      = rand_money(100, 5000, n)
invoice_applied    = np.minimum(invoice_total, rand_money(0, invoice_total, n))
invoice_rate       = np.round(rng.uniform(0.8, 1.2, n), 4)

payment_total      = rand_money(0, 4000, n)
payment_applied    = np.minimum(payment_total, rand_money(0, payment_total, n))
payment_rate       = np.round(rng.uniform(0.8, 1.2, n), 4)

credit_total       = rand_money(0, 2000, n)
credit_applied     = np.minimum(credit_total, rand_money(0, credit_total, n))
credit_rate        = np.round(rng.uniform(0.8, 1.2, n), 4)

adjust_total       = rand_money(0, 1500, n)
adjust_applied     = np.minimum(adjust_total, rand_money(0, adjust_total, n))
adjust_rate        = np.round(rng.uniform(0.8, 1.2, n), 4)

# True balance by ERP formula:
# Balance = ((inv.total - inv.applied)*inv.rate)
#         - ((pay.total - pay.applied)*pay.rate)
#         - ((cred.total - cred.applied)*cred.rate)
#         + ((adj.total - adj.applied)*adj.rate)
true_balance = (
    (invoice_total - invoice_applied) * invoice_rate
    - (payment_total - payment_applied) * payment_rate
    - (credit_total - credit_applied) * credit_rate
    + (adjust_total - adjust_applied) * adjust_rate
)

# Start with correct balances
customer_balance = np.round(true_balance, 2)

# Inject realistic “integration mistakes” for ~12% rows
error_mask = rng.random(n) < 0.12
for i in np.where(error_mask)[0]:
    mode = rng.integers(0, 4)
    if mode == 0:
        # Misapplied amounts rounding/truncation
        customer_balance[i] = np.round(customer_balance[i] + rng.normal(0, 25), 2)
    elif mode == 1:
        # Wrong exchange rate used in balance
        wrong_rate = rng.choice([0.85, 0.95, 1.05, 1.15])
        customer_balance[i] = np.round(
            (invoice_total[i] - invoice_applied[i]) * wrong_rate
            - (payment_total[i] - payment_applied[i]) * wrong_rate
            - (credit_total[i] - credit_applied[i]) * wrong_rate
            + (adjust_total[i] - adjust_applied[i]) * wrong_rate,
            2
        )
    elif mode == 2:
        # Dropped a component (e.g., credits not included)
        customer_balance[i] = np.round(
            (invoice_total[i] - invoice_applied[i]) * invoice_rate[i]
            - (payment_total[i] - payment_applied[i]) * payment_rate[i]
            # - credits omitted
            + (adjust_total[i] - adjust_applied[i]) * adjust_rate[i],
            2
        )
    else:
        # Applied > total due to bad ETL
        bump = np.round(rng.uniform(10, 80), 2)
        customer_balance[i] = np.round(customer_balance[i] + bump, 2)

df = pd.DataFrame({
    "Customer ID": customer_ids,
    "Customer Name": customer_names,
    "Customer Balance": customer_balance,
    "Invoice Total": invoice_total,
    "Invoice applied amount": invoice_applied,
    "Invoice exchange rate": invoice_rate,
    "Payment Total": payment_total,
    "Payment applied amount": payment_applied,
    "Payment exchange rate": payment_rate,
    "Credit Total": credit_total,
    "Credit applied amount": credit_applied,
    "Credit exchange rate": credit_rate,
    "Adjustment Total": adjust_total,
    "Adjustment applied amount": adjust_applied,
    "Adjustment exchange rate": adjust_rate,
})

df.to_csv(OUT_PATH, index=False)
print(f"✅ ERP export generated: {OUT_PATH} ({len(df)} rows)")
print(f"   Injected error rows: {error_mask.sum()} ({error_mask.mean()*100:.1f}%)")
