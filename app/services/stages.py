import random
import asyncio

# AR reconciliation stages. Each operates on the running `data` dict
# (initially the submitted payload, then accumulated stage outputs).
# Stages may randomly raise SimulatedFailureError to exercise the
# orchestrator's retry/resume behavior — required by the spec.

FAILURE_RATE = 0.2
TOLERANCE = 0.05  # matches docs/reconcile.py: ignore penny rounding

REQUIRED_COLUMNS = (
    "Customer ID", "Customer Balance",
    "Invoice Total", "Invoice applied amount", "Invoice exchange rate",
    "Payment Total", "Payment applied amount", "Payment exchange rate",
    "Credit Total", "Credit applied amount", "Credit exchange rate",
    "Adjustment Total", "Adjustment applied amount", "Adjustment exchange rate",
)


class SimulatedFailureError(Exception):
    """Transient stage failure — orchestrator re-raises so ARQ retries."""


async def _maybe_fail(stage_name: str):
    await asyncio.sleep(0.05)
    if random.random() < FAILURE_RATE:
        raise SimulatedFailureError(f"{stage_name} failed randomly. Needs retry.")


async def run_ingestion(data: dict) -> dict:
    """Validate the payload carries the expected AR columns."""
    await _maybe_fail("Ingestion")
    missing = [c for c in REQUIRED_COLUMNS if c not in data]
    if missing:
        # Permanent error — not a SimulatedFailureError, so no retry.
        raise ValueError(f"Missing required AR columns: {missing}")
    return {"ingested": True, "customer_id": data["Customer ID"]}


async def run_matching(data: dict) -> dict:
    """Recompute the expected balance using the ERP formula."""
    await _maybe_fail("Matching")
    expected = (
        (data["Invoice Total"] - data["Invoice applied amount"]) * data["Invoice exchange rate"]
        - (data["Payment Total"] - data["Payment applied amount"]) * data["Payment exchange rate"]
        - (data["Credit Total"] - data["Credit applied amount"]) * data["Credit exchange rate"]
        + (data["Adjustment Total"] - data["Adjustment applied amount"]) * data["Adjustment exchange rate"]
    )
    expected = round(expected, 2)
    delta = round(data["Customer Balance"] - expected, 2)
    return {
        "expected_balance": expected,
        "delta": delta,
        "abs_delta": abs(delta),
    }


def _likely_cause(data: dict) -> str:
    if data["Invoice applied amount"] > data["Invoice Total"] + 0.01:
        return "Invoice applied exceeds total"
    if data["Payment applied amount"] > data["Payment Total"] + 0.01:
        return "Payment applied exceeds total"
    if data["Credit applied amount"] > data["Credit Total"] + 0.01:
        return "Credit applied exceeds total"
    if data["Adjustment applied amount"] > data["Adjustment Total"] + 0.01:
        return "Adjustment applied exceeds total"

    components = {
        "Invoice": (data["Invoice Total"] - data["Invoice applied amount"]) * data["Invoice exchange rate"],
        "Payments": -(data["Payment Total"] - data["Payment applied amount"]) * data["Payment exchange rate"],
        "Credits": -(data["Credit Total"] - data["Credit applied amount"]) * data["Credit exchange rate"],
        "Adjustments": (data["Adjustment Total"] - data["Adjustment applied amount"]) * data["Adjustment exchange rate"],
    }
    driver = max(components.items(), key=lambda kv: abs(kv[1]))[0]
    return f"Check {driver}/exchange rate/rounding"


async def run_validation(data: dict) -> dict:
    """Flag rows whose recomputed balance disagrees beyond tolerance."""
    await _maybe_fail("Validation")
    is_valid = data["abs_delta"] <= TOLERANCE
    cause = None if is_valid else _likely_cause(data)
    return {"is_valid": is_valid, "likely_cause": cause}


async def run_decision_routing(data: dict) -> dict:
    """Route based on validation outcome."""
    await _maybe_fail("Routing")
    if data["is_valid"]:
        queue = "AUTO_CLEAR"
    elif data.get("likely_cause") and "exceeds total" in data["likely_cause"]:
        queue = "ANOMALY_QUEUE"
    else:
        queue = "MANUAL_REVIEW"
    return {"routed_to": queue}
