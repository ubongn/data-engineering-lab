from datetime import datetime

def transform_orders(records) -> tuple[list[dict], list[dict]]:
    """Validate, clean, and enrich order records"""
    valid = []
    errors = []

    for row in records:
        issues = []

        # Validate order_id
        if not row.get("order_id", "").strip():
            issues.append("Missing order_id")

        # Validate and parse amount
        raw_amount = row.get("amount", "").strip()
        if not raw_amount:
            issues.append("Missing amount")
        else:
            try:
                amount = float(raw_amount)
                if amount < 0:
                    issues.append(f"Negative amount: {amount}")
            except ValueError:
                issues.append(f"Invalid amount: {raw_amount}")
                amount = None


        # Validate and parse date
        raw_date = row.get("order_date", "").strip()
        try:
            datetime.strptime(raw_date, "%Y-%m-%d")
        except ValueError:
            issues.append(f"Invalid date: {raw_date}")


        # Validate customer_name
        name = row.get("customer_name", "").strip()
        if not name:
            issues.append("Missing customer_name")

        if issues:
            errors.append({"raw": row, "issues": issues})
            continue

        # Clean and enrich
        cleaned = {
            "order_id": row["order_id"].strip(),
            "customer_name": name,
            "amount": amount,
            "order_date": raw_date,
            "status": row.get("status", "").strip() or "unknown",
        }
        

        # Enrich: amount category
        if amount <= 0:
            cleaned["amount_category"] = "zero"
        elif amount < 100:
            cleaned["amount_category"] = "small"
        elif amount < 500:
            cleaned["amount_category"] = "medium"
        else:
            cleaned["amount_category"] = "large"

        valid.append(cleaned)

    return valid, errors        


