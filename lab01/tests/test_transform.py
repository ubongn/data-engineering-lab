from src.transform import transform_orders

def test_valid_record_passes():
    records = [{"order_id": "1", "customer_name": "Test", "amount": "100", "order_date": "2026-01-01", "status": "completed"}]
    valid, errors = transform_orders(records)
    assert len(valid) == 1
    assert len(errors) == 0

def test_negative_amount_rejected():
    records = [{"order_id": "1", "customer_name": "Test", "amount": "-50", "order_date": "2026-01-01", "status": "completed"}]
    valid, errors = transform_orders(records)
    assert len(valid) == 0
    assert len(errors) == 1

def test_missing_order_id_rejected():
    records = [{"order_id": "", "customer_name": "Test", "amount": "100", "order_date": "2026-01-01", "status": "completed"}]
    valid, errors = transform_orders(records)
    assert len(valid) == 0
    assert len(errors) == 1

def test_amount_category_assigned():
    records = [
        {"order_id": "1", "customer_name": "A", "amount": "50", "order_date": "2026-01-01", "status": "ok"},
        {"order_id": "2", "customer_name": "B", "amount": "200", "order_date": "2026-01-01", "status": "ok"},
        {"order_id": "3", "customer_name": "C", "amount": "600", "order_date": "2026-01-01", "status": "ok"},
    ]
    valid, _ = transform_orders(records)
    assert valid[0]["amount_category"] == "small"
    assert valid[1]["amount_category"] == "medium"
    assert valid[2]["amount_category"] == "large"