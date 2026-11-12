import json
from pathlib import Path

def test_reports_exist():
    dq = json.loads(Path("reports/data_quality.json").read_text())
    assert dq["lead_rows_total"] == 5087
    assert dq["events_total"] == 12168
    assert dq["duplicate_events_dropped"] == 3640

def test_no_pii_in_reports():
    txt = Path("reports/lead_report.csv").read_text()
    assert "+91******" in txt
    assert "98765" not in txt