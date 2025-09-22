import json
from unittest.mock import Mock
import enrich as mod

def test_parse_amount():
    assert mod.parse_amount("12.5") == 12.5
    assert mod.parse_amount("-1") is None
    assert mod.parse_amount("abc") is None

def test_enrich_one_happy_path(monkeypatch):
    session = Mock()

    def fake_get(url, params=None, timeout=None):
        class FakeResp:
            def __init__(self, data): self._data = data
            def raise_for_status(self): pass
            def json(self): return self._data

        if "geocoding-api" in url:
            return FakeResp({"results": [{"latitude": 52.52, "longitude": 13.405}]})
        if "forecast" in url:
            return FakeResp({"current_weather": {"temperature": 12.3, "windspeed": 3.8}})
        if "exchangerate.host" in url:
            return FakeResp({"result": 96.19, "info": {"rate": 1.07}})
        raise AssertionError("Unexpected URL " + url)

    session.get = fake_get

    row = {"city": "Berlin", "country_code": "DE", "local_currency": "EUR", "amount": "89.90"}
    out = mod.enrich_one(row, session, verbose=False)

    assert out.latitude == 52.52 and out.longitude == 13.405
    assert out.temperature_c == 12.3 and out.wind_speed_mps == 3.8
    assert out.fx_rate_to_usd == 1.07 and out.amount_usd == 96.19
    assert out.city == "Berlin" and out.country_code == "DE"