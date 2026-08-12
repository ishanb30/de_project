
import pytest
from src.fetch import _validate_watermark, _convert_watermark_to_epoch_ms, _get_lookback_window_mins, _validate_recently_played
from datetime import datetime, timezone, timedelta

#_validate_watermark() tests
def test_return_none():
    watermark = datetime(
        2002, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc
    )

    assert _validate_watermark(watermark) is None

def test_invalid_watermark():
    watermark = datetime.now(timezone.utc) + timedelta(days=1)

    with pytest.raises(RuntimeError):
        _validate_watermark(watermark)

#_convert_watermark_to_epoch_ms() tests
def test_watermark_is_none():
    run_id = "test1"
    watermark = None
    lookback_window_mins = 75

    assert _convert_watermark_to_epoch_ms(run_id, watermark, lookback_window_mins) is None

def test_watermark_valid_datetime():
    run_id = "test2"
    watermark = datetime(
        2002, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc
    )
    lookback_window_mins = 75

    expected_ms = 1009838700000
    result = _convert_watermark_to_epoch_ms(run_id, watermark, lookback_window_mins)

    assert isinstance(result, int)
    assert result == expected_ms

#_get_lookback_window_mins() tests
def test_lookback_window_mins():
    run_id = "test3"
    result = _get_lookback_window_mins(run_id)

    assert isinstance(result, int)
    assert result > 0

#_validate_recently_played() tests
def test_json_response_dict():
    data = []

    with pytest.raises(RuntimeError, match="not a dict"):
        _validate_recently_played(data)

def test_missing_items():
    data = {}

    with pytest.raises(RuntimeError, match="Required top-level key"):
        _validate_recently_played(data)

def test_empty_items():
    data = {"items": []}

    assert _validate_recently_played(data) is None

def test_required_keys():
    data = {"items": [{"track": "Let It Happen", "played_at": "2026-08-12T08:52:57Z"}]}

    with pytest.raises(RuntimeError, match="Required key"):
        _validate_recently_played(data)

def test_non_nulls():
    data = {"items": [{"track": "Let It Happen", "played_at": "", "context": "playlist"}]}

    with pytest.raises(ValueError):
        _validate_recently_played(data)

def test_valid_item():
    data = {"items": [{"track": "Let It Happen", "played_at": "2026-08-12T08:52:57Z", "context": "playlist"}]}

    assert _validate_recently_played(data) is None





