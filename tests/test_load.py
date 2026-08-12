
import pytest
from src.load import _compute_watermark
from datetime import datetime, timezone

#_compute_watermark() tests
def test_max_datetime():
    data = [{"played_at": "2026-08-10T12:00:00.000Z"}, {"played_at": "2026-08-10T15:00:00.000Z"}]
    expected = datetime(
        2026, 8, 10, 15, 0, 0, 0, tzinfo=timezone.utc
    )
    assert _compute_watermark(data) == expected

def test_key_error():
    data = [{"played_at": "2026-08-10T12:00:00.000Z"}, {"context": "playlist"}]
    with pytest.raises(KeyError):
        _compute_watermark(data)