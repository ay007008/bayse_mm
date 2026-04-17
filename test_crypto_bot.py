import pytest
# Import the actual function and class names from your grep output
from crypto_bot import (
    compute_fair,
    _standard_normal_cdf,
    compute_skewed_bids,
    FillTracker
)

def test_standard_normal_cdf():
    assert pytest.approx(_standard_normal_cdf(0), 0.001) == 0.5

def test_compute_fair():
    # Matches your signature: sol_price, threshold, minutes_left
    fair = compute_fair(sol_price=150.0, threshold=151.0, minutes_left=10.0)
    assert 0 <= fair <= 1

def test_fill_tracker_adverse():
    # is_adverse belongs to the FillTracker class
    tracker = FillTracker()
    # Assuming it starts False until fills are recorded
    assert tracker.is_adverse() is False

def test_compute_skewed_bids():
    # Remove 'fair_up' and use the name found in your grep output
    bids = compute_skewed_bids(up_shares=10, dn_shares=0, fair=0.5)
    assert bids is not None

