import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))

import affinity_calibration as ac  # noqa: E402


def test_frame_has_the_columns_the_harness_reads():
    frame = ac.slates_to_frame(num_items=40, num_users=8, slates_per_user=5, seed=3)

    for column in ("user_id", "item_id", "impression_ts", "position", "clicked", "ordered"):
        assert column in frame.columns
    assert len(frame) > 0
    assert frame["clicked"].isin([0, 1]).all()


def test_impression_timestamps_advance_so_a_split_is_possible():
    """resolve_cutoff raises on a single-instant dataset; the frame must span time."""
    frame = ac.slates_to_frame(num_items=40, num_users=8, slates_per_user=5, seed=3)

    assert frame["impression_ts"].nunique() >= 2


def test_measure_reports_both_systems_and_the_support_it_used():
    result = ac.measure(strength=0.15, num_items=40, num_users=12, slates_per_user=6,
                        seed=3, epochs=5)

    assert set(result) >= {"strength", "test_users", "most_popular", "next_item_transformer"}
    assert result["strength"] == 0.15
    for system in ("most_popular", "next_item_transformer"):
        assert "hit_rate@10" in result[system]


def test_zero_strength_and_high_strength_are_distinguishable():
    """The calibration tool must be able to see the effect it exists to size.

    Not an assertion about which system wins — only that turning the knob moves the
    preferred-family share of clicks, which is the mechanism the harness reads.
    """
    flat = ac.preferred_share(strength=0.0, num_items=60, num_users=20, slates_per_user=8, seed=5)
    peaked = ac.preferred_share(strength=0.30, num_items=60, num_users=20, slates_per_user=8, seed=5)

    assert peaked > flat + 0.05
