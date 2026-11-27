"""Behavioral checks for lifetime history, including incomplete evidence."""

import unittest
from datetime import UTC, datetime

from speedrun_etl.runner_tenure import audit_history, reconstruct


class RunnerTenureTests(unittest.TestCase):
    def seed(self):
        return [(0, chr(65 + i), 100 + i * 10) for i in range(10)]

    def test_self_improvements_do_not_reset_or_multiply_credit(self):
        s, _ = reconstruct(self.seed(), 30 * 86400)
        improved, _ = reconstruct(
            self.seed() + [(5 * 86400, "A", 90), (10 * 86400, "A", 80)], 30 * 86400
        )
        self.assertAlmostEqual(s["A"]["quality_days"], improved["A"]["quality_days"])
        self.assertEqual(improved["A"]["self_improvements"], 2)
        self.assertEqual(improved["A"]["episodes"], 1)

    def test_loss_keeps_earned_days_and_return_adds_an_episode(self):
        challengers = [(10 * 86400, p, v) for p, v in [("D", 90), ("E", 80), ("F", 70)]]
        before, _ = reconstruct(self.seed(), 10 * 86400)
        lost, _ = reconstruct(self.seed() + challengers, 30 * 86400)
        returned, _ = reconstruct(
            self.seed() + challengers + [(20 * 86400, "A", 60)], 30 * 86400
        )
        self.assertEqual(before["A"]["quality_days"], lost["A"]["quality_days"])
        self.assertGreater(returned["A"]["quality_days"], lost["A"]["quality_days"])
        self.assertEqual(returned["A"]["episodes"], 2)

    def test_no_credit_before_tenth_player_and_ties_share_slots(self):
        events = [self.seed()[0]] + [(10 * 86400, p, v) for _, p, v in self.seed()[1:]]
        s, _ = reconstruct(events, 20 * 86400)
        self.assertEqual(s["A"]["days"][0], 10)
        tied, _ = reconstruct([(0, str(i), 100) for i in range(10)], 10 * 86400)
        self.assertEqual(len({s["quality_days"] for s in tied.values()}), 1)
        self.assertLess(tied["0"]["quality_days"], s["A"]["quality_days"])

    def test_audit_rejects_missing_future_and_unmatched_end_state(self):
        at = datetime(2026, 1, 1, tzinfo=UTC)
        end = at.timestamp() + 86400
        status, _ = audit_history([("a", 10, None)], {"a": 10}, end)
        self.assertIn("missing_date", status[0])
        status, _ = audit_history([("a", 10, at)], {"a": 9}, end)
        self.assertIn("end_state_mismatch", status[0])
        status, _ = audit_history([("a", 10, at)], {"a": 10}, at.timestamp() - 1)
        self.assertIn("future_date", status[0])


if __name__ == "__main__":
    unittest.main()
