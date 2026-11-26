"""Regression checks for simultaneous WRs, competition ties and censored cohorts."""

import unittest
from datetime import UTC, datetime, timedelta

from speedrun_etl.cohort_survival import survival_curve
from speedrun_etl.record_history import reconstruct
from speedrun_etl.timing import duration_ns


class Lab3MathTests(unittest.TestCase):
    def rows(self, entries, direction=1):
        start = datetime(2020, 1, 1, tzinfo=UTC)
        return [
            (
                "scope",
                start + timedelta(days=day),
                holder,
                run,
                seconds * 10**9 * direction,
                direction,
            )
            for day, holder, run, seconds in entries
        ]

    def test_simultaneous_improvements_have_one_record_event(self):
        events = list(
            reconstruct(
                self.rows([(0, "a", "r1", 10), (0, "b", "r2", 8)]), "performed_at"
            )
        )
        records = [event for event in events if event[4] == "wr"]
        self.assertEqual(1, len(records))
        self.assertEqual(["b"], records[0][5])
        self.assertEqual(8, records[0][7])
        self.assertEqual(
            {"a": 2, "b": 1}, {e[5][0]: e[8] for e in events if e[4] == "top10_pb"}
        )

    def test_shared_record_and_competition_ranks(self):
        events = list(
            reconstruct(
                self.rows(
                    [
                        (0, "a", "r1", 10),
                        (0, "b", "r2", 10),
                        (1, "c", "r3", 11),
                        (2, "a", "r4", 9),
                    ]
                ),
                "performed_at",
            )
        )
        records = [e for e in events if e[4] == "wr"]
        self.assertEqual(["a", "b"], records[0][5])
        self.assertEqual(["r1", "r2"], records[1][12])
        self.assertEqual(
            3, next(e[8] for e in events if e[4] == "top10_pb" and e[5] == ["c"])
        )

    def test_place_changes_without_a_new_personal_best(self):
        events = list(
            reconstruct(
                self.rows([(0, "a", "r1", 10), (1, "b", "r2", 9)]), "performed_at"
            )
        )
        changes = [e for e in events if e[4] == "place_change" and e[5] == ["a"]]
        self.assertEqual([1, 2], [e[8] for e in changes])

    def test_larger_is_better(self):
        events = list(
            reconstruct(
                self.rows([(0, "a", "r1", 10), (1, "b", "r2", 12)], -1), "performed_at"
            )
        )
        records = [e for e in events if e[4] == "wr"]
        self.assertEqual([10, 12], [e[7] for e in records])
        self.assertEqual(20, records[1][13])

    def test_slower_attempt_does_not_create_a_record(self):
        events = list(
            reconstruct(
                self.rows([(0, "a", "r1", 10), (1, "a", "r2", 11)]), "performed_at"
            )
        )
        self.assertEqual(1, sum(e[4] == "wr" for e in events))
        self.assertEqual(1, sum(e[4] == "top10_pb" for e in events))

    def test_censoring_reduces_the_later_risk_set(self):
        curve = list(survival_curve([(None, 5), (10, 20)]))
        self.assertEqual((5.0, 2, 0, 1, 0.0, 2), curve[0])
        self.assertEqual((10.0, 1, 1, 0, 100.0, 2), curve[1])

    def test_event_precedes_censoring_at_the_same_time(self):
        self.assertEqual(
            (5.0, 2, 1, 1, 50.0, 2), next(survival_curve([(5, 10), (None, 5)]))
        )

    def test_duration_keeps_exact_precision(self):
        self.assertEqual(1000000001, duration_ns("1.000000001s"))
        self.assertIsNone(duration_ns("1.0000000001s"))
        self.assertIsNone(duration_ns("-1s"))


if __name__ == "__main__":
    unittest.main()
