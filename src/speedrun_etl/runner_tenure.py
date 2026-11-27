"""Accumulate podium achievements from a comparable, audited solo history."""

import math
from bisect import bisect_left, insort
from collections import defaultdict
from itertools import groupby


def reconstruct(events, end):
    """Return lifetime statistics and final PBs; scores are smaller-is-better.

    events are (UTC epoch seconds, holder_id, exact integer score). All events
    at a timestamp take effect together. No credit before ten unique players.
    """
    best, ordered, active = {}, [], {}
    stats = defaultdict(
        lambda: {
            "quality_days": 0.0,
            "days": [0.0] * 3,
            "episodes": 0,
            "self_improvements": 0,
        }
    )
    last = None

    def accrue(until):
        if last is None:
            return
        days = (until - last) / 86400
        if days < 0:
            raise ValueError("History events must not exceed the cut-off")
        for player, (rank, weight, q) in active.items():
            stats[player]["days"][rank - 1] += days
            stats[player]["quality_days"] += days * weight * q * q

    for at, group in groupby(sorted(events), key=lambda e: e[0]):
        if at > end:
            raise ValueError("Future history event")
        accrue(at)
        improved = set()
        for _, player, score in group:
            old = best.get(player)
            if old is not None and score >= old:
                continue
            if old is not None:
                ordered.pop(bisect_left(ordered, (old, player)))
                improved.add(player)
            best[player] = score
            insort(ordered, (score, player))
        new_active = {}
        n = len(ordered)
        if n >= 10:
            i = 0
            while i < min(3, n):
                j = i + 1
                while j < n and ordered[j][0] == ordered[i][0]:
                    j += 1
                rank = i + 1
                weight = sum((1.0, 0.6, 0.3)[i : min(j, 3)]) / (j - i)
                midrank = rank + (j - i - 1) / 2
                q = (n - 1) / (n - 1 + 20) * (1 - math.log(midrank) / math.log(n))
                for _, player in ordered[i:j]:
                    new_active[player] = (rank, weight, q)
                i = j
        for player in new_active:
            if player not in active:
                stats[player]["episodes"] += 1
            elif player in improved:
                stats[player]["self_improvements"] += 1
        active = new_active
        last = at
    accrue(end)
    return dict(stats), best


def audit_history(rows, current, end):
    """No missing-date fallback: unknown history gets no invented duration."""
    events = []
    missing = future = 0
    for holder, score, verified_at in rows:
        if verified_at is None:
            missing += 1
            continue
        at = verified_at.timestamp()
        if at > end:
            future += 1
            continue
        events.append((at, holder, int(score)))
    statistics, final = reconstruct(events, end)
    mismatches = sum(final.get(p) != score for p, score in current.items())
    extra = len(final.keys() - current.keys())
    reasons = [
        reason
        for reason, count in (
            ("missing_date", missing),
            ("future_date", future),
            ("end_state_mismatch", mismatches + extra),
        )
        if count
    ]
    return (
        ";".join(reasons) or "known",
        missing,
        future,
        mismatches,
        extra,
    ), statistics


def build_tenure(conn):
    """Populate only this unpublished build, in its existing transaction."""
    end = conn.execute("SELECT as_of FROM context").fetchone()[0].timestamp()
    current = defaultdict(dict)
    for sid, holder, score in conn.execute(
        "SELECT scope_id,holder_id,score FROM _ranking_pb WHERE solo_field_size>=10"
    ):
        current[sid][holder] = int(score)
    audits, records, seen = [], [], set()
    with conn.cursor(name="runner_tenure_input") as cursor:
        cursor.itersize = 5000
        cursor.execute("""SELECT r.scope_id,r.holder_id,r.score,r.verified_at
            FROM run r JOIN runner_field f USING(scope_id)
            WHERE cardinality(r.participant_ids)<=1 AND f.solo_field_size>=10
            ORDER BY r.scope_id,r.run_id""")
        for sid, grouped in groupby(cursor, key=lambda r: r[0]):
            seen.add(sid)
            audit, statistics = audit_history(
                [(p, score, at) for _, p, score, at in grouped], current[sid], end
            )
            audits.append((*audit, sid))
            if audit[0] == "known":
                for holder, s in statistics.items():
                    if s["quality_days"] > 0:
                        records.append(
                            (
                                sid,
                                holder,
                                s["quality_days"],
                                *s["days"],
                                s["episodes"],
                                s["self_improvements"],
                            )
                        )
    for sid in current.keys() - seen:
        audits.append(("end_state_mismatch", 0, 0, len(current[sid]), 0, sid))
    with conn.cursor() as cursor:
        cursor.executemany(
            """UPDATE runner_field SET history_status=%s,missing_dates=%s,
            future_dates=%s,mismatched_players=%s,extra_players=%s WHERE scope_id=%s""",
            audits,
        )
        with cursor.copy("COPY runner_tenure FROM STDIN") as copy:
            for record in records:
                copy.write_row(record)
