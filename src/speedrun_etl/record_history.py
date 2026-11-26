"""Retrospective PB/top-ten and WR events with simultaneous submissions batched."""

from bisect import bisect_left, insort
from itertools import groupby

from .jsonutil import digest


def reconstruct(rows, basis):
    """Rows are sorted by scope and event time; score is smaller-is-better."""
    for scope_id, scope_rows in groupby(rows, key=lambda row: row[0]):
        best = {}
        ordered = []
        record_score = None
        record_at = None
        record_runs = []
        record_holders = []
        for at, simultaneous in groupby(scope_rows, key=lambda row: row[1]):
            improved = {}
            together = list(simultaneous)
            previous_places = {
                holder: bisect_left(ordered, (best[holder][0], "")) + 1
                for _, _, holder, _, _, _ in together
                if holder in best
            }
            previous_top = {}
            for score, holder in ordered:
                place = bisect_left(ordered, (score, "")) + 1
                if place > 10:
                    break
                previous_top[holder] = place
            for _, _, holder, run, score, direction in together:
                previous = best.get(holder)
                if previous is not None and score >= previous[0]:
                    continue
                if previous is not None:
                    ordered.pop(bisect_left(ordered, (previous[0], holder)))
                best[holder] = (score, run)
                insort(ordered, (score, holder))
                improved[holder] = (score, run)
            if not improved:
                continue
            first_score = ordered[0][0]
            winner_end = bisect_left(ordered, (first_score + 1, ""))
            holders = [holder for _, holder in ordered[:winner_end]]
            runs = [best[holder][1] for holder in holders]
            prior_seconds = (
                None if record_score is None else record_score * direction / 1e9
            )
            for holder, (score, run) in improved.items():
                rank = bisect_left(ordered, (score, "")) + 1
                if rank <= 10:
                    yield (
                        digest([scope_id, basis, at.isoformat(), "top10_pb", holder]),
                        scope_id,
                        basis,
                        at,
                        "top10_pb",
                        [holder],
                        [run],
                        score * direction / 1e9,
                        rank,
                        len(ordered),
                        prior_seconds,
                        record_at,
                        record_runs,
                        None,
                        previous_places.get(holder),
                    )
            current_top = {}
            for score, holder in ordered:
                place = bisect_left(ordered, (score, "")) + 1
                if place > 10:
                    break
                current_top[holder] = place
            for holder in sorted(previous_top.keys() | current_top.keys()):
                score, run = best[holder]
                place = bisect_left(ordered, (score, "")) + 1
                if previous_top.get(holder) != current_top.get(holder):
                    yield (
                        digest(
                            [scope_id, basis, at.isoformat(), "place_change", holder]
                        ),
                        scope_id,
                        basis,
                        at,
                        "place_change",
                        [holder],
                        [run],
                        score * direction / 1e9,
                        place,
                        len(ordered),
                        prior_seconds,
                        record_at,
                        record_runs,
                        None,
                        previous_top.get(holder),
                    )
            if record_score is None or first_score < record_score:
                change = (
                    None
                    if record_score is None
                    else 100.0 * (record_score - first_score) / abs(record_score)
                )
                yield (
                    digest([scope_id, basis, at.isoformat(), "wr"]),
                    scope_id,
                    basis,
                    at,
                    "wr",
                    holders,
                    runs,
                    first_score * direction / 1e9,
                    1,
                    len(ordered),
                    prior_seconds,
                    record_at,
                    record_runs,
                    change,
                    None,
                )
                record_score, record_at = first_score, at
                record_runs, record_holders = runs, holders
            elif holders != record_holders:
                yield (
                    digest([scope_id, basis, at.isoformat(), "wr_tie"]),
                    scope_id,
                    basis,
                    at,
                    "wr_tie",
                    holders,
                    runs,
                    first_score * direction / 1e9,
                    1,
                    len(ordered),
                    prior_seconds,
                    record_at,
                    record_runs,
                    0.0,
                    None,
                )
                record_runs, record_holders = runs, holders


def build_history(conn):
    conn.execute("""CREATE TABLE record_event (
        event_id text PRIMARY KEY,scope_id text NOT NULL,time_basis text NOT NULL,
        event_at timestamptz NOT NULL,event_kind text NOT NULL,holder_ids text[] NOT NULL,
        run_ids text[] NOT NULL,seconds double precision NOT NULL,place bigint NOT NULL,
        field_size bigint NOT NULL,previous_seconds double precision,previous_record_at timestamptz,
        previous_run_ids text[] NOT NULL,improvement_pct double precision,previous_place bigint
    )""")
    # Flush a bounded buffer between server-cursor fetches.
    for basis in ("performed_at", "submitted_at"):
        with conn.cursor(name="history_input") as cursor:
            cursor.itersize = 5000
            cursor.execute(f"""
                SELECT r.scope_id,r.{basis},r.holder_id,r.run_id,r.score,s.direction
                FROM run r JOIN scope s USING (scope_id)
                WHERE r.{basis} IS NOT NULL AND r.{basis}<=(SELECT as_of FROM context)
                ORDER BY r.scope_id,r.{basis},r.holder_id,r.run_id
            """)
            buffer = []
            for row in reconstruct(cursor, basis):
                buffer.append(row)
                if len(buffer) == 2000:
                    with conn.cursor().copy("COPY record_event FROM STDIN") as copy:
                        for item in buffer:
                            copy.write_row(item)
                    buffer.clear()
            if buffer:
                with conn.cursor().copy("COPY record_event FROM STDIN") as copy:
                    for item in buffer:
                        copy.write_row(item)
    conn.execute("CREATE INDEX ON record_event (scope_id,time_basis,event_at)")
    conn.execute("ANALYZE record_event")
