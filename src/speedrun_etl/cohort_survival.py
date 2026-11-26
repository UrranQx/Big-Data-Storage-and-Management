"""Kaplan-Meier curves for observable calendar time to a fixed target."""

from collections import Counter
from itertools import groupby


def survival_curve(observations):
    events = Counter()
    censored = Counter()
    for reached, followed in observations:
        if reached is not None:
            events[float(reached)] += 1
        else:
            censored[float(followed)] += 1
    risk = sum(events.values()) + sum(censored.values())
    initial = risk
    survival = 1.0
    for day in sorted(events.keys() | censored.keys()):
        deaths = events[day]
        if deaths:
            survival *= 1.0 - deaths / risk
        yield day, risk, deaths, censored[day], 100.0 * (1.0 - survival), initial
        risk -= deaths + censored[day]


def build_survival(conn):
    conn.execute("""CREATE TABLE cohort_survival (
        scope_id text,target_code text,entry_year integer,start_gap_bin integer,
        day double precision,at_risk bigint,reached_people bigint,censored_people bigint,
        reached_pct double precision,cohort_size bigint
    )""")
    with conn.cursor(name="cohort_input") as cursor:
        cursor.itersize = 5000
        cursor.execute("""
            SELECT scope_id,target_code,entry_year,start_gap_bin,days_to_target,followup_days
            FROM cohort_member WHERE NOT already_at_target AND target_code<>'top25pct'
            ORDER BY scope_id,target_code,entry_year,start_gap_bin
        """)
        for key, members in groupby(cursor, key=lambda row: row[:4]):
            rows = [(row[4], row[5]) for row in members]
            if len(rows) < 30:
                continue
            curve = list(survival_curve(rows))
            with conn.cursor().copy("COPY cohort_survival FROM STDIN") as copy:
                for values in curve:
                    copy.write_row((*key, *values))
    conn.execute(
        "CREATE INDEX ON cohort_survival (scope_id,target_code,entry_year,start_gap_bin)"
    )
