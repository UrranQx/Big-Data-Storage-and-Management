"""Exercise native Superset filters and assert the returned analytical values."""

import json
import os
import re
import time
from pathlib import Path

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import sync_playwright
from verify_lab3_ui import local_environment

ROOT = Path(__file__).resolve().parents[1]


def main():
    output = ROOT / os.environ.get(
        "LAB3_FILTER_OUTPUT", "reports/lab3/acceptance_20261126"
    )
    output.mkdir(parents=True, exist_ok=True)
    evidence = {}
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            channel=os.environ.get("LAB3_BROWSER_CHANNEL", "msedge"), headless=True
        )
        page = browser.new_page(viewport={"width": 1700, "height": 1100})
        responses = {}
        filter_responses = []

        def capture(response):
            if "/api/v1/chart/data" not in response.url:
                return
            body = response.request.post_data_json or {}
            filter_id = body.get("form_data", {}).get("native_filter_id")
            if filter_id:
                try:
                    result = response.json()
                    filter_responses.append(
                        {
                            "filter": filter_id,
                            "queries": body.get("queries"),
                            "rows": [
                                r.get("data", [])[:3] for r in result.get("result", [])
                            ],
                        }
                    )
                except PlaywrightError:
                    pass
            chart_id = body.get("form_data", {}).get("slice_id")
            if chart_id:
                try:
                    responses[int(chart_id)] = response.json()
                except PlaywrightError:
                    # Navigation cancels unrelated charts; required charts are
                    # checked explicitly by rows() before moving to the next page.
                    pass

        page.on("response", capture)
        page.goto("http://localhost:8088/login/")
        page.locator("#username").fill("admin")
        page.locator("#password").fill(local_environment()["SUPERSET_ADMIN_PASSWORD"])
        page.locator("input[type=submit],button[type=submit]").click()
        page.wait_for_url("**/superset/**")

        def chart_id(title):
            link = page.get_by_role("link", name=title, exact=True)
            link.wait_for()
            return int(re.search(r"slice_id=(\d+)", link.get_attribute("href"))[1])

        def rows(key):
            deadline = time.monotonic() + 100
            while key not in responses and time.monotonic() < deadline:
                page.wait_for_timeout(100)
            assert key in responses, f"Chart {key} did not finish"
            result = responses[key]
            assert "result" in result, result
            assert not result["result"][0].get("error"), result
            return result["result"][0]["data"]

        def select(key, value):
            # Wait for the previous filter update to settle before typing into
            # the next React-controlled, debounced option search.
            page.wait_for_load_state("networkidle", timeout=120000)
            control = page.get_by_role(
                "combobox", name="NATIVE_FILTER-" + key, exact=True
            )
            control.locator(
                "xpath=ancestor::div[contains(@class,'ant-select-selector')]"
            ).click()
            control.fill("")
            control.press_sequentially(value, delay=25)
            option = page.locator(
                ".ant-select-dropdown:not(.ant-select-dropdown-hidden) .ant-select-item-option"
            ).filter(has_text=re.compile("^" + re.escape(value) + "$"))
            try:
                option.first.click(timeout=30000)
            except PlaywrightError:
                print(
                    json.dumps(
                        {
                            "key": key,
                            "input": control.input_value(),
                            "dropdowns": page.locator(
                                ".ant-select-dropdown:not(.ant-select-dropdown-hidden)"
                            ).all_text_contents(),
                            "recent_filter_requests": filter_responses[-6:],
                        },
                        ensure_ascii=True,
                    ),
                    flush=True,
                )
                raise
            control.press("Escape")

        def apply():
            responses.clear()
            page.get_by_role("button", name="Apply filters", exact=True).click()

        page.goto("http://localhost:8088/superset/dashboard/lab3-countries/")
        medals = chart_id("Олимпийский зачёт стран")
        baseline = rows(medals)
        russia = next(row for row in baseline if row["country_name"] == "Россия")
        select("countries-country_name", "Россия")
        apply()
        selected = rows(medals)
        assert selected == [russia], (selected, russia)
        assert selected[0]["overall_rank"] > 1
        evidence["country_filter_preserves_global_rank"] = selected
        page.screenshot(
            path=str(output / "screenshots/country_filter.png"), full_page=True
        )

        responses.clear()
        page.get_by_role("button", name="Clear all", exact=True).click()
        assert rows(medals) == baseline
        select("countries-game_name", "Celeste")
        apply()
        celeste = rows(medals)
        assert celeste and celeste != baseline, responses[medals]
        select("countries-category_name", "Any%")
        apply()
        any_percent = rows(medals)
        assert any_percent and any_percent != celeste
        evidence["game_and_percent_category_filters"] = {
            "game": "Celeste",
            "category": "Any%",
            "countries": len(any_percent),
            "podium_contributions": sum(
                r[k] for r in any_percent for k in ("gold", "silver", "bronze")
            ),
        }

        page.goto("http://localhost:8088/superset/dashboard/lab3-runner/")
        goal = chart_id("Следующая цель")
        initial_goal = rows(goal)
        assert len(initial_goal) == 1 and initial_goal[0]["target_code"] == "top10"
        select("runner-target_code", "top50")
        apply()
        changed_goal = rows(goal)
        assert changed_goal[0]["target_code"] == "top50"
        assert changed_goal[0]["place"] == initial_goal[0]["place"]
        assert (
            changed_goal[0]["improvement_seconds"]
            <= initial_goal[0]["improvement_seconds"]
        )
        evidence["target_filter"] = {"top10": initial_goal, "top50": changed_goal}

        profile = chart_id("Паутинка · при наличии всех 6 осей")
        profile_values = chart_id("Профиль: оценки и наличие данных")
        select("runner-runner", "Chexhuman [5j5o2pzx]")
        apply()
        full_profile = rows(profile)
        values = rows(profile_values)
        assert len(full_profile) == 1
        assert len(values) == 6 and all(r["strength"] is not None for r in values)
        evidence["complete_radar"] = values
        link = page.get_by_role(
            "link", name="Паутинка · при наличии всех 6 осей", exact=True
        )
        panel = page.locator(".dashboard-component-chart-holder").filter(has=link)
        panel.screenshot(path=str(output / "screenshots/radar_complete.png"))

        select("runner-runner", "iTaKu [48gvyr28]")
        apply()
        rows(profile_values)
        select("runner-scope_name", "Celeste · Any% · Все платформы · igt")
        select("runner-target_code", "top10")
        apply()
        survival = chart_id("Календарные дни до цели · Kaplan–Meier")
        curve = rows(survival)
        assert curve, "The known Celeste cohort must produce a survival curve"
        evidence["sufficient_cohort"] = {
            "runner": "iTaKu",
            "game": "Celeste",
            "points": len(curve),
        }
        curve_link = page.get_by_role(
            "link", name="Календарные дни до цели · Kaplan–Meier", exact=True
        )
        page.locator(".dashboard-component-chart-holder").filter(
            has=curve_link
        ).screenshot(path=str(output / "screenshots/cohort_sufficient.png"))
        page.remove_listener("response", capture)
        browser.close()
    (output / "filter_validation.json").write_text(
        json.dumps(evidence, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print("Native filter assertions passed:", ", ".join(evidence))


if __name__ == "__main__":
    main()
