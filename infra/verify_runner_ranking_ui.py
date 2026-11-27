"""Check ranking filters against responses from the real local Superset UI."""

import json
import re
import time
from pathlib import Path

from playwright.sync_api import Error, sync_playwright
from verify_lab3_ui import local_environment


def main():
    output = (
        Path(__file__).resolve().parents[1] / "reports/lab3/runner_ranking_v2_20261128"
    )
    output.mkdir(parents=True, exist_ok=True)
    evidence = {}
    responses = {}
    with sync_playwright() as pw:
        browser = pw.chromium.launch(channel="msedge", headless=True)
        page = browser.new_page(viewport={"width": 1800, "height": 1100})

        def capture(response):
            if "/api/v1/chart/data" in response.url:
                key = (
                    (response.request.post_data_json or {})
                    .get("form_data", {})
                    .get("slice_id")
                )
                if key:
                    try:
                        responses[int(key)] = response.json()
                    except Error:
                        pass

        page.on("response", capture)
        page.goto("http://localhost:8088/login/")
        page.locator("#username").fill("admin")
        page.locator("#password").fill(local_environment()["SUPERSET_ADMIN_PASSWORD"])
        page.locator("input[type=submit],button[type=submit]").click()
        page.wait_for_url("**/superset/**", timeout=60000)
        page.goto("http://localhost:8088/superset/dashboard/lab3-ranking/")

        def chart(title):
            link = page.get_by_role("link", name=title, exact=True)
            link.wait_for(timeout=60000)
            return int(re.search(r"slice_id=(\d+)", link.get_attribute("href"))[1])

        def rows(key):
            until = time.monotonic() + 120
            while key not in responses and time.monotonic() < until:
                page.wait_for_timeout(200)
            result = responses[key]
            assert "result" in result and not result["result"][0].get("error"), result
            return result["result"][0]["data"]

        def select(key, value):
            page.wait_for_load_state("networkidle", timeout=120000)
            control = page.get_by_role(
                "combobox", name="NATIVE_FILTER-ranking-" + key, exact=True
            )
            control.locator(
                "xpath=ancestor::div[contains(@class,'ant-select-selector')]"
            ).click()
            control.fill("")
            control.press_sequentially(value, delay=25)
            page.locator(
                ".ant-select-dropdown:not(.ant-select-dropdown-hidden) .ant-select-item-option"
            ).filter(has_text=re.compile("^" + re.escape(value) + "$")).first.click(
                timeout=30000
            )
            control.press("Escape")

        def apply():
            responses.clear()
            page.get_by_role("button", name="Apply filters", exact=True).click()

        portfolio = chart("Универсальность · все игры · топ-100")
        contributions = chart("Вклад каждой игры · выберите раннера")
        coverage = chart("Охват аналитического среза")
        initial = rows(portfolio)
        base_coverage = rows(coverage)
        chosen = initial[9]
        select("runner", chosen["runner"])
        apply()
        selected = rows(portfolio)
        assert selected == [chosen] and selected[0]["portfolio_rank"] > 1
        evidence["runner_preserves_global_rank"] = selected
        parts = rows(contributions)
        assert len(parts) == chosen["eligible_games"]
        assert (
            abs(
                sum(float(row["weighted_points"]) for row in parts)
                - float(chosen["portfolio_points"])
            )
            < 1e-6
        )
        assert len({row["game_name"] for row in parts}) == len(parts)
        evidence["game_contributions_match_portfolio"] = parts
        page.screenshot(path=str(output / "selected_runner.png"), full_page=True)
        responses.clear()
        page.get_by_role("button", name="Clear all", exact=True).click()
        rows(portfolio)
        select("minimum_field_size", "100")
        apply()
        changed = rows(coverage)
        assert changed[0]["archive_people"] == base_coverage[0]["archive_people"]
        assert changed[0]["ranked_people"] < base_coverage[0]["ranked_people"]
        evidence["minimum_field_recalculates_population"] = {
            "default": base_coverage,
            "minimum_100": changed,
        }
        page.get_by_role("button", name="Clear all", exact=True).click()
        rows(portfolio)
        select("minimum_field_size", "10")
        select("runner", "Niftski [qjne76wj]")
        apply()
        niftski = rows(contributions)
        retained = next(row for row in niftski if row["game_name"] == "Red Ball")
        assert retained["solo_place"] > 3 and float(retained["tenure_points"]) > 0
        assert any(row["tenure_points"] is None for row in niftski)
        evidence["retained_podium_and_unknown_history"] = niftski
        page.screenshot(path=str(output / "retained_history.png"), full_page=True)
        browser.close()
    (output / "filter_validation.json").write_text(
        json.dumps(evidence, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print("Ranking filters passed: global place preserved; population recalculated.")


if __name__ == "__main__":
    main()
