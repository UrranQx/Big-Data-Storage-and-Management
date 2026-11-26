"""Exercise the real local Superset UI and save screenshots without credentials."""

import argparse
import json
import time
from pathlib import Path

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]


def local_environment():
    return dict(
        line.split("=", 1)
        for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines()
        if "=" in line and not line.lstrip().startswith("#")
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", default="reports/lab3/acceptance_20261126/screenshots"
    )
    parser.add_argument(
        "--dashboard", choices=["runner", "records", "countries", "all"], default="all"
    )
    args = parser.parse_args()
    destination = ROOT / args.output
    destination.mkdir(parents=True, exist_ok=True)
    env = local_environment()
    results = {}
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(channel="msedge", headless=True)
        context = browser.new_context(
            viewport={"width": 1700, "height": 1100}, locale="ru-RU"
        )
        page = context.new_page()
        failures = []
        requests = []
        client_errors = []
        page.on("pageerror", lambda error: client_errors.append(str(error)))

        def inspect(response):
            if "/api/v1/chart/data" not in response.url:
                return
            try:
                data = response.json()
            except (json.JSONDecodeError, PlaywrightError):
                failures.append(
                    {"status": response.status, "error": "Non-JSON chart response"}
                )
                return
            if response.status != 200:
                failures.append({"status": response.status, "error": data})
            for result in data.get("result", []):
                if result.get("error"):
                    failures.append({"error": result["error"]})
            requests.append(
                {
                    "chart_id": (response.request.post_data_json or {})
                    .get("form_data", {})
                    .get("slice_id"),
                    "status": response.status,
                    "rows": [x.get("rowcount") for x in data.get("result", [])],
                }
            )

        page.on("response", inspect)
        page.goto("http://localhost:8088/login/", wait_until="domcontentloaded")
        page.locator("#username").fill("admin")
        page.locator("#password").fill(env["SUPERSET_ADMIN_PASSWORD"])
        page.locator("input[type=submit],button[type=submit]").click()
        page.wait_for_url("**/superset/**", timeout=60000)
        dashboards = (
            ["runner", "records", "countries"]
            if args.dashboard == "all"
            else [args.dashboard]
        )
        for key in dashboards:
            failures.clear()
            requests.clear()
            client_errors.clear()
            page.goto(
                "http://localhost:8088/superset/dashboard/lab3-" + key + "/",
                wait_until="domcontentloaded",
            )
            page.wait_for_selector(".dashboard-component-chart-holder", timeout=60000)
            # Scroll through the page to trigger charts outside the first viewport.
            height = page.evaluate("document.body.scrollHeight")
            for position in range(0, height, 700):
                page.evaluate("y => window.scrollTo(0, y)", position)
                page.wait_for_timeout(500)
            deadline = time.monotonic() + 120
            while page.locator(".loading").count() and time.monotonic() < deadline:
                page.wait_for_timeout(1000)
            page.wait_for_timeout(3000)
            page.evaluate("window.scrollTo(0,0)")
            page.screenshot(path=str(destination / (key + ".png")), full_page=True)
            alerts = page.locator('[role="alert"]').all_text_contents()
            actual_chart_ids = {r["chart_id"] for r in requests if r["chart_id"]}
            expected_charts = {"runner": 14, "records": 7, "countries": 3}[key]
            if len(actual_chart_ids) != expected_charts:
                failures.append(
                    {
                        "error": f"Expected {expected_charts} charts, got {len(actual_chart_ids)}"
                    }
                )
            results[key] = {
                "charts": len(actual_chart_ids),
                "chart_requests": list(requests),
                "failures": list(failures),
                "alerts": alerts,
                "client_errors": list(client_errors),
            }
            print(key, json.dumps(results[key], ensure_ascii=False), flush=True)
        browser.close()
    (destination.parent / "ui_validation.json").write_text(
        json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    if any(
        result["failures"] or result["client_errors"] or result["alerts"]
        for result in results.values()
    ):
        raise SystemExit("Some chart requests failed")


if __name__ == "__main__":
    main()
