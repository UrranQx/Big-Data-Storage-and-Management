"""Capture authenticated Airflow and OpenMetadata views in a disposable browser."""

from pathlib import Path

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import sync_playwright
from verify_lab3_ui import local_environment


def main():
    env = local_environment()
    output = Path(__file__).resolve().parents[1] / "reports/lab3/screenshots"
    output.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(channel="msedge", headless=True)
        page = browser.new_page(viewport={"width": 1700, "height": 1100})
        try:
            page.goto("http://localhost:8080", wait_until="domcontentloaded")
            page.locator('input[name="username"]').fill("admin")
            page.locator('input[name="password"]').fill(env["AIRFLOW_ADMIN_PASSWORD"])
            page.locator('button[type="submit"]').click()
            page.wait_for_function(
                "!location.pathname.includes('/auth/')", timeout=60000
            )
        except PlaywrightError:
            raise RuntimeError("Airflow UI login failed; credentials omitted") from None
        page.goto(
            "http://localhost:8080/dags/refresh_marts", wait_until="domcontentloaded"
        )
        page.wait_for_timeout(4000)
        page.screenshot(path=str(output / "airflow.png"), full_page=True)
        try:
            page.goto("http://localhost:8585/signin", wait_until="domcontentloaded")
            page.locator("#email").fill("admin@open-metadata.org")
            page.locator("#password").fill(env["OPENMETADATA_ADMIN_PASSWORD"])
            page.locator('button[type="submit"]').click()
            page.wait_for_function(
                "!location.pathname.includes('/signin')", timeout=60000
            )
        except PlaywrightError:
            raise RuntimeError(
                "OpenMetadata UI login failed; credentials omitted"
            ) from None
        page.goto(
            "http://localhost:8585/table/speedrun_postgres.speedrun_ods.speedrun_marts.country_medals/lineage",
            wait_until="domcontentloaded",
        )
        page.wait_for_timeout(8000)
        page.screenshot(path=str(output / "lineage.png"), full_page=True)
        print("Airflow and OpenMetadata screenshots captured")
        browser.close()


if __name__ == "__main__":
    main()
