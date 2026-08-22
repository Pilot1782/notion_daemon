import logging
import os
from logging.handlers import RotatingFileHandler

from playwright.sync_api import sync_playwright, Page

from main import LOG_FILE
from privVars import MICROSOFT_USER, MICROSOFT_PASS

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s"
)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=50 * 1024 * 1024,
    backupCount=5,
    encoding="utf-8",
)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)


def signIn(page: Page, pUrl) -> None:
    page.goto(pUrl)

    for _ in range(10):
        page.wait_for_timeout(500)
        if page.url.startswith("https://app.notion.com/p/"):
            return
        elif page.get_by_role("button", name="Microsoft").is_visible():
            break
    logger.info("Signing in")

    with page.expect_popup() as page1_info:
        page.get_by_role("button", name="Microsoft").click()
    page1 = page1_info.value

    try:
        page1.get_by_role("textbox", name="Enter your email, phone, or").wait_for(
            state="visible")
    except Exception:
        logger.exception("Error occurred while waiting for email input")

    if not page1.is_closed():
        page1.get_by_role("textbox", name="Enter your email, phone, or").click()
        page1.get_by_role("textbox", name="Enter your email, phone, or").fill(
            MICROSOFT_USER)
        page1.get_by_role("button", name="Next").click()
        page1.get_by_role("textbox", name="Enter the password for notion").click()
        page1.get_by_role("textbox", name="Enter the password for notion").fill(
            MICROSOFT_PASS)
        page1.get_by_role("button", name="Sign in").click()
        page1.get_by_role("button", name="Yes").click()
        page1.close()

    try:
        while not page.url.startswith("https://app.notion.com/p/"):
            page.wait_for_timeout(500)
    except Exception:
        pass

    page.wait_for_timeout(5_000)
    logger.info("Signed in")


def updateReminder(page: Page, pUrl):
    page.goto(pUrl)
    logger.info(f"Updating reminder for {pUrl}")

    for _ in range(10):
        page.wait_for_timeout(500)

        if page.get_by_role("button", name="Microsoft").is_visible():
            page.get_by_role("button", name="Microsoft").click()
            page.wait_for_timeout(500)
        elif page.url.startswith("https://app.notion.com/p/"):
            break

    if page.get_by_role("button", name="Close", exact=True).is_visible():
        page.get_by_role("button", name="Close", exact=True).click()

    logger.debug("Page done loading")
    props = page.locator('[aria-label="Page properties"]').first
    row = props.get_by_role("row").filter(
        has=page.get_by_role("cell").filter(has_text="Date").first).first
    row.locator('[role="button"][data-testid="property-value"]').click()
    page.get_by_role("button").filter(has=page.locator('div[role="presentation"]').filter(
        has_text="Remind").first).first.click()
    page.get_by_role("menuitem").filter(has_text="5 minutes before").first.click()
    page.locator("body").press("Escape")
    page.wait_for_timeout(1_000)
    logger.info("Page done loading")


def updateReminders(pages: list) -> None:
    if not os.path.exists("auth.json"):
        with open("auth.json", "w") as f:
            f.write("{}")

    with sync_playwright() as playwright:
        browser = playwright.firefox.launch(headless=True, slow_mo=500)
        context = browser.new_context(storage_state="auth.json")

        context.tracing.start(name="trace", screenshots=True, snapshots=True)

        try:
            context.tracing.group("Login")
            page = context.new_page()
            signIn(page, "https://www.notion.so/login")
            context.tracing.group_end()

            context.storage_state(path="auth.json")

            for pUrl in pages:
                context.tracing.group(f"Update reminder for {pUrl}")
                updateReminder(page, pUrl)
                context.tracing.group_end()
            page.close()
        except Exception as err:
            logger.error(f"Error occurred while updating reminders: {err}")
            logger.exception(err)
        finally:
            context.tracing.stop(path="trace.zip")
            context.close()
            browser.close()
