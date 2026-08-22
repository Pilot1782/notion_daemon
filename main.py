import logging
import re
from datetime import date, datetime, time, timedelta, timezone
from logging.handlers import RotatingFileHandler
from urllib.parse import parse_qs, urlparse

import icalendar
import requests
from notion_client import Client

from notionPlaywright import updateReminders
from privVars import NOTION_API_KEY, NOTION_DATABASE_ID, name_map, ICAL_URL, CANVAS_URL

LOG_FILE = "calendar_notion_sync.log"
SUMMARY_CLASS_PAT = re.compile(r" \[\d{4}[^-]+-([^-]+-[^-]+[^]]+)]$")
SUMMARY_TITLE_PAT = re.compile(r" \[[^]]+]$")

now = datetime.now(timezone.utc)
week_out = now + timedelta(days=7 * 3)

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


logger.debug("Logger initialized")
logger.debug("Configuration loaded")
logger.debug("Mapped class names: %s", list(name_map.keys()))

logger.info("Initializing Notion client")
notion = Client(auth=NOTION_API_KEY)

MIN_EVENT_DURATION = timedelta(minutes=30)
LOCAL_TIMEZONE = datetime.now().astimezone().tzinfo or timezone.utc


def normalize_event_text(value: object) -> str:
    return str(value) if value is not None else ""


def strip_summary_class(summary: str) -> tuple[str, str | None]:
    class_key_match = SUMMARY_CLASS_PAT.search(summary)
    if class_key_match:
        class_key = class_key_match.group(1)
    else:
        class_key = summary.split(" ")[-1]
    title = SUMMARY_TITLE_PAT.sub("", summary).strip()
    return title, class_key


def extract_assignment_url(raw_url: str) -> str | None:
    parsed_url = urlparse(raw_url)
    query = parse_qs(parsed_url.query)
    contexts = query.get("include_contexts", [])
    fragment = parsed_url.fragment

    if not contexts or not fragment:
        return raw_url or None

    course_context = contexts[0]
    if "_" not in course_context or "_" not in fragment:
        return raw_url or None

    course_id = course_context.split("_", 1)[1]
    assignment_id = fragment.split("_", 1)[1]
    return f"{CANVAS_URL}courses/{course_id}/assignments/{assignment_id}"


def normalize_event_datetime(value: object) -> datetime | date:
    decoded = value.dt if hasattr(value, "dt") else value
    if isinstance(decoded, datetime):
        if decoded.tzinfo is None:
            return decoded.replace(tzinfo=LOCAL_TIMEZONE)
        return decoded.astimezone(LOCAL_TIMEZONE)
    if isinstance(decoded, date):
        return decoded
    raise TypeError(f"Unsupported calendar datetime value: {type(decoded)!r}")


def format_notion_date(value: datetime | date) -> str:
    return value.isoformat()


def ensure_minimum_event_duration(
    start_at: datetime | date,
    end_at: datetime | date,
) -> datetime | date:
    if isinstance(start_at, datetime) and isinstance(end_at, datetime):
        min_end_at = start_at + MIN_EVENT_DURATION
        return end_at if end_at >= min_end_at else min_end_at
    return end_at


def adjust_event_datetime_for_notion(value: datetime | date) -> datetime:
    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime.combine(value - timedelta(days=1), time(21, 0), tzinfo=LOCAL_TIMEZONE)

    local_value = value.astimezone(LOCAL_TIMEZONE)
    if local_value.hour < 12:
        return datetime.combine(
            local_value.date() - timedelta(days=1),
            time(22, 0),
            tzinfo=LOCAL_TIMEZONE,
        )
    return local_value


def fetch_existing_event_ids() -> set[str]:
    logger.info("Fetching existing event IDs from Notion")

    ids: set[str] = set()
    cursor = None
    batch = 0

    while True:
        batch += 1
        logger.debug("Querying Notion (batch %d, cursor=%s)", batch, cursor)

        resp = notion.data_sources.query(
            data_source_id=NOTION_DATABASE_ID,
            start_cursor=cursor,
            page_size=100,
        )

        results = resp.get("results", [])
        logger.debug("Received %d results from Notion", len(results))

        for page in results:
            prop = page["properties"].get("Canvas ID")
            if prop and prop["rich_text"]:
                cur_event_id = prop["rich_text"][0]["plain_text"]
                ids.add(cur_event_id)
                logger.debug("Found existing event ID: %s", cur_event_id)

        if not resp.get("has_more"):
            logger.debug("No more Notion pages")
            break

        cursor = resp.get("next_cursor")

    logger.info("Loaded %d existing event IDs", len(ids))
    return ids


existing_event_ids = fetch_existing_event_ids()

logger.info(
    "Scanning calendar events due between %s and %s",
    now.isoformat(),
    week_out.isoformat(),
)

logger.info("Fetching calendar feed from ICS URL")
calendar_response = requests.get(ICAL_URL, timeout=30)
calendar_response.raise_for_status()
calendar_feed = icalendar.Calendar.from_ical(calendar_response.content)

event_count = 0

for event in calendar_feed.walk("VEVENT"):
    event_count += 1

    summary = normalize_event_text(event.get("SUMMARY"))
    assign_title, class_id = strip_summary_class(summary)
    event_uid = normalize_event_text(event.get("UID")).split("-")[-1]
    raw_url = normalize_event_text(event.get("URL"))
    assignment_url = extract_assignment_url(raw_url) if raw_url else None

    logger.debug(
        "Checking event: %s (UID %s)",
        assign_title,
        event_uid,
    )

    if not event_uid:
        logger.debug("No UID available, skipping event")
        continue

    start_value = event.get("DTSTART")
    if not start_value:
        logger.debug("No start date, skipping event")
        continue

    start_at = normalize_event_datetime(start_value)
    end_value = event.get("DTEND")
    end_at = normalize_event_datetime(end_value) if end_value else start_at
    if start_at.isoformat() > end_at.isoformat():
        end_at = start_at

    duration = max(end_at - start_at, timedelta(minutes=30))

    start_at = adjust_event_datetime_for_notion(start_at)
    end_at = start_at + duration

    start_compare = start_at if isinstance(start_at, datetime) else datetime.combine(start_at, datetime.min.time(), tzinfo=timezone.utc)

    if not (now <= start_compare <= week_out):
        logger.debug(
            "Event outside window (start %s), skipping",
            start_compare.isoformat(),
        )
        continue

    if event_uid in existing_event_ids:
        logger.debug("Duplicate detected, already synced (event UID %s)", event_uid)
        continue

    class_select = name_map.get(class_id, class_id or "Unmapped")

    notion_properties = {
        "Name": {"title": [{"text": {"content": assign_title or summary or event_uid}}]},
        "Date": {
            "date": {
                "start": format_notion_date(start_at),
                "end": format_notion_date(end_at),
            }
        },
        "Class": {"select": {"name": class_select}},
        "Status": {"status": {"name": "Not started"}},
        "Canvas ID": {"rich_text": [{"text": {"content": event_uid}}]},
    }

    if assignment_url:
        notion_properties["Ref"] = {"url": assignment_url}
    elif raw_url:
        notion_properties["Ref"] = {"url": raw_url}

    logger.debug(
        "Creating Notion page for event '%s' (UID %s)",
        assign_title or summary or event_uid,
        event_uid,
        )

    newPages = []
    try:
        newPage = notion.pages.create(
            parent={"data_source_id": NOTION_DATABASE_ID},
            properties=notion_properties,
            template={"type": "default"},
        )
        newPages.append(newPage["url"])
        existing_event_ids.add(event_uid)
        logger.info("Added event: %s -> %s", assign_title or summary or event_uid, class_select)
    except Exception:
        logger.exception(
            "Failed to create Notion page for event '%s' (UID %s)",
            assign_title or summary or event_uid,
            event_uid,
            )
    
    try:
        updateReminders(newPages)
    except Exception as err:
        logger.exception(f"Failed to update reminders: {err}")

logger.debug("Finished feed scan, checked %d events", event_count)
logger.info("Sync run complete")
