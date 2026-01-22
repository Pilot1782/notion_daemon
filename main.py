import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta, timezone

from canvasapi import Canvas
from notion_client import Client

# =====================
# LOGGING CONFIG
# =====================
LOG_FILE = "canvas_notion_sync.log"

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s"
)

# Console handler (INFO+)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# Rotating file handler (DEBUG+)
file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=50 * 1024 * 1024,  # 50 MB
    backupCount=5,
    encoding="utf-8",
)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

logger.debug("Logger initialized")

# =====================
# Configuration
# =====================
from privVars import (
    CANVAS_API_KEY,
    CANVAS_URL,
    name_map,
    NOTION_API_KEY,
    NOTION_DATABASE_ID,
)

logger.debug("Configuration loaded")
logger.debug("Mapped course names: %s", list(name_map.keys()))

# =====================
# CLIENTS
# =====================
logger.info("Initializing Canvas and Notion clients")

canvas = Canvas(CANVAS_URL, CANVAS_API_KEY)
notion = Client(auth=NOTION_API_KEY)

# =====================
# FETCH EXISTING CANVAS IDS (BATCHED)
# =====================
def fetch_existing_canvas_ids() -> set[str]:
    logger.info("Fetching existing Canvas IDs from Notion")

    ids = set()
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
                cur_canvas_id = prop["rich_text"][0]["plain_text"]
                ids.add(cur_canvas_id)
                logger.debug("Found existing Canvas ID: %s", cur_canvas_id)

        if not resp.get("has_more"):
            logger.debug("No more Notion pages")
            break

        cursor = resp.get("next_cursor")

    logger.info("Loaded %d existing Canvas IDs", len(ids))
    return ids


existing_canvas_ids = fetch_existing_canvas_ids()

# =====================
# TIME WINDOW
# =====================
now = datetime.now(timezone.utc)
week_out = now + timedelta(days=7)

logger.info(
    "Scanning assignments due between %s and %s",
    now.isoformat(),
    week_out.isoformat(),
)

# =====================
# FETCH COURSES
# =====================
courses = canvas.get_courses(enrollment_state="active")
logger.debug("Canvas returned courses iterator")

for course in courses:
    logger.debug("Evaluating course: %s (ID %s)", course.name, course.id)

    if course.name not in name_map:
        logger.debug("Course not in name_map, skipping: %s", course.name)
        continue

    class_select = name_map[course.name]
    logger.info("Processing course: %s -> %s", course.name, class_select)

    assignments = course.get_assignments(
        include=["due_at"],
        bucket="upcoming",
    )

    assignment_count = 0

    for assignment in assignments:
        assignment_count += 1
        logger.debug(
            "Checking assignment: %s (ID %s)",
            assignment.name,
            assignment.id,
        )

        if not assignment.due_at:
            logger.debug("No due date, skipping assignment")
            continue

        due_at = datetime.fromisoformat(
            assignment.due_at.replace("Z", "+00:00")
        )

        logger.debug("Parsed due date: %s", due_at.isoformat())

        if not (now <= due_at <= week_out):
            logger.debug(
                "Assignment outside window (due %s), skipping",
                due_at.isoformat(),
            )
            continue

        canvas_id = str(assignment.id)

        # =====================
        # FAST LOCAL DEDUP
        # =====================
        if canvas_id in existing_canvas_ids:
            logger.debug(
                "Duplicate detected, already synced (Canvas ID %s)",
                canvas_id,
            )
            continue

        end_at = due_at + timedelta(minutes=30)

        logger.debug(
            "Creating Notion page for assignment '%s' (Canvas ID %s)",
            assignment.name,
            canvas_id,
        )

        try:
            notion.pages.create(
                parent={"data_source_id": NOTION_DATABASE_ID},
                properties={
                    "Name": {
                        "title": [{"text": {"content": assignment.name}}]
                    },
                    "Date": {
                        "date": {
                            "start": due_at.isoformat(),
                            "end": end_at.isoformat(),
                        }
                    },
                    "Class": {"select": {"name": class_select}},
                    "Ref": {"url": assignment.html_url},
                    "Status": {"status": {"name": "Not started"}},
                    "Canvas ID": {
                        "rich_text": [{"text": {"content": canvas_id}}]
                    },
                },
            )

            existing_canvas_ids.add(canvas_id)

            logger.info(
                "Added assignment: %s -> %s",
                assignment.name,
                class_select,
            )

        except Exception:
            logger.exception(
                "Failed to create Notion page for assignment '%s' (Canvas ID %s)",
                assignment.name,
                canvas_id,
            )

    logger.debug(
        "Finished course %s, checked %d assignments",
        course.name,
        assignment_count,
    )

logger.info("Sync run complete")
