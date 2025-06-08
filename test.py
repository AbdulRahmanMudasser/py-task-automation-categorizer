import os
from dotenv import load_dotenv
from notion_client import Client
from notion_client.errors import APIResponseError
import logging
from datetime import date

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Validate and retrieve environment variables
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
TASK_CATEGORY_DB_ID = os.getenv("TASK_CATEGORY_DB_ID")
TODAYS_TASKS_DB_ID = os.getenv("TODAYS_TASKS_DB_ID")

print(NOTION_API_KEY)
print(TASK_CATEGORY_DB_ID)
print(TODAYS_TASKS_DB_ID)

if not all([NOTION_API_KEY, TASK_CATEGORY_DB_ID, TODAYS_TASKS_DB_ID]):
    logger.error("Missing required environment variables in .env file.")
    raise ValueError("Please ensure NOTION_API_KEY, TASK_CATEGORY_DB_ID, and TODAYS_TASKS_DB_ID are set in .env.")

# Initialize Notion client
try:
    notion = Client(auth=NOTION_API_KEY)
    logger.info("Successfully initialized Notion client.")
except Exception as e:
    logger.error(f"Failed to initialize Notion client: {e}")
    raise

# Fetch and print column names from TASK_CATEGORY_DB_ID
try:
    database_schema = notion.databases.retrieve(database_id=TASK_CATEGORY_DB_ID)
    column_names = [prop["name"] for prop in database_schema["properties"].values()]
    logger.info(f"Column names in {TASK_CATEGORY_DB_ID}: {column_names}")
    print("Column names:", column_names)
except APIResponseError as e:
    logger.error(f"API error retrieving database schema for {TASK_CATEGORY_DB_ID}: {e}")
    raise
except Exception as e:
    logger.error(f"Unexpected error retrieving database schema for {TASK_CATEGORY_DB_ID}: {e}")
    raise

# Function to get all pages from a database with error handling
def get_all_pages(database_id):
    pages = []
    start_cursor = None
    while True:
        try:
            response = notion.databases.query(
                database_id=database_id,
                start_cursor=start_cursor
            )
            pages.extend(response["results"])
            if not response["has_more"]:
                break
            start_cursor = response["next_cursor"]
        except APIResponseError as e:
            logger.error(f"API error querying database {database_id}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error querying database {database_id}: {e}")
            raise
    return pages

# Function to clean and update database schema with tag options and colors
def clean_and_update_tag_options(database_id, valid_options):
    try:
        current_schema = notion.databases.retrieve(database_id=database_id)
        tag_prop = next((prop for prop in current_schema["properties"].values() if prop["name"] == "Tag"), None)
        if not tag_prop or tag_prop["type"] != "select":
            logger.error(f"Tag property not found or not a select type in {database_id}")
            raise ValueError("Tag must be a select property in the target database.")

        current_options_map = {option["name"]: option for option in tag_prop["select"]["options"]}
        # Define color mapping for all available Notion colors
        color_mapping = {
            "Work": "blue",
            "Spiritual": "green",
            "Fitness": "yellow",
            "Health": "red",
            "Hygiene": "pink",
            "Career": "purple",
            "Personal Growth": "orange",
            "Household": "brown",
            "Networking": "gray",
            "Trading": "default",
            "Logs": "default",  # Use 'default' for any additional unmapped tags
            "Unknown": "gray"   # Default for any unmapped or missing tags
        }

        # Build updated options, preserving existing colors for valid options
        updated_options = []
        for opt in valid_options:
            if opt in current_options_map:
                # Preserve existing option with its current color
                updated_options.append(current_options_map[opt])
            else:
                # Add new option with mapped color
                updated_options.append({"name": opt, "color": color_mapping.get(opt, "gray")})

        if updated_options != tag_prop["select"]["options"]:
            notion.databases.update(
                database_id=database_id,
                properties={
                    "Tag": {
                        "select": {
                            "options": updated_options
                        }
                    }
                }
            )
            logger.info(f"Updated Tag options in {database_id} with {valid_options}")
        else:
            logger.info(f"No changes needed for Tag options in {database_id}")
    except APIResponseError as e:
        logger.error(f"API error updating schema for {database_id}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error updating schema for {database_id}: {e}")
        raise

# Fetch all categories and build task mapping
try:
    categories_pages = get_all_pages(TASK_CATEGORY_DB_ID)
    task_mapping = {}
    required_props = {"Category Name", "Tag", "High Priority Tasks", "Medium Priority Tasks", "Low Priority Tasks"}
    if not all(prop in [p["name"] for p in database_schema["properties"].values()] for prop in required_props):
        logger.error("Required properties missing in database schema.")
        raise ValueError("Database must contain 'Category Name', 'Tag', 'High Priority Tasks', 'Medium Priority Tasks', and 'Low Priority Tasks'.")

    unique_tags = set()
    for page in categories_pages:
        try:
            category_name = page["properties"]["Category Name"]["title"][0]["plain_text"]
            # Ensure tag is always a string (name from select)
            tag_data = page["properties"]["Tag"]
            tag = tag_data["select"]["name"] if tag_data and tag_data.get("select") and "name" in tag_data["select"] else "Unknown"
            logger.debug(f"Extracted tag for category '{category_name}': {tag}")
            unique_tags.add(tag)
            for priority, prop_name in [("High", "High Priority Tasks"), ("Medium", "Medium Priority Tasks"), ("Low", "Low Priority Tasks")]:
                tasks_rich_text = page["properties"][prop_name]["rich_text"]
                if tasks_rich_text:
                    tasks_str = tasks_rich_text[0]["plain_text"]
                    task_list = [task.strip() for task in tasks_str.split(",") if task.strip() and task.strip()]
                    for task in task_list:
                        if task:  # Ensure task is not empty
                            task_mapping[task] = (category_name, priority, tag)
            logger.debug(f"Mapped tasks for category '{category_name}'")
        except (KeyError, IndexError) as e:
            logger.warning(f"Skipping malformed page data: {e}")
            continue
    logger.info(f"Loaded {len(task_mapping)} tasks from Categories database. Unique tags: {unique_tags}")

    # Update Tag options in TODAYS_TASKS_DB_ID with cleaning and colors
    clean_and_update_tag_options(TODAYS_TASKS_DB_ID, list(unique_tags))
except Exception as e:
    logger.error(f"Failed to load categories: {e}")
    raise

# Query daily tasks where "Start Automation" is "yes"
try:
    today = date.today().isoformat()
    filter_conditions = {
        "and": [
            {
                "property": "Start Automation",
                "select": {
                    "equals": "yes"
                }
            }
        ]
    }
    daily_tasks = notion.databases.query(
        database_id=TODAYS_TASKS_DB_ID,
        filter=filter_conditions
    )["results"]
    logger.info(f"Found {len(daily_tasks)} tasks to process where Start Automation is 'yes'.")
except APIResponseError as e:
    logger.error(f"API error querying daily tasks: {e}")
    raise
except Exception as e:
    logger.error(f"Unexpected error querying daily tasks: {e}")
    raise

# Process each daily task
for page in daily_tasks:
    try:
        task_name = page["properties"]["Task Name"]["title"][0]["plain_text"]
        if task_name in task_mapping:
            category, priority, tag = task_mapping[task_name]
            notion.pages.update(
                page_id=page["id"],
                properties={
                    "Category Name": {"select": {"name": category}},
                    "Priority": {"select": {"name": priority}},
                    "Tag": {"select": {"name": tag}},  # Select the matching tag option
                    "Select": {"select": {"name": "no"}}
                }
            )
            logger.info(f"Updated task '{task_name}' with category '{category}', priority '{priority}', tag '{tag}'")
        else:
            logger.warning(f"Task '{task_name}' not found in Categories database")
    except Exception as e:
        logger.error(f"Failed to update task '{task_name}': {e}")

# # Testing mode: Assign and print "Side Project" task (commented out)
# test_task = "Side Project"
# if test_task in task_mapping:
#     test_category, test_priority, test_tag = task_mapping[test_task]
#     logger.info(f"Test assignment for '{test_task}': Category = '{test_category}', Priority = '{test_priority}', Tag = '{test_tag}'")
# else:
#     logger.warning(f"Test task '{test_task}' not found in Categories database. Please add it to the Categories database.")