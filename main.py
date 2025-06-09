import os
from dotenv import load_dotenv
from notion_client import Client
from notion_client.errors import APIResponseError
import logging
from datetime import date, datetime, timedelta
import re
from difflib import SequenceMatcher

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

# Fetch and print column names from both databases
try:
    category_schema = notion.databases.retrieve(database_id=TASK_CATEGORY_DB_ID)
    category_column_names = [prop["name"] for prop in category_schema["properties"].values()]
    logger.info(f"Column names in {TASK_CATEGORY_DB_ID}: {category_column_names}")

    todays_schema = notion.databases.retrieve(database_id=TODAYS_TASKS_DB_ID)
    todays_column_names = [prop["name"] for prop in todays_schema["properties"].values()]
    logger.info(f"Column names in {TODAYS_TASKS_DB_ID}: {todays_column_names}")
except APIResponseError as e:
    logger.error(f"API error retrieving database schema: {e}")
    raise
except Exception as e:
    logger.error(f"Unexpected error retrieving database schema: {e}")
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

# Function to update database schema with new tag options only
def clean_and_update_tag_options(database_id, valid_options):
    try:
        current_schema = notion.databases.retrieve(database_id=database_id)
        tag_prop = next((prop for prop in current_schema["properties"].values() if prop["name"] == "Tag"), None)
        if not tag_prop or tag_prop["type"] != "select":
            logger.error(f"Tag property not found or not a select type in {database_id}")
            raise ValueError("Tag must be a select property in the target database.")

        current_options_map = {option["name"]: option for option in tag_prop["select"]["options"]}
        new_options_to_add = [opt for opt in valid_options if opt not in current_options_map]
        if new_options_to_add:
            updated_options = current_options_map.values()
            updated_options = list(updated_options) + [{"name": opt} for opt in new_options_to_add]
            notion.databases.update(
                database_id=database_id,
                properties={
                    "Tag": {
                        "select": {
                            "options": list(updated_options)
                        }
                    }
                }
            )
            logger.info(f"Added new Tag options in {database_id}: {new_options_to_add}")
        else:
            logger.info(f"No new Tag options to add in {database_id}")
    except APIResponseError as e:
        logger.error(f"API error updating schema for {database_id}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error updating schema for {database_id}: {e}")
        raise

# Function to safely get property value with original case preservation
def get_property(page, prop_name, default=""):
    try:
        prop = page["properties"].get(prop_name, {})
        if prop.get("type") == "title":
            return prop["title"][0]["plain_text"].strip() if prop.get("title") and len(prop["title"]) > 0 else default
        elif prop.get("type") == "select":
            return prop["select"]["name"] if prop.get("select") and "name" in prop["select"] else default
        elif prop.get("type") == "last_edited_time":
            return prop["last_edited_time"] if prop.get("last_edited_time") else default
        return default
    except (KeyError, AttributeError, IndexError) as e:
        logger.warning(f"Failed to get property '{prop_name}': {e}, Properties: {page['properties']}")
        return default

# Function to extract sub-tasks from a task string
def extract_sub_tasks(task_str):
    sub_tasks = []
    if not task_str or not isinstance(task_str, str):
        logger.debug(f"Empty or invalid task_str: {task_str}")
        return sub_tasks
    task_str = task_str.lower().strip()
    logger.debug(f"Processing task_str: {task_str}")
    
    # Split by comma, preserving content within parentheses
    parts = re.split(r',(?![^(]*\))', task_str)
    logger.debug(f"Split parts: {parts}")
    for part in parts:
        part = part.strip()
        if not part:
            logger.debug(f"Skipping empty part: {part}")
            continue
        # Extract content within parentheses
        start = part.find('(')
        end = part.find(')')
        if start != -1 and end != -1 and start < end:
            paren_content = part[start + 1:end].strip()
            logger.debug(f"Found parentheses content: {paren_content}")
            sub_tasks.extend([s.strip() for s in paren_content.split(',') if s.strip()])
            base = part[:start].strip() or part[end + 1:].strip()
            if base and base not in sub_tasks:
                sub_tasks.append(base)
        else:
            if part and part not in sub_tasks:
                sub_tasks.append(part)
                logger.debug(f"Added standalone task: {part}")
    return list(dict.fromkeys(sub_tasks))  # Remove duplicates

# Function to find closest match with case-insensitive lookup
def find_closest_match(task_name, options, threshold=0.8):
    if not task_name or not options:
        return None
    task_name_lower = task_name.lower()
    best_match = max(options, key=lambda x: SequenceMatcher(None, task_name_lower, x.lower()).ratio() if x else 0)
    return best_match if SequenceMatcher(None, task_name_lower, best_match.lower()).ratio() >= threshold else None

# Fetch all categories and build task mapping with sub-tasks from all priority columns
try:
    categories_pages = get_all_pages(TASK_CATEGORY_DB_ID)
    task_mapping = {}
    sub_task_mapping = {}
    required_props = {"Category Name", "Tag", "High Priority Tasks", "Medium Priority Tasks", "Low Priority Tasks"}
    if not all(prop in [p["name"] for p in category_schema["properties"].values()] for prop in required_props):
        logger.error("Required properties missing in database schema.")
        raise ValueError("Database must contain 'Category Name', 'Tag', 'High Priority Tasks', 'Medium Priority Tasks', and 'Low Priority Tasks'.")

    unique_tags = set()
    for page in categories_pages:
        try:
            category_name = get_property(page, "Category Name")
            tag = get_property(page, "Tag", "Unknown")
            logger.debug(f"Extracted tag for category '{category_name}': {tag}")
            unique_tags.add(tag)
            priority_columns = {
                "High": "High Priority Tasks",
                "Medium": "Medium Priority Tasks",
                "Low": "Low Priority Tasks"
            }
            for priority, prop_name in priority_columns.items():
                tasks_rich_text = page["properties"].get(prop_name, {}).get("rich_text", [])
                logger.debug(f"Raw tasks_rich_text for {prop_name}: {tasks_rich_text}")
                if tasks_rich_text:
                    # Concatenate all plain_text values into a single string
                    tasks_str = " ".join([text.get("plain_text", "").strip() for text in tasks_rich_text if text.get("plain_text")])
                    logger.debug(f"Concatenated tasks_str for {prop_name}: {tasks_str}")
                    task_list = re.split(r',(?![^(]*\))', tasks_str)
                    task_list = [task.strip() for task in task_list if task.strip()]
                    for task in task_list:
                        if task:
                            task_key = task.lower().strip()
                            task_mapping[task_key] = (category_name, priority, tag)
                            sub_tasks = extract_sub_tasks(task)
                            logger.debug(f"Extracted sub-tasks for '{task}' in {priority}: {sub_tasks}")
                            for sub_task in sub_tasks:
                                if sub_task and sub_task != task_key:
                                    sub_task_mapping[sub_task] = (category_name, priority, tag)
            logger.debug(f"Mapped tasks for category '{category_name}': {task_mapping.keys()}")
        except Exception as e:
            logger.warning(f"Skipping malformed page data: {e}")
            continue
    logger.info(f"Loaded {len(task_mapping)} tasks from Categories database. Unique tags: {unique_tags}")
    logger.debug(f"Task mapping: {task_mapping}")
    logger.debug(f"Sub-task mapping: {sub_task_mapping}")

    clean_and_update_tag_options(TODAYS_TASKS_DB_ID, list(unique_tags))
except Exception as e:
    logger.error(f"Failed to load categories: {e}")
    raise

# Load or create last run timestamp
last_run_file = "last_run.txt"
last_run = None
if os.path.exists(last_run_file):
    with open(last_run_file, "r") as f:
        last_run = datetime.fromisoformat(f.read().strip())
else:
    last_run = datetime.now() - timedelta(days=1)  # Default to a day ago for initial run
logger.info(f"Last run time: {last_run}")

# Query daily tasks where "Start Automation" is "Yes"
try:
    today = date.today().isoformat()
    filter_conditions = {
        "property": "Start Automation",
        "select": {
            "equals": "Yes"
        }
    }
    daily_tasks = notion.databases.query(
        database_id=TODAYS_TASKS_DB_ID,
        filter=filter_conditions
    )
    for task in daily_tasks["results"]:
        task_name = get_property(task, "Task Name")
        logger.debug(f"Task properties: Task Name = {task_name}, Full Properties = {task['properties']}")
    logger.info(f"Found {len(daily_tasks['results'])} tasks to process where Start Automation is 'Yes'.")
except APIResponseError as e:
    logger.error(f"API error querying daily tasks: {e}")
    raise
except Exception as e:
    logger.error(f"Unexpected error querying daily tasks: {e}")
    raise

# Process each daily task
for page in daily_tasks["results"]:
    try:
        task_name = get_property(page, "Task Name")
        if not task_name:
            logger.warning(f"Skipping task with missing or invalid Task Name: {page['properties']}")
            continue
        logger.debug(f"Processing task: {task_name}, Properties: {page['properties']}")
        exact_match = task_name.lower() in task_mapping
        partial_match = find_closest_match(task_name, sub_task_mapping.keys()) if not exact_match else None

        if exact_match:
            category, priority, tag = task_mapping[task_name.lower()]
            update_properties = {}
            todays_schema = notion.databases.retrieve(database_id=TODAYS_TASKS_DB_ID)
            category_options = {opt["name"] for opt in todays_schema["properties"].get("Category Name", {}).get("select", {}).get("options", [])}
            priority_options = {opt["name"] for opt in todays_schema["properties"].get("Priority", {}).get("select", {}).get("options", [])}
            tag_options = {opt["name"] for opt in todays_schema["properties"].get("Tag", {}).get("select", {}).get("options", [])}

            current_category = get_property(page, "Category Name")
            current_priority = get_property(page, "Priority")
            current_tag = get_property(page, "Tag")

            if not current_category and category in category_options and "Category Name" in todays_column_names:
                update_properties["Category Name"] = {"select": {"name": category}}
            if not current_priority and priority in priority_options and "Priority" in todays_column_names:
                update_properties["Priority"] = {"select": {"name": priority}}
            if not current_tag and tag in tag_options and "Tag" in todays_column_names:
                update_properties["Tag"] = {"select": {"name": tag}}

            if update_properties:
                try:
                    notion.pages.update(page_id=page["id"], properties=update_properties)
                    logger.info(f"Updated task '{task_name}' with {update_properties}")
                except Exception as e:
                    logger.error(f"Failed to update page for task '{task_name}': {e}")
        elif partial_match:
            category, priority, tag = sub_task_mapping[partial_match]
            update_properties = {}
            todays_schema = notion.databases.retrieve(database_id=TODAYS_TASKS_DB_ID)
            category_options = {opt["name"] for opt in todays_schema["properties"].get("Category Name", {}).get("select", {}).get("options", [])}
            priority_options = {opt["name"] for opt in todays_schema["properties"].get("Priority", {}).get("select", {}).get("options", [])}
            tag_options = {opt["name"] for opt in todays_schema["properties"].get("Tag", {}).get("select", {}).get("options", [])}

            current_category = get_property(page, "Category Name")
            current_priority = get_property(page, "Priority")
            current_tag = get_property(page, "Tag")

            if not current_category and category in category_options and "Category Name" in todays_column_names:
                update_properties["Category Name"] = {"select": {"name": category}}
            if not current_priority and priority in priority_options and "Priority" in todays_column_names:
                update_properties["Priority"] = {"select": {"name": priority}}
            if not current_tag and tag in tag_options and "Tag" in todays_column_names:
                update_properties["Tag"] = {"select": {"name": tag}}

            if update_properties:
                try:
                    notion.pages.update(page_id=page["id"], properties=update_properties)
                    logger.info(f"Updated task '{task_name}' with partial match to sub-task '{partial_match}' using {update_properties}")
                except Exception as e:
                    logger.error(f"Failed to update page for task '{task_name}': {e}")
        else:
            logger.warning(f"Task '{task_name}' not found in Categories database or its sub-tasks. Available tasks: {list(task_mapping.keys())}, Sub-tasks: {list(sub_task_mapping.keys())}")
    except Exception as e:
        logger.error(f"Failed to process task '{task_name}': {e}")

# Update last run timestamp
with open(last_run_file, "w") as f:
    f.write(datetime.now().isoformat())
logger.info(f"Updated last run time to: {datetime.now().isoformat()}")