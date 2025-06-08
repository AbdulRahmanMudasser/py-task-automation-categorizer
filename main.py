import os
from notion_client import Client
from dotenv import load_dotenv

load_dotenv()
notion = Client(auth=os.getenv("NOTION_API_SECRET"))

CATEGORIES_DB_ID = os.getenv("TASK_CATEGORY_DB_ID")
TODAYS_TASKS_DB_ID = os.getenv("TODAYS_TASKS_DB_ID")

def fetch_categories():
    task_map = {}
    response = notion.databases.query(database_id=CATEGORIES_DB_ID)
    for row in response['results']:
        props = row['properties']
        cat_name = props['Category Name']['title'][0]['text']['content']
        tag = props['Tag']['rich_text'][0]['text']['content'] if props['Tag']['rich_text'] else ""

        def extract_tasks(priority_field):
            return [item['text']['content'] for item in props[priority_field]['rich_text']]

        for task in extract_tasks('High Priority Task'):
            task_map[task.lower()] = (cat_name, 'High', tag)
        for task in extract_tasks('Medium Priority Task'):
            task_map[task.lower()] = (cat_name, 'Medium', tag)
        for task in extract_tasks('Low Priority Task'):
            task_map[task.lower()] = (cat_name, 'Low', tag)

    return task_map

def update_today_tasks(task_map):
    response = notion.databases.query(database_id=TODAYS_TASKS_DB_ID)
    for row in response['results']:
        row_id = row['id']
        props = row['properties']
        task_name = props['Task Name']['title'][0]['text']['content']
        selected = props['Select']['select']['name'] if props['Select']['select'] else None

        if selected != 'Yes':
            continue

        match = task_map.get(task_name.lower())
        if not match:
            print(f"❌ No match found for: {task_name}")
            continue

        category, priority, tag = match
        notion.pages.update(
            page_id=row_id,
            properties={
                'Category': {"rich_text": [{"text": {"content": category}}]},
                'Priority': {"select": {"name": priority}},
                'Tag': {"rich_text": [{"text": {"content": tag}}]},
                'Select': {"select": {"name": "No"}}
            }
        )
        print(f"✅ Updated: {task_name} → {category}, {priority}, {tag}")

if __name__ == "__main__":
    print("🔍 Loading category mappings...")
    task_map = fetch_categories()
    print("📌 Processing today's tasks...")
    update_today_tasks(task_map)
    print("✅ All done.")
