import os
import pandas as pd
import requests
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import logging

# Set up logging
logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')


# Load environment variables from .env file
load_dotenv()

# Fetch data from Todoist
todoist_token = os.getenv('TODOIST_TOKEN')

# Connect to MariaDB
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASS')

db = mysql.connector.connect(
    host="localhost",
    user=db_user,
    password=db_pass,
    database="database_name"
)    

try:

    # Fetch data from Todoist
    headers = {
        "Authorization": f"Bearer {todoist_token}"
    }
    response = requests.get('https://api.todoist.com/rest/v2/tasks', headers=headers)
    response.raise_for_status()

    tasks = response.json()

    with db.cursor() as cursor:
        # Insert tasks into MariaDB
        for task in tasks:
            try:
                id = task['id']
                content = task['content']
                due = task['due']['date'] if task['due'] else None
                priority = task['priority']
                is_completed = 1 if task.get('completed') else 0  # Use the get() method to avoid KeyError
                description = task['description'][:255] if 'description' in task else None
                project_id = task['project_id'] if 'project_id' in task else None

                cursor.execute("SELECT COUNT(*) FROM tasks WHERE id = %s", (id,))
                count = cursor.fetchone()[0]

                if count == 0:
                    cursor.execute("""
                        INSERT INTO tasks (id, content, due, priority, is_completed, description, project_id) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, 
                        (id, content, due, priority, is_completed, description, project_id))
                    logging.info(f"Inserted task: {content}")
                else:
                    cursor.execute("""
                        UPDATE tasks 
                        SET content = %s, due = %s, priority = %s, is_completed = %s, description = %s, project_id = %s 
                        WHERE id = %s
                        """, 
                        (content, due, priority, is_completed, description, project_id, id))
                    logging.info(f"Updated task: {content}")

            except Exception as e:
                logging.error(f"Error processing task '{content}': {e}")
    db.commit()

except (mysql.connector.Error, requests.HTTPError, Exception) as error:
    logging.error(f"An error occurred: {error}")

# Create a new pandas DataFrame from the SQL query
df = pd.read_sql_query("SELECT * FROM tasks", db)

# Write the DataFrame to an Excel file
df.to_excel("sheet_name.xlsx", index=False)

# Close the database connection
db.close()
