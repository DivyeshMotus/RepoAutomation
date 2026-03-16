import os
import time
import boto3
import requests
import psycopg2
from dotenv import load_dotenv
from io import BytesIO
from twilio.rest import Client
from config import *

load_dotenv()

print("REPO AUTOMATION\n")

DIVYESH_PHONE = os.getenv("DIVYESH_PHONE")
TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN')
TWILIO_PHONE_NUMBER = os.getenv('TWILIO_PHONE_NUMBER')

def create_connection():
    params = game_db_config()
    game_db_conn = psycopg2.connect(**params)
    game_db_cur = game_db_conn.cursor()
    return game_db_conn, game_db_cur

def get_insurance_ids_that_entered_repo(cursor):
    query = """
    SELECT 
        s.story_id
    FROM story_fresh s
    LEFT JOIN contacts_fresh c
    ON s.destination = c.contact_id
    LEFT JOIN story_fresh profile_sf 
    ON s.story_id = profile_sf.origin 
        AND profile_sf.type = 'newProfile'
    LEFT JOIN devices_patients_pairing dpp ON profile_sf.destination = dpp.patient_id
    LEFT JOIN LATERAL (
        SELECT timestamp
        FROM austin_happy
        WHERE dpp.device_id IS NOT NULL
            AND sn = CONCAT('MPC', dpp.device_id)
            AND step_id = '!1.0.0.0.0'
        ORDER BY timestamp DESC
        LIMIT 1
        ) ah ON true
    WHERE
        s.type = 'insurance'
        AND (
            s.status IN ('reject', 'close', 'optout', 'needDME', 'notCovered')
            OR (s.status = 'sixtyDayFree' AND s.future_timestamp < NOW())
            )
        AND c.contact_id NOT IN (
            SELECT destination 
            FROM story_fresh 
            WHERE type = 'cash' 
                AND destination IS NOT NULL
            )
        AND s.story_id NOT IN (
            SELECT destination 
            FROM story_fresh 
            WHERE type = 'repo'
            )
        AND dpp.device_id IS NOT NULL 
        AND (ah.timestamp IS NULL OR dpp.timestamp > ah.timestamp)
        AND dpp.device_id BETWEEN 100000 AND 9999999
    ORDER by s.created_at, s.story_id, dpp.device_id;
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    return [dict(zip(column_names, row)) for row in rows]

def get_active_customer_support(db_cursor):
    query = """SELECT contact_id
        FROM contacts_fresh cf
        WHERE cf.type = 'customerSupport'
        AND cf.subtype = 'active'
        ORDER BY RANDOM()
        LIMIT 1;
        """
    db_cursor.execute(query)
    row = db_cursor.fetchall()
    contact_id = row[0][0]
    return contact_id

def get_max_story_id(db_cursor):
    query = "SELECT MAX(story_id) FROM story_fresh"
    db_cursor.execute(query)
    row = db_cursor.fetchall()
    max_story_id = row[0][0]
    return max_story_id

def insert_into_age_table(story_id, story_type, db_cursor, db_connection):
    query = """
    INSERT INTO age_table
    (story_id, story_type, status, begin_time)
    VALUES (%s, %s, 'start', NOW())
    RETURNING age_id
    """
    db_cursor.execute(query, (story_id, story_type))
    age_id = db_cursor.fetchone()[0]
    db_connection.commit()
    return age_id

def insert_story_fresh_table(story_id, insurance_id, active_customer_support_contact_id, db_cursor, db_connection):
    query = """
    INSERT INTO story_fresh
        (story_id,
        origin,
        destination,
        type,
        status,
        created_at,
        username)
    VALUES (%s, %s, %s, 'repo', 'start', NOW(), 'service@motusnova.com')
    """
    db_cursor.execute(query, (story_id, active_customer_support_contact_id, insurance_id))
    db_connection.commit()

def insert_into_story_table(story_id, insurance_id, active_customer_support_contact_id, db_cursor, db_connection):
    query = """
    INSERT INTO story
        (story_id,
        origin,
        destination,
        type,
        status,
        created_at,
        username)
    VALUES (%s, %s, %s, 'repo', 'start', NOW(), 'service@motusnova.com')
    """
    db_cursor.execute(query, (story_id, active_customer_support_contact_id, insurance_id))
    db_connection.commit()

def update_age_in_story_fresh_table(story_id, age_id, db_cursor, db_connection):
    query = """
    UPDATE story_fresh
    SET created_at = NOW(),
        username = 'service@motusnova.com',
        age_id = %s
    WHERE story_id = %s
    """
    db_cursor.execute(query, (age_id, story_id,))
    db_connection.commit()

def insert_age_in_story_table(story_id, age_id, db_cursor, db_connection):
    query = """
    INSERT INTO story
        (story_id,
        created_at,
        username,
        age_id)
    VALUES (%s, NOW(), 'service@motusnova.com', %s)
    """
    db_cursor.execute(query, (story_id, age_id))
    db_connection.commit()

def send_completion_sms(insurance_id_list):
    phone_to_send = [DIVYESH_PHONE]
    client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    message_body = (
        f"Made Repo stories for insurance IDs - {insurance_id_list}")
    for phone_number in phone_to_send:
        try:
            message = client.messages.create(
                body=message_body,
                from_=TWILIO_PHONE_NUMBER,
                to=phone_number
            )
            print(f"[SMS] Sent to {phone_number}: SID {message.sid}")
        except Exception as e:
            print(f"[SMS] Failed to send to {phone_number}: {e}")

def make_repo_stories():
    # 1. Connect to DB
    db_connection, db_cursor = create_connection()
    
    # 2. Get the insurance IDs that we will be making repo stories
    insurance_ids = get_insurance_ids_that_entered_repo(db_cursor)
    print(f"List of insurance IDs that we are going to make repo stories for: {insurance_ids}")
    insurance_ids_list = []
    # 3. Iterate through all the insurance IDs
    for insurance_id in insurance_ids:
        try:
            # 4. Get random active customer support
            active_customer_support_contact_id = get_active_customer_support(db_cursor)
            print(f"Got active customer support id: {active_customer_support_contact_id}")
            
            # 5. Get latest story id
            latest_story_id = get_max_story_id(db_cursor)
            print(f"Latest story ID: {latest_story_id}")

            # 6. Get next story id
            next_story_id = latest_story_id + 1
            print(f"Next story ID: {next_story_id}")

            # 7. Insert into story table
            insert_into_story_table(next_story_id, insurance_id['story_id'], active_customer_support_contact_id, db_cursor, db_connection)
            print("Updated story table")

            # 8. Update story fresh table
            insert_story_fresh_table(next_story_id, insurance_id['story_id'], active_customer_support_contact_id, db_cursor, db_connection)
            print("Updated story fresh table")
            
            # 9. Insert into Age Table and get age_id
            age_id = insert_into_age_table(next_story_id, 'repo', db_cursor, db_connection)
            print(f"Generated age id: {age_id}")

            # 10. Update story fresh table with age_id
            update_age_in_story_fresh_table(next_story_id, age_id, db_cursor, db_connection)
            print("Updated story fresh table with age_id")

            # 11. Insert age_id into story table
            insert_age_in_story_table(next_story_id, age_id, db_cursor, db_connection)
            print("Updated story table with age_id")

            # 12. Append to insurance_id list
            insurance_ids_list.append(insurance_id['story_id'])
            print("Added to list")
        except Exception as e:
            print("❌ Error:", e)
    
    # 13. Close DB connection
    db_cursor.close()
    db_connection.close()
    
    # 14. Send Completion text
    send_completion_sms(insurance_ids_list)

if __name__ == "__main__":
    make_repo_stories()
    print("\nScript completed")