from flask import Flask, jsonify, request, session
import mysql.connector
from mysql.connector import Error
import os
import json
from flask_cors import CORS
import requests
from dateutil import parser
from datetime import datetime, timedelta
from modelApplicationPath import PrefixMiddleware, ReverseProxied
import base64
import xml.etree.ElementTree as ET
import secrets
import string
from functools import wraps
from werkzeug.security import check_password_hash, generate_password_hash
import threading
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
try:
    from PIL import Image
    import io
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    print("⚠️ PIL/Pillow not available. SVG to PNG conversion will be skipped.")
# Create Flask app instance
app = Flask(__name__)
app.wsgi_app = PrefixMiddleware(app.wsgi_app, "/aiml/corporatewebsite")  #Baseurl
# app.wsgi_app = ReverseProxied(app.wsgi_app)
CORS(app, origins="*", supports_credentials=False)
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=8)  # 8-hour session

# Database Configuration - Load from environment variables
app.config["MYSQL_HOST"] = os.getenv("MYSQL_HOST")
app.config["MYSQL_USER"] = os.getenv("MYSQL_USER")
app.config["MYSQL_PASSWORD"] = os.getenv("MYSQL_PASSWORD")
app.config["MYSQL_DB"] = os.getenv("MYSQL_DB")
app.config["MYSQL_PORT"] = int(os.getenv("MYSQL_PORT", 3306))

# Validate required database configuration
required_db_vars = ["MYSQL_HOST", "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DB"]
missing_vars = [var for var in required_db_vars if not app.config.get(var)]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}. Please check your .env file.")

# TCI API Configuration for Driver License Data - Load from environment variables
TCI_API_BASE_URL = os.getenv("TCI_API_BASE_URL", "https://api.tcil.in/WhatsAppAPILive/api/TcilApi")
TCI_USERID = os.getenv("TCI_USERID")
TCI_PASSWORD = os.getenv("TCI_PASSWORD")

# Validate TCI API configuration
if not TCI_USERID or not TCI_PASSWORD:
    print("⚠️ Warning: TCI_USERID or TCI_PASSWORD not set in environment variables")

# Gmail SMTP Configuration - Load from environment variables
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "True").lower() == "true"

# Validate SMTP configuration
if not SMTP_USERNAME or not SMTP_PASSWORD:
    print("⚠️ Warning: SMTP_USERNAME or SMTP_PASSWORD not set in environment variables")

# OTP Configuration - Load from environment variables
OTP_EXPIRY_MINUTES = int(os.getenv("OTP_EXPIRY_MINUTES", 10))
OTP_LENGTH = int(os.getenv("OTP_LENGTH", 6))
MAX_OTP_ATTEMPTS = int(os.getenv("MAX_OTP_ATTEMPTS", 10))
# Database Configuration

def get_db_connection():
    """Create a new database connection for each request"""
    try:


        connection = mysql.connector.connect(
            host=app.config["MYSQL_HOST"],
            user=app.config["MYSQL_USER"],
            password=app.config["MYSQL_PASSWORD"],
            database=app.config["MYSQL_DB"],
            port=app.config["MYSQL_PORT"],
            autocommit=False,
            charset="utf8mb4",
            use_unicode=True,
            connect_timeout=10,
            auth_plugin="mysql_native_password",
        )
        if connection.is_connected():
            return connection
        return None
    except Error as e:
        print(f"MySQL Error: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

def execute_query(query, params=None, fetch_all=False, fetch_one=False):
    """Execute a database query with proper connection management"""
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        if not connection:
            return {
                "success": False,
                "error": "Could not establish database connection",
            }
        cursor = connection.cursor(dictionary=True, buffered=True)
        cursor.execute(query, params)
        # Determine if this is a write operation
        query_upper = query.strip().upper()
        is_write_operation = query_upper.startswith(("INSERT", "UPDATE", "DELETE"))
        if fetch_one:
            result = cursor.fetchone()
        elif fetch_all:
            result = cursor.fetchall()
        else:
            # For INSERT operations, get the last inserted ID
            if query_upper.startswith("INSERT"):
                result = cursor.lastrowid
            else:
                result = cursor.rowcount
        # Commit write operations
        if is_write_operation:
            connection.commit()
            print(f"✅ Transaction committed for query: {query_upper[:50]}...")
        return {"success": True, "data": result}
    except Error as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"❌ Database error in execute_query: {e}")
        print(f"Query: {query}")
        print(f"Params: {params}")
        print(f"Traceback: {error_trace}")
        if connection:
            connection.rollback()
        return {"success": False, "error": f"Database error: {e}"}
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"❌ Unexpected error in execute_query: {e}")
        print(f"Query: {query}")
        print(f"Params: {params}")
        print(f"Traceback: {error_trace}")
        if connection:
            connection.rollback()
        return {"success": False, "error": f"Unexpected error: {e}"}
    finally:
        # Always close cursor first
        if cursor:
            try:
                cursor.close()
            except Exception as e:
                print(f"Error closing cursor: {e}")
        # Always close connection
        if connection:
            try:
                if connection.is_connected():
                    connection.close()
            except Exception as e:
                print(f"Error closing connection: {e}")

# Multi-Pickup Route Management Functions
def create_multi_pickup_assignment(route_date, driver_dl, vehicle_no):
    """Create a new multi-pickup assignment"""
    try:
        # Ensure route_date is in correct format (YYYY-MM-DD)
        try:
            date_obj = datetime.strptime(route_date, "%Y-%m-%d")
            formatted_route_date = date_obj.strftime("%Y-%m-%d")
        except ValueError:
            # If invalid date, use today
            formatted_route_date = datetime.now().strftime("%Y-%m-%d")
        insert_sql = """
        INSERT INTO b2b_route_assignments (
            route_date, driver_dl, vehicle_no, status, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, NOW(), NOW())
        """
        params = (formatted_route_date, driver_dl, vehicle_no, "pending")
        result = execute_query(insert_sql, params)
        if result.get("success"):
            # Get the route_id
            get_id_sql = "SELECT route_id FROM b2b_route_assignments WHERE driver_dl = %s AND vehicle_no = %s AND DATE(route_date) = %s ORDER BY route_id DESC LIMIT 1"
            id_result = execute_query(
                get_id_sql,
                (driver_dl, vehicle_no, formatted_route_date),
                fetch_one=True,
            )
            if id_result.get("success"):
                return {
                    "success": True,
                    "route_id": id_result.get("data", {}).get("route_id"),
                }
        return {"success": False, "error": result.get("error")}
    except Exception as e:
        return {"success": False, "error": str(e)}
def add_pickup_stop(
    route_id, sequence, latitude, longitude, branch_name, address, contact, branch_code
):
    """Add a pickup stop to an assignment.
    
    IMPORTANT: Weight is NOT set during route stop creation.
    Weight should ONLY be set when completing a stop via:
    - /multi-pickup/complete-stop/<stop_id>
    - /multi-pickup/complete-stop-by-sequence/<route_id>/<sequence>
    - /multi-pickup/auto-complete-current
    
    Weight should NEVER be taken from est_weight (Customer Master table).
    Weight must be provided explicitly via the completion API endpoints.
    """
    try:
        insert_sql = """
        INSERT INTO b2b_route_stops (
            route_id, sequence, latitude, longitude, branch_name, address, 
            contact, branch_code, status, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        """
        params = (
            route_id,
            sequence,
            latitude,
            longitude,
            branch_name,
            address,
            contact,
            branch_code,
            "pending",
        )
        result = execute_query(insert_sql, params)
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}
def get_assignment_details(route_id):
    """Get assignment details with all stops"""
    try:
        # Get assignment info
        assignment_sql = """
        SELECT * FROM b2b_route_assignments WHERE route_id = %s
        """
        assignment_result = execute_query(assignment_sql, (route_id,), fetch_one=True)
        if not assignment_result.get("success"):
            return assignment_result
        # Get all stops for this assignment
        stops_sql = """
        SELECT * FROM b2b_route_stops 
        WHERE route_id = %s 
        ORDER BY sequence ASC
        """
        stops_result = execute_query(stops_sql, (route_id,), fetch_all=True)
        if not stops_result.get("success"):
            return stops_result
        assignment_data = assignment_result.get("data")
        stops_data = stops_result.get("data", [])
        return {
            "success": True,
            "data": {"assignment": assignment_data, "stops": stops_data},
        }
    except Exception as e:
        return {"success": False, "error": str(e)}
def update_stop_status(
    stop_id,
    status,
    weight=None,
    remark=None,
    waste_image_url=None,
    receipt_image_url=None,
    poc_name=None,
    poc_designation=None,
    poc_signature=None,
):
    """Update stop status and completion details with photo, receipt, and POC details support.
    Automatically updates branch_pickup_frequency status when a stop is completed."""
    try:
        update_fields = ["status = %s", "updated_at = NOW()"]
        params = [status]
        if status == "completed":
            update_fields.append("completed_at = NOW()")
            update_fields.append("pickup_ended_at = NOW()")
            if weight is not None:
                update_fields.append("weight = %s")
                params.append(weight)
                print(f"✅ [update_stop_status] Updating weight to {weight} kg for stop_id {stop_id}")
            else:
                print(f"⚠️ [update_stop_status] No weight provided for stop_id {stop_id} - weight will not be updated")
            if remark:
                update_fields.append("remark = %s")
                params.append(remark)
            if waste_image_url:
                update_fields.append("waste_image_url = %s")
                params.append(waste_image_url)
            if receipt_image_url:
                update_fields.append("receipt_image_url = %s")
                params.append(receipt_image_url)
            if poc_name:
                update_fields.append("poc_name = %s")
                params.append(poc_name)
                print(f"✅ [update_stop_status] Updating POC name: {poc_name} for stop_id {stop_id}")
            if poc_designation:
                update_fields.append("poc_designation = %s")
                params.append(poc_designation)
                print(f"✅ [update_stop_status] Updating POC designation: {poc_designation} for stop_id {stop_id}")
            if poc_signature:
                update_fields.append("poc_signature = %s")
                params.append(poc_signature)
                print(f"✅ [update_stop_status] Updating POC signature for stop_id {stop_id}")
        elif status == "in_progress":
            update_fields.append("pickup_started_at = NOW()")
        params.append(stop_id)
        update_sql = f"""
        UPDATE b2b_route_stops 
        SET {", ".join(update_fields)}
        WHERE id = %s
        """
        print(f"🔍 [update_stop_status] SQL: {update_sql}")
        print(f"🔍 [update_stop_status] Params: {params}")
        result = execute_query(update_sql, params)
        if result.get("success"):
            print(f"✅ [update_stop_status] Successfully updated stop_id {stop_id}")
        else:
            print(f"❌ [update_stop_status] Failed to update stop_id {stop_id}: {result.get('error')}")
        
        # Automatically update branch_pickup_frequency when status changes to completed or in_progress
        if result.get("success") and status in ("completed", "in_progress"):
            try:
                # Get branch_code and route_date from the route stop
                get_stop_info_sql = """
                SELECT rs.branch_code, DATE(ra.route_date) as route_date
                FROM b2b_route_stops rs
                JOIN b2b_route_assignments ra ON rs.route_id = ra.route_id
                WHERE rs.id = %s
                """
                stop_info_result = execute_query(get_stop_info_sql, (stop_id,), fetch_one=True)
                if stop_info_result.get("success") and stop_info_result.get("data"):
                    stop_data = stop_info_result.get("data")
                    branch_code = stop_data.get("branch_code")
                    route_date = stop_data.get("route_date")
                    if branch_code and route_date:
                        # Update branch_pickup_frequency status
                        update_branch_pickup_frequency_status(branch_code, route_date, status)
            except Exception as update_error:
                # Log error but don't fail the main update
                print(f"⚠️ Warning: Failed to update branch_pickup_frequency in update_stop_status: {str(update_error)}")
        
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}
def sync_segregation_to_impact(branch_code, corporate_code=None):
    """
    Sync data from b2b_segregation to b2b_impact table.
    Aggregates all segregation data for a branch and updates/inserts into b2b_impact.
    
    Args:
        branch_code: The branch code to sync
        corporate_code: The corporate code for this branch (optional, will be fetched if not provided)
    
    Returns:
        dict: Result with success status
    """
    try:
        # If corporate_code not provided, fetch it from branch_master
        if not corporate_code:
            get_corporate_sql = """
            SELECT corporate_code 
            FROM b2b_corporate_branch_master 
            WHERE branch_code COLLATE utf8mb4_unicode_ci = %s COLLATE utf8mb4_unicode_ci
            LIMIT 1
            """
            corp_result = execute_query(get_corporate_sql, (branch_code,), fetch_one=True)
            if corp_result.get("success") and corp_result.get("data"):
                corporate_code = corp_result.get("data").get("corporate_code")
            else:
                print(f"⚠️ [sync_segregation_to_impact] Could not find corporate_code for branch {branch_code}")
                return {"success": False, "error": "Corporate code not found"}
        
        # Ensure corporate_code is string (b2b_impact uses varchar)
        corporate_code_str = str(corporate_code)
        
        # Aggregate all segregation data for this branch
        # Note: b2b_impact table has total_cardboard, but b2b_segregation doesn't have cardboard column
        # So we'll set total_cardboard to 0 or you can add a cardboard column to b2b_segregation later
        # Handle corporate_code as both string and int (b2b_segregation might have it as varchar, b2b_impact as varchar)
        aggregation_sql = """
        SELECT 
            branch_code,
            CAST(corporate_code AS CHAR) as corporate_code,
            COALESCE(SUM(total_weight), 0) as total_weight,
            COALESCE(SUM(plastic), 0) as total_plastic,
            COALESCE(SUM(paper), 0) as total_paper,
            COALESCE(SUM(e_waste), 0) as total_ewaste,
            COALESCE(SUM(metal), 0) as total_metal,
            COALESCE(SUM(glass), 0) as total_glass
        FROM b2b_segregation
        WHERE branch_code COLLATE utf8mb4_unicode_ci = %s COLLATE utf8mb4_unicode_ci
            AND CAST(corporate_code AS CHAR) = %s
        GROUP BY branch_code, CAST(corporate_code AS CHAR)
        """
        
        result = execute_query(aggregation_sql, (branch_code, corporate_code_str), fetch_one=True)
        
        if not result.get("success") or not result.get("data"):
            print(f"⚠️ [sync_segregation_to_impact] No segregation data found for branch {branch_code}")
            return {"success": False, "error": "No segregation data found"}
        
        seg_data = result.get("data")
        
        # Calculate cardboard from paper (assuming paper includes cardboard)
        # If you have a separate cardboard column, use it instead
        total_cardboard = 0  # You may need to add a cardboard column to b2b_segregation
        
        # Calculate impact metrics (trees, water, energy saved, landfill saved)
        # Conversion factors (adjust based on your requirements):
        # - 1 kg paper = ~0.02 trees saved
        # - 1 kg waste = ~3.5 liters water saved
        # - 1 kg waste = ~0.5 kWh energy saved
        total_waste = float(seg_data.get("total_weight", 0) or 0)
        paper_weight = float(seg_data.get("total_paper", 0) or 0)
        
        trees_saved = round(paper_weight * 0.02, 2)  # Approximate: 1 kg paper = 0.02 trees
        water_saved = round(total_waste * 3.5, 2)  # Approximate: 1 kg waste = 3.5 liters water
        energy_saved = round(total_waste * 0.5, 2)  # Approximate: 1 kg waste = 0.5 kWh energy saved
        landfill_saved = round(total_waste, 2)  # Landfill saved equals total waste diverted
        
        # Check if record exists in b2b_impact
        # Handle corporate_code matching (b2b_impact uses varchar, so convert to string for comparison)
        check_sql = """
        SELECT id FROM b2b_impact
        WHERE branch_code COLLATE utf8mb4_unicode_ci = %s COLLATE utf8mb4_unicode_ci
            AND CAST(corporate_code AS CHAR) = %s
        """
        check_result = execute_query(check_sql, (branch_code, corporate_code_str), fetch_one=True)
        
        if check_result.get("success") and check_result.get("data"):
            # Update existing record
            update_sql = """
            UPDATE b2b_impact
            SET 
                total_weight = %s,
                total_plastic = %s,
                total_cardboard = %s,
                total_paper = %s,
                total_ewaste = %s,
                trees_saved = %s,
                water_saved = %s,
                energy_saved = %s,
                landfill_saved = %s,
                updated_at = NOW()
            WHERE branch_code COLLATE utf8mb4_unicode_ci = %s COLLATE utf8mb4_unicode_ci
                AND CAST(corporate_code AS CHAR) = %s
            """
            update_params = (
                total_waste,
                float(seg_data.get("total_plastic", 0) or 0),
                total_cardboard,
                paper_weight,
                float(seg_data.get("total_ewaste", 0) or 0),
                trees_saved,
                water_saved,
                energy_saved,
                landfill_saved,
                branch_code,
                corporate_code_str
            )
            update_result = execute_query(update_sql, update_params)
            
            if update_result.get("success"):
                print(f"✅ [sync_segregation_to_impact] Updated b2b_impact for branch {branch_code}, corporate {corporate_code_str}")
                return {"success": True, "action": "updated"}
            else:
                print(f"❌ [sync_segregation_to_impact] Failed to update: {update_result.get('error')}")
                return {"success": False, "error": update_result.get("error")}
        else:
            # Insert new record
            # landfill_saved is a decimal field, not a date field
            insert_sql = """
            INSERT INTO b2b_impact (
                corporate_code, branch_code, total_weight, total_plastic,
                total_cardboard, total_paper, total_ewaste,
                trees_saved, water_saved, energy_saved, landfill_saved,
                created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            """
            insert_params = (
                corporate_code_str,
                branch_code,
                total_waste,
                float(seg_data.get("total_plastic", 0) or 0),
                total_cardboard,
                paper_weight,
                float(seg_data.get("total_ewaste", 0) or 0),
                trees_saved,
                water_saved,
                energy_saved,
                landfill_saved
            )
            insert_result = execute_query(insert_sql, insert_params)
            
            if insert_result.get("success"):
                print(f"✅ [sync_segregation_to_impact] Inserted new b2b_impact record for branch {branch_code}, corporate {corporate_code_str}")
                return {"success": True, "action": "inserted"}
            else:
                print(f"❌ [sync_segregation_to_impact] Failed to insert: {insert_result.get('error')}")
                return {"success": False, "error": insert_result.get("error")}
                
    except Exception as e:
        print(f"❌ [sync_segregation_to_impact] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}

def update_branch_pickup_frequency_status(branch_code, pickup_date, new_status):
    """Update branch_pickup_frequency status when route stop status changes"""
    try:
        # Find matching pickup in branch_pickup_frequency by branch_code and pickup_date
        # Map route stop status to branch_pickup_frequency status
        status_map = {
            "completed": "COMPLETED",
            "in_progress": "IN_PROGRESS",
            "pending": "PENDING"
        }
        mapped_status = status_map.get(new_status.lower(), new_status.upper())
        
        update_sql = """
        UPDATE branch_pickup_frequency 
        SET status = %s, updated_at = NOW()
        WHERE branch_code COLLATE utf8mb4_unicode_ci = %s COLLATE utf8mb4_unicode_ci
        AND DATE(pickup_date) = DATE(%s)
        AND UPPER(status) != %s
        """
        result = execute_query(update_sql, (mapped_status, branch_code, pickup_date, mapped_status))
        if result.get("success"):
            print(f"✅ Updated branch_pickup_frequency status to {mapped_status} for branch {branch_code} on {pickup_date}")
        else:
            print(f"⚠️ Warning: Failed to update branch_pickup_frequency: {result.get('error')}")
        return result
    except Exception as e:
        print(f"❌ Error updating branch_pickup_frequency: {str(e)}")
        return {"success": False, "error": str(e)}

def update_stop_status_by_sequence(
    route_id,
    sequence,
    status,
    weight=None,
    remark=None,
    waste_image_url=None,
    receipt_image_url=None,
    poc_name=None,
    poc_designation=None,
    poc_signature=None,
):
    """Update stop status by route_id + sequence instead of stop_id.
    Automatically updates branch_pickup_frequency status when a stop is completed."""
    try:
        update_fields = ["status = %s", "updated_at = NOW()"]
        params = [status]
        if status == "completed":
            update_fields.append("completed_at = NOW()")
            update_fields.append("pickup_ended_at = NOW()")
            if weight is not None:
                update_fields.append("weight = %s")
                params.append(weight)
                print(f"✅ Updating weight to {weight} kg for route_id {route_id}, sequence {sequence}")
            else:
                print(f"⚠️ No weight provided for route_id {route_id}, sequence {sequence} - weight will not be updated")
            if remark:
                update_fields.append("remark = %s")
                params.append(remark)
            if waste_image_url:
                update_fields.append("waste_image_url = %s")
                params.append(waste_image_url)
            if receipt_image_url:
                update_fields.append("receipt_image_url = %s")
                params.append(receipt_image_url)
            if poc_name:
                update_fields.append("poc_name = %s")
                params.append(poc_name)
                print(f"✅ [update_stop_status_by_sequence] Updating POC name: {poc_name} for route_id {route_id}, sequence {sequence}")
            if poc_designation:
                update_fields.append("poc_designation = %s")
                params.append(poc_designation)
                print(f"✅ [update_stop_status_by_sequence] Updating POC designation: {poc_designation} for route_id {route_id}, sequence {sequence}")
            if poc_signature:
                update_fields.append("poc_signature = %s")
                params.append(poc_signature)
                print(f"✅ [update_stop_status_by_sequence] Updating POC signature for route_id {route_id}, sequence {sequence}")
        elif status == "in_progress":
            update_fields.append("pickup_started_at = NOW()")
        # Add route_id and sequence to WHERE clause
        params.extend([route_id, sequence])
        update_sql = f"""
        UPDATE b2b_route_stops 
        SET {", ".join(update_fields)}
        WHERE route_id = %s AND sequence = %s
        """
        result = execute_query(update_sql, params)
        
        # Automatically update branch_pickup_frequency when status changes to completed or in_progress
        if result.get("success") and status in ("completed", "in_progress"):
            try:
                # Get branch_code and route_date from the route stop
                get_stop_info_sql = """
                SELECT rs.branch_code, DATE(ra.route_date) as route_date
                FROM b2b_route_stops rs
                JOIN b2b_route_assignments ra ON rs.route_id = ra.route_id
                WHERE rs.route_id = %s AND rs.sequence = %s
                """
                stop_info_result = execute_query(get_stop_info_sql, (route_id, sequence), fetch_one=True)
                if stop_info_result.get("success") and stop_info_result.get("data"):
                    stop_data = stop_info_result.get("data")
                    branch_code = stop_data.get("branch_code")
                    route_date = stop_data.get("route_date")
                    if branch_code and route_date:
                        # Update branch_pickup_frequency status
                        update_branch_pickup_frequency_status(branch_code, route_date, status)
            except Exception as update_error:
                # Log error but don't fail the main update
                print(f"⚠️ Warning: Failed to update branch_pickup_frequency in update_stop_status_by_sequence: {str(update_error)}")
        
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}
def get_next_sequence(route_id):
    """Get the next sequence number that should be completed (sequential logic)"""
    try:
        # Get all sequences and their status, ordered by sequence
        sql = """
        SELECT sequence, status 
        FROM b2b_route_stops 
        WHERE route_id = %s 
        ORDER BY sequence ASC
        """
        result = execute_query(sql, (route_id,), fetch_all=True)
        if not result.get("success"):
            return {"success": False, "error": "Failed to fetch sequences"}
        sequences = result.get("data", [])
        # Find the first non-completed sequence
        for seq_data in sequences:
            if seq_data.get("status") in ["pending", "in_progress"]:
                return {
                    "success": True,
                    "next_sequence": seq_data.get("sequence"),
                    "status": seq_data.get("status"),
                }
        # All sequences completed
        return {"success": True, "next_sequence": None, "status": "all_completed"}
    except Exception as e:
        return {"success": False, "error": str(e)}
def validate_sequential_pickup(route_id, sequence, action):
    """Validate that pickup follows sequential order"""
    try:
        # Get next sequence info
        next_info = get_next_sequence(route_id)
        if not next_info.get("success"):
            return {"valid": False, "error": next_info.get("error")}
        next_sequence = next_info.get("next_sequence")
        current_status = next_info.get("status")
        if action == "start":
            # Can only start if this is the next sequence and it's pending
            if sequence != next_sequence:
                return {
                    "valid": False,
                    "error": f"Must complete sequences in order. Next sequence is {next_sequence}",
                    "next_sequence": next_sequence,
                }
            if current_status != "pending":
                return {
                    "valid": False,
                    "error": f"Sequence {sequence} is already {current_status}",
                    "current_status": current_status,
                }
        elif action == "complete":
            # Can only complete if this is the next sequence and it's in_progress
            if sequence != next_sequence:
                return {
                    "valid": False,
                    "error": f"Must complete sequences in order. Next sequence is {next_sequence}",
                    "next_sequence": next_sequence,
                }
            if current_status != "in_progress":
                return {
                    "valid": False,
                    "error": f"Must start sequence {sequence} before completing it",
                    "current_status": current_status,
                }
        return {"valid": True}
    except Exception as e:
        return {"valid": False, "error": str(e)}
def update_assignment_status(route_id, status):
    """Update assignment status"""
    try:
        update_fields = ["status = %s", "updated_at = NOW()"]
        params = [status]
        if status == "in_progress":
            update_fields.append("trip_started_at = NOW()")
        elif status == "completed":
            update_fields.append("trip_ended_at = NOW()")
        params.append(route_id)
        update_sql = f"""
        UPDATE b2b_route_assignments 
        SET {", ".join(update_fields)}
        WHERE route_id = %s
        """
        result = execute_query(update_sql, params)
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}
def handle_signature_upload(signature_data, signature_file=None):
    """
    Handle POC signature upload - supports both file upload and base64 string.
    Automatically converts SVG to PNG for better compatibility with SOAP service.
    Returns the file path (URL) if successful, None otherwise.
    """
    try:
        # If signature_file is provided (multipart/form-data), use file upload
        if signature_file and signature_file.filename:
            upload_result = upload_and_get_path(signature_file)
            if upload_result["status"] == "success":
                print(f"[handle_signature_upload] File uploaded successfully: {upload_result['file_path']}")
                return upload_result["file_path"]
            else:
                print(f"[handle_signature_upload] File upload failed: {upload_result.get('message', 'Unknown error')}")
                return None
        
        # If signature_data is provided as base64 string, convert to file and upload
        if signature_data:
            # Check if it's already a URL/path, return it as is
            if isinstance(signature_data, str) and (signature_data.startswith('http') or signature_data.startswith('/')):
                print(f"[handle_signature_upload] Signature is already a URL: {signature_data}")
                return signature_data
            
            # Check if it's base64 encoded (with or without data URI prefix)
            if isinstance(signature_data, str):
                try:
                    # Extract base64 data and detect image type
                    image_type = None
                    base64_data = None
                    
                    if signature_data.startswith('data:image'):
                        # Extract image type and base64 data
                        # Format: data:image/svg+xml;base64,... or data:image/png;base64,...
                        parts = signature_data.split(',')
                        if len(parts) < 2:
                            print("[handle_signature_upload] Invalid data URI format")
                            return None
                        
                        header = parts[0]
                        base64_data = parts[1]
                        
                        # Detect image type from header
                        if 'svg+xml' in header or 'svg' in header:
                            image_type = 'svg'
                        elif 'png' in header:
                            image_type = 'png'
                        elif 'jpeg' in header or 'jpg' in header:
                            image_type = 'jpeg'
                        else:
                            image_type = 'png'  # Default fallback
                    else:
                        # Assume raw base64 - try to detect if it's SVG by checking content
                        base64_data = signature_data
                        try:
                            decoded = base64.b64decode(base64_data, validate=True)
                            # Check first 500 bytes for SVG markers
                            preview = decoded[:500] if len(decoded) > 500 else decoded
                            if preview.startswith(b'<?xml') or preview.startswith(b'<svg') or b'<svg' in preview:
                                image_type = 'svg'
                            else:
                                image_type = 'png'  # Default to PNG for binary images
                        except Exception as decode_error:
                            print(f"[handle_signature_upload] Base64 decode validation failed: {str(decode_error)}")
                            return None
                    
                    if not base64_data:
                        print("[handle_signature_upload] No base64 data found")
                        return None
                    
                    # Decode base64 to binary
                    try:
                        image_data = base64.b64decode(base64_data, validate=True)
                        if len(image_data) == 0:
                            print("[handle_signature_upload] Decoded image data is empty")
                            return None
                    except Exception as decode_error:
                        print(f"[handle_signature_upload] Base64 decode failed: {str(decode_error)}")
                        return None
                    
                    # Convert SVG to PNG if needed - REQUIRED for SOAP service compatibility
                    final_image_data = image_data
                    filename = "poc_signature.png"
                    conversion_success = False
                    
                    if image_type == 'svg':
                        print("[handle_signature_upload] Converting SVG to PNG...")
                        
                        # Method 1: Try cairosvg (most reliable)
                        try:
                            import cairosvg
                            final_image_data = cairosvg.svg2png(bytestring=image_data)
                            if final_image_data and len(final_image_data) > 0:
                                print("[handle_signature_upload] SVG converted to PNG using cairosvg")
                                conversion_success = True
                            else:
                                print("[handle_signature_upload] cairosvg conversion returned empty data")
                        except ImportError:
                            print("[handle_signature_upload] cairosvg library not available")
                        except Exception as e:
                            print(f"[handle_signature_upload] cairosvg conversion failed: {str(e)}")
                        
                        # Method 2: Try svglib + reportlab
                        if not conversion_success:
                            try:
                                from svglib.svglib import svg2rlg
                                from reportlab.graphics import renderPM
                                import io as io_module
                                
                                drawing = svg2rlg(io_module.BytesIO(image_data))
                                if drawing:
                                    png_buffer = io_module.BytesIO()
                                    renderPM.drawToFile(drawing, png_buffer, fmt='PNG')
                                    final_image_data = png_buffer.getvalue()
                                    if final_image_data and len(final_image_data) > 0:
                                        print("[handle_signature_upload] SVG converted to PNG using svglib+reportlab")
                                        conversion_success = True
                                    else:
                                        print("[handle_signature_upload] svglib conversion returned empty data")
                                else:
                                    print("[handle_signature_upload] svglib failed to parse SVG")
                            except ImportError:
                                print("[handle_signature_upload] svglib library not available")
                            except Exception as e:
                                print(f"[handle_signature_upload] svglib conversion failed: {str(e)}")
                        
                        # If conversion failed, return error - don't upload SVG
                        if not conversion_success:
                            error_msg = "SVG to PNG conversion failed. Please install cairosvg or svglib library."
                            print(f"[handle_signature_upload] ERROR: {error_msg}")
                            return None
                    
                    # Validate converted image data
                    if not final_image_data or len(final_image_data) == 0:
                        print("[handle_signature_upload] Final image data is empty after processing")
                        return None
                    
                    # Create proper file-like object for SOAP upload
                    class SignatureFileWrapper:
                        """File-like wrapper for signature data that properly handles multiple read() calls"""
                        def __init__(self, file_data, filename):
                            if isinstance(file_data, io.BytesIO):
                                self._data = file_data.getvalue()
                            elif isinstance(file_data, bytes):
                                self._data = file_data
                            else:
                                raise ValueError("file_data must be bytes or BytesIO")
                            
                            self.filename = filename
                            self._position = 0
                            self._size = len(self._data)
                        
                        def read(self, size=-1):
                            """Read data from the file, supporting multiple calls"""
                            if self._position >= self._size:
                                return b''
                            
                            if size == -1:
                                data = self._data[self._position:]
                                self._position = self._size
                            else:
                                end_pos = min(self._position + size, self._size)
                                data = self._data[self._position:end_pos]
                                self._position = end_pos
                            return data
                        
                        def seek(self, pos, whence=0):
                            """Seek to position in file"""
                            if whence == 0:  # Absolute position
                                self._position = max(0, min(pos, self._size))
                            elif whence == 1:  # Relative to current position
                                self._position = max(0, min(self._position + pos, self._size))
                            elif whence == 2:  # Relative to end
                                self._position = max(0, min(self._size + pos, self._size))
                            return self._position
                        
                        def tell(self):
                            """Get current position in file"""
                            return self._position
                        
                        def __len__(self):
                            """Get file size"""
                            return self._size
                        
                        def close(self):
                            """Close file (no-op for in-memory data)"""
                            pass
                    
                    try:
                        file_wrapper = SignatureFileWrapper(final_image_data, filename)
                        print(f"[handle_signature_upload] Uploading signature as {filename} (size: {len(file_wrapper)} bytes)")
                        
                        # Verify the file wrapper has data
                        if len(file_wrapper) == 0:
                            print("[handle_signature_upload] File wrapper is empty, cannot upload")
                            return None
                        
                        upload_result = upload_and_get_path(file_wrapper)
                        
                        if upload_result["status"] == "success":
                            print(f"[handle_signature_upload] Signature uploaded successfully: {upload_result['file_path']}")
                            return upload_result["file_path"]
                        else:
                            error_msg = upload_result.get('message', 'Unknown upload error')
                            print(f"[handle_signature_upload] Upload failed: {error_msg}")
                            return None
                            
                    except Exception as wrapper_error:
                        print(f"[handle_signature_upload] File wrapper creation/upload failed: {str(wrapper_error)}")
                        import traceback
                        traceback.print_exc()
                        return None
                        
                except Exception as e:
                    print(f"[handle_signature_upload] Base64 conversion/upload failed: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    return None
        
        print("[handle_signature_upload] No signature data or file provided")
        return None
    except Exception as e:
        print(f"[handle_signature_upload] Error handling signature: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def upload_and_get_path(file):
    """Upload file through SOAP service and get file path"""
    if not file:
        return {"status": "error", "message": "No file provided for upload"}
    
    # Hardcoded parameters - should be moved to environment variables
    awsparam = {
        "CompanyCode": "80",
        "DivisionCode": "80",
        "DocType": "OSG",
        "DocNo": "2025",
        "DocBranch": "OSGG",
        "UserCode": "88808",
        "UserBranch": "OSGG",
        "DocDate": datetime.today().strftime("%d-%b-%Y"),
    }
    
    # Check file size and read content
    try:
        # Reset to beginning
        if hasattr(file, 'seek'):
            file.seek(0)
        
        # Read file content
        byte_file = file.read()
        
        # Reset again for potential reuse
        if hasattr(file, 'seek'):
            file.seek(0)
        
        if not byte_file or len(byte_file) == 0:
            return {"status": "error", "message": "File is empty, cannot upload"}
        
        # Validate file size (max 10MB)
        max_size = 10 * 1024 * 1024  # 10MB
        if len(byte_file) > max_size:
            return {"status": "error", "message": f"File size exceeds maximum allowed size of {max_size} bytes"}
            
    except Exception as read_error:
        return {"status": "error", "message": f"Error reading file: {str(read_error)}"}
    # Prepare the SOAP request for file upload
    soap_upload_request = f"""
<soapenv:Envelope xmlns:soapenv='http://schemas.xmlsoap.org/soap/envelope/' xmlns:tem='http://tempuri.org/'>
<soapenv:Header/>
<soapenv:Body>
<tem:ScanUpload>
<tem:byteFile>{base64.b64encode(byte_file).decode()}</tem:byteFile>
<tem:CompanyCode>{awsparam["CompanyCode"]}</tem:CompanyCode>
<tem:DivisionCode>{awsparam["DivisionCode"]}</tem:DivisionCode>
<tem:DocType>{awsparam["DocType"]}</tem:DocType>
<tem:DocNo>{awsparam["DocNo"]}</tem:DocNo>
<tem:DocDate>{awsparam["DocDate"]}</tem:DocDate>
<tem:DocBrn>{awsparam["DocBranch"]}</tem:DocBrn>
<tem:UserCode>{awsparam["UserCode"]}</tem:UserCode>
<tem:UserBranch>{awsparam["UserBranch"]}</tem:UserBranch>
<tem:FileName>{file.filename}</tem:FileName>
<tem:strFileExt>{os.path.splitext(file.filename)[1]}</tem:strFileExt>
</tem:ScanUpload>
</soapenv:Body>
</soapenv:Envelope>
    """
    headers = {
        "Content-Type": "text/xml",
        "SOAPAction": "http://tempuri.org/ScanUpload",
    }
    upload_url = (
        "https://tlog.grouptci.in/WebServices/TCIL_DocumentScan/DocumentScan.asmx"
    )
    try:
        # Upload file with timeout
        try:
            upload_response = requests.post(
                upload_url, 
                data=soap_upload_request, 
                headers=headers,
                timeout=30  # 30 second timeout
            )
        except requests.exceptions.Timeout:
            return {
                "status": "error",
                "message": "Upload request timed out. Please try again.",
            }
        except requests.exceptions.RequestException as req_error:
            return {
                "status": "error",
                "message": f"Network error during upload: {str(req_error)}",
            }
        
        # Check if upload was successful
        if upload_response.status_code != 200:
            return {
                "status": "error",
                "message": f"Upload failed with HTTP status {upload_response.status_code}",
            }
        
        # Parse upload response to check for errors
        try:
            upload_root = ET.fromstring(upload_response.text)
            upload_ns = {
                "soap": "http://schemas.xmlsoap.org/soap/envelope/",
                "tem": "http://tempuri.org/",
            }
            upload_result = upload_root.find(".//tem:ScanUploadResult", upload_ns)
            if upload_result is not None and upload_result.text:
                result_text = upload_result.text.strip()
                if (
                    "Please Contact With Administrator" in result_text
                    or "Alert" in result_text
                    or "Error" in result_text
                    or "Failed" in result_text
                ):
                    return {
                        "status": "error",
                        "message": f"SOAP Service Error: {result_text}",
                    }
        except ET.ParseError as parse_error:
            print(f"[upload_and_get_path] XML parse error in upload response: {str(parse_error)}")
            # Continue to try retrieving file path anyway
        except Exception as parse_error:
            print(f"[upload_and_get_path] Error parsing upload response: {str(parse_error)}")
            # Continue to try retrieving file path anyway
        # SOAP request to retrieve the file path
        soap_retrieve_request = f"""
<soapenv:Envelope xmlns:soapenv='http://schemas.xmlsoap.org/soap/envelope/' xmlns:tem='http://tempuri.org/'>
<soapenv:Header/>
<soapenv:Body>
<tem:ShowScanUploadDoc>
<tem:CompanyCode>{awsparam["CompanyCode"]}</tem:CompanyCode>
<tem:DivisionCode>{awsparam["DivisionCode"]}</tem:DivisionCode>
<tem:DocType>{awsparam["DocType"]}</tem:DocType>
<tem:DocNo>{awsparam["DocNo"]}</tem:DocNo>
<tem:DocDate>{awsparam["DocDate"]}</tem:DocDate>
<tem:DocBrn>{awsparam["DocBranch"]}</tem:DocBrn>
<tem:UserCode>{awsparam["UserCode"]}</tem:UserCode>
</tem:ShowScanUploadDoc>
</soapenv:Body>
</soapenv:Envelope>
        """
        retrieve_headers = {
            "Content-Type": "text/xml",
            "SOAPAction": "http://tempuri.org/ShowScanUploadDoc",
        }
        # Retrieve file path with timeout
        try:
            retrieve_response = requests.post(
                upload_url, 
                data=soap_retrieve_request, 
                headers=retrieve_headers,
                timeout=30  # 30 second timeout
            )
        except requests.exceptions.Timeout:
            return {
                "status": "error",
                "message": "Retrieve file path request timed out. Please try again.",
            }
        except requests.exceptions.RequestException as req_error:
            return {
                "status": "error",
                "message": f"Network error retrieving file path: {str(req_error)}",
            }
        
        if retrieve_response.status_code != 200:
            return {
                "status": "error",
                "message": f"Retrieve file path failed with HTTP status {retrieve_response.status_code}",
            }
        
        # Parse XML to extract file path
        try:
            root = ET.fromstring(retrieve_response.text)
            ns = {
                "soapenv": "http://schemas.xmlsoap.org/soap/envelope/",
                "tem": "http://tempuri.org/",
            }
            url_element = root.find(".//tem:ShowScanUploadDocResult", ns)
            if url_element is not None and url_element.text:
                file_path = url_element.text.strip()
                if file_path:
                    return {"status": "success", "file_path": file_path}
            
            # Try alternative element search without namespace
            alt_element = root.find(".//ShowScanUploadDocResult")
            if alt_element is not None and alt_element.text:
                file_path = alt_element.text.strip()
                if file_path:
                    return {"status": "success", "file_path": file_path}
            
            return {
                "status": "error",
                "message": "File path not found in SOAP response. Upload may have failed.",
            }
        except ET.ParseError as parse_error:
            print(f"[upload_and_get_path] XML parsing error: {str(parse_error)}")
            print(f"[upload_and_get_path] Response text: {retrieve_response.text[:500]}")
            return {
                "status": "error", 
                "message": f"XML parsing error while retrieving file path: {str(parse_error)}"
            }
        except Exception as parse_error:
            print(f"[upload_and_get_path] Error parsing retrieve response: {str(parse_error)}")
            return {
                "status": "error",
                "message": f"Error parsing SOAP response: {str(parse_error)}",
            }
    except Exception as e:
        print(f"[upload_and_get_path] Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "status": "error", 
            "message": f"Upload failed: {str(e)}"
        }
def calculate_pickup_dates(frequency, selected_days, pickup_count=15):
    """Calculate pickup dates based on frequency and selected days for next N pickup occurrences"""
    pickup_dates = []
    current_date = datetime.now().date()
    # Parse selected days if it's a JSON string
    if isinstance(selected_days, str):
        try:
            selected_days = json.loads(selected_days)
        except (json.JSONDecodeError, TypeError):
            selected_days = []
    if not selected_days:
        return pickup_dates
    # Map day names to weekday numbers (Monday=0, Sunday=6)
    day_mapping = {
        "monday": 0,
        "tuesday": 1,
        "wednesday": 2,
        "thursday": 3,
        "friday": 4,
        "saturday": 5,
        "sunday": 6,
    }
    selected_weekdays = [
        day_mapping.get(day.lower())
        for day in selected_days
        if day.lower() in day_mapping
    ]
    if not selected_weekdays:
        return pickup_dates
    # Generate pickup_count number of pickup dates starting from tomorrow
    check_date = current_date + timedelta(days=1)
    max_days_to_check = 60  # Maximum 60 calendar days to find pickup_count pickups
    days_checked = 0
    while len(pickup_dates) < pickup_count and days_checked < max_days_to_check:
        if check_date.weekday() in selected_weekdays:
            pickup_dates.append(check_date)
        check_date += timedelta(days=1)
        days_checked += 1
    return pickup_dates
def generate_pickup_schedule(
    branch_code, frequency, selected_days, branch_lat, branch_long
):
    """Generate 15 days of pickup requests for a branch"""
    try:
        # Calculate pickup dates - generate 15 pickup occurrences
        pickup_dates = calculate_pickup_dates(frequency, selected_days, 15)
        if not pickup_dates:
            print(f"No pickup dates generated for branch {branch_code}")
            return
        current_date = datetime.now().date()
        # Insert pickup requests for each calculated date
        for pickup_date in pickup_dates:
            insert_sql = """
            INSERT INTO branch_pickup_frequency (
                branch_code, request_date, est_weight, latitude, longitude, 
                average_contamination, pickup_date, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = (
                branch_code,
                current_date,
                None,
                branch_lat,
                branch_long,
                None,
                pickup_date,
                "pending",
            )
            result = execute_query(insert_sql, params)
            if result.get("success"):
                print(f"✅ Generated pickup request for {branch_code} on {pickup_date}")
            else:
                print(
                    f"❌ Failed to generate pickup request for {branch_code} on {pickup_date}: {result.get('error')}"
                )
    except Exception as e:
        print(f"Error generating pickup schedule for {branch_code}: {str(e)}")
def check_and_renew_pickup_schedules():
    """Check all branches and renew pickup schedules based on completion or time remaining"""
    try:
        # Find branches that need renewal based on two conditions:
        # 1. 10+ completed pickups OR 2. Max pickup date <= 4 days from now
        check_sql = """
        SELECT DISTINCT 
            br.branch_code, 
            cb.frequency, 
            cb.days, 
            cb.latitude, 
            cb.longitude,
            COUNT(CASE WHEN br.status = 'completed' THEN 1 END) as completed_count,
            COUNT(CASE WHEN br.status = 'pending' THEN 1 END) as pending_count,
            MAX(CASE WHEN br.status = 'pending' THEN br.pickup_date END) as max_pending_date,
            DATEDIFF(MAX(CASE WHEN br.status = 'pending' THEN br.pickup_date END), CURDATE()) as days_remaining
        FROM branch_pickup_frequency br
        JOIN b2b_corporate_branch_master cb ON br.branch_code = cb.branch_code
        GROUP BY br.branch_code, cb.frequency, cb.days, cb.latitude, cb.longitude
        HAVING 
            completed_count >= 10 OR 
            (pending_count > 0 AND days_remaining <= 4)
        """
        result = execute_query(check_sql, fetch_all=True)
        if result.get("success") and result.get("data"):
            for branch_data in result.get("data"):
                branch_code = branch_data["branch_code"]
                frequency = branch_data["frequency"]
                days = branch_data["days"]
                branch_lat = branch_data["latitude"]
                branch_long = branch_data["longitude"]
                completed_count = branch_data["completed_count"]
                days_remaining = branch_data["days_remaining"]
                # Determine renewal reason
                if completed_count >= 10:
                    print(
                        f"🔄 Auto-renewing pickup schedule for branch {branch_code}: {completed_count} pickups completed"
                    )
                else:
                    print(
                        f"🔄 Auto-renewing pickup schedule for branch {branch_code}: {days_remaining} days remaining"
                    )
                generate_pickup_schedule(
                    branch_code, frequency, days, branch_lat, branch_long
                )
    except Exception as e:
        print(f"Error in auto-renewal check: {str(e)}")

# Session Management Functions
# File-based token storage - persists tokens across server restarts
# Tokens stored in JSON file and kept for 20 hours
TOKEN_STORAGE_FILE = "driver_session_tokens.json"
TOKEN_EXPIRY_HOURS = 20  # 20 hours session duration
active_tokens = {}

def save_tokens_to_file():
    """Save active tokens to JSON file for persistence"""
    try:
        # Convert datetime objects to ISO strings for JSON serialization
        tokens_to_save = {}
        for token, token_data in active_tokens.items():
            # Only save non-expired tokens
            if datetime.now() <= token_data.get("expires_at", datetime.now()):
                tokens_to_save[token] = {
                    "vehicle_no": token_data.get("vehicle_no"),
                    "dl_no": token_data.get("dl_no"),
                    "created_at": token_data.get("created_at").isoformat() if isinstance(token_data.get("created_at"), datetime) else token_data.get("created_at"),
                    "expires_at": token_data.get("expires_at").isoformat() if isinstance(token_data.get("expires_at"), datetime) else token_data.get("expires_at"),
                    "session_type": token_data.get("session_type"),
                    "app_state": token_data.get("app_state", {}),
                }
                if token_data.get("route_id"):
                    tokens_to_save[token]["route_id"] = token_data.get("route_id")
                if token_data.get("pickup_id"):
                    tokens_to_save[token]["pickup_id"] = token_data.get("pickup_id")
                if token_data.get("branch_code"):
                    tokens_to_save[token]["branch_code"] = token_data.get("branch_code")
        
        # Save to file atomically (write to temp file then rename)
        import tempfile
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, dir=os.path.dirname(os.path.abspath(__file__)))
        json.dump(tokens_to_save, temp_file, indent=2)
        temp_file.close()
        
        # Atomic rename
        import shutil
        file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), TOKEN_STORAGE_FILE)
        shutil.move(temp_file.name, file_path)
        
        print(f"✅ Saved {len(tokens_to_save)} tokens to file")
    except Exception as e:
        print(f"⚠️ Error saving tokens to file: {str(e)}")

def load_tokens_from_file():
    """Load tokens from JSON file on server startup"""
    try:
        file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), TOKEN_STORAGE_FILE)
        
        if not os.path.exists(file_path):
            print(f"ℹ️ No token storage file found, starting with empty tokens")
            return
        
        with open(file_path, 'r') as f:
            tokens_data = json.load(f)
        
        # Convert ISO strings back to datetime objects and filter expired tokens
        now = datetime.now()
        loaded_count = 0
        expired_count = 0
        
        for token, token_data in tokens_data.items():
            try:
                # Parse datetime strings
                expires_at_str = token_data.get("expires_at")
                if expires_at_str:
                    expires_at = parser.parse(expires_at_str) if isinstance(expires_at_str, str) else expires_at_str
                    
                    # Only load non-expired tokens
                    if now <= expires_at:
                        created_at_str = token_data.get("created_at")
                        created_at = parser.parse(created_at_str) if isinstance(created_at_str, str) and created_at_str else datetime.now()
                        
                        # Reconstruct token_data with datetime objects
                        restored_token_data = {
                            "vehicle_no": token_data.get("vehicle_no"),
                            "dl_no": token_data.get("dl_no"),
                            "created_at": created_at,
                            "expires_at": expires_at,
                            "session_type": token_data.get("session_type"),
                            "app_state": token_data.get("app_state", {}),
                        }
                        if token_data.get("route_id"):
                            restored_token_data["route_id"] = token_data.get("route_id")
                        if token_data.get("pickup_id"):
                            restored_token_data["pickup_id"] = token_data.get("pickup_id")
                        if token_data.get("branch_code"):
                            restored_token_data["branch_code"] = token_data.get("branch_code")
                        
                        active_tokens[token] = restored_token_data
                        loaded_count += 1
                    else:
                        expired_count += 1
            except Exception as e:
                print(f"⚠️ Error loading token {token[:10]}...: {str(e)}")
                continue
        
        print(f"✅ Loaded {loaded_count} valid tokens from file (skipped {expired_count} expired)")
    except Exception as e:
        print(f"⚠️ Error loading tokens from file: {str(e)}")

def cleanup_expired_tokens():
    """Remove expired tokens from memory and save to file"""
    try:
        now = datetime.now()
        expired_tokens = []
        for token, token_data in active_tokens.items():
            if now > token_data.get("expires_at", now):
                expired_tokens.append(token)
        
        for token in expired_tokens:
            del active_tokens[token]
        
        if expired_tokens:
            save_tokens_to_file()
            print(f"🧹 Cleaned up {len(expired_tokens)} expired tokens")
    except Exception as e:
        print(f"⚠️ Error cleaning up expired tokens: {str(e)}")

def periodic_token_cleanup():
    """Periodically clean up expired tokens every hour"""
    while True:
        try:
            time.sleep(3600)  # Run every hour
            cleanup_expired_tokens()
        except Exception as e:
            print(f"⚠️ Error in periodic token cleanup: {str(e)}")

# Load tokens on module import (server startup)
load_tokens_from_file()

# Start cleanup thread in background (daemon thread)
cleanup_thread = threading.Thread(target=periodic_token_cleanup, daemon=True)
cleanup_thread.start()
print("✅ Started periodic token cleanup thread")

def generate_session_token(
    vehicle_no, dl_no, pickup_id=None, branch_code=None, route_id=None
):
    """Generate session token for driver authentication (supports both single and multi-pickup)"""
    try:
        token = "".join(
            secrets.choice(string.ascii_letters + string.digits) for _ in range(32)
        )
        # Base token data
        token_data = {
            "vehicle_no": vehicle_no,
            "dl_no": dl_no,
            "created_at": datetime.now(),
            "expires_at": datetime.now() + timedelta(hours=TOKEN_EXPIRY_HOURS),  # 20 hours session duration
        }
        if route_id:
            # Multi-pickup session
            token_data.update(
                {
                    "route_id": route_id,
                    "session_type": "multi_pickup",
                    "app_state": {
                        "current_page": "route_dashboard",
                        "trip_started": False,
                        "current_stop_index": 0,
                        "completed_stops": [],
                        "last_activity": datetime.now().isoformat(),
                    },
                }
            )
        else:
            # Single pickup session (existing logic)
            token_data.update(
                {
                    "pickup_id": pickup_id,
                    "branch_code": branch_code,
                    "session_type": "single_pickup",
                    "app_state": {
                        "current_page": "dashboard",
                        "navigation_started": False,
                        "pickup_form_data": {},
                        "completed_steps": [],
                        "last_activity": datetime.now().isoformat(),
                    },
                }
            )
        # Store token data
        active_tokens[token] = token_data
        # Save to file for persistence
        save_tokens_to_file()
        print(f"✅ Session token generated for vehicle {vehicle_no}, driver {dl_no} (expires in {TOKEN_EXPIRY_HOURS} hours)")
        return token
    except Exception as e:
        print(f"❌ Error generating session token: {str(e)}")
        return None

def validate_token(token):
    """Validate session token"""
    try:
        # Cleanup expired tokens periodically (every 100th validation to reduce overhead)
        if len(active_tokens) > 0 and hash(token) % 100 == 0:
            cleanup_expired_tokens()
        
        if not token:
            print(f"⚠️ Token validation failed: No token provided")
            return False, "Invalid token. Please login again."
        if token not in active_tokens:
            print(f"⚠️ Token validation failed: Token '{token[:10]}...' not found in active_tokens (total tokens: {len(active_tokens)})")
            return False, "Invalid token. Session may have expired or backend was restarted. Please login again."
        token_data = active_tokens[token]
        
        # Check if token expired (with 5 minute grace period for clock skew)
        expires_at = token_data["expires_at"]
        grace_period = timedelta(minutes=5)
        if datetime.now() > (expires_at + grace_period):
            del active_tokens[token]  # Remove expired token
            save_tokens_to_file()  # Save updated tokens
            print(f"⚠️ Token validation failed: Token '{token[:10]}...' has expired")
            return False, "Token expired. Please login again."
        
        # Update last activity
        token_data["app_state"]["last_activity"] = datetime.now().isoformat()
        # Periodically save tokens (every 10th validation to reduce file I/O)
        if hash(token) % 10 == 0:
            save_tokens_to_file()
        return True, token_data
    except Exception as e:
        print(f"❌ Error validating token: {str(e)}")
        return False, "Token validation error. Please login again."

def clear_driver_session(token):
    """Clear driver session and remove token"""
    try:
        if token and token in active_tokens:
            del active_tokens[token]
            save_tokens_to_file()  # Save updated tokens
            print(f"✅ Session cleared for token: {token[:8]}...")
        return True
    except Exception as e:
        print(f"❌ Error clearing session: {str(e)}")
        return False

def require_multi_pickup_auth(f):
    """Decorator to require valid multi-pickup token authentication"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            # Get token from header
            auth_header = request.headers.get("Authorization")
            if not auth_header:
                return jsonify(
                    {"status": "error", "message": "No authorization token provided"}
                ), 401
            # Remove 'Bearer ' prefix if present
            token = (
                auth_header.replace("Bearer ", "")
                if auth_header.startswith("Bearer ")
                else auth_header
            )
            # Validate token
            is_valid, token_data = validate_token(token)
            if not is_valid:
                return jsonify({"status": "error", "message": token_data}), 401
            # Check if it's a multi-pickup session
            if token_data.get("session_type") != "multi_pickup":
                return jsonify(
                    {
                        "status": "error",
                        "message": "Invalid session type. Multi-pickup token required.",
                    }
                ), 401
            # Add token data to request context
            request.token_data = token_data
            return f(*args, **kwargs)
        except Exception as e:
            return jsonify(
                {
                    "status": "error",
                    "message": f"Multi-pickup token validation error: {str(e)}",
                }
            ), 500
    return decorated_function

def require_auth(f):
    """Decorator to require valid token authentication (works for any session type)"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            # Get token from header
            auth_header = request.headers.get("Authorization")
            if not auth_header:
                return jsonify(
                    {"status": "error", "message": "No authorization token provided"}
                ), 401
            # Remove 'Bearer ' prefix if present
            token = (
                auth_header.replace("Bearer ", "")
                if auth_header.startswith("Bearer ")
                else auth_header
            )
            # Validate token
            is_valid, token_data = validate_token(token)
            if not is_valid:
                return jsonify({"status": "error", "message": token_data}), 401
            # Add token data to request context
            request.token_data = token_data
            return f(*args, **kwargs)
        except Exception as e:
            return jsonify(
                {
                    "status": "error",
                    "message": f"Token validation error: {str(e)}",
                }
            ), 500
    return decorated_function
# Test database connection endpoint
# Multi-Pickup Route API Endpoints
@app.route("/multi-pickup/create-assignment", methods=["POST"])
def create_assignment():
    """Create a new multi-pickup assignment (without DL - DL posted from vehicle app)"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "No data provided"}), 400
        required_fields = ["route_date", "vehicle_no"]
        for field in required_fields:
            if field not in data:
                return jsonify(
                    {"status": "error", "message": f"Missing required field: {field}"}
                ), 400
        route_date = data["route_date"]
        vehicle_no = data["vehicle_no"]
        # Ensure route_date is in correct format (YYYY-MM-DD)
        try:
            date_obj = datetime.strptime(route_date, "%Y-%m-%d")
            formatted_route_date = date_obj.strftime("%Y-%m-%d")
        except ValueError:
            return jsonify(
                {
                    "status": "error",
                    "message": "Invalid date format. Use YYYY-MM-DD format.",
                }
            ), 400
        # Create assignment without driver_dl
        insert_sql = """
        INSERT INTO b2b_route_assignments (
            route_date, vehicle_no, status, created_at, updated_at
        ) VALUES (%s, %s, %s, NOW(), NOW())
        """
        params = (formatted_route_date, vehicle_no, "pending")
        result = execute_query(insert_sql, params)
        if result.get("success"):
            # Get the route_id
            get_id_sql = "SELECT route_id FROM b2b_route_assignments WHERE DATE(route_date) = %s ORDER BY route_id DESC LIMIT 1"
            id_result = execute_query(
                get_id_sql, (formatted_route_date,), fetch_one=True
            )
            if id_result.get("success"):
                route_id = id_result.get("data", {}).get("route_id")
                return jsonify(
                    {
                        "status": "success",
                        "message": "Assignment created successfully (driver will be assigned when vehicle app requests)",
                        "data": {
                            "route_id": route_id,
                            "route_date": formatted_route_date,
                            "vehicle_no": vehicle_no,
                            "driver_dl": None,
                            "status": "pending",
                        },
                    }
                )
        return jsonify(
            {
                "status": "error",
                "message": "Failed to create assignment",
                "error": result.get("error"),
            }
        ), 500
    except Exception as e:
        return jsonify(
            {"status": "error", "message": f"Error creating assignment: {str(e)}"}
        ), 500
@app.route("/multi-pickup/assign-driver", methods=["POST"])
def assign_driver():
    """Assign driver to an assignment (called from vehicle app)"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "No data provided"}), 400
        required_fields = ["route_id", "driver_dl"]
        for field in required_fields:
            if field not in data:
                return jsonify(
                    {"status": "error", "message": f"Missing required field: {field}"}
                ), 400
        route_id = data["route_id"]
        driver_dl = data["driver_dl"]
        # Update assignment with driver
        update_sql = """
        UPDATE b2b_route_assignments 
        SET driver_dl = %s, updated_at = NOW()
        WHERE route_id = %s
        """
        result = execute_query(update_sql, (driver_dl, route_id))
        if result.get("success"):
            return jsonify(
                {
                    "status": "success",
                    "message": "Driver assigned successfully",
                    "data": {
                        "route_id": route_id,
                        "driver_dl": driver_dl,
                        "status": "assigned",
                    },
                }
            )
        else:
            return jsonify(
                {
                    "status": "error",
                    "message": "Failed to assign driver",
                    "error": result.get("error"),
                }
            ), 500
    except Exception as e:
        return jsonify(
            {"status": "error", "message": f"Error assigning driver: {str(e)}"}
        ), 500
@app.route("/multi-pickup/add-stop", methods=["POST"])
def add_stop():
    """Add a pickup stop to an assignment"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "No data provided"}), 400
        required_fields = [
            "route_id",
            "sequence",
            "latitude",
            "longitude",
            "branch_name",
            "address",
            "contact",
            "branch_code",
        ]
        for field in required_fields:
            if field not in data:
                return jsonify(
                    {"status": "error", "message": f"Missing required field: {field}"}
                ), 400
        route_id = data["route_id"]
        sequence = data["sequence"]
        latitude = data["latitude"]
        longitude = data["longitude"]
        branch_name = data["branch_name"]
        address = data["address"]
        contact = data["contact"]
        branch_code = data["branch_code"]
        result = add_pickup_stop(
            route_id,
            sequence,
            latitude,
            longitude,
            branch_name,
            address,
            contact,
            branch_code,
        )
        if result.get("success"):
            return jsonify(
                {
                    "status": "success",
                    "message": "Stop added successfully",
                    "data": {
                        "route_id": route_id,
                        "sequence": sequence,
                        "branch_name": branch_name,
                        "branch_code": branch_code,
                        "status": "pending",
                    },
                }
            )
        else:
            return jsonify(
                {
                    "status": "error",
                    "message": "Failed to add stop",
                    "error": result.get("error"),
                }
            ), 500
    except Exception as e:
        return jsonify(
            {"status": "error", "message": f"Error adding stop: {str(e)}"}
        ), 500
@app.route("/multi-pickup/assignment/<int:route_id>", methods=["GET"])
def get_assignment(route_id):
    """Get assignment details with all stops"""
    try:
        result = get_assignment_details(route_id)
        if result.get("success"):
            return jsonify(
                {
                    "status": "success",
                    "message": "Assignment details retrieved successfully",
                    "data": result.get("data"),
                }
            )
        else:
            return jsonify(
                {
                    "status": "error",
                    "message": "Failed to retrieve assignment details",
                    "error": result.get("error"),
                }
            ), 500
    except Exception as e:
        return jsonify(
            {"status": "error", "message": f"Error retrieving assignment: {str(e)}"}
        ), 500
@app.route("/multi-pickup/start-trip/<int:route_id>", methods=["POST"])
@require_multi_pickup_auth
def start_trip(route_id):
    """Start a multi-pickup trip"""
    try:
        result = update_assignment_status(route_id, "in_progress")
        if result.get("success"):
            return jsonify(
                {
                    "status": "success",
                    "message": "Trip started successfully",
                    "data": {"route_id": route_id, "status": "in_progress"},
                }
            )
        else:
            return jsonify(
                {
                    "status": "error",
                    "message": "Failed to start trip",
                    "error": result.get("error"),
                }
            ), 500
    except Exception as e:
        return jsonify(
            {"status": "error", "message": f"Error starting trip: {str(e)}"}
        ), 500
@app.route("/multi-pickup/start-stop/<int:stop_id>", methods=["POST"])
@require_multi_pickup_auth
def start_stop(stop_id):
    """Start pickup at a specific stop"""
    try:
        result = update_stop_status(stop_id, "in_progress")
        if result.get("success"):
            # Note: branch_pickup_frequency status is automatically updated by update_stop_status()
            # when status changes to "in_progress" - no manual update needed
            
            return jsonify(
                {
                    "status": "success",
                    "message": "Stop started successfully",
                    "data": {"stop_id": stop_id, "status": "in_progress"},
                }
            )
        else:
            return jsonify(
                {
                    "status": "error",
                    "message": "Failed to start stop",
                    "error": result.get("error"),
                }
            ), 500
    except Exception as e:
        return jsonify(
            {"status": "error", "message": f"Error starting stop: {str(e)}"}
        ), 500
@app.route("/multi-pickup/complete-stop/<int:stop_id>", methods=["POST"])
@require_multi_pickup_auth
def complete_stop(stop_id):
    """Complete pickup at a specific stop with photo and receipt support"""
    try:
        # Handle both JSON data and form data for compatibility
        if request.content_type and "multipart/form-data" in request.content_type:
            # Form data (with file uploads)
            weight = request.form.get("weight")
            remark = request.form.get("remark")
        else:
            # JSON data
            data = request.get_json() or {}
            weight = data.get("weight")
            remark = data.get("remark")
        waste_image_url = None
        receipt_image_url = None
        # Handle photo upload if present (following existing pattern)
        if "photo" in request.files:
            file = request.files["photo"]
            if file and file.filename != "":
                upload_result = upload_and_get_path(file)
                if upload_result["status"] == "success":
                    waste_image_url = upload_result["file_path"]
                else:
                    return jsonify(
                        {
                            "status": "error",
                            "message": f"Photo upload failed: {upload_result['message']}",
                        }
                    ), 400
        # Handle receipt upload if present (following existing pattern)
        if "receipt_image" in request.files:
            file = request.files["receipt_image"]
            if file and file.filename != "":
                upload_result = upload_and_get_path(file)
                if upload_result["status"] == "success":
                    receipt_image_url = upload_result["file_path"]
                else:
                    return jsonify(
                        {
                            "status": "error",
                            "message": f"Receipt upload failed: {upload_result['message']}",
                        }
                    ), 400
        
        # Handle POC details (name, designation, signature)
        poc_name = None
        poc_designation = None
        poc_signature_url = None
        
        if request.content_type and "multipart/form-data" in request.content_type:
            poc_name = request.form.get("poc_name")
            poc_designation = request.form.get("poc_designation")
            # Handle signature as file upload
            if "poc_signature" in request.files:
                signature_file = request.files["poc_signature"]
                poc_signature_url = handle_signature_upload(None, signature_file)
        else:
            # JSON data
            poc_name = data.get("poc_name")
            poc_designation = data.get("poc_designation")
            poc_signature_data = data.get("poc_signature")
            # Handle signature as base64 or URL
            if poc_signature_data:
                poc_signature_url = handle_signature_upload(poc_signature_data)
        
        # Convert weight to float if provided
        # Handle weight: strip whitespace, remove units (kg, g, etc.), convert to float
        weight_value = None
        if weight is not None:
            weight_str = str(weight).strip()
            # Only process if weight is not empty after stripping
            if weight_str:
                try:
                    # Remove any units (kg, g, etc.) and whitespace, then convert to float
                    weight_str_clean = weight_str.lower()
                    # Remove common weight units (handle "kgs" before "kg" to avoid issues)
                    weight_str_clean = weight_str_clean.replace("kgs", "").replace("kg", "").replace("g", "").strip()
                    if weight_str_clean:  # Only convert if there's still a value after stripping
                        weight_value = round(float(weight_str_clean), 2)  # Round to 2 decimal places for decimal(10,2) column
                        # Validate weight is positive
                        if weight_value <= 0:
                            return jsonify(
                                {"status": "error", "message": f"Invalid weight: {weight_value} kg. Weight must be greater than 0."}
                            ), 400
                        print(f"✅ [complete_stop] Weight parsed: '{weight}' -> {weight_value} kg")
                    else:
                        print(f"⚠️ [complete_stop] Weight '{weight}' became empty after removing units")
                        return jsonify(
                            {"status": "error", "message": f"Invalid weight format: {weight}. Please provide a numeric value."}
                        ), 400
                except (ValueError, TypeError) as e:
                    print(f"❌ [complete_stop] Error parsing weight '{weight}': {str(e)}")
                    return jsonify(
                        {"status": "error", "message": f"Invalid weight format: {weight}. Please provide a numeric value."}
                    ), 400
            else:
                print(f"⚠️ [complete_stop] Weight is empty or whitespace only")
        else:
            print(f"⚠️ [complete_stop] No weight provided in request")
        
        # Update stop status with all data including POC details
        result = update_stop_status(
            stop_id, "completed", weight_value, remark, waste_image_url, receipt_image_url,
            poc_name, poc_designation, poc_signature_url
        )
        if result.get("success"):
            # Get branch_code and route_date from the route stop to update branch_pickup_frequency
            get_stop_info_sql = """
            SELECT rs.branch_code, DATE(ra.route_date) as route_date
            FROM b2b_route_stops rs
            JOIN b2b_route_assignments ra ON rs.route_id = ra.route_id
            WHERE rs.id = %s
            """
            stop_info_result = execute_query(get_stop_info_sql, (stop_id,), fetch_one=True)
            if stop_info_result.get("success") and stop_info_result.get("data"):
                stop_data = stop_info_result.get("data")
                branch_code = stop_data.get("branch_code")
                route_date = stop_data.get("route_date")
                if branch_code and route_date:
                    # Update branch_pickup_frequency status to COMPLETED
                    update_branch_pickup_frequency_status(branch_code, route_date, "completed")
            
            response_data = {
                "stop_id": stop_id,
                "status": "completed",
                "pickup_ended_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            # Add optional fields to response
            if weight_value is not None:
                response_data["weight"] = weight_value
            if remark:
                response_data["remark"] = remark
            if waste_image_url:
                response_data["waste_image_url"] = waste_image_url
            if receipt_image_url:
                response_data["receipt_image_url"] = receipt_image_url
            if poc_name:
                response_data["poc_name"] = poc_name
            if poc_designation:
                response_data["poc_designation"] = poc_designation
            if poc_signature_url:
                response_data["poc_signature"] = poc_signature_url
            return jsonify(
                {
                    "status": "success",
                    "message": "Stop completed successfully",
                    "data": response_data,
                }
            )
        else:
            return jsonify(
                {
                    "status": "error",
                    "message": "Failed to complete stop",
                    "error": result.get("error"),
                }
            ), 500
    except Exception as e:
        return jsonify(
            {"status": "error", "message": f"Error completing stop: {str(e)}"}
        ), 500
@app.route(
    "/multi-pickup/start-stop-by-sequence/<int:route_id>/<int:sequence>",
    methods=["POST"],
)
@require_multi_pickup_auth
def start_stop_by_sequence(route_id, sequence):
    """Start pickup at a specific stop using route_id + sequence (with sequential validation)"""
    try:
        # Validate sequential pickup
        validation = validate_sequential_pickup(route_id, sequence, "start")
        if not validation.get("valid"):
            return jsonify(
                {
                    "status": "error",
                    "message": validation.get("error"),
                    "next_sequence": validation.get("next_sequence"),
                    "current_status": validation.get("current_status"),
                }
            ), 400
        result = update_stop_status_by_sequence(route_id, sequence, "in_progress")
        if result.get("success"):
            # Note: branch_pickup_frequency status is automatically updated by update_stop_status_by_sequence()
            # when status changes to "in_progress" - no manual update needed
            
            # Get next sequence info for response
            next_info = get_next_sequence(route_id)
            return jsonify(
                {
                    "status": "success",
                    "message": "Stop started successfully",
                    "data": {
                        "route_id": route_id,
                        "sequence": sequence,
                        "status": "in_progress",
                        "pickup_started_at": datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                        "next_action": "complete_this_sequence",
                        "next_sequence_after_completion": next_info.get(
                            "next_sequence"
                        ),
                    },
                }
            )
        else:
            return jsonify(
                {
                    "status": "error",
                    "message": "Failed to start stop",
                    "error": result.get("error"),
                }
            ), 500
    except Exception as e:
        return jsonify(
            {"status": "error", "message": f"Error starting stop: {str(e)}"}
        ), 500
@app.route(
    "/multi-pickup/complete-stop-by-sequence/<int:route_id>/<int:sequence>",
    methods=["POST"],
)
@require_multi_pickup_auth
def complete_stop_by_sequence(route_id, sequence):
    """Complete pickup at a specific stop using route_id + sequence with sequential validation and file support"""
    try:
        # Validate sequential pickup
        validation = validate_sequential_pickup(route_id, sequence, "complete")
        if not validation.get("valid"):
            return jsonify(
                {
                    "status": "error",
                    "message": validation.get("error"),
                    "next_sequence": validation.get("next_sequence"),
                    "current_status": validation.get("current_status"),
                }
            ), 400
        # Handle both JSON data and form data for compatibility
        if request.content_type and "multipart/form-data" in request.content_type:
            # Form data (with file uploads)
            weight = request.form.get("weight")
            remark = request.form.get("remark")
        else:
            # JSON data
            data = request.get_json() or {}
            weight = data.get("weight")
            remark = data.get("remark")
        waste_image_url = None
        receipt_image_url = None
        # Handle photo upload if present (following existing pattern)
        if "photo" in request.files:
            file = request.files["photo"]
            if file and file.filename != "":
                upload_result = upload_and_get_path(file)
                if upload_result["status"] == "success":
                    waste_image_url = upload_result["file_path"]
                else:
                    return jsonify(
                        {
                            "status": "error",
                            "message": f"Photo upload failed: {upload_result['message']}",
                        }
                    ), 400
        # Handle receipt upload if present (following existing pattern)
        if "receipt_image" in request.files:
            file = request.files["receipt_image"]
            if file and file.filename != "":
                upload_result = upload_and_get_path(file)
                if upload_result["status"] == "success":
                    receipt_image_url = upload_result["file_path"]
                else:
                    return jsonify(
                        {
                            "status": "error",
                            "message": f"Receipt upload failed: {upload_result['message']}",
                        }
                    ), 400
        
        # Handle POC details (name, designation, signature)
        poc_name = None
        poc_designation = None
        poc_signature_url = None
        
        if request.content_type and "multipart/form-data" in request.content_type:
            poc_name = request.form.get("poc_name")
            poc_designation = request.form.get("poc_designation")
            # Handle signature as file upload
            if "poc_signature" in request.files:
                signature_file = request.files["poc_signature"]
                poc_signature_url = handle_signature_upload(None, signature_file)
        else:
            # JSON data
            poc_name = data.get("poc_name")
            poc_designation = data.get("poc_designation")
            poc_signature_data = data.get("poc_signature")
            # Handle signature as base64 or URL
            if poc_signature_data:
                poc_signature_url = handle_signature_upload(poc_signature_data)
        
        # Convert weight to float if provided
        # Handle weight: strip whitespace, remove units (kg, g, etc.), convert to float
        weight_value = None
        if weight is not None:
            weight_str = str(weight).strip()
            # Only process if weight is not empty after stripping
            if weight_str:
                try:
                    # Remove any units (kg, g, etc.) and whitespace, then convert to float
                    weight_str_clean = weight_str.lower()
                    # Remove common weight units (handle "kgs" before "kg" to avoid issues)
                    weight_str_clean = weight_str_clean.replace("kgs", "").replace("kg", "").replace("g", "").strip()
                    if weight_str_clean:  # Only convert if there's still a value after stripping
                        weight_value = round(float(weight_str_clean), 2)  # Round to 2 decimal places for decimal(10,2) column
                        # Validate weight is positive
                        if weight_value <= 0:
                            return jsonify(
                                {"status": "error", "message": f"Invalid weight: {weight_value} kg. Weight must be greater than 0."}
                            ), 400
                        print(f"✅ [complete_stop_by_sequence] Weight parsed: '{weight}' -> {weight_value} kg")
                    else:
                        print(f"⚠️ [complete_stop_by_sequence] Weight '{weight}' became empty after removing units")
                        return jsonify(
                            {"status": "error", "message": f"Invalid weight format: {weight}. Please provide a numeric value."}
                        ), 400
                except (ValueError, TypeError) as e:
                    print(f"❌ [complete_stop_by_sequence] Error parsing weight '{weight}': {str(e)}")
                    return jsonify(
                        {"status": "error", "message": f"Invalid weight format: {weight}. Please provide a numeric value."}
                    ), 400
            else:
                print(f"⚠️ [complete_stop_by_sequence] Weight is empty or whitespace only")
        else:
            print(f"⚠️ [complete_stop_by_sequence] No weight provided in request")
        
        # Update stop status with all data using route_id + sequence including POC details
        result = update_stop_status_by_sequence(
            route_id,
            sequence,
            "completed",
            weight_value,
            remark,
            waste_image_url,
            receipt_image_url,
            poc_name,
            poc_designation,
            poc_signature_url,
        )
        if result.get("success"):
            # Get branch_code and route_date from the route stop to update branch_pickup_frequency
            get_stop_info_sql = """
            SELECT rs.branch_code, DATE(ra.route_date) as route_date
            FROM b2b_route_stops rs
            JOIN b2b_route_assignments ra ON rs.route_id = ra.route_id
            WHERE rs.route_id = %s AND rs.sequence = %s
            """
            stop_info_result = execute_query(get_stop_info_sql, (route_id, sequence), fetch_one=True)
            if stop_info_result.get("success") and stop_info_result.get("data"):
                stop_data = stop_info_result.get("data")
                branch_code = stop_data.get("branch_code")
                route_date = stop_data.get("route_date")
                if branch_code and route_date:
                    # Update branch_pickup_frequency status to COMPLETED
                    update_branch_pickup_frequency_status(branch_code, route_date, "completed")
            
            # Get next sequence info for response
            next_info = get_next_sequence(route_id)
            next_sequence = next_info.get("next_sequence")
            response_data = {
                "route_id": route_id,
                "sequence": sequence,
                "status": "completed",
                "pickup_ended_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "next_sequence": next_sequence,
                "route_status": "completed" if next_sequence is None else "in_progress",
            }
            # Add optional fields to response
            if weight_value is not None:
                response_data["weight"] = weight_value
            if remark:
                response_data["remark"] = remark
            if waste_image_url:
                response_data["waste_image_url"] = waste_image_url
            if receipt_image_url:
                response_data["receipt_image_url"] = receipt_image_url
            if poc_name:
                response_data["poc_name"] = poc_name
            if poc_designation:
                response_data["poc_designation"] = poc_designation
            if poc_signature_url:
                response_data["poc_signature"] = poc_signature_url
            return jsonify(
                {
                    "status": "success",
                    "message": "Stop completed successfully"
                    + (
                        f". Next sequence: {next_sequence}"
                        if next_sequence
                        else ". All sequences completed!"
                    ),
                    "data": response_data,
                }
            )
        else:
            return jsonify(
                {
                    "status": "error",
                    "message": "Failed to complete stop",
                    "error": result.get("error"),
                }
            ), 500
    except Exception as e:
        return jsonify(
            {"status": "error", "message": f"Error completing stop: {str(e)}"}
        ), 500
@app.route("/multi-pickup/driver-assignments/<driver_dl>", methods=["GET"])
def get_driver_assignments(driver_dl):
    """Get all assignments for a specific driver"""
    try:
        sql = """
        SELECT 
            mpa.*,
            COUNT(mps.route_id) as total_stops,
            COUNT(CASE WHEN mps.status = 'completed' THEN 1 END) as completed_stops
        FROM b2b_route_assignments mpa
        LEFT JOIN b2b_route_stops mps ON mpa.route_id = mps.route_id
        WHERE mpa.driver_dl = %s
        GROUP BY mpa.route_id
        ORDER BY mpa.route_date DESC, mpa.created_at DESC
        """
        result = execute_query(sql, (driver_dl,), fetch_all=True)
        if result.get("success"):
            return jsonify(
                {
                    "status": "success",
                    "message": "Driver assignments retrieved successfully",
                    "data": result.get("data", []),
                }
            )
        else:
            return jsonify(
                {
                    "status": "error",
                    "message": "Failed to retrieve driver assignments",
                    "error": result.get("error"),
                }
            ), 500
    except Exception as e:
        return jsonify(
            {
                "status": "error",
                "message": f"Error retrieving driver assignments: {str(e)}",
            }
        ), 500
@app.route("/multi-pickup/today-assignment", methods=["POST"])
def get_today_assignment_post():
    """Get today's assignment - accepts DL and vehicle_no in POST body"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "No data provided"}), 400
        required_fields = ["driver_dl", "vehicle_no"]
        for field in required_fields:
            if field not in data:
                return jsonify(
                    {"status": "error", "message": f"Missing required field: {field}"}
                ), 400
        driver_dl = data["driver_dl"]
        vehicle_no = data["vehicle_no"]
        # Get today's date in YYYY-MM-DD format
        today_date = datetime.now().strftime("%Y-%m-%d")
        # Find assignment by vehicle and date (driver_dl can be NULL or match)
        sql = """
        SELECT 
            mpa.*,
            COUNT(mps.route_id) as total_stops,
            COUNT(CASE WHEN mps.status = 'completed' THEN 1 END) as completed_stops,
            COUNT(CASE WHEN mps.status = 'in_progress' THEN 1 END) as in_progress_stops
        FROM b2b_route_assignments mpa
        LEFT JOIN b2b_route_stops mps ON mpa.route_id = mps.route_id
        WHERE mpa.vehicle_no = %s 
        AND DATE(mpa.route_date) = %s
        AND (mpa.driver_dl IS NULL OR mpa.driver_dl = '' OR mpa.driver_dl = %s)
        GROUP BY mpa.route_id
        ORDER BY mpa.created_at DESC
        LIMIT 1
        """
        result = execute_query(sql, (vehicle_no, today_date, driver_dl), fetch_one=True)
        if result.get("success") and result.get("data"):
            assignment_data = result.get("data")
            route_id = assignment_data["route_id"]
            # If assignment has no driver assigned, assign this driver
            if (
                not assignment_data.get("driver_dl")
                or assignment_data.get("driver_dl") == ""
            ):
                update_sql = """
                UPDATE b2b_route_assignments 
                SET driver_dl = %s, updated_at = NOW()
                WHERE route_id = %s
                """
                update_result = execute_query(update_sql, (driver_dl, route_id))
                if update_result.get("success"):
                    assignment_data["driver_dl"] = driver_dl
                    print(f"✅ Driver {driver_dl} assigned to assignment {route_id}")
                else:
                    print(f"⚠️ Failed to assign driver: {update_result.get('error')}")
            # Generate session token for multi-pickup
            session_token = generate_session_token(
                vehicle_no=vehicle_no, dl_no=driver_dl, route_id=route_id
            )
            if not session_token:
                return jsonify(
                    {"status": "error", "message": "Failed to create session token"}
                ), 500
            # Get detailed stops for this assignment
            stops_result = get_assignment_details(route_id)
            if stops_result.get("success"):
                assignment_data["stops"] = stops_result.get("data", {}).get("stops", [])
            # Add session info to response
            assignment_data["session_token"] = session_token
            assignment_data["token_expires_in"] = TOKEN_EXPIRY_HOURS * 3600  # 20 hours in seconds
            assignment_data["session_type"] = "multi_pickup"
            return jsonify(
                {
                    "status": "success",
                    "message": "Today's assignment retrieved successfully with session created",
                    "data": assignment_data,
                }
            )
        else:
            return jsonify(
                {"status": "error", "message": "No assignment found for today"}
            ), 404
    except Exception as e:
        return jsonify(
            {
                "status": "error",
                "message": f"Error retrieving today's assignment: {str(e)}",
            }
        ), 500
@app.route("/multi-pickup/session-status", methods=["GET"])
@require_multi_pickup_auth
def get_multi_pickup_session_status():
    """Get current multi-pickup session status"""
    try:
        token_data = request.token_data
        return jsonify(
            {
                "status": "success",
                "message": "Session status retrieved successfully",
                "data": {
                    "route_id": token_data.get("route_id"),
                    "vehicle_no": token_data.get("vehicle_no"),
                    "dl_no": token_data.get("dl_no"),
                    "session_type": token_data.get("session_type"),
                    "app_state": token_data.get("app_state"),
                    "expires_at": token_data.get("expires_at").isoformat()
                    if token_data.get("expires_at")
                    else None,
                    "created_at": token_data.get("created_at").isoformat()
                    if token_data.get("created_at")
                    else None,
                },
            }
        )
    except Exception as e:
        return jsonify(
            {"status": "error", "message": f"Error retrieving session status: {str(e)}"}
        ), 500
@app.route("/multi-pickup/refresh-token", methods=["POST"])
@require_multi_pickup_auth
def refresh_multi_pickup_token():
    """Refresh multi-pickup session token before expiration"""
    try:
        # Get current token data
        token_data = request.token_data
        auth_header = request.headers.get("Authorization")
        old_token = (
            auth_header.replace("Bearer ", "")
            if auth_header.startswith("Bearer ")
            else auth_header
        )
        
        # Generate new token with same data (20 hours expiry)
        new_token = generate_session_token(
            vehicle_no=token_data["vehicle_no"],
            dl_no=token_data["dl_no"],
            route_id=token_data.get("route_id"),
        )
        
        if not new_token:
            return jsonify(
                {"status": "error", "message": "Failed to generate new token"}
            ), 500
        
        # Remove old token
        clear_driver_session(old_token)
        
        return jsonify(
            {
                "status": "success",
                "message": "Token refreshed successfully",
                "data": {
                    "session_token": new_token,
                    "token_expires_in": TOKEN_EXPIRY_HOURS * 3600,  # 20 hours in seconds
                }
            }
        )
    except Exception as e:
        return jsonify(
            {"status": "error", "message": f"Error refreshing token: {str(e)}"}
        ), 500

@app.route("/multi-pickup/update-app-state", methods=["POST"])
@require_multi_pickup_auth
def update_multi_pickup_app_state():
    """Update app state for multi-pickup session"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "No data provided"}), 400
        # Get token from header
        auth_header = request.headers.get("Authorization")
        token = (
            auth_header.replace("Bearer ", "")
            if auth_header.startswith("Bearer ")
            else auth_header
        )
        # Update app state
        if token in active_tokens:
            token_info = active_tokens[token]
            # Update specific app state fields
            if "current_page" in data:
                token_info["app_state"]["current_page"] = data["current_page"]
            if "trip_started" in data:
                token_info["app_state"]["trip_started"] = data["trip_started"]
            if "current_stop_index" in data:
                token_info["app_state"]["current_stop_index"] = data[
                    "current_stop_index"
                ]
            if "completed_stops" in data:
                token_info["app_state"]["completed_stops"] = data["completed_stops"]
            # Always update last activity
            token_info["app_state"]["last_activity"] = datetime.now().isoformat()
            # Save tokens to file after updating app state
            save_tokens_to_file()
            return jsonify(
                {
                    "status": "success",
                    "message": "App state updated successfully",
                    "data": {"app_state": token_info["app_state"]},
                }
            )
        else:
            return jsonify({"status": "error", "message": "Invalid token"}), 401
    except Exception as e:
        return jsonify(
            {"status": "error", "message": f"Error updating app state: {str(e)}"}
        ), 500
@app.route("/multi-pickup/assignment-sequences/<int:route_id>", methods=["GET"])
def get_assignment_sequences(route_id):
    """Get assignment with sequence-based stop details"""
    try:
        # Get assignment info
        assignment_sql = """
        SELECT * FROM b2b_route_assignments WHERE route_id = %s
        """
        assignment_result = execute_query(assignment_sql, (route_id,), fetch_one=True)
        if not assignment_result.get("success"):
            return jsonify({"status": "error", "message": "Assignment not found"}), 404
        # Get all stops with sequences for this assignment
        stops_sql = """
        SELECT 
            route_id,
            sequence,
            branch_name,
            address,
            contact,
            branch_code,
            status,
            weight,
            remark,
            waste_image_url,
            receipt_image_url,
            latitude,
            longitude,
            created_at,
            completed_at,
            pickup_started_at,
            pickup_ended_at
        FROM b2b_route_stops 
        WHERE route_id = %s 
        ORDER BY sequence ASC
        """
        stops_result = execute_query(stops_sql, (route_id,), fetch_all=True)
        assignment_data = assignment_result.get("data")
        stops_data = stops_result.get("data", [])
        # Get next sequence info for sequential flow
        next_info = get_next_sequence(route_id)
        return jsonify(
            {
                "status": "success",
                "message": "Assignment with sequences retrieved successfully",
                "data": {
                    "assignment": assignment_data,
                    "stops": stops_data,
                    "total_stops": len(stops_data),
                    "sequential_info": {
                        "next_sequence": next_info.get("next_sequence"),
                        "next_status": next_info.get("status"),
                        "all_completed": next_info.get("status") == "all_completed",
                    },
                    "usage": {
                        "start_stop": f"POST /multi-pickup/start-stop-by-sequence/{route_id}/{{sequence}}",
                        "complete_stop": f"POST /multi-pickup/complete-stop-by-sequence/{route_id}/{{sequence}}",
                        "get_next": f"GET /multi-pickup/next-sequence/{route_id}",
                        "sequences_available": [
                            stop.get("sequence") for stop in stops_data
                        ],
                    },
                },
            }
        )
    except Exception as e:
        return jsonify(
            {"status": "error", "message": f"Error retrieving assignment: {str(e)}"}
        ), 500
@app.route("/multi-pickup/auto-start-next", methods=["POST"])
@require_multi_pickup_auth
def auto_start_next_sequence():
    """Automatically start the next sequence based on session authentication"""
    try:
        # Get route_id from session token
        token_data = request.token_data
        route_id = token_data.get("route_id")
        if not route_id:
            return jsonify(
                {"status": "error", "message": "No assignment found in session"}
            ), 400
        # Get next sequence to start
        next_info = get_next_sequence(route_id)
        if not next_info.get("success"):
            return jsonify({"status": "error", "message": next_info.get("error")}), 500
        next_sequence = next_info.get("next_sequence")
        current_status = next_info.get("status")
        if current_status == "all_completed":
            return jsonify(
                {
                    "status": "success",
                    "message": "All sequences completed! Ready to complete trip.",
                    "data": {
                        "route_id": route_id,
                        "action": "complete_trip",
                        "all_completed": True,
                    },
                }
            )
        if current_status == "in_progress":
            return jsonify(
                {
                    "status": "error",
                    "message": f"Sequence {next_sequence} is already in progress. Complete it first.",
                    "data": {
                        "route_id": route_id,
                        "current_sequence": next_sequence,
                        "status": "in_progress",
                        "action": "complete_current",
                    },
                }
            ), 400
        # Start the next sequence
        result = update_stop_status_by_sequence(route_id, next_sequence, "in_progress")
        if result.get("success"):
            # Get sequence details with fallback to branch master table
            # Note: contact column was removed from b2b_route_stops, use branch master table
            sequence_details_sql = """
            SELECT 
                rs.sequence,
                COALESCE(rs.branch_name, bm.branch_name) as branch_name,
                COALESCE(rs.address, bm.address) as address,
                bm.contact_phone as contact,
                COALESCE(rs.latitude, bm.latitude) as latitude,
                COALESCE(rs.longitude, bm.longitude) as longitude,
                rs.branch_code
            FROM b2b_route_stops rs
            LEFT JOIN b2b_corporate_branch_master bm ON rs.branch_code = bm.branch_code
            WHERE rs.route_id = %s AND rs.sequence = %s
            """
            details_result = execute_query(
                sequence_details_sql, (route_id, next_sequence), fetch_one=True
            )
            sequence_data = (
                details_result.get("data", {}) if details_result.get("success") else {}
            )
            return jsonify(
                {
                    "status": "success",
                    "message": f"Sequence {next_sequence} started successfully",
                    "data": {
                        "route_id": route_id,
                        "sequence": next_sequence,
                        "status": "in_progress",
                        "pickup_started_at": datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                        "sequence_details": {
                            "branch_name": sequence_data.get("branch_name"),
                            "address": sequence_data.get("address"),
                            "contact": sequence_data.get("contact"),
                            "latitude": sequence_data.get("latitude"),
                            "longitude": sequence_data.get("longitude"),
                        },
                    },
                }
            )
        else:
            return jsonify(
                {
                    "status": "error",
                    "message": "Failed to start sequence",
                    "error": result.get("error"),
                }
            ), 500
    except Exception as e:
        return jsonify(
            {"status": "error", "message": f"Error auto-starting sequence: {str(e)}"}
        ), 500
@app.route("/multi-pickup/auto-complete-current", methods=["POST"])
@require_multi_pickup_auth
def auto_complete_current_sequence():
    """Automatically complete the current in-progress sequence with photo/receipt support"""
    try:
        # Get route_id from session token
        token_data = request.token_data
        route_id = token_data.get("route_id")
        if not route_id:
            return jsonify(
                {"status": "error", "message": "No assignment found in session"}
            ), 400
        # Get current sequence info
        next_info = get_next_sequence(route_id)
        if not next_info.get("success"):
            return jsonify({"status": "error", "message": next_info.get("error")}), 500
        current_sequence = next_info.get("next_sequence")
        current_status = next_info.get("status")
        if current_status == "all_completed":
            return jsonify(
                {
                    "status": "success",
                    "message": "All sequences already completed!",
                    "data": {
                        "route_id": route_id,
                        "action": "complete_trip",
                        "all_completed": True,
                    },
                }
            )
        if current_status == "pending":
            return jsonify(
                {
                    "status": "error",
                    "message": f"Sequence {current_sequence} not started yet. Start it first.",
                    "data": {
                        "route_id": route_id,
                        "current_sequence": current_sequence,
                        "status": "pending",
                        "action": "start_sequence",
                    },
                }
            ), 400
        # Handle form data for file uploads
        if request.content_type and "multipart/form-data" in request.content_type:
            weight = request.form.get("weight")
            remark = request.form.get("remark")
            contamination_status = request.form.get("contamination_status")
        else:
            data = request.get_json() or {}
            weight = data.get("weight")
            remark = data.get("remark")
            contamination_status = data.get("contamination_status")
        waste_image_url = None
        receipt_image_url = None
        # Handle photo upload
        if "photo" in request.files:
            file = request.files["photo"]
            if file and file.filename != "":
                upload_result = upload_and_get_path(file)
                if upload_result["status"] == "success":
                    waste_image_url = upload_result["file_path"]
                else:
                    return jsonify(
                        {
                            "status": "error",
                            "message": f"Photo upload failed: {upload_result['message']}",
                        }
                    ), 400
        # Handle receipt upload
        if "receipt_image" in request.files:
            file = request.files["receipt_image"]
            if file and file.filename != "":
                upload_result = upload_and_get_path(file)
                if upload_result["status"] == "success":
                    receipt_image_url = upload_result["file_path"]
                else:
                    return jsonify(
                        {
                            "status": "error",
                            "message": f"Receipt upload failed: {upload_result['message']}",
                        }
                    ), 400
        
        # Handle POC details (name, designation, signature)
        poc_name = None
        poc_designation = None
        poc_signature_url = None
        
        if request.content_type and "multipart/form-data" in request.content_type:
            poc_name = request.form.get("poc_name")
            poc_designation = request.form.get("poc_designation")
            # Handle signature as file upload
            if "poc_signature" in request.files:
                signature_file = request.files["poc_signature"]
                poc_signature_url = handle_signature_upload(None, signature_file)
        else:
            # JSON data
            poc_name = data.get("poc_name")
            poc_designation = data.get("poc_designation")
            poc_signature_data = data.get("poc_signature")
            # Handle signature as base64 or URL
            if poc_signature_data:
                print(f"📥 [auto_complete_current] Received POC signature data (length: {len(poc_signature_data) if poc_signature_data else 0})")
                poc_signature_url = handle_signature_upload(poc_signature_data)
                if poc_signature_url:
                    print(f"✅ [auto_complete_current] POC signature uploaded successfully: {poc_signature_url}")
                else:
                    print(f"⚠️ [auto_complete_current] POC signature upload returned None - upload may have failed")
            else:
                print(f"⚠️ [auto_complete_current] No poc_signature_data found in request")
        
        # Convert weight to float if provided
        # Handle weight: strip whitespace, remove units (kg, g, etc.), convert to float
        weight_value = None
        if weight is not None:
            weight_str = str(weight).strip()
            # Only process if weight is not empty after stripping
            if weight_str:
                try:
                    # Remove any units (kg, g, etc.) and whitespace, then convert to float
                    weight_str_clean = weight_str.lower()
                    # Remove common weight units (handle "kgs" before "kg" to avoid issues)
                    weight_str_clean = weight_str_clean.replace("kgs", "").replace("kg", "").replace("g", "").strip()
                    if weight_str_clean:  # Only convert if there's still a value after stripping
                        weight_value = round(float(weight_str_clean), 2)  # Round to 2 decimal places for decimal(10,2) column
                        # Validate weight is positive
                        if weight_value <= 0:
                            return jsonify(
                                {"status": "error", "message": f"Invalid weight: {weight_value} kg. Weight must be greater than 0."}
                            ), 400
                        print(f"✅ [auto_complete_current] Weight parsed: '{weight}' -> {weight_value} kg")
                    else:
                        print(f"⚠️ [auto_complete_current] Weight '{weight}' became empty after removing units")
                        return jsonify(
                            {"status": "error", "message": f"Invalid weight format: {weight}. Please provide a numeric value."}
                        ), 400
                except (ValueError, TypeError) as e:
                    print(f"❌ [auto_complete_current] Error parsing weight '{weight}': {str(e)}")
                    return jsonify(
                        {"status": "error", "message": f"Invalid weight format: {weight}. Please provide a numeric value."}
                    ), 400
            else:
                print(f"⚠️ [auto_complete_current] Weight is empty or whitespace only")
        else:
            print(f"⚠️ [auto_complete_current] No weight provided in request")
        
        # Normalize contamination_status value if provided
        contaminated_normalized = None
        if contamination_status is not None:
            val = str(contamination_status).strip().lower()
            if val in ["yes", "y", "true", "1"]:
                contaminated_normalized = "yes"
            elif val in ["no", "n", "false", "0"]:
                contaminated_normalized = "no"
            elif val in [""]:
                contaminated_normalized = None
            else:
                return jsonify(
                    {
                        "status": "error",
                        "message": "Invalid 'contamination_status' value. Use yes/no.",
                    }
                ), 400
        # Complete the sequence
        # Note: weight_value will be None if not provided, which means weight won't be updated in the database
        # Weight should only be set when explicitly provided via the API
        result = update_stop_status_by_sequence(
            route_id,
            current_sequence,
            "completed",
            weight_value,  # Use parsed weight_value instead of raw weight
            remark,
            waste_image_url,
            receipt_image_url,
            poc_name,
            poc_designation,
            poc_signature_url,
        )
        if result.get("success"):
            # Persist contamination_status flag if provided
            if contaminated_normalized in ("yes", "no"):
                try:
                    update_cont_sql = """
                    UPDATE b2b_route_stops
                    SET contamination_status = %s, updated_at = NOW()
                    WHERE route_id = %s AND sequence = %s
                    """
                    execute_query(
                        update_cont_sql,
                        (contaminated_normalized, route_id, current_sequence),
                    )
                except Exception:
                    pass
            # Fetch the updated stop to return authoritative values
            # Only return weight from b2b_route_stops table (not from est_weight or any other source)
            stop_fetch_sql = """
            SELECT route_id, sequence, status, weight, remark, waste_image_url, receipt_image_url,
                   contamination_status, poc_name, poc_designation, poc_signature,
                   pickup_started_at, pickup_ended_at, completed_at
            FROM b2b_route_stops
            WHERE route_id = %s AND sequence = %s
            """
            stop_row = None
            try:
                stop_res = execute_query(
                    stop_fetch_sql, (route_id, current_sequence), fetch_one=True
                )
                if stop_res.get("success"):
                    stop_row = stop_res.get("data")
                    # Log the weight that was actually saved
                    if stop_row:
                        saved_weight = stop_row.get("weight")
                        print(f"✅ [auto_complete_current] Weight saved in database: {saved_weight} kg for route_id {route_id}, sequence {current_sequence}")
            except Exception as e:
                print(f"❌ [auto_complete_current] Error fetching stop data: {str(e)}")
                stop_row = None
            # Get next sequence info after completion
            next_info_after = get_next_sequence(route_id)
            next_sequence = next_info_after.get("next_sequence")
            # Build response from DB row if available
            response_data = {
                "route_id": route_id,
                "completed_sequence": current_sequence,
                "status": "completed",
                "pickup_ended_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "next_sequence": next_sequence,
                "all_completed": next_sequence is None,
            }
            if stop_row:
                if stop_row.get("weight") is not None:
                    response_data["weight"] = float(stop_row.get("weight"))
                if stop_row.get("remark"):
                    response_data["remark"] = stop_row.get("remark")
                if stop_row.get("waste_image_url"):
                    response_data["waste_image_url"] = stop_row.get("waste_image_url")
                if stop_row.get("receipt_image_url"):
                    response_data["receipt_image_url"] = stop_row.get(
                        "receipt_image_url"
                    )
                if stop_row.get("contamination_status") is not None:
                    response_data["contamination_status"] = stop_row.get(
                        "contamination_status"
                    )
                if stop_row.get("poc_name"):
                    response_data["poc_name"] = stop_row.get("poc_name")
                if stop_row.get("poc_designation"):
                    response_data["poc_designation"] = stop_row.get("poc_designation")
                if stop_row.get("poc_signature"):
                    response_data["poc_signature"] = stop_row.get("poc_signature")
                # also include DB timestamps if present
                response_data["db_timestamps"] = {
                    "pickup_started_at": stop_row.get("pickup_started_at"),
                    "pickup_ended_at_db": stop_row.get("pickup_ended_at"),
                    "completed_at": stop_row.get("completed_at"),
                }
            else:
                # Fallback to local variables
                if weight is not None:
                    response_data["weight"] = weight
                if remark:
                    response_data["remark"] = remark
                if waste_image_url:
                    response_data["waste_image_url"] = waste_image_url
                if receipt_image_url:
                    response_data["receipt_image_url"] = receipt_image_url
                if contaminated_normalized in ("yes", "no"):
                    response_data["contamination_status"] = contaminated_normalized
                if poc_name:
                    response_data["poc_name"] = poc_name
                if poc_designation:
                    response_data["poc_designation"] = poc_designation
                if poc_signature_url:
                    response_data["poc_signature"] = poc_signature_url
            message = f"Sequence {current_sequence} completed successfully"
            if next_sequence:
                message += f". Next sequence: {next_sequence}"
            else:
                message += ". All sequences completed!"
            return jsonify(
                {"status": "success", "message": message, "data": response_data}
            )
        else:
            return jsonify(
                {
                    "status": "error",
                    "message": "Failed to complete sequence",
                    "error": result.get("error"),
                }
            ), 500
    except Exception as e:
        return jsonify(
            {"status": "error", "message": f"Error auto-completing sequence: {str(e)}"}
        ), 500
@app.route("/multi-pickup/current-status", methods=["GET"])
@require_multi_pickup_auth
def get_current_pickup_status():
    """Get current pickup status based on session authentication"""
    try:
        # Get route_id from session token
        token_data = request.token_data
        route_id = token_data.get("route_id")
        driver_dl = token_data.get("dl_no")
        vehicle_no = token_data.get("vehicle_no")
        if not route_id:
            return jsonify(
                {"status": "error", "message": "No assignment found in session"}
            ), 400
        # Get assignment details
        assignment_sql = """
        SELECT route_id, route_date, status, 
               trip_started_at, trip_ended_at, created_at
        FROM b2b_route_assignments 
        WHERE route_id = %s
        """
        assignment_result = execute_query(assignment_sql, (route_id,), fetch_one=True)
        if not assignment_result.get("success"):
            return jsonify({"status": "error", "message": "Assignment not found"}), 404
        assignment_data = assignment_result.get("data")
        # Get next sequence info
        next_info = get_next_sequence(route_id)
        next_sequence = next_info.get("next_sequence")
        sequence_status = next_info.get("status")
        # Get progress summary
        progress_sql = """
        SELECT 
            COUNT(*) as total_sequences,
            SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_sequences,
            SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) as in_progress_sequences,
            SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending_sequences
        FROM b2b_route_stops 
        WHERE route_id = %s
        """
        progress_result = execute_query(progress_sql, (route_id,), fetch_one=True)
        progress_data = (
            progress_result.get("data", {}) if progress_result.get("success") else {}
        )
        # Determine next action
        if sequence_status == "all_completed":
            next_action = "complete_trip"
            next_endpoint = "/multi-pickup/complete-trip"
        elif sequence_status == "pending":
            next_action = "start_next_sequence"
            next_endpoint = "/multi-pickup/auto-start-next"
        elif sequence_status == "in_progress":
            next_action = "complete_current_sequence"
            next_endpoint = "/multi-pickup/auto-complete-current"
        else:
            next_action = "unknown"
            next_endpoint = None
        return jsonify(
            {
                "status": "success",
                "message": "Current status retrieved successfully",
                "data": {
                    "driver_info": {"driver_dl": driver_dl, "vehicle_no": vehicle_no},
                    "assignment": {
                        "route_id": route_id,
                        "route_date": assignment_data.get("route_date"),
                        "status": assignment_data.get("status"),
                        "trip_started_at": assignment_data.get("trip_started_at"),
                        "created_at": assignment_data.get("created_at"),
                    },
                    "progress": {
                        "total_sequences": progress_data.get("total_sequences", 0),
                        "completed_sequences": progress_data.get(
                            "completed_sequences", 0
                        ),
                        "in_progress_sequences": progress_data.get(
                            "in_progress_sequences", 0
                        ),
                        "pending_sequences": progress_data.get("pending_sequences", 0),
                        "completion_percentage": round(
                            (
                                progress_data.get("completed_sequences", 0)
                                / max(progress_data.get("total_sequences", 1), 1)
                            )
                            * 100,
                            1,
                        ),
                    },
                    "current_sequence": {
                        "next_sequence": next_sequence,
                        "status": sequence_status,
                        "all_completed": sequence_status == "all_completed",
                    },
                    "next_action": {
                        "action": next_action,
                        "endpoint": next_endpoint,
                        "description": {
                            "start_next_sequence": f"Start sequence {next_sequence}",
                            "complete_current_sequence": f"Complete sequence {next_sequence}",
                            "complete_trip": "Complete the entire trip",
                            "unknown": "Status unclear",
                        }.get(next_action, "Unknown action"),
                    },
                },
            }
        )
    except Exception as e:
        return jsonify(
            {"status": "error", "message": f"Error getting current status: {str(e)}"}
        ), 500
@app.route("/multi-pickup/auto-start-trip", methods=["POST"])
@require_multi_pickup_auth
def auto_start_trip():
    """Automatically start trip based on session authentication (no manual route_id)"""
    try:
        # Get route_id from session token
        token_data = request.token_data
        route_id = token_data.get("route_id")
        if not route_id:
            return jsonify(
                {"status": "error", "message": "No assignment found in session"}
            ), 400
        # Start the trip
        result = update_assignment_status(route_id, "in_progress")
        if result.get("success"):
            # Get assignment details for response
            assignment_sql = """
            SELECT route_id, route_date, driver_dl, vehicle_no, status
            FROM b2b_route_assignments 
            WHERE route_id = %s
            """
            assignment_result = execute_query(
                assignment_sql, (route_id,), fetch_one=True
            )
            assignment_data = (
                assignment_result.get("data", {})
                if assignment_result.get("success")
                else {}
            )
            # Get total sequences count
            count_sql = "SELECT COUNT(*) as total_sequences FROM b2b_route_stops WHERE route_id = %s"
            count_result = execute_query(count_sql, (route_id,), fetch_one=True)
            total_sequences = (
                count_result.get("data", {}).get("total_sequences", 0)
                if count_result.get("success")
                else 0
            )
            return jsonify(
                {
                    "status": "success",
                    "message": "Trip started successfully",
                    "data": {
                        "route_id": route_id,
                        "route_date": assignment_data.get("route_date"),
                        "driver_dl": assignment_data.get("driver_dl"),
                        "vehicle_no": assignment_data.get("vehicle_no"),
                        "status": "in_progress",
                        "trip_started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "total_sequences": total_sequences,
                        "next_action": "start_first_sequence",
                    },
                }
            )
        else:
            return jsonify(
                {
                    "status": "error",
                    "message": "Failed to start trip",
                    "error": result.get("error"),
                }
            ), 500
    except Exception as e:
        return jsonify(
            {"status": "error", "message": f"Error starting trip: {str(e)}"}
        ), 500
@app.route("/multi-pickup/auto-complete-trip", methods=["POST"])
@require_multi_pickup_auth
def auto_complete_trip():
    """Automatically complete trip based on session authentication (no manual route_id)"""
    try:
        # Get route_id from session token
        token_data = request.token_data
        route_id = token_data.get("route_id")
        if not route_id:
            return jsonify(
                {"status": "error", "message": "No assignment found in session"}
            ), 400
        # Check if all sequences are completed
        next_info = get_next_sequence(route_id)
        if next_info.get("status") != "all_completed":
            return jsonify(
                {
                    "status": "error",
                    "message": "Cannot complete trip. Not all sequences are completed.",
                    "data": {
                        "route_id": route_id,
                        "next_sequence": next_info.get("next_sequence"),
                        "remaining_sequences": "pending",
                    },
                }
            ), 400
        # Complete the trip
        result = update_assignment_status(route_id, "completed")
        if result.get("success"):
            # Get final statistics
            stats_sql = """
            SELECT 
                COUNT(*) as total_sequences,
                SUM(COALESCE(weight, 0)) as total_weight,
                MIN(pickup_started_at) as first_pickup_time,
                MAX(pickup_ended_at) as last_pickup_time
            FROM b2b_route_stops 
            WHERE route_id = %s AND status = 'completed'
            """
            stats_result = execute_query(stats_sql, (route_id,), fetch_one=True)
            stats_data = (
                stats_result.get("data", {}) if stats_result.get("success") else {}
            )
            return jsonify(
                {
                    "status": "success",
                    "message": "Trip completed successfully!",
                    "data": {
                        "route_id": route_id,
                        "status": "completed",
                        "trip_ended_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "statistics": {
                            "total_sequences_completed": stats_data.get(
                                "total_sequences", 0
                            ),
                            "total_weight_collected": float(
                                stats_data.get("total_weight", 0)
                            ),
                            "first_pickup_time": stats_data.get("first_pickup_time"),
                            "last_pickup_time": stats_data.get("last_pickup_time"),
                        },
                    },
                }
            )
        else:
            return jsonify(
                {
                    "status": "error",
                    "message": "Failed to complete trip",
                    "error": result.get("error"),
                }
            ), 500
    except Exception as e:
        return jsonify(
            {"status": "error", "message": f"Error completing trip: {str(e)}"}
        ), 500
@app.route("/multi-pickup/test-setup", methods=["POST"])
def test_multi_pickup_setup():
    """Test endpoint to create a complete multi-pickup assignment with stops"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "No data provided"}), 400
        # Create assignment with proper date format (no driver_dl - will be assigned by vehicle app)
        today_date = datetime.now().strftime("%Y-%m-%d")
        assignment_data = {
            "route_date": data.get("route_date", today_date),
            "driver_dl": None,  # No driver assigned initially
            "vehicle_no": data.get("vehicle_no", "KA01AB1234"),
        }
        # Ensure route_date is in correct format
        try:
            # Validate and reformat the date
            route_date_obj = datetime.strptime(
                assignment_data["route_date"], "%Y-%m-%d"
            )
            assignment_data["route_date"] = route_date_obj.strftime("%Y-%m-%d")
        except ValueError:
            assignment_data["route_date"] = today_date
        # Create assignment without driver (driver will be assigned when they request it)
        insert_sql = """
        INSERT INTO b2b_route_assignments (
            route_date, vehicle_no, status, created_at, updated_at
        ) VALUES (%s, %s, %s, NOW(), NOW())
        """
        params = (
            assignment_data["route_date"],
            assignment_data["vehicle_no"],
            "pending",
        )
        assignment_result = execute_query(insert_sql, params)
        if assignment_result.get("success"):
            # Get the route_id
            get_id_sql = "SELECT route_id FROM b2b_route_assignments WHERE DATE(route_date) = %s AND vehicle_no = %s ORDER BY route_id DESC LIMIT 1"
            id_result = execute_query(
                get_id_sql,
                (assignment_data["route_date"], assignment_data["vehicle_no"]),
                fetch_one=True,
            )
            if id_result.get("success"):
                route_id = id_result.get("data", {}).get("route_id")
                assignment_data["route_id"] = route_id
            else:
                return jsonify(
                    {"status": "error", "message": "Failed to retrieve assignment ID"}
                ), 500
        else:
            return jsonify(
                {
                    "status": "error",
                    "message": "Failed to create assignment",
                    "error": assignment_result.get("error"),
                }
            ), 500
        route_id = assignment_data["route_id"]
        # Add sample stops
        sample_stops = data.get(
            "stops",
            [
                {
                    "sequence": 1,
                    "latitude": 12.9716,
                    "longitude": 77.5946,
                    "branch_name": "Sample Branch 1",
                    "address": "123 Main Street, Bangalore",
                    "contact": "9876543210",
                    "branch_code": "BR001",
                },
                {
                    "sequence": 2,
                    "latitude": 12.9756,
                    "longitude": 77.5986,
                    "branch_name": "Sample Branch 2",
                    "address": "456 Park Avenue, Bangalore",
                    "contact": "9876543211",
                    "branch_code": "BR002",
                },
            ],
        )
        added_stops = []
        for stop in sample_stops:
            stop_result = add_pickup_stop(
                route_id,
                stop["sequence"],
                stop["latitude"],
                stop["longitude"],
                stop["branch_name"],
                stop["address"],
                stop["contact"],
                stop["branch_code"],
            )
            if stop_result.get("success"):
                added_stops.append(
                    {
                        "sequence": stop["sequence"],
                        "branch_name": stop["branch_name"],
                        "branch_code": stop["branch_code"],
                        "status": "pending",
                    }
                )
        # Get complete assignment details
        assignment_details = get_assignment_details(route_id)
        return jsonify(
            {
                "status": "success",
                "message": "Multi-pickup test setup completed successfully (no driver assigned - will be assigned by vehicle app)",
                "data": {
                    "route_id": route_id,
                    "assignment": {
                        "route_date": assignment_data["route_date"],
                        "vehicle_no": assignment_data["vehicle_no"],
                        "driver_dl": None,  # Will be assigned when driver requests it
                        "status": "pending",
                    },
                    "stops_added": added_stops,
                    "complete_details": assignment_details.get("data")
                    if assignment_details.get("success")
                    else None,
                },
            }
        )
    except Exception as e:
        return jsonify(
            {"status": "error", "message": f"Error in test setup: {str(e)}"}
        ), 500

# ==================== BARCODE SCANNER API ENDPOINTS ====================

@app.route("/barcode/test", methods=["GET"])
def test_barcode_endpoint():
    """Test endpoint to verify barcode routes are registered"""
    # Get all registered routes
    routes = []
    for rule in app.url_map.iter_rules():
        if 'barcode' in rule.rule:
            routes.append({
                "path": rule.rule,
                "methods": list(rule.methods),
                "endpoint": rule.endpoint
            })
    
    return jsonify({
        "status": "success",
        "message": "Barcode endpoints are active",
        "total_barcode_routes": len(routes),
        "registered_routes": routes,
        "all_endpoints": [
            "/barcode/scan",
            "/barcode/register",
            "/barcode/master/list",
            "/barcode/cycle/start",
            "/barcode/cycle/scan-and-start",
            "/barcode/cycle/<id>/update-status",
            "/barcode/cycle/<id>",
            "/barcode/cycle/list",
            "/barcode/cycle/by-barcode/<barcode_id>"
        ]
    }), 200

@app.route("/debug/routes", methods=["GET"])
def debug_all_routes():
    """Debug endpoint to list ALL registered routes"""
    routes = []
    for rule in app.url_map.iter_rules():
        routes.append({
            "path": rule.rule,
            "methods": list(rule.methods),
            "endpoint": rule.endpoint
        })
    return jsonify({
        "status": "success",
        "total_routes": len(routes),
        "routes": sorted(routes, key=lambda x: x["path"])
    }), 200

@app.route("/barcode/scan", methods=["POST"])
@require_auth
def scan_barcode():
    """
    Scan and validate a barcode
    Returns barcode information if found and active
    """
    try:
        print(f"🔍 [scan_barcode] Request received: {request.method} {request.path}")
        print(f"🔍 [scan_barcode] Request URL: {request.url}")
        print(f"🔍 [scan_barcode] Request headers: {dict(request.headers)}")
        
        data = request.get_json()
        print(f"🔍 [scan_barcode] Request data: {data}")
        
        if not data or "barcode_id" not in data:
            return jsonify(
                {"status": "error", "message": "barcode_id is required"}
            ), 400

        barcode_id = data["barcode_id"]
        print(f"🔍 [scan_barcode] Scanning barcode: {barcode_id}")
        
        # Check if barcode exists and is active
        query = """
            SELECT id, barcode_id, bagtype, is_active, created_at
            FROM barcode_master_table
            WHERE barcode_id = %s AND is_active = 1
        """
        print(f"🔍 [scan_barcode] Executing query for barcode: {barcode_id}")
        result = execute_query(query, (barcode_id,), fetch_one=True)
        print(f"🔍 [scan_barcode] Query result success: {result.get('success')}")
        print(f"🔍 [scan_barcode] Query result data: {result.get('data')}")
        
        if not result.get("success"):
            print(f"❌ [scan_barcode] Database error: {result.get('error')}")
            return jsonify(
                {"status": "error", "message": "Database error occurred"}
            ), 500
        
        barcode_data = result.get("data")
        
        # If barcode not found, auto-register it
        if not barcode_data:
            print(f"⚠️ [scan_barcode] Barcode not found, auto-registering: {barcode_id}")
            # Auto-register with default bagtype 'B2B'
            bagtype = data.get("bagtype", "B2B")  # Default to B2B if not provided
            insert_query = """
                INSERT INTO barcode_master_table (barcode_id, bagtype, is_active, created_at)
                VALUES (%s, %s, 1, NOW())
            """
            insert_result = execute_query(insert_query, (barcode_id, bagtype))
            
            if not insert_result.get("success"):
                print(f"❌ [scan_barcode] Failed to auto-register barcode: {insert_result.get('error')}")
                return jsonify(
                    {
                        "status": "error",
                        "message": f"Barcode not found and failed to register: {insert_result.get('error')}",
                        "barcode_id": barcode_id,
                    }
                ), 500
            
            # Get the newly registered barcode
            get_new_query = """
                SELECT id, barcode_id, bagtype, is_active, created_at
                FROM barcode_master_table
                WHERE barcode_id = %s
            """
            new_result = execute_query(get_new_query, (barcode_id,), fetch_one=True)
            barcode_data = new_result.get("data") if new_result.get("success") else None
            
            if not barcode_data:
                return jsonify(
                    {
                        "status": "error",
                        "message": "Barcode registered but failed to retrieve",
                        "barcode_id": barcode_id,
                    }
                ), 500
            
            print(f"✅ [scan_barcode] Barcode auto-registered: {barcode_data}")
        
        print(f"✅ [scan_barcode] Barcode found: {barcode_data}")
        response = jsonify(
            {
                "status": "success",
                "message": "Barcode found",
                "data": barcode_data,
            }
        )
        print(f"✅ [scan_barcode] Returning response: {response.get_data(as_text=True)}")
        return response
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"❌ [scan_barcode] Exception occurred: {str(e)}")
        print(f"❌ [scan_barcode] Traceback: {error_trace}")
        return jsonify(
            {"status": "error", "message": f"Error scanning barcode: {str(e)}"}
        ), 500


@app.route("/barcode/register", methods=["POST"])
@require_auth
def register_barcode():
    """
    Register a new barcode in the master table
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify(
                {"status": "error", "message": "No data provided"}
            ), 400
        
        required_fields = ["barcode_id", "bagtype"]
        for field in required_fields:
            if field not in data:
                return jsonify(
                    {"status": "error", "message": f"Missing required field: {field}"}
                ), 400
        
        barcode_id = data["barcode_id"]
        bagtype = data["bagtype"]
        is_active = data.get("is_active", 1)  # Default to active
        
        # Check if barcode already exists
        check_query = """
            SELECT id FROM barcode_master_table WHERE barcode_id = %s
        """
        check_result = execute_query(check_query, (barcode_id,), fetch_one=True)
        
        if not check_result.get("success"):
            return jsonify(
                {"status": "error", "message": "Database error occurred"}
            ), 500
        
        if check_result.get("data"):
            return jsonify(
                {
                    "status": "error",
                    "message": "Barcode already exists",
                    "barcode_id": barcode_id,
                }
            ), 409
        
        # Insert new barcode
        insert_query = """
            INSERT INTO barcode_master_table (barcode_id, bagtype, is_active, created_at)
            VALUES (%s, %s, %s, NOW())
        """
        insert_result = execute_query(insert_query, (barcode_id, bagtype, is_active))
        
        if not insert_result.get("success"):
            return jsonify(
                {"status": "error", "message": "Failed to register barcode"}
            ), 500
        
        barcode_id_inserted = insert_result.get("data")
        
        # Get the created barcode
        get_query = """
            SELECT id, barcode_id, bagtype, is_active, created_at
            FROM barcode_master_table
            WHERE id = %s
        """
        get_result = execute_query(get_query, (barcode_id_inserted,), fetch_one=True)
        
        return jsonify(
            {
                "status": "success",
                "message": "Barcode registered successfully",
                "data": get_result.get("data") if get_result.get("success") else None,
            }
        )
    except Exception as e:
        return jsonify(
            {"status": "error", "message": f"Error registering barcode: {str(e)}"}
        ), 500


@app.route("/barcode/master/list", methods=["GET"])
@require_auth
def list_barcode_master():
    """
    List all barcodes from master table with optional filters
    Query params: is_active (0/1), bagtype, limit, offset
    """
    try:
        is_active = request.args.get("is_active")
        bagtype = request.args.get("bagtype")
        limit = request.args.get("limit", 100, type=int)
        offset = request.args.get("offset", 0, type=int)
        
        # Build query with filters
        query = """
            SELECT id, barcode_id, bagtype, is_active, created_at
            FROM barcode_master_table
            WHERE 1=1
        """
        params = []
        
        if is_active is not None:
            query += " AND is_active = %s"
            params.append(int(is_active))
        
        if bagtype:
            query += " AND bagtype = %s"
            params.append(bagtype)
        
        query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        result = execute_query(query, tuple(params), fetch_all=True)
        
        if not result.get("success"):
            return jsonify(
                {"status": "error", "message": "Database error occurred"}
            ), 500
        
        # Get total count
        count_query = """
            SELECT COUNT(*) as total FROM barcode_master_table WHERE 1=1
        """
        count_params = []
        if is_active is not None:
            count_query += " AND is_active = %s"
            count_params.append(int(is_active))
        if bagtype:
            count_query += " AND bagtype = %s"
            count_params.append(bagtype)
        
        count_result = execute_query(count_query, tuple(count_params), fetch_one=True)
        total = count_result.get("data", {}).get("total", 0) if count_result.get("success") else 0
        
        return jsonify(
            {
                "status": "success",
                "data": result.get("data", []),
                "pagination": {
                    "total": total,
                    "limit": limit,
                    "offset": offset,
                },
            }
        )
    except Exception as e:
        return jsonify(
            {"status": "error", "message": f"Error listing barcodes: {str(e)}"}
        ), 500


@app.route("/barcode/cycle/start", methods=["POST"])
@require_auth
def start_pickup_cycle():
    """
    Start a new pickup bag cycle (status: 'picked')
    Creates entry in pickup_bag_cycle table
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify(
                {"status": "error", "message": "No data provided"}
            ), 400
        
        required_fields = ["barcode_id", "branch_code", "pickup_weight"]
        for field in required_fields:
            if field not in data:
                return jsonify(
                    {"status": "error", "message": f"Missing required field: {field}"}
                ), 400
        
        barcode_id = data["barcode_id"]
        branch_code = data["branch_code"]
        pickup_weight = data["pickup_weight"]
        
        # Validate barcode exists and is active
        barcode_check = """
            SELECT id, barcode_id, bagtype FROM barcode_master_table
            WHERE barcode_id = %s AND is_active = 1
        """
        barcode_result = execute_query(barcode_check, (barcode_id,), fetch_one=True)
        
        if not barcode_result.get("success"):
            return jsonify(
                {"status": "error", "message": "Database error occurred"}
            ), 500
        
        if not barcode_result.get("data"):
            return jsonify(
                {
                    "status": "error",
                    "message": "Barcode not found or inactive",
                    "barcode_id": barcode_id,
                }
            ), 404
        
        # Generate cycle_id (format: CYCLE_YYYYMMDD_BARCODEID or custom)
        cycle_id = data.get("cycle_id")
        if not cycle_id:
            date_str = datetime.now().strftime("%Y%m%d")
            cycle_id = f"CYCLE_{date_str}_{barcode_id[:8]}"
        
        # Get route_id from token if available
        route_id = None
        try:
            if hasattr(request, 'token_data') and request.token_data:
                route_id = request.token_data.get("route_id")
        except:
            pass
        
        # Also try to get route_id from request data
        if not route_id:
            route_id = data.get("route_id")
        
        # Check if cycle already exists for this barcode
        existing_check = """
            SELECT id FROM pickup_bag_cycle
            WHERE barcode_id = %s AND status != 'completed'
        """
        existing_result = execute_query(existing_check, (barcode_id,), fetch_one=True)
        
        if existing_result.get("success") and existing_result.get("data"):
            return jsonify(
                {
                    "status": "error",
                    "message": "Active cycle already exists for this barcode",
                    "barcode_id": barcode_id,
                }
            ), 409
        
        # Insert new cycle
        insert_query = """
            INSERT INTO pickup_bag_cycle (
                cycle_id, barcode_id, branch_code, pickup_weight, route_id,
                status, picked_at, created_at
            ) VALUES (%s, %s, %s, %s, %s, 'picked', NOW(), NOW())
        """
        insert_result = execute_query(
            insert_query, (cycle_id, barcode_id, branch_code, pickup_weight, route_id)
        )
        
        if not insert_result.get("success"):
            return jsonify(
                {"status": "error", "message": "Failed to start pickup cycle"}
            ), 500
        
        cycle_db_id = insert_result.get("data")
        
        # Get the created cycle
        get_query = """
            SELECT id, cycle_id, barcode_id, branch_code, pickup_weight,
                   inbound_weight, status, picked_at, inbound_at, sorted_at,
                   completed_at, created_at
            FROM pickup_bag_cycle
            WHERE id = %s
        """
        get_result = execute_query(get_query, (cycle_db_id,), fetch_one=True)
        
        return jsonify(
            {
                "status": "success",
                "message": "Pickup cycle started successfully",
                "data": get_result.get("data") if get_result.get("success") else None,
            }
        )
    except Exception as e:
        return jsonify(
            {"status": "error", "message": f"Error starting pickup cycle: {str(e)}"}
        ), 500


@app.route("/barcode/cycle/<int:cycle_id>/update-status", methods=["POST"])
@require_auth
def update_cycle_status(cycle_id):
    """
    Update the status of a pickup bag cycle
    Valid transitions: picked -> inbound -> sorting -> completed
    """
    try:
        data = request.get_json()
        if not data or "status" not in data:
            return jsonify(
                {"status": "error", "message": "status is required"}
            ), 400
        
        new_status = data["status"]
        valid_statuses = ["picked", "inbound", "sorting", "completed"]
        
        if new_status not in valid_statuses:
            return jsonify(
                {
                    "status": "error",
                    "message": f"Invalid status. Must be one of: {', '.join(valid_statuses)}",
                }
            ), 400
        
        # Get current cycle
        get_query = """
            SELECT id, cycle_id, barcode_id, route_id, status, picked_at, inbound_at,
                   sorted_at, completed_at
            FROM pickup_bag_cycle
            WHERE id = %s
        """
        get_result = execute_query(get_query, (cycle_id,), fetch_one=True)
        
        if not get_result.get("success"):
            return jsonify(
                {"status": "error", "message": "Database error occurred"}
            ), 500
        
        if not get_result.get("data"):
            return jsonify(
                {"status": "error", "message": "Cycle not found"}
            ), 404
        
        current_cycle = get_result.get("data")
        current_status = current_cycle["status"]
        
        # Validate status transition
        status_order = ["picked", "inbound", "sorting", "completed"]
        current_index = status_order.index(current_status) if current_status in status_order else -1
        new_index = status_order.index(new_status)
        
        if new_index <= current_index:
            return jsonify(
                {
                    "status": "error",
                    "message": f"Invalid status transition. Current: {current_status}, Requested: {new_status}",
                }
            ), 400
        
        # Build update query based on status
        update_fields = ["status = %s"]
        params = [new_status]
        
        if new_status == "inbound":
            update_fields.append("inbound_at = NOW()")
            if "inbound_weight" in data:
                update_fields.append("inbound_weight = %s")
                params.append(data["inbound_weight"])
        elif new_status == "sorting":
            update_fields.append("sorted_at = NOW()")
        elif new_status == "completed":
            update_fields.append("completed_at = NOW()")
        
        update_query = f"""
            UPDATE pickup_bag_cycle
            SET {', '.join(update_fields)}
            WHERE id = %s
        """
        params.append(cycle_id)
        
        update_result = execute_query(update_query, tuple(params))
        
        if not update_result.get("success"):
            return jsonify(
                {"status": "error", "message": "Failed to update cycle status"}
            ), 500
        
        # Get updated cycle
        get_updated_query = """
            SELECT id, cycle_id, barcode_id, branch_code, pickup_weight,
                   inbound_weight, status, picked_at, inbound_at, sorted_at,
                   completed_at, created_at
            FROM pickup_bag_cycle
            WHERE id = %s
        """
        get_updated_result = execute_query(get_updated_query, (cycle_id,), fetch_one=True)
        
        return jsonify(
            {
                "status": "success",
                "message": f"Cycle status updated to {new_status}",
                "data": get_updated_result.get("data") if get_updated_result.get("success") else None,
            }
        )
    except Exception as e:
        return jsonify(
            {"status": "error", "message": f"Error updating cycle status: {str(e)}"}
        ), 500


@app.route("/barcode/cycle/<int:cycle_id>", methods=["GET"])
@require_auth
def get_cycle_details(cycle_id):
    """
    Get details of a specific pickup bag cycle
    """
    try:
        query = """
            SELECT id, cycle_id, barcode_id, branch_code, pickup_weight, route_id,
                   inbound_weight, status, picked_at, inbound_at, sorted_at,
                   completed_at, created_at
            FROM pickup_bag_cycle
            WHERE id = %s
        """
        result = execute_query(query, (cycle_id,), fetch_one=True)
        
        if not result.get("success"):
            return jsonify(
                {"status": "error", "message": "Database error occurred"}
            ), 500
        
        if not result.get("data"):
            return jsonify(
                {"status": "error", "message": "Cycle not found"}
            ), 404
        
        # Get barcode details
        cycle_data = result.get("data")
        barcode_query = """
            SELECT id, barcode_id, bagtype, is_active
            FROM barcode_master_table
            WHERE barcode_id = %s
        """
        barcode_result = execute_query(
            barcode_query, (cycle_data["barcode_id"],), fetch_one=True
        )
        
        cycle_data["barcode_info"] = (
            barcode_result.get("data") if barcode_result.get("success") else None
        )
        
        return jsonify(
            {
                "status": "success",
                "data": cycle_data,
            }
        )
    except Exception as e:
        return jsonify(
            {"status": "error", "message": f"Error getting cycle details: {str(e)}"}
        ), 500


@app.route("/barcode/cycle/list", methods=["GET"])
@require_auth
def list_cycles():
    """
    List pickup bag cycles with optional filters
    Query params: status, branch_code, barcode_id, limit, offset
    """
    try:
        status = request.args.get("status")
        branch_code = request.args.get("branch_code")
        barcode_id = request.args.get("barcode_id")
        route_id = request.args.get("route_id")
        limit = request.args.get("limit", 100, type=int)
        offset = request.args.get("offset", 0, type=int)
        
        # Build query with filters
        query = """
            SELECT id, cycle_id, barcode_id, branch_code, pickup_weight, route_id,
                   inbound_weight, status, picked_at, inbound_at, sorted_at,
                   completed_at, created_at
            FROM pickup_bag_cycle
            WHERE 1=1
        """
        params = []
        
        if status:
            query += " AND status = %s"
            params.append(status)
        
        if branch_code:
            query += " AND branch_code = %s"
            params.append(branch_code)
        
        if barcode_id:
            query += " AND barcode_id = %s"
            params.append(barcode_id)
        
        if route_id:
            query += " AND route_id = %s"
            params.append(route_id)
        
        query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        result = execute_query(query, tuple(params), fetch_all=True)
        
        if not result.get("success"):
            return jsonify(
                {"status": "error", "message": "Database error occurred"}
            ), 500
        
        # Get total count
        count_query = """
            SELECT COUNT(*) as total FROM pickup_bag_cycle WHERE 1=1
        """
        count_params = []
        if status:
            count_query += " AND status = %s"
            count_params.append(status)
        if branch_code:
            count_query += " AND branch_code = %s"
            count_params.append(branch_code)
        if barcode_id:
            count_query += " AND barcode_id = %s"
            count_params.append(barcode_id)
        if route_id:
            count_query += " AND route_id = %s"
            count_params.append(route_id)
        
        count_result = execute_query(count_query, tuple(count_params), fetch_one=True)
        total = count_result.get("data", {}).get("total", 0) if count_result.get("success") else 0
        
        return jsonify(
            {
                "status": "success",
                "data": result.get("data", []),
                "pagination": {
                    "total": total,
                    "limit": limit,
                    "offset": offset,
                },
            }
        )
    except Exception as e:
        return jsonify(
            {"status": "error", "message": f"Error listing cycles: {str(e)}"}
        ), 500


@app.route("/barcode/cycle/by-barcode/<barcode_id>", methods=["GET"])
@require_auth
def get_cycles_by_barcode(barcode_id):
    """
    Get all cycles for a specific barcode
    """
    try:
        query = """
            SELECT id, cycle_id, barcode_id, branch_code, pickup_weight, route_id,
                   inbound_weight, status, picked_at, inbound_at, sorted_at,
                   completed_at, created_at
            FROM pickup_bag_cycle
            WHERE barcode_id = %s
            ORDER BY created_at DESC
        """
        result = execute_query(query, (barcode_id,), fetch_all=True)
        
        if not result.get("success"):
            return jsonify(
                {"status": "error", "message": "Database error occurred"}
            ), 500
        
        return jsonify(
            {
                "status": "success",
                "data": result.get("data", []),
                "count": len(result.get("data", [])),
            }
        )
    except Exception as e:
        return jsonify(
            {"status": "error", "message": f"Error getting cycles by barcode: {str(e)}"}
        ), 500


@app.route("/barcode/cycle/scan-and-start", methods=["POST"])
@require_auth
def scan_and_start_cycle():
    """
    Combined endpoint: Scan barcode and start pickup cycle in one call
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify(
                {"status": "error", "message": "No data provided"}
            ), 400
        
        required_fields = ["barcode_id", "branch_code", "pickup_weight"]
        for field in required_fields:
            if field not in data:
                return jsonify(
                    {"status": "error", "message": f"Missing required field: {field}"}
                ), 400
        
        barcode_id = data["barcode_id"]
        branch_code = data["branch_code"]
        pickup_weight = data["pickup_weight"]
        
        # Validate and convert pickup_weight to float
        try:
            pickup_weight = float(pickup_weight) if pickup_weight is not None else 0.0
        except (ValueError, TypeError):
            return jsonify(
                {"status": "error", "message": "pickup_weight must be a valid number"}
            ), 400
        
        # Validate barcode (auto-register if not found)
        barcode_check = """
            SELECT id, barcode_id, bagtype FROM barcode_master_table
            WHERE barcode_id = %s AND is_active = 1
        """
        barcode_result = execute_query(barcode_check, (barcode_id,), fetch_one=True)
        
        if not barcode_result.get("success"):
            return jsonify(
                {"status": "error", "message": "Database error occurred"}
            ), 500
        
        # Auto-register barcode if not found
        if not barcode_result.get("data"):
            print(f"⚠️ [scan_and_start_cycle] Barcode not found, auto-registering: {barcode_id}")
            bagtype = data.get("bagtype", "B2B")  # Default to B2B
            insert_barcode_query = """
                INSERT INTO barcode_master_table (barcode_id, bagtype, is_active, created_at)
                VALUES (%s, %s, 1, NOW())
            """
            insert_barcode_result = execute_query(insert_barcode_query, (barcode_id, bagtype))
            
            if not insert_barcode_result.get("success"):
                return jsonify(
                    {
                        "status": "error",
                        "message": f"Barcode not found and failed to register: {insert_barcode_result.get('error')}",
                        "barcode_id": barcode_id,
                    }
                ), 500
            
            # Get the newly registered barcode
            get_new_query = """
                SELECT id, barcode_id, bagtype FROM barcode_master_table
                WHERE barcode_id = %s
            """
            new_result = execute_query(get_new_query, (barcode_id,), fetch_one=True)
            barcode_info = new_result.get("data") if new_result.get("success") else None
            
            if not barcode_info:
                return jsonify(
                    {
                        "status": "error",
                        "message": "Barcode registered but failed to retrieve",
                        "barcode_id": barcode_id,
                    }
                ), 500
            print(f"✅ [scan_and_start_cycle] Barcode auto-registered: {barcode_info}")
        else:
            barcode_info = barcode_result.get("data")
        
        # Check for existing active cycle
        existing_check = """
            SELECT id FROM pickup_bag_cycle
            WHERE barcode_id = %s AND status != 'completed'
        """
        existing_result = execute_query(existing_check, (barcode_id,), fetch_one=True)
        
        if existing_result.get("success") and existing_result.get("data"):
            return jsonify(
                {
                    "status": "error",
                    "message": "Active cycle already exists for this barcode",
                    "barcode_id": barcode_id,
                }
            ), 409
        
        # Get route_id from token if available (for b2b_route_stops)
        route_id = None
        try:
            # Try to get route_id from token if authentication is present
            if hasattr(request, 'token_data') and request.token_data:
                route_id = request.token_data.get("route_id")
        except:
            pass
        
        # Also try to get route_id from request data
        if not route_id:
            route_id = data.get("route_id")
        
        # Generate cycle_id - Fix: Handle barcode_id shorter than 8 characters
        date_str = datetime.now().strftime("%Y%m%d")
        barcode_suffix = barcode_id[:8] if len(barcode_id) >= 8 else barcode_id
        cycle_id = f"CYCLE_{date_str}_{barcode_suffix}"
        
        # Save to b2b_route_stops if route_id is available
        stop_id = None
        if route_id:
            try:
                # Get branch details for route_stops
                branch_name = data.get("branch_name", f"Branch {branch_code}")
                address = data.get("address", "")
                contact = data.get("contact", "")
                latitude = data.get("latitude")
                longitude = data.get("longitude")
                
                # Get next sequence for this route - Fix: Handle None data properly
                sequence_query = """
                    SELECT COALESCE(MAX(sequence), 0) + 1 as next_sequence
                    FROM b2b_route_stops
                    WHERE route_id = %s
                """
                seq_result = execute_query(sequence_query, (route_id,), fetch_one=True)
                if seq_result.get("success") and seq_result.get("data"):
                    sequence = seq_result.get("data").get("next_sequence", 1)
                else:
                    sequence = 1
                
                # Insert into b2b_route_stops
                if latitude and longitude:
                    stop_insert_query = """
                        INSERT INTO b2b_route_stops (
                            route_id, sequence, latitude, longitude, branch_name, 
                            address, contact, branch_code, status, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'pending', NOW(), NOW())
                    """
                    stop_insert_result = execute_query(
                        stop_insert_query, 
                        (route_id, sequence, latitude, longitude, branch_name, address, contact, branch_code)
                    )
                    
                    if stop_insert_result.get("success"):
                        stop_id = stop_insert_result.get("data")
                        print(f"✅ [scan_and_start_cycle] Saved to b2b_route_stops: stop_id={stop_id}, sequence={sequence}")
                    else:
                        print(f"⚠️ [scan_and_start_cycle] Failed to save to b2b_route_stops: {stop_insert_result.get('error')}")
                else:
                    print(f"⚠️ [scan_and_start_cycle] Skipping b2b_route_stops: missing latitude/longitude")
            except Exception as e:
                print(f"⚠️ [scan_and_start_cycle] Error saving to b2b_route_stops: {str(e)}")
        
        # Start cycle in pickup_bag_cycle
        print(f"🔍 [scan_and_start_cycle] Attempting to insert cycle: cycle_id={cycle_id}, barcode_id={barcode_id}, branch_code={branch_code}, pickup_weight={pickup_weight}, route_id={route_id}")
        insert_query = """
            INSERT INTO pickup_bag_cycle (
                cycle_id, barcode_id, branch_code, pickup_weight, route_id,
                status, picked_at, created_at
            ) VALUES (%s, %s, %s, %s, %s, 'picked', NOW(), NOW())
        """
        insert_result = execute_query(
            insert_query, (cycle_id, barcode_id, branch_code, pickup_weight, route_id)
        )
        
        if not insert_result.get("success"):
            error_message = insert_result.get("error", "Unknown database error")
            print(f"❌ [scan_and_start_cycle] Failed to insert cycle: {error_message}")
            print(f"❌ [scan_and_start_cycle] Query: {insert_query}")
            print(f"❌ [scan_and_start_cycle] Params: cycle_id={cycle_id}, barcode_id={barcode_id}, branch_code={branch_code}, pickup_weight={pickup_weight}")
            return jsonify(
                {
                    "status": "error", 
                    "message": f"Failed to start pickup cycle: {error_message}",
                    "details": {
                        "cycle_id": cycle_id,
                        "barcode_id": barcode_id,
                        "branch_code": branch_code,
                        "pickup_weight": pickup_weight
                    }
                }
            ), 500
        
        cycle_db_id = insert_result.get("data")
        print(f"✅ [scan_and_start_cycle] Saved to pickup_bag_cycle: cycle_id={cycle_db_id}")
        
        # Get the created cycle
        get_query = """
            SELECT id, cycle_id, barcode_id, branch_code, pickup_weight, route_id,
                   inbound_weight, status, picked_at, inbound_at, sorted_at,
                   completed_at, created_at
            FROM pickup_bag_cycle
            WHERE id = %s
        """
        get_result = execute_query(get_query, (cycle_db_id,), fetch_one=True)
        
        cycle_data = get_result.get("data") if get_result.get("success") else None
        
        # Fix: Validate cycle_data before returning
        if not cycle_data:
            return jsonify(
                {
                    "status": "error",
                    "message": "Cycle created but failed to retrieve cycle data",
                    "cycle_db_id": cycle_db_id,
                }
            ), 500
        
        return jsonify(
            {
                "status": "success",
                "message": "Barcode scanned and pickup cycle started",
                "data": {
                    "barcode_info": barcode_info,
                    "cycle": cycle_data,
                    "route_stop": {
                        "stop_id": stop_id,
                        "route_id": route_id,
                    } if stop_id else None,
                },
            }
        )
    except Exception as e:
        print(f"❌ [scan_and_start_cycle] Exception: {str(e)}")
        import traceback
        print(f"❌ [scan_and_start_cycle] Traceback: {traceback.format_exc()}")
        return jsonify(
            {"status": "error", "message": f"Error in scan and start: {str(e)}"}
        ), 500


@app.route("/barcode/cycle/batch-scan-and-start", methods=["POST"])
@require_auth
def batch_scan_and_start_cycle():
    """
    Batch endpoint: Process multiple barcodes in one call
    Accepts array of barcodes with weights and processes all at once
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify(
                {"status": "error", "message": "No data provided"}
            ), 400
        
        # Validate required fields
        if "barcodes" not in data or not isinstance(data["barcodes"], list):
            return jsonify(
                {"status": "error", "message": "barcodes array is required"}
            ), 400
        
        if "branch_code" not in data:
            return jsonify(
                {"status": "error", "message": "branch_code is required"}
            ), 400
        
        barcodes = data["barcodes"]
        branch_code = data["branch_code"]
        
        if len(barcodes) == 0:
            return jsonify(
                {"status": "error", "message": "barcodes array cannot be empty"}
            ), 400
        
        # Get route_id from token or data
        route_id = None
        try:
            if hasattr(request, 'token_data') and request.token_data:
                route_id = request.token_data.get("route_id")
        except:
            pass
        
        if not route_id:
            route_id = data.get("route_id")
        
        # Get additional data
        additionalData = data.get("additionalData", {})
        branch_name = additionalData.get("branch_name", f"Branch {branch_code}")
        address = additionalData.get("address", "")
        contact = additionalData.get("contact", "")
        latitude = additionalData.get("latitude")
        longitude = additionalData.get("longitude")
        
        # Validate all barcodes first
        validated_barcodes = []
        for idx, barcode_item in enumerate(barcodes):
            if not isinstance(barcode_item, dict):
                return jsonify(
                    {
                        "status": "error",
                        "message": f"Barcode at index {idx} must be an object"
                    }
                ), 400
            
            if "barcode_id" not in barcode_item:
                return jsonify(
                    {
                        "status": "error",
                        "message": f"barcode_id is required for barcode at index {idx}"
                    }
                ), 400
            
            if "pickup_weight" not in barcode_item:
                return jsonify(
                    {
                        "status": "error",
                        "message": f"pickup_weight is required for barcode at index {idx}"
                    }
                ), 400
            
            barcode_id = barcode_item["barcode_id"]
            pickup_weight = barcode_item["pickup_weight"]
            
            # Validate weight
            try:
                pickup_weight = float(pickup_weight) if pickup_weight is not None else 0.0
                if pickup_weight <= 0:
                    return jsonify(
                        {
                            "status": "error",
                            "message": f"pickup_weight must be greater than 0 for barcode {barcode_id}"
                        }
                    ), 400
            except (ValueError, TypeError):
                return jsonify(
                    {
                        "status": "error",
                        "message": f"pickup_weight must be a valid number for barcode {barcode_id}"
                    }
                ), 400
            
            # Check for duplicate barcodes in request
            if any(b["barcode_id"] == barcode_id for b in validated_barcodes):
                return jsonify(
                    {
                        "status": "error",
                        "message": f"Duplicate barcode_id in request: {barcode_id}"
                    }
                ), 400
            
            validated_barcodes.append({
                "barcode_id": barcode_id,
                "pickup_weight": pickup_weight,
                "bagtype": barcode_item.get("bagtype", "B2B")
            })
        
        # Calculate total weight from all barcodes
        total_weight = sum(item["pickup_weight"] for item in validated_barcodes)
        print(f"[batch_scan_and_start] Total weight for all barcodes: {total_weight} kg")
        
        # Process each barcode
        results = []
        date_str = datetime.now().strftime("%Y%m%d")
        route_stop_created = False  # Track if route stop was created
        route_stop_id = None  # Store route stop ID for weight update
        
        for idx, barcode_item in enumerate(validated_barcodes):
            barcode_id = barcode_item["barcode_id"]
            pickup_weight = barcode_item["pickup_weight"]
            bagtype = barcode_item["bagtype"]
            
            try:
                # Validate barcode (auto-register if not found)
                barcode_check = """
                    SELECT id, barcode_id, bagtype FROM barcode_master_table
                    WHERE barcode_id = %s AND is_active = 1
                """
                barcode_result = execute_query(barcode_check, (barcode_id,), fetch_one=True)
                
                if not barcode_result.get("success"):
                    results.append({
                        "barcode_id": barcode_id,
                        "success": False,
                        "error": "Database error occurred",
                        "error_code": "DATABASE_ERROR"
                    })
                    continue
                
                # Auto-register barcode if not found
                if not barcode_result.get("data"):
                    print(f"⚠️ [batch_scan_and_start] Barcode not found, auto-registering: {barcode_id}")
                    insert_barcode_query = """
                        INSERT INTO barcode_master_table (barcode_id, bagtype, is_active, created_at)
                        VALUES (%s, %s, 1, NOW())
                    """
                    insert_barcode_result = execute_query(insert_barcode_query, (barcode_id, bagtype))
                    
                    if not insert_barcode_result.get("success"):
                        results.append({
                            "barcode_id": barcode_id,
                            "success": False,
                            "error": f"Failed to register barcode: {insert_barcode_result.get('error')}",
                            "error_code": "REGISTRATION_FAILED"
                        })
                        continue
                    
                    print(f"✅ [batch_scan_and_start] Barcode auto-registered: {barcode_id}")
                
                # Check for existing active cycle
                existing_check = """
                    SELECT id FROM pickup_bag_cycle
                    WHERE barcode_id = %s AND status != 'completed'
                """
                existing_result = execute_query(existing_check, (barcode_id,), fetch_one=True)
                
                if existing_result.get("success") and existing_result.get("data"):
                    results.append({
                        "barcode_id": barcode_id,
                        "success": False,
                        "error": "Active cycle already exists for this barcode",
                        "error_code": "DUPLICATE_CYCLE"
                    })
                    continue
                
                # Generate cycle_id
                barcode_suffix = barcode_id[:8] if len(barcode_id) >= 8 else barcode_id
                cycle_id = f"CYCLE_{date_str}_{barcode_suffix}"
                
                # Save to b2b_route_stops if route_id is available (only for first barcode to avoid duplicates)
                # Store total weight of all barcodes in route_stops
                stop_id = None
                if route_id and idx == 0 and not route_stop_created and latitude and longitude:
                    try:
                        sequence_query = """
                            SELECT COALESCE(MAX(sequence), 0) + 1 as next_sequence
                            FROM b2b_route_stops
                            WHERE route_id = %s
                        """
                        seq_result = execute_query(sequence_query, (route_id,), fetch_one=True)
                        if seq_result.get("success") and seq_result.get("data"):
                            sequence = seq_result.get("data").get("next_sequence", 1)
                        else:
                            sequence = 1
                        
                        # Insert route stop with total weight (sum of all barcode weights)
                        stop_insert_query = """
                            INSERT INTO b2b_route_stops (
                                route_id, sequence, latitude, longitude, branch_name, 
                                address, contact, branch_code, weight, status, created_at, updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', NOW(), NOW())
                        """
                        stop_insert_result = execute_query(
                            stop_insert_query, 
                            (route_id, sequence, latitude, longitude, branch_name, address, contact, branch_code, total_weight)
                        )
                        
                        if stop_insert_result.get("success"):
                            stop_id = stop_insert_result.get("data")
                            route_stop_id = stop_id
                            route_stop_created = True
                            print(f"[batch_scan_and_start] Saved to b2b_route_stops: stop_id={stop_id}, total_weight={total_weight} kg")
                    except Exception as e:
                        print(f"[batch_scan_and_start] Error saving to b2b_route_stops: {str(e)}")
                
                # Insert into pickup_bag_cycle
                insert_query = """
                    INSERT INTO pickup_bag_cycle (
                        cycle_id, barcode_id, branch_code, pickup_weight, route_id,
                        status, picked_at, created_at
                    ) VALUES (%s, %s, %s, %s, %s, 'picked', NOW(), NOW())
                """
                insert_result = execute_query(
                    insert_query, (cycle_id, barcode_id, branch_code, pickup_weight, route_id)
                )
                
                if not insert_result.get("success"):
                    results.append({
                        "barcode_id": barcode_id,
                        "success": False,
                        "error": f"Failed to start pickup cycle: {insert_result.get('error')}",
                        "error_code": "CYCLE_CREATION_FAILED"
                    })
                    continue
                
                cycle_db_id = insert_result.get("data")
                
                # Get the created cycle
                get_query = """
                    SELECT id, cycle_id, barcode_id, branch_code, pickup_weight, route_id,
                           inbound_weight, status, picked_at, inbound_at, sorted_at,
                           completed_at, created_at
                    FROM pickup_bag_cycle
                    WHERE id = %s
                """
                get_result = execute_query(get_query, (cycle_db_id,), fetch_one=True)
                cycle_data = get_result.get("data") if get_result.get("success") else None
                
                results.append({
                    "barcode_id": barcode_id,
                    "success": True,
                    "cycle_id": cycle_id,
                    "cycle_db_id": cycle_db_id,
                    "data": cycle_data,
                    "message": "Cycle created successfully"
                })
                
            except Exception as e:
                print(f"❌ [batch_scan_and_start] Error processing barcode {barcode_id}: {str(e)}")
                results.append({
                    "barcode_id": barcode_id,
                    "success": False,
                    "error": f"Error processing barcode: {str(e)}",
                    "error_code": "PROCESSING_ERROR"
                })
        
        # Calculate summary
        total = len(results)
        successful = sum(1 for r in results if r.get("success"))
        failed = total - successful
        
        # Calculate actual total weight from successfully processed barcodes
        # This ensures route_stops weight matches only successful barcodes
        successful_weight = 0.0
        for result in results:
            if result.get("success"):
                # Find the corresponding barcode weight
                barcode_id = result.get("barcode_id")
                matching_barcode = next((b for b in validated_barcodes if b["barcode_id"] == barcode_id), None)
                if matching_barcode:
                    successful_weight += matching_barcode["pickup_weight"]
        
        # Update route_stops weight with actual successful weight if route stop was created
        if route_stop_id and route_stop_created:
            try:
                update_weight_query = """
                    UPDATE b2b_route_stops 
                    SET weight = %s, updated_at = NOW()
                    WHERE id = %s
                """
                update_result = execute_query(update_weight_query, (successful_weight, route_stop_id))
                if update_result.get("success"):
                    print(f"[batch_scan_and_start] Updated route_stops weight to {successful_weight} kg (from {successful} successful barcodes)")
                else:
                    print(f"[batch_scan_and_start] Failed to update route_stops weight: {update_result.get('error')}")
            except Exception as e:
                print(f"[batch_scan_and_start] Error updating route_stops weight: {str(e)}")
        
        # Determine overall status
        if successful == total:
            status = "success"
            message = f"Successfully processed all {total} barcodes. Total weight: {successful_weight} kg"
        elif successful > 0:
            status = "partial_success"
            message = f"Processed {successful} of {total} barcodes successfully. Total weight: {successful_weight} kg"
        else:
            status = "error"
            message = f"Failed to process all {total} barcodes"
        
        return jsonify({
            "status": status,
            "message": message,
            "summary": {
                "total": total,
                "successful": successful,
                "failed": failed,
                "total_weight": successful_weight
            },
            "results": results
        }), 200 if successful > 0 else 400
        
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"❌ [batch_scan_and_start] Exception occurred: {str(e)}")
        print(f"❌ [batch_scan_and_start] Traceback: {error_trace}")
        return jsonify(
            {"status": "error", "message": f"Error in batch scan and start cycle: {str(e)}"}
        ), 500

# ==================== END BARCODE SCANNER API ENDPOINTS ====================

# Debug: Print all registered routes on startup
def print_registered_routes():
    """Print all registered routes for debugging"""
    print("\n" + "="*80)
    print("📋 REGISTERED ROUTES:")
    print("="*80)
    barcode_routes = []
    for rule in app.url_map.iter_rules():
        if 'barcode' in rule.rule:
            barcode_routes.append(f"  {list(rule.methods)} {rule.rule}")
            print(f"  ✅ {list(rule.methods)} {rule.rule}")
    print("="*80)
    print(f"Total barcode routes registered: {len(barcode_routes)}")
    print("="*80 + "\n")
    
    if len(barcode_routes) == 0:
        print("⚠️ WARNING: No barcode routes found! Check route definitions.")
    else:
        print("✅ Barcode routes are registered correctly!")

# Print routes when module loads
print_registered_routes()

if __name__ == "__main__":
    print("\n🚀 Starting Flask server...")
    print("📍 Barcode endpoints should be available at:")
    print("   POST /aiml/corporatewebsite/barcode/scan")
    print("   GET  /aiml/corporatewebsite/barcode/test")
    print("   GET  /aiml/corporatewebsite/debug/routes\n")
    app.run(debug=True, host="0.0.0.0", port=5000)
