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
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def log_request_error(endpoint_name, error, request_data=None):
    """Helper function to log request errors with full details"""
    import traceback
    error_trace = traceback.format_exc()
    logger.error(f"[{endpoint_name}] Error occurred: {str(error)}")
    logger.error(f"[{endpoint_name}] Error Type: {type(error).__name__}")
    if request_data:
        logger.error(f"[{endpoint_name}] Request Data: {request_data}")
    logger.error(f"[{endpoint_name}] Full Traceback:\n{error_trace}")
    print(f"❌ [{endpoint_name}] Error: {str(error)}")
    print(f"❌ [{endpoint_name}] Error Type: {type(error).__name__}")
    if request_data:
        print(f"❌ [{endpoint_name}] Request Data: {request_data}")
    print(f"❌ [{endpoint_name}] Full Traceback:\n{error_trace}")
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

app.config["MYSQL_HOST"] = os.getenv("MYSQL_HOST", "1.7.139.173")
# app.config['MYSQL_HOST'] = os.getenv('MYSQL_HOST', '172.16.139.189')
app.config["MYSQL_USER"] = os.getenv("MYSQL_USER", "OSGCORER")
app.config["MYSQL_PASSWORD"] = os.getenv("MYSQL_PASSWORD", "OSG@9123")
# app.config["MYSQL_HOST"] = os.getenv("MYSQL_HOST", "127.0.0.1")
# app.config["MYSQL_USER"] = os.getenv("MYSQL_USER", "root")
# app.config["MYSQL_PASSWORD"] = os.getenv("MYSQL_PASSWORD", "OSG@1212")
app.config["MYSQL_DB"] = os.getenv("MYSQL_DB", "osg_database_testing")
app.config["MYSQL_PORT"] = int(os.getenv("MYSQL_PORT", 3306))
# ==================== END GEOFENCING HELPER FUNCTIONS ====================
# TCI API Configuration for Driver License Data
TCI_API_BASE_URL = "https://api.tcil.in/WhatsAppAPILive/api/TcilApi"
TCI_USERID = "TCICOE_Ulip_01"
TCI_PASSWORD = "T3c1i6c5o3e1U7l9i6p3H"
# Gmail SMTP Configuration
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USERNAME = "dev1@onestepgreener.org"
SMTP_PASSWORD = "ooti hqvf zwxt rlvr"  # App password
SMTP_USE_TLS = True
# OTP Configuration
OTP_EXPIRY_MINUTES = 10  # OTP expires after 10 minutes
OTP_LENGTH = 6  # 6-digit OTP
MAX_OTP_ATTEMPTS = 10  # Maximum attempts per email per hour
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
            error_msg = "Could not establish database connection"
            print(f"❌ [execute_query] {error_msg}")
            print(f"❌ [execute_query] Config - Host: {app.config['MYSQL_HOST']}, Port: {app.config['MYSQL_PORT']}, DB: {app.config['MYSQL_DB']}, User: {app.config['MYSQL_USER']}")
            return {
                "success": False,
                "error": error_msg,
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
        print(f"❌ [execute_query] MySQL Error: {e}")
        print(f"❌ [execute_query] Error Code: {e.errno if hasattr(e, 'errno') else 'N/A'}")
        print(f"❌ [execute_query] SQL State: {e.sqlstate if hasattr(e, 'sqlstate') else 'N/A'}")
        print(f"❌ [execute_query] Query: {query}")
        print(f"❌ [execute_query] Params: {params}")
        print(f"❌ [execute_query] Full Traceback:\n{error_trace}")
        if connection:
            connection.rollback()
            print(f"❌ [execute_query] Transaction rolled back")
        return {"success": False, "error": f"Database error: {e}"}
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"❌ [execute_query] Unexpected error: {e}")
        print(f"❌ [execute_query] Error Type: {type(e).__name__}")
        print(f"❌ [execute_query] Query: {query}")
        print(f"❌ [execute_query] Params: {params}")
        print(f"❌ [execute_query] Full Traceback:\n{error_trace}")
        if connection:
            connection.rollback()
            print(f"❌ [execute_query] Transaction rolled back")
        return {"success": False, "error": f"Unexpected error: {e}"}
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

# Multi-Pickup Route Management Functions
def create_multi_pickup_assignment(route_date, driver_dl, vehicle_no):
    """Create a new multi-pickup assignment"""
    try:
        print(f"🔍 [create_multi_pickup_assignment] Creating assignment - route_date: {route_date}, driver_dl: {driver_dl}, vehicle_no: {vehicle_no}")
        # Ensure route_date is in correct format (YYYY-MM-DD)
        try:
            date_obj = datetime.strptime(route_date, "%Y-%m-%d")
            formatted_route_date = date_obj.strftime("%Y-%m-%d")
        except ValueError as ve:
            # If invalid date, use today
            print(f"⚠️ [create_multi_pickup_assignment] Invalid date format '{route_date}', using today's date. Error: {ve}")
            formatted_route_date = datetime.now().strftime("%Y-%m-%d")
        
        insert_sql = """
        INSERT INTO b2b_route_assignments (
            route_date, driver_dl, vehicle_no, status, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, NOW(), NOW())
        """
        params = (formatted_route_date, driver_dl, vehicle_no, "pending")
        print(f"🔍 [create_multi_pickup_assignment] Executing INSERT with params: {params}")
        result = execute_query(insert_sql, params)
        
        if result.get("success"):
            print(f"✅ [create_multi_pickup_assignment] Assignment created successfully")
            # Get the route_id
            get_id_sql = "SELECT route_id FROM b2b_route_assignments WHERE driver_dl = %s AND vehicle_no = %s AND DATE(route_date) = %s ORDER BY route_id DESC LIMIT 1"
            id_result = execute_query(
                get_id_sql,
                (driver_dl, vehicle_no, formatted_route_date),
                fetch_one=True,
            )
            if id_result.get("success"):
                route_id = id_result.get("data", {}).get("route_id")
                print(f"✅ [create_multi_pickup_assignment] Retrieved route_id: {route_id}")
                return {
                    "success": True,
                    "route_id": route_id,
                }
            else:
                print(f"⚠️ [create_multi_pickup_assignment] Failed to retrieve route_id: {id_result.get('error')}")
        else:
            print(f"❌ [create_multi_pickup_assignment] Failed to create assignment: {result.get('error')}")
        
        return {"success": False, "error": result.get("error", "Unknown error")}
    except Exception as e:
        error_msg = f"Exception in create_multi_pickup_assignment: {str(e)}"
        log_request_error("create_multi_pickup_assignment", e, {
            "route_date": route_date,
            "driver_dl": driver_dl,
            "vehicle_no": vehicle_no
        })
        return {"success": False, "error": error_msg}
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
    Automatically converts SVG to PNG for better compatibility.
    Returns the file path (URL) if successful, None otherwise.
    """
    try:
        # If signature_file is provided (multipart/form-data), use file upload
        if signature_file and signature_file.filename:
            upload_result = upload_and_get_path(signature_file)
            if upload_result["status"] == "success":
                print(f"✅ [handle_signature_upload] File uploaded successfully: {upload_result['file_path']}")
                return upload_result["file_path"]
            else:
                print(f"⚠️ [handle_signature_upload] File upload failed: {upload_result['message']}")
                return None
        
        # If signature_data is provided as base64 string, convert to file and upload
        if signature_data:
            # Check if it's already a URL/path, return it as is
            if isinstance(signature_data, str) and (signature_data.startswith('http') or signature_data.startswith('/')):
                print(f"✅ [handle_signature_upload] Signature is already a URL: {signature_data}")
                return signature_data
            
            # Check if it's base64 encoded (with or without data URI prefix)
            if isinstance(signature_data, str):
                try:
                    # Extract base64 data and detect image type
                    image_type = None
                    if signature_data.startswith('data:image'):
                        # Extract image type and base64 data
                        # Format: data:image/svg+xml;base64,... or data:image/png;base64,...
                        parts = signature_data.split(',')
                        header = parts[0] if len(parts) > 1 else ''
                        base64_data = parts[1] if len(parts) > 1 else signature_data
                        
                        # Detect image type from header
                        if 'svg+xml' in header:
                            image_type = 'svg'
                        elif 'png' in header:
                            image_type = 'png'
                        elif 'jpeg' in header or 'jpg' in header:
                            image_type = 'jpeg'
                    else:
                        # Assume raw base64 - try to detect if it's SVG by checking content
                        base64_data = signature_data
                        try:
                            decoded = base64.b64decode(base64_data)
                            if decoded.startswith(b'<?xml') or decoded.startswith(b'<svg') or b'<svg' in decoded[:500]:
                                image_type = 'svg'
                            else:
                                image_type = 'png'  # Default to PNG for binary images
                        except:
                            image_type = 'png'  # Default fallback
                    
                    # Decode base64 to binary
                    image_data = base64.b64decode(base64_data)
                    
                    # Convert SVG to PNG if needed
                    final_image_data = image_data
                    filename = "poc_signature.png"
                    
                    if image_type == 'svg':
                        try:
                            print("🔄 [handle_signature_upload] Converting SVG to PNG...")
                            
                            # Try multiple conversion methods in order of preference
                            conversion_success = False
                            
                            # Method 1: Try cairosvg (most reliable)
                            try:
                                import cairosvg
                                final_image_data = cairosvg.svg2png(bytestring=image_data)
                                print("✅ [handle_signature_upload] SVG converted to PNG using cairosvg")
                                conversion_success = True
                            except ImportError:
                                pass  # Try next method
                            except Exception as e:
                                print(f"⚠️ [handle_signature_upload] cairosvg conversion failed: {str(e)}")
                                pass  # Try next method
                            
                            # Method 2: Try svglib + reportlab
                            if not conversion_success:
                                try:
                                    from svglib.svglib import svg2rlg
                                    from reportlab.graphics import renderPM
                                    import io as io_module
                                    
                                    drawing = svg2rlg(io_module.BytesIO(image_data))
                                    png_buffer = io_module.BytesIO()
                                    renderPM.drawToFile(drawing, png_buffer, fmt='PNG')
                                    final_image_data = png_buffer.getvalue()
                                    print("✅ [handle_signature_upload] SVG converted to PNG using svglib+reportlab")
                                    conversion_success = True
                                except ImportError:
                                    pass  # Try next method
                                except Exception as e:
                                    print(f"⚠️ [handle_signature_upload] svglib conversion failed: {str(e)}")
                                    pass  # Try next method
                            
                            # Method 3: If conversion libraries not available, upload as SVG
                            # The SOAP service should handle SVG files
                            if not conversion_success:
                                print("⚠️ [handle_signature_upload] SVG conversion libraries not available (cairosvg or svglib).")
                                print("⚠️ [handle_signature_upload] Uploading as SVG - SOAP service will handle it.")
                                filename = "poc_signature.svg"
                                
                        except Exception as svg_error:
                            print(f"⚠️ [handle_signature_upload] SVG conversion error: {str(svg_error)}")
                            print("⚠️ [handle_signature_upload] Uploading as SVG...")
                            filename = "poc_signature.svg"
                    
                    # Create file-like object with filename for SOAP upload
                    image_bytes = io.BytesIO(final_image_data)
                    
                    class FileWrapper:
                        def __init__(self, file_data, filename):
                            self.file_data = file_data
                            self.filename = filename
                            self._position = 0
                            # Store the data length for seek operations
                            self._data = file_data.getvalue()
                            self._size = len(self._data)
                        
                        def read(self, size=-1):
                            if size == -1:
                                data = self._data[self._position:]
                                self._position = self._size
                            else:
                                end_pos = min(self._position + size, self._size)
                                data = self._data[self._position:end_pos]
                                self._position = end_pos
                            return data
                        
                        def seek(self, pos, whence=0):
                            if whence == 0:
                                self._position = max(0, min(pos, self._size))
                            elif whence == 1:
                                self._position = max(0, min(self._position + pos, self._size))
                            elif whence == 2:
                                self._position = max(0, min(self._size + pos, self._size))
                            return self._position
                        
                        def tell(self):
                            return self._position
                        
                        def __len__(self):
                            return self._size
                    
                    file_wrapper = FileWrapper(image_bytes, filename)
                    print(f"📤 [handle_signature_upload] Uploading signature as {filename} (size: {len(file_wrapper)} bytes)...")
                    
                    # Verify the file wrapper has data
                    if len(file_wrapper) == 0:
                        print("❌ [handle_signature_upload] File wrapper is empty! Cannot upload.")
                        return None
                    
                    upload_result = upload_and_get_path(file_wrapper)
                    
                    if upload_result["status"] == "success":
                        print(f"✅ [handle_signature_upload] Signature uploaded successfully: {upload_result['file_path']}")
                        return upload_result["file_path"]
                    else:
                        print(f"❌ [handle_signature_upload] Upload failed: {upload_result.get('message', 'Unknown error')}")
                        return None
                        
                except Exception as e:
                    print(f"⚠️ [handle_signature_upload] Base64 conversion/upload failed: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    return None
        
        return None
    except Exception as e:
        print(f"❌ [handle_signature_upload] Error handling signature: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def upload_and_get_path(file):
    """Upload file through SOAP service and get file path"""
    # Hardcoded parameters
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
    if not file or len(file.read()) == 0:
        file.seek(0)  # Reset file pointer
        return {"status": "error", "message": "Please select a file to upload."}
    file.seek(0)  # Reset file pointer after checking
    # Convert file to byte array
    byte_file = file.read()
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
        # Upload file
        upload_response = requests.post(
            upload_url, data=soap_upload_request, headers=headers
        )
        # Check if upload was successful
        if upload_response.status_code != 200:
            return {
                "status": "error",
                "message": f"Upload failed with status {upload_response.status_code}",
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
                if (
                    "Please Contact With Administrator" in upload_result.text
                    or "Alert" in upload_result.text
                ):
                    return {
                        "status": "error",
                        "message": f"SOAP Service Error: {upload_result.text}",
                    }
        except:
            pass  # Continue if we can't parse the response
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
        retrieve_response = requests.post(
            upload_url, data=soap_retrieve_request, headers=retrieve_headers
        )
        # Parse XML to extract file path
        try:
            root = ET.fromstring(retrieve_response.text)
            ns = {
                "soapenv": "http://schemas.xmlsoap.org/soap/envelope/",
                "tem": "http://tempuri.org/",
            }
            url_element = root.find(".//tem:ShowScanUploadDocResult", ns)
            if url_element is not None and url_element.text:
                return {"status": "success", "file_path": url_element.text.strip()}
            else:
                # Try alternative element search
                alt_element = root.find(".//ShowScanUploadDocResult")
                if alt_element is not None and alt_element.text:
                    return {"status": "success", "file_path": alt_element.text.strip()}
                return {
                    "status": "error",
                    "message": "Error extracting file path from SOAP response.",
                }
        except ET.ParseError as e:
            return {"status": "error", "message": f"XML parsing error: {str(e)}"}
    except Exception as e:
        return {"status": "error", "message": f"Upload failed: {str(e)}"}
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

# Load tokens on module import (server startup)
load_tokens_from_file()

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
        if not token:
            print(f"⚠️ Token validation failed: No token provided")
            return False, "Invalid token. Please login again."
        if token not in active_tokens:
            print(f"⚠️ Token validation failed: Token '{token[:10]}...' not found in active_tokens (total tokens: {len(active_tokens)})")
            return False, "Invalid token. Session may have expired or backend was restarted. Please login again."
        token_data = active_tokens[token]
        # Check if token expired
        if datetime.now() > token_data["expires_at"]:
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

@app.route("/test/connection", methods=["GET"])
def test_connection():
    """
    Test endpoint to verify frontend-backend connectivity
    Returns server status and configuration
    """
    try:
        return jsonify({
            "status": "success",
            "message": "Backend server is running and accessible",
            "server_info": {
                "timestamp": datetime.now().isoformat(),
                "server_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "base_url_prefix": "/aiml/corporatewebsite",
                "cors_enabled": True,
                "cors_origins": "*",
            },
            "api_endpoints": {
                "test_database": "/test/database",
                "test_connection": "/test/connection",
                "scan_barcode": "/barcode/scan",
                "inbound_weight": "/barcode/inbound/scan-weight",
            },
            "database_status": "Check /test/database for details"
        }), 200
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Error in test endpoint: {str(e)}"
        }), 500


@app.route("/test/database", methods=["GET"])
def test_database_connection():
    """
    Test database connection endpoint
    Returns connection status and database information
    """
    try:
        # Test connection
        connection = get_db_connection()
        
        if not connection:
            return jsonify({
                "status": "error",
                "message": "Failed to establish database connection",
                "config": {
                    "host": app.config["MYSQL_HOST"],
                    "port": app.config["MYSQL_PORT"],
                    "database": app.config["MYSQL_DB"],
                    "user": app.config["MYSQL_USER"],
                    "password_set": bool(app.config["MYSQL_PASSWORD"]),
                }
            }), 500
        
        # Get database info
        cursor = connection.cursor(dictionary=True)
        
        # Test query
        cursor.execute("SELECT VERSION() as version, DATABASE() as current_db, NOW() as server_time")
        db_info = cursor.fetchone()
        
        # Get table count
        cursor.execute("""
            SELECT COUNT(*) as table_count 
            FROM information_schema.tables 
            WHERE table_schema = %s
        """, (app.config["MYSQL_DB"],))
        table_info = cursor.fetchone()
        
        # Check if key tables exist
        key_tables = ['pickup_bag_cycle', 'b2b_route_stops', 'barcode_master_table']
        existing_tables = []
        for table in key_tables:
            cursor.execute("""
                SELECT COUNT(*) as count 
                FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            """, (app.config["MYSQL_DB"], table))
            result = cursor.fetchone()
            if result['count'] > 0:
                existing_tables.append(table)
        
        cursor.close()
        connection.close()
        
        return jsonify({
            "status": "success",
            "message": "Database connection successful",
            "database_info": {
                "version": db_info.get("version"),
                "current_database": db_info.get("current_db"),
                "server_time": str(db_info.get("server_time")),
                "total_tables": table_info.get("table_count"),
                "key_tables_found": existing_tables,
                "key_tables_missing": [t for t in key_tables if t not in existing_tables]
            },
            "connection_config": {
                "host": app.config["MYSQL_HOST"],
                "port": app.config["MYSQL_PORT"],
                "database": app.config["MYSQL_DB"],
                "user": app.config["MYSQL_USER"],
                "password_set": bool(app.config["MYSQL_PASSWORD"]),
            }
        }), 200
        
    except Error as e:
        return jsonify({
            "status": "error",
            "message": f"MySQL Error: {str(e)}",
            "error_type": "MySQL Error",
            "config": {
                "host": app.config["MYSQL_HOST"],
                "port": app.config["MYSQL_PORT"],
                "database": app.config["MYSQL_DB"],
                "user": app.config["MYSQL_USER"],
            }
        }), 500
    except Exception as e:
        import traceback
        return jsonify({
            "status": "error",
            "message": f"Unexpected error: {str(e)}",
            "error_type": "Exception",
            "traceback": traceback.format_exc(),
            "config": {
                "host": app.config["MYSQL_HOST"],
                "port": app.config["MYSQL_PORT"],
                "database": app.config["MYSQL_DB"],
                "user": app.config["MYSQL_USER"],
            }
        }), 500


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
                cycle_id, barcode_id, branch_code, pickup_weight,
                status, picked_at, created_at
            ) VALUES (%s, %s, %s, %s, 'picked', NOW(), NOW())
        """
        insert_result = execute_query(
            insert_query, (cycle_id, barcode_id, branch_code, pickup_weight)
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
            SELECT id, cycle_id, barcode_id, status, picked_at, inbound_at,
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
def get_cycle_details(cycle_id):
    """
    Get details of a specific pickup bag cycle
    """
    try:
        query = """
            SELECT id, cycle_id, barcode_id, branch_code, pickup_weight,
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
def list_cycles():
    """
    List pickup bag cycles with optional filters
    Query params: status, branch_code, barcode_id, limit, offset
    """
    try:
        status = request.args.get("status")
        branch_code = request.args.get("branch_code")
        barcode_id = request.args.get("barcode_id")
        limit = request.args.get("limit", 100, type=int)
        offset = request.args.get("offset", 0, type=int)
        
        # Build query with filters
        query = """
            SELECT id, cycle_id, barcode_id, branch_code, pickup_weight,
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
def get_cycles_by_barcode(barcode_id):
    """
    Get all cycles for a specific barcode
    """
    try:
        query = """
            SELECT id, cycle_id, barcode_id, branch_code, pickup_weight,
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
        print(f"🔍 [scan_and_start_cycle] Attempting to insert cycle: cycle_id={cycle_id}, barcode_id={barcode_id}, branch_code={branch_code}, pickup_weight={pickup_weight}")
        insert_query = """
            INSERT INTO pickup_bag_cycle (
                cycle_id, barcode_id, branch_code, pickup_weight,
                status, picked_at, created_at
            ) VALUES (%s, %s, %s, %s, 'picked', NOW(), NOW())
        """
        insert_result = execute_query(
            insert_query, (cycle_id, barcode_id, branch_code, pickup_weight)
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
            SELECT id, cycle_id, barcode_id, branch_code, pickup_weight,
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


@app.route("/barcode/inbound/scan-weight", methods=["POST"])
def scan_and_record_inbound_weight():
    """
    Scan barcode and record inbound weight
    Updates pickup_bag_cycle.inbound_weight and status to 'inbound'
    Also updates b2b_route_stops.inbound_weight and status to 'inbound' using route_id
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify(
                {"status": "error", "message": "No data provided"}
            ), 400
        
        # Validate required fields
        barcode_id = data.get("barcode_id")
        cycle_id = data.get("cycle_id")
        inbound_weight = data.get("inbound_weight")
        
        if not inbound_weight:
            return jsonify(
                {"status": "error", "message": "inbound_weight is required"}
            ), 400
        
        if not barcode_id and not cycle_id:
            return jsonify(
                {"status": "error", "message": "Either barcode_id or cycle_id is required"}
            ), 400
        
        # Validate and convert inbound_weight to float
        try:
            inbound_weight = float(inbound_weight)
            if inbound_weight <= 0:
                return jsonify(
                    {"status": "error", "message": "inbound_weight must be a positive number"}
                ), 400
        except (ValueError, TypeError):
            return jsonify(
                {"status": "error", "message": "inbound_weight must be a valid number"}
            ), 400
        
        print(f"🔍 [scan_and_record_inbound_weight] Request: barcode_id={barcode_id}, cycle_id={cycle_id}, inbound_weight={inbound_weight}")
        
        # Find the pickup_bag_cycle record
        cycle_query = None
        cycle_params = None
        
        if cycle_id:
            cycle_query = """
                SELECT id, cycle_id, barcode_id, branch_code, route_id, pickup_weight,
                       inbound_weight, status, picked_at, inbound_at, sorted_at,
                       completed_at, created_at
                FROM pickup_bag_cycle
                WHERE id = %s AND status != 'completed'
            """
            cycle_params = (cycle_id,)
        else:
            cycle_query = """
                SELECT id, cycle_id, barcode_id, branch_code, route_id, pickup_weight,
                       inbound_weight, status, picked_at, inbound_at, sorted_at,
                       completed_at, created_at
                FROM pickup_bag_cycle
                WHERE barcode_id = %s AND status != 'completed'
                ORDER BY id DESC
                LIMIT 1
            """
            cycle_params = (barcode_id,)
        
        cycle_result = execute_query(cycle_query, cycle_params, fetch_one=True)
        
        if not cycle_result.get("success"):
            return jsonify(
                {"status": "error", "message": "Database error occurred"}
            ), 500
        
        if not cycle_result.get("data"):
            return jsonify(
                {
                    "status": "error",
                    "message": "Active cycle not found for the provided barcode_id or cycle_id"
                }
            ), 404
        
        cycle_data = cycle_result.get("data")
        current_status = cycle_data.get("status")
        cycle_db_id = cycle_data.get("id")
        route_id = cycle_data.get("route_id")
        branch_code = cycle_data.get("branch_code")
        
        print(f"🔍 [scan_and_record_inbound_weight] Found cycle: id={cycle_db_id}, status={current_status}, route_id={route_id}, branch_code={branch_code}")
        
        # Validate status transition (allow 'picked' -> 'inbound' or update existing 'inbound')
        if current_status not in ["picked", "inbound"]:
            return jsonify(
                {
                    "status": "error",
                    "message": f"Invalid status transition. Current status: {current_status}. Can only update from 'picked' or 'inbound' status."
                }
            ), 400
        
        # Update pickup_bag_cycle
        update_cycle_query = """
            UPDATE pickup_bag_cycle
            SET inbound_weight = %s, status = 'inbound', inbound_at = NOW()
            WHERE id = %s
        """
        update_cycle_result = execute_query(update_cycle_query, (inbound_weight, cycle_db_id))
        
        if not update_cycle_result.get("success"):
            return jsonify(
                {"status": "error", "message": "Failed to update pickup_bag_cycle"}
            ), 500
        
        print(f"✅ [scan_and_record_inbound_weight] Updated pickup_bag_cycle: id={cycle_db_id}, inbound_weight={inbound_weight}")
        
        # Update b2b_route_stops if route_id is available
        route_stop_data = None
        if route_id:
            # Find the route_stop record by route_id and branch_code (to handle multiple stops)
            find_route_stop_query = """
                SELECT id, route_id, sequence, branch_code, status, inbound_weight
                FROM b2b_route_stops
                WHERE route_id = %s AND branch_code = %s
                ORDER BY id DESC
                LIMIT 1
            """
            find_route_stop_result = execute_query(
                find_route_stop_query, (route_id, branch_code), fetch_one=True
            )
            
            if find_route_stop_result.get("success") and find_route_stop_result.get("data"):
                route_stop_id = find_route_stop_result.get("data").get("id")
                
                # Update b2b_route_stops
                update_route_stop_query = """
                    UPDATE b2b_route_stops
                    SET inbound_weight = %s, status = 'inbound', updated_at = NOW()
                    WHERE id = %s
                """
                update_route_stop_result = execute_query(
                    update_route_stop_query, (inbound_weight, route_stop_id)
                )
                
                if update_route_stop_result.get("success"):
                    # Get updated route_stop data
                    get_route_stop_query = """
                        SELECT id, route_id, sequence, branch_code, status, inbound_weight, updated_at
                        FROM b2b_route_stops
                        WHERE id = %s
                    """
                    get_route_stop_result = execute_query(
                        get_route_stop_query, (route_stop_id,), fetch_one=True
                    )
                    if get_route_stop_result.get("success"):
                        route_stop_data = get_route_stop_result.get("data")
                    print(f"✅ [scan_and_record_inbound_weight] Updated b2b_route_stops: id={route_stop_id}, inbound_weight={inbound_weight}")
                else:
                    print(f"⚠️ [scan_and_record_inbound_weight] Failed to update b2b_route_stops: {update_route_stop_result.get('error')}")
            else:
                print(f"⚠️ [scan_and_record_inbound_weight] No matching b2b_route_stops found for route_id={route_id}, branch_code={branch_code}")
        else:
            print(f"⚠️ [scan_and_record_inbound_weight] No route_id in cycle, skipping b2b_route_stops update")
        
        # Get updated cycle data
        get_updated_cycle_query = """
            SELECT id, cycle_id, barcode_id, branch_code, route_id, pickup_weight,
                   inbound_weight, status, picked_at, inbound_at, sorted_at,
                   completed_at, created_at
            FROM pickup_bag_cycle
            WHERE id = %s
        """
        get_updated_cycle_result = execute_query(
            get_updated_cycle_query, (cycle_db_id,), fetch_one=True
        )
        
        updated_cycle_data = None
        if get_updated_cycle_result.get("success"):
            updated_cycle_data = get_updated_cycle_result.get("data")
        
        return jsonify(
            {
                "status": "success",
                "message": "Inbound weight recorded successfully",
                "data": {
                    "cycle": updated_cycle_data,
                    "route_stop": route_stop_data,
                }
            }
        )
    except Exception as e:
        print(f"❌ [scan_and_record_inbound_weight] Exception: {str(e)}")
        import traceback
        print(f"❌ [scan_and_record_inbound_weight] Traceback: {traceback.format_exc()}")
        return jsonify(
            {"status": "error", "message": f"Error recording inbound weight: {str(e)}"}
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

# Global error handler for unhandled exceptions
@app.errorhandler(Exception)
def handle_exception(e):
    """Global exception handler to log all unhandled errors"""
    import traceback
    error_trace = traceback.format_exc()
    error_info = {
        "error_type": type(e).__name__,
        "error_message": str(e),
        "traceback": error_trace,
        "request_method": request.method,
        "request_path": request.path,
        "request_data": request.get_json() if request.is_json else None,
        "request_args": dict(request.args),
    }
    
    log_request_error("GLOBAL_ERROR_HANDLER", e, error_info)
    logger.error(f"Unhandled exception: {error_info}")
    
    return jsonify({
        "status": "error",
        "message": f"Internal server error: {str(e)}",
        "error_type": type(e).__name__,
        "details": "Check server logs (app.log) and console for full error details"
    }), 500

# Request logging middleware
@app.before_request
def log_request_info():
    """Log all incoming requests for debugging"""
    if request.method in ['POST', 'PUT', 'PATCH']:
        try:
            request_data = request.get_json() if request.is_json else None
            logger.info(f"Request: {request.method} {request.path}")
            if request_data:
                # Log request data but mask sensitive fields
                masked_data = {}
                for key, value in request_data.items():
                    if 'password' in key.lower() or 'token' in key.lower():
                        masked_data[key] = "***MASKED***"
                    else:
                        masked_data[key] = value
                logger.info(f"Request Data: {masked_data}")
                print(f"🔍 [REQUEST] {request.method} {request.path} - Data: {masked_data}")
        except Exception as e:
            logger.warning(f"Failed to log request data: {e}")

# Print routes when module loads
print_registered_routes()

if __name__ == "__main__":
    logger.info("Starting Flask application...")
    logger.info(f"Database Config - Host: {app.config['MYSQL_HOST']}, Port: {app.config['MYSQL_PORT']}, DB: {app.config['MYSQL_DB']}")
    print(f"📝 Logging to file: app.log")
    print(f"📝 Check app.log file for detailed error logs")
    app.run(debug=True, host="0.0.0.0", port=5000)