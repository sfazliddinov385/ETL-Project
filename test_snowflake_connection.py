import snowflake.connector
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_connection():
    print("🔄 Testing Snowflake connection...")
    print(f"Account: {os.getenv('SNOWFLAKE_ACCOUNT')}")
    print(f"User: {os.getenv('SNOWFLAKE_USER')}")
    print(f"Warehouse: {os.getenv('SNOWFLAKE_WAREHOUSE')}")
    
    try:
        # First try basic connection without database/schema
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
        )

        cursor = conn.cursor()
        
        # Test basic query
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
        result = cursor.fetchone()

        print("\n✅ Connection Successful!")
        print(f"User: {result[0]}")
        print(f"Role: {result[1]}")
        print(f"Warehouse: {result[2]}")
        
        # Test database creation
        try:
            db_name = os.getenv("SNOWFLAKE_DATABASE", "TECH_COMPANIES_DB")
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            cursor.execute(f"USE DATABASE {db_name}")
            print(f"✅ Database {db_name} ready")
            
            # Test schema creation
            schema_name = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            cursor.execute(f"USE SCHEMA {schema_name}")
            print(f"✅ Schema {schema_name} ready")
            
        except Exception as db_error:
            print(f"⚠️ Database/Schema issue: {db_error}")

        cursor.close()
        conn.close()
        print("\n🎉 All tests passed! You can now run LoadData.py")
        return True

    except Exception as e:
        print("\n❌ Connection Failed")
        print(f"Error: {str(e)}")
        
        # Common troubleshooting
        if "404 Not Found" in str(e):
            print("\n💡 Troubleshooting:")
            print("- Check your account identifier")
            print("- Try using the full account identifier from your URL")
        elif "Incorrect username or password" in str(e):
            print("\n💡 Check your username and password in .env file")
        elif "SSL" in str(e):
            print("\n💡 Network/SSL issue - check firewall settings")
            
        return False

if __name__ == "__main__":
    success = test_connection()
    exit(0 if success else 1)
