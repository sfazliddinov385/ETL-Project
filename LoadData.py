import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
import logging
from datetime import datetime
import json
from typing import Optional, Dict, Any, Tuple
from dotenv import load_dotenv
import hashlib

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('snowflake_load.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SnowflakeDataLoader:
    """Class to handle loading cleaned tech companies data into Snowflake"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the Snowflake loader with configuration
        
        Args:
            config: Dictionary containing Snowflake connection parameters
                   If None, will try to load from environment variables
        """
        self.config = config or self._load_config_from_env()
        self.connection = None
        self.etl_run_id = f"ETL_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.load_metrics = {
            'start_time': None,
            'end_time': None,
            'records_read': 0,
            'records_loaded': 0,
            'records_updated': 0,
            'records_failed': 0
        }
        
    def _load_config_from_env(self) -> Dict[str, Any]:
        """Load Snowflake configuration from environment variables"""
        config = {
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
            'database': os.getenv('SNOWFLAKE_DATABASE', 'TECH_COMPANIES_DB'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC'),
            'role': os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
        }
        
        # Validate required parameters
        required_params = ['user', 'password', 'account']
        missing_params = [param for param in required_params if not config.get(param)]
        
        if missing_params:
            raise ValueError(f"Missing required Snowflake configuration parameters: {missing_params}")
            
        return config
    
    def connect(self) -> bool:
        """Establish connection to Snowflake"""
        try:
            logger.info("Connecting to Snowflake...")
            self.connection = snowflake.connector.connect(
                user=self.config['user'],
                password=self.config['password'],
                account=self.config['account'],
                warehouse=self.config['warehouse'],
                database=self.config['database'],
                schema=self.config['schema'],
                role=self.config['role']
            )
            logger.info("Successfully connected to Snowflake")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            return False
    
    def disconnect(self):
        """Close Snowflake connection"""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from Snowflake")
    
    def create_database_and_schema(self) -> bool:
        """Create database and schema if they don't exist"""
        try:
            cursor = self.connection.cursor()
            
            # Create database
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.config['database']}")
            logger.info(f"Database {self.config['database']} created/verified")
            
            # Use the database
            cursor.execute(f"USE DATABASE {self.config['database']}")
            
            # Create schema
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.config['schema']}")
            logger.info(f"Schema {self.config['schema']} created/verified")
            
            cursor.close()
            return True
            
        except Exception as e:
            logger.error(f"Failed to create database/schema: {str(e)}")
            return False
    
    def create_tables(self) -> bool:
        """Create all necessary tables matching the cleaned data structure"""
        try:
            cursor = self.connection.cursor()
            
            # Main companies table matching your cleaned data columns
            create_main_table_sql = """
            CREATE TABLE IF NOT EXISTS TECH_COMPANIES (
                -- Primary Key
                SYMBOL VARCHAR(50) PRIMARY KEY,
                
                -- Company Information
                COMPANY_NAME VARCHAR(500),
                INDUSTRY VARCHAR(100),
                
                -- Location Information
                COUNTRY VARCHAR(10),
                COUNTRY_NAME VARCHAR(100),
                REGION VARCHAR(50),
                
                -- Exchange Information
                EXCHANGE_CODE VARCHAR(20),
                TICKER VARCHAR(50),
                EXCHANGE_NAME VARCHAR(200),
                
                -- Classification
                TECH_CATEGORY VARCHAR(100),
                
                -- Data Quality Metrics
                HAS_VALID_SYMBOL BOOLEAN,
                HAS_VALID_NAME BOOLEAN,
                HAS_VALID_COUNTRY BOOLEAN,
                HAS_VALID_INDUSTRY BOOLEAN,
                DATA_QUALITY_SCORE NUMBER(3,2),
                IS_COMPLETE_RECORD BOOLEAN,
                
                -- ETL Metadata
                ETL_TIMESTAMP VARCHAR(50),
                ETL_DATE DATE,
                ETL_RUN_ID VARCHAR(50),
                RECORD_HASH VARCHAR(64),
                
                -- Audit Columns
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
            cursor.execute(create_main_table_sql)
            logger.info("Main table TECH_COMPANIES created/verified")
            
            # Staging table for efficient loading
            create_staging_table_sql = """
            CREATE TABLE IF NOT EXISTS TECH_COMPANIES_STAGING (
                LIKE TECH_COMPANIES
            )
            """
            cursor.execute(create_staging_table_sql)
            logger.info("Staging table created/verified")
            
            # ETL audit table
            create_audit_table_sql = """
            CREATE TABLE IF NOT EXISTS ETL_AUDIT (
                AUDIT_ID INTEGER AUTOINCREMENT PRIMARY KEY,
                ETL_RUN_ID VARCHAR(50),
                PROCESS_NAME VARCHAR(100),
                PROCESS_TYPE VARCHAR(50),
                STATUS VARCHAR(20),
                RECORDS_READ INTEGER,
                RECORDS_LOADED INTEGER,
                RECORDS_UPDATED INTEGER,
                RECORDS_FAILED INTEGER,
                ERROR_MESSAGE VARCHAR(4000),
                START_TIME TIMESTAMP_NTZ,
                END_TIME TIMESTAMP_NTZ,
                DURATION_SECONDS NUMBER(10,2),
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
            cursor.execute(create_audit_table_sql)
            logger.info("Audit table created/verified")
            
            # Data quality metrics table
            create_dq_table_sql = """
            CREATE TABLE IF NOT EXISTS DATA_QUALITY_METRICS (
                METRIC_ID INTEGER AUTOINCREMENT PRIMARY KEY,
                ETL_RUN_ID VARCHAR(50),
                METRIC_NAME VARCHAR(100),
                METRIC_VALUE NUMBER(10,4),
                METRIC_DETAILS VARIANT,
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
            cursor.execute(create_dq_table_sql)
            logger.info("Data quality metrics table created/verified")
            
            cursor.close()
            return True
            
        except Exception as e:
            logger.error(f"Failed to create tables: {str(e)}")
            return False
    
    def load_data_from_csv(self, csv_file: str = 'cleaned_tech_companies.csv', use_staging: bool = True) -> bool:
        """
        Load data from cleaned CSV file into Snowflake table
        
        Args:
            csv_file: Path to the cleaned CSV file
            use_staging: Whether to use staging table for loading
        """
        self.load_metrics['start_time'] = datetime.now()
        
        try:
            logger.info(f"Loading data from {csv_file}")
            
            # Read CSV file
            df = pd.read_csv(csv_file)
            self.load_metrics['records_read'] = len(df)
            logger.info(f"Read {len(df)} rows from CSV")
            logger.info(f"Columns in CSV: {list(df.columns)}")
            
            # Add ETL metadata
            df = self._add_etl_metadata(df)
            
            # Prepare data for Snowflake
            df = self._prepare_dataframe(df)
            
            if use_staging:
                # Load to staging first
                if not self._load_to_staging(df):
                    return False
                
                # Merge from staging to target
                return self._merge_staging_to_target()
            else:
                # Direct load (simpler but less efficient for updates)
                return self._direct_load(df, 'TECH_COMPANIES')
            
        except Exception as e:
            logger.error(f"Failed to load data from CSV: {str(e)}")
            self._log_audit('FAILED', str(e))
            return False
        finally:
            self.load_metrics['end_time'] = datetime.now()
    
    def _add_etl_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add ETL metadata columns to dataframe"""
        # Add ETL run ID
        df['ETL_RUN_ID'] = self.etl_run_id
        
        # Generate record hash for change detection
        def generate_hash(row):
            # Create hash from key fields
            hash_input = f"{row.get('Symbol', '')}_{row.get('Company_Name', '')}_{row.get('Country', '')}_{row.get('Industry', '')}"
            return hashlib.sha256(hash_input.encode()).hexdigest()
        
        df['RECORD_HASH'] = df.apply(generate_hash, axis=1)
        
        return df
    
    def _prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare dataframe for loading into Snowflake"""
        # Convert column names to uppercase (Snowflake standard)
        df.columns = [col.upper() for col in df.columns]
        
        # Ensure boolean columns are properly typed
        boolean_columns = ['HAS_VALID_SYMBOL', 'HAS_VALID_NAME', 'HAS_VALID_COUNTRY', 
                          'HAS_VALID_INDUSTRY', 'IS_COMPLETE_RECORD']
        
        for col in boolean_columns:
            if col in df.columns:
                # Convert string 'True'/'False' to boolean if needed
                if df[col].dtype == 'object':
                    df[col] = df[col].map({'TRUE': True, 'FALSE': False, 'True': True, 'False': False, True: True, False: False})
                df[col] = df[col].astype(bool)
        
        # Convert date columns
        if 'ETL_DATE' in df.columns:
            df['ETL_DATE'] = pd.to_datetime(df['ETL_DATE'], errors='coerce').dt.date
        
        # Convert numeric columns
        if 'DATA_QUALITY_SCORE' in df.columns:
            df['DATA_QUALITY_SCORE'] = pd.to_numeric(df['DATA_QUALITY_SCORE'], errors='coerce')
        
        # Handle NaN values appropriately
        df = df.where(pd.notnull(df), None)
        
        # Log data types for verification
        logger.info("Data types after preparation:")
        for col in df.columns:
            logger.info(f"  {col}: {df[col].dtype}")
        
        return df
    
    def _load_to_staging(self, df: pd.DataFrame) -> bool:
        """Load data to staging table using efficient write_pandas"""
        try:
            logger.info("Loading data to staging table...")
            
            # Clear staging table
            cursor = self.connection.cursor()
            cursor.execute("TRUNCATE TABLE TECH_COMPANIES_STAGING")
            cursor.close()
            
            # Use write_pandas for efficient bulk loading
            success, nchunks, nrows, _ = write_pandas(
                self.connection,
                df,
                'TECH_COMPANIES_STAGING',
                auto_create_table=False,
                overwrite=False
            )
            
            if success:
                logger.info(f"Successfully loaded {nrows} rows to staging in {nchunks} chunks")
                return True
            else:
                logger.error("Failed to load data to staging")
                return False
                
        except Exception as e:
            logger.error(f"Error loading to staging: {str(e)}")
            return False
    
    def _merge_staging_to_target(self) -> bool:
        """Merge data from staging to target table with upsert logic"""
        try:
            cursor = self.connection.cursor()
            
            logger.info("Merging data from staging to target...")
            
            # Count records that will be updated
            check_updates_sql = """
            SELECT COUNT(*) 
            FROM TECH_COMPANIES_STAGING s
            JOIN TECH_COMPANIES t ON s.SYMBOL = t.SYMBOL
            WHERE s.RECORD_HASH != t.RECORD_HASH
            """
            cursor.execute(check_updates_sql)
            updates_count = cursor.fetchone()[0]
            logger.info(f"Records to be updated: {updates_count}")
            
            # Perform MERGE operation
            merge_sql = """
            MERGE INTO TECH_COMPANIES t
            USING TECH_COMPANIES_STAGING s
            ON t.SYMBOL = s.SYMBOL
            WHEN MATCHED AND t.RECORD_HASH != s.RECORD_HASH THEN
                UPDATE SET
                    COMPANY_NAME = s.COMPANY_NAME,
                    INDUSTRY = s.INDUSTRY,
                    COUNTRY = s.COUNTRY,
                    COUNTRY_NAME = s.COUNTRY_NAME,
                    REGION = s.REGION,
                    EXCHANGE_CODE = s.EXCHANGE_CODE,
                    TICKER = s.TICKER,
                    EXCHANGE_NAME = s.EXCHANGE_NAME,
                    TECH_CATEGORY = s.TECH_CATEGORY,
                    HAS_VALID_SYMBOL = s.HAS_VALID_SYMBOL,
                    HAS_VALID_NAME = s.HAS_VALID_NAME,
                    HAS_VALID_COUNTRY = s.HAS_VALID_COUNTRY,
                    HAS_VALID_INDUSTRY = s.HAS_VALID_INDUSTRY,
                    DATA_QUALITY_SCORE = s.DATA_QUALITY_SCORE,
                    IS_COMPLETE_RECORD = s.IS_COMPLETE_RECORD,
                    ETL_TIMESTAMP = s.ETL_TIMESTAMP,
                    ETL_DATE = s.ETL_DATE,
                    ETL_RUN_ID = s.ETL_RUN_ID,
                    RECORD_HASH = s.RECORD_HASH,
                    UPDATED_AT = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT (
                    SYMBOL, COMPANY_NAME, INDUSTRY,
                    COUNTRY, COUNTRY_NAME, REGION,
                    EXCHANGE_CODE, TICKER, EXCHANGE_NAME,
                    TECH_CATEGORY, HAS_VALID_SYMBOL, HAS_VALID_NAME,
                    HAS_VALID_COUNTRY, HAS_VALID_INDUSTRY,
                    DATA_QUALITY_SCORE, IS_COMPLETE_RECORD,
                    ETL_TIMESTAMP, ETL_DATE, ETL_RUN_ID, RECORD_HASH
                )
                VALUES (
                    s.SYMBOL, s.COMPANY_NAME, s.INDUSTRY,
                    s.COUNTRY, s.COUNTRY_NAME, s.REGION,
                    s.EXCHANGE_CODE, s.TICKER, s.EXCHANGE_NAME,
                    s.TECH_CATEGORY, s.HAS_VALID_SYMBOL, s.HAS_VALID_NAME,
                    s.HAS_VALID_COUNTRY, s.HAS_VALID_INDUSTRY,
                    s.DATA_QUALITY_SCORE, s.IS_COMPLETE_RECORD,
                    s.ETL_TIMESTAMP, s.ETL_DATE, s.ETL_RUN_ID, s.RECORD_HASH
                )
            """
            
            cursor.execute(merge_sql)
            rows_affected = cursor.rowcount
            
            # Calculate inserts
            inserts_count = rows_affected - updates_count
            
            self.load_metrics['records_loaded'] = inserts_count
            self.load_metrics['records_updated'] = updates_count
            
            logger.info(f"Merge completed. Total rows affected: {rows_affected}")
            logger.info(f"  - New records inserted: {inserts_count}")
            logger.info(f"  - Existing records updated: {updates_count}")
            
            cursor.close()
            
            # Log successful audit
            self._log_audit('SUCCESS')
            
            return True
            
        except Exception as e:
            logger.error(f"Error during merge: {str(e)}")
            self._log_audit('FAILED', str(e))
            return False
    
    def _direct_load(self, df: pd.DataFrame, table_name: str) -> bool:
        """Direct load to table (alternative to staging)"""
        try:
            success, nchunks, nrows, _ = write_pandas(
                self.connection,
                df,
                table_name,
                auto_create_table=False,
                overwrite=False
            )
            
            if success:
                logger.info(f"Successfully loaded {nrows} rows to {table_name}")
                self.load_metrics['records_loaded'] = nrows
                self._log_audit('SUCCESS')
                return True
            else:
                logger.error(f"Failed to load data to {table_name}")
                self._log_audit('FAILED', 'write_pandas failed')
                return False
                
        except Exception as e:
            logger.error(f"Error in direct load: {str(e)}")
            self._log_audit('FAILED', str(e))
            return False
    
    def _log_audit(self, status: str, error_message: str = None):
        """Log ETL audit information"""
        try:
            cursor = self.connection.cursor()
            
            duration = None
            if self.load_metrics['start_time'] and self.load_metrics['end_time']:
                duration = (self.load_metrics['end_time'] - self.load_metrics['start_time']).total_seconds()
            
            audit_sql = """
            INSERT INTO ETL_AUDIT (
                ETL_RUN_ID, PROCESS_NAME, PROCESS_TYPE, STATUS,
                RECORDS_READ, RECORDS_LOADED, RECORDS_UPDATED, RECORDS_FAILED,
                ERROR_MESSAGE, START_TIME, END_TIME, DURATION_SECONDS
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            cursor.execute(audit_sql, (
                self.etl_run_id,
                'TECH_COMPANIES_LOAD',
                'LOAD',
                status,
                self.load_metrics['records_read'],
                self.load_metrics['records_loaded'],
                self.load_metrics['records_updated'],
                self.load_metrics['records_failed'],
                error_message,
                self.load_metrics['start_time'],
                self.load_metrics['end_time'],
                duration
            ))
            
            self.connection.commit()
            cursor.close()
            
            logger.info(f"Audit logged: {status}")
            
        except Exception as e:
            logger.error(f"Failed to log audit: {str(e)}")
    
    def log_data_quality_metrics(self, df: pd.DataFrame = None):
        """Log data quality metrics"""
        try:
            cursor = self.connection.cursor()
            
            # If no dataframe provided, query from the table
            if df is None:
                cursor.execute("SELECT * FROM TECH_COMPANIES")
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                df = pd.DataFrame(data, columns=columns)
            
            # Calculate various metrics
            metrics = {
                'total_records': len(df),
                'unique_symbols': df['SYMBOL'].nunique() if 'SYMBOL' in df.columns else 0,
                'avg_data_quality_score': float(df['DATA_QUALITY_SCORE'].mean()) if 'DATA_QUALITY_SCORE' in df.columns else 0,
                'complete_records_pct': float((df['IS_COMPLETE_RECORD'].sum() / len(df) * 100)) if 'IS_COMPLETE_RECORD' in df.columns and len(df) > 0 else 0,
                'unique_countries': df['COUNTRY_NAME'].nunique() if 'COUNTRY_NAME' in df.columns else 0,
                'unique_tech_categories': df['TECH_CATEGORY'].nunique() if 'TECH_CATEGORY' in df.columns else 0,
                'unique_regions': df['REGION'].nunique() if 'REGION' in df.columns else 0,
                'unique_exchanges': df['EXCHANGE_CODE'].nunique() if 'EXCHANGE_CODE' in df.columns else 0
            }
            
            # Insert metrics
            for metric_name, metric_value in metrics.items():
                dq_sql = """
                INSERT INTO DATA_QUALITY_METRICS (
                    ETL_RUN_ID, METRIC_NAME, METRIC_VALUE, METRIC_DETAILS
                ) VALUES (?, ?, ?, PARSE_JSON(?))
                """
                
                details = json.dumps({
                    'description': metric_name.replace('_', ' ').title(),
                    'timestamp': datetime.now().isoformat()
                })
                
                cursor.execute(dq_sql, (
                    self.etl_run_id,
                    metric_name,
                    float(metric_value),
                    details
                ))
            
            self.connection.commit()
            cursor.close()
            
            logger.info(f"Logged {len(metrics)} data quality metrics")
            
        except Exception as e:
            logger.error(f"Failed to log data quality metrics: {str(e)}")
    
    def verify_data_load(self, table_name: str = 'TECH_COMPANIES') -> Tuple[bool, Dict[str, Any]]:
        """Verify that data was loaded correctly and return statistics"""
        try:
            cursor = self.connection.cursor()
            
            stats = {}
            
            # Count total rows
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            stats['total_rows'] = cursor.fetchone()[0]
            
            # Count by region
            cursor.execute(f"""
                SELECT REGION, COUNT(*) as cnt 
                FROM {table_name} 
                WHERE REGION IS NOT NULL
                GROUP BY REGION 
                ORDER BY cnt DESC
            """)
            stats['by_region'] = cursor.fetchall()
            
            # Count by tech category
            cursor.execute(f"""
                SELECT TECH_CATEGORY, COUNT(*) as cnt 
                FROM {table_name} 
                WHERE TECH_CATEGORY IS NOT NULL
                GROUP BY TECH_CATEGORY 
                ORDER BY cnt DESC 
                LIMIT 10
            """)
            stats['top_categories'] = cursor.fetchall()
            
            # Count by country
            cursor.execute(f"""
                SELECT COUNTRY_NAME, COUNT(*) as cnt 
                FROM {table_name} 
                WHERE COUNTRY_NAME IS NOT NULL
                GROUP BY COUNTRY_NAME 
                ORDER BY cnt DESC 
                LIMIT 10
            """)
            stats['top_countries'] = cursor.fetchall()
            
            # Average data quality
            cursor.execute(f"SELECT AVG(DATA_QUALITY_SCORE) FROM {table_name}")
            result = cursor.fetchone()
            stats['avg_data_quality'] = float(result[0]) if result[0] else 0
            
            # Complete records percentage
            cursor.execute(f"""
                SELECT 
                    COUNT(CASE WHEN IS_COMPLETE_RECORD = TRUE THEN 1 END) as complete,
                    COUNT(*) as total
                FROM {table_name}
            """)
            result = cursor.fetchone()
            stats['complete_records_pct'] = (result[0] / result[1] * 100) if result[1] > 0 else 0
            
            cursor.close()
            
            # Log statistics
            logger.info("Verification Summary:")
            logger.info(f"  Total rows: {stats['total_rows']:,}")
            logger.info(f"  Average data quality: {stats['avg_data_quality']:.2f}")
            logger.info(f"  Complete records: {stats['complete_records_pct']:.1f}%")
            logger.info(f"  Regions: {len(stats['by_region'])}")
            logger.info(f"  Top countries: {[country[0] for country in stats['top_countries'][:5]]}")
            logger.info(f"  Top categories: {[cat[0] for cat in stats['top_categories'][:5]]}")
            
            return True, stats
            
        except Exception as e:
            logger.error(f"Failed to verify data load: {str(e)}")
            return False, {}
    
    def generate_load_report(self) -> str:
        """Generate a comprehensive load report"""
        cursor = self.connection.cursor()
        
        try:
            # Get summary statistics
            cursor.execute("SELECT COUNT(*) FROM TECH_COMPANIES")
            total_records = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT COUNTRY_NAME) FROM TECH_COMPANIES WHERE COUNTRY_NAME IS NOT NULL")
            total_countries = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT TECH_CATEGORY) FROM TECH_COMPANIES WHERE TECH_CATEGORY IS NOT NULL")
            total_categories = cursor.fetchone()[0]
            
            cursor.execute("SELECT AVG(DATA_QUALITY_SCORE) FROM TECH_COMPANIES")
            result = cursor.fetchone()
            avg_quality = float(result[0]) if result[0] else 0
            
            # Get top categories
            cursor.execute("""
                SELECT TECH_CATEGORY, COUNT(*) as cnt 
                FROM TECH_COMPANIES 
                WHERE TECH_CATEGORY IS NOT NULL
                GROUP BY TECH_CATEGORY 
                ORDER BY cnt DESC 
                LIMIT 5
            """)
            top_categories = cursor.fetchall()
            
            # Get latest ETL run info
            cursor.execute(f"""
                SELECT STATUS, RECORDS_READ, RECORDS_LOADED, RECORDS_UPDATED, DURATION_SECONDS
                FROM ETL_AUDIT
                WHERE ETL_RUN_ID = '{self.etl_run_id}'
                ORDER BY CREATED_AT DESC
                LIMIT 1
            """)
            audit_info = cursor.fetchone()
            
            report = f"""
╔════════════════════════════════════════════════════╗
║         SNOWFLAKE DATA LOAD REPORT                 ║
╚════════════════════════════════════════════════════╝

📊 ETL Run Information
─────────────────────────────────────────────────────
  • ETL Run ID: {self.etl_run_id}
  • Status: {audit_info[0] if audit_info else 'N/A'}
  • Duration: {audit_info[4] if audit_info else 'N/A'} seconds

📈 Load Statistics
─────────────────────────────────────────────────────
  • Records Read: {audit_info[1] if audit_info else 'N/A'}
  • New Records Loaded: {audit_info[2] if audit_info else 'N/A'}
  • Existing Records Updated: {audit_info[3] if audit_info else 'N/A'}

🏢 Database Summary
─────────────────────────────────────────────────────
  • Total Companies: {total_records:,}
  • Countries: {total_countries}
  • Tech Categories: {total_categories}
  • Average Data Quality: {avg_quality:.2f}/1.0 ({avg_quality*100:.1f}%)

📂 Top Tech Categories
─────────────────────────────────────────────────────"""
            
            for category, count in top_categories:
                report += f"\n  • {category}: {count:,} companies"
            
            report += f"""

🔗 Connection Details
─────────────────────────────────────────────────────
  • Account: {self.config['account']}
  • Database: {self.config['database']}
  • Schema: {self.config['schema']}
  • Warehouse: {self.config['warehouse']}

✅ Load completed successfully!
"""
            
            cursor.close()
            return report
            
        except Exception as e:
            logger.error(f"Error generating report: {str(e)}")
            return f"Error generating report: {str(e)}"

def main():
    """Main function to orchestrate the data loading process"""
    try:
        # Initialize loader
        loader = SnowflakeDataLoader()
        
        # Connect to Snowflake
        if not loader.connect():
            logger.error("Failed to connect to Snowflake. Exiting.")
            return False
        
        # Create database and schema
        if not loader.create_database_and_schema():
            logger.error("Failed to create database/schema. Exiting.")
            return False
        
        # Create tables
        if not loader.create_tables():
            logger.error("Failed to create tables. Exiting.")
            return False
        
        # Load data
        csv_file = 'cleaned_tech_companies.csv'
        if not os.path.exists(csv_file):
            logger.error(f"CSV file {csv_file} not found. Please run DataCleaning.py first.")
            return False
        
        # Load with staging (recommended for production)
        if not loader.load_data_from_csv(csv_file, use_staging=True):
            logger.error("Failed to load data. Exiting.")
            return False
        
        # Log data quality metrics
        loader.log_data_quality_metrics()
        
        # Verify data load
        success, stats = loader.verify_data_load()
        if not success:
            logger.error("Failed to verify data load.")
            return False
        
        # Generate and print report
        report = loader.generate_load_report()
        print(report)
        
        logger.info("Data loading process completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Unexpected error in main process: {str(e)}")
        return False
    
    finally:
        if 'loader' in locals():
            loader.disconnect()

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)