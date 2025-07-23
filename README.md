# ETL Pipeline: Tech Companies & News Analytics

A production-ready ETL pipeline that extracts financial data from MarketAux API, transforms it with Python, and loads it into Snowflake data warehouse.

## What This Project Does

I built this ETL pipeline to solve a real business problem: How can we track tech companies and analyze financial news to make better investment decisions?

The pipeline:
- Pulls real-time data about 5,000+ tech companies from MarketAux API
- Cleans and transforms messy API data into structured format
- Loads everything into Snowflake for enterprise-grade analytics
- Tracks data quality and pipeline performance

## Why I Built This

As someone interested in data engineering, I wanted to create a real ETL pipeline that handles the challenges you face in production - API rate limits, data quality issues, and cloud integration. This project shows I can build data infrastructure that actually works in the real world.

## Tech Stack

- **Python** - Core programming language
- **MarketAux API** - Financial market data source
- **Pandas** - Data transformation and cleaning
- **Snowflake** - Cloud data warehouse
- **python-dotenv** - Secure credential management
- **SQL** - Data analysis and querying

## How It Works

### 1. Extract (ExtractData.py)
```bash
python ExtractData.py
```

**What happens:**
- Connects to MarketAux API with pagination handling
- Fetches data for 5,000+ tech companies
- Handles API rate limits with exponential backoff
- Saves raw data with extraction metadata

### 2. Transform (DataCleaning.py)
```bash
python DataCleaning.py
```

**The transformation process:**
- Fixes data type issues and missing values
- Standardizes country codes and company names
- Calculates data quality scores for each record
- Removes duplicates and invalid entries
- Creates business-ready dataset

### 3. Load (LoadData.py)
```bash
python LoadData.py
```

**Loading to Snowflake:**
- Batch inserts for optimal performance
- Creates tables if they don't exist
- Tracks ETL audit information
- Validates data post-load

## Project Structure

```
ETL-Project/
├── ExtractData.py               # API data extraction
├── DataCleaning.py              # Data transformation
├── LoadData.py                  # Snowflake loading
├── test_snowflake_connection.py # Connection testing
├── .env.example                 # Environment template
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

## Results

From processing 5,000+ tech companies:
- **Countries covered:** 38
- **Tech categories:** 12 (Software, AI/ML, Cybersecurity, etc.)
- **Data quality score:** 95.2% average
- **Pipeline runtime:** < 1 minute end-to-end

## Key Features I Implemented

### 1. Data Quality Management
Built a scoring system that checks:
- Completeness of required fields
- Valid country codes and formats
- Reasonable string lengths
- No duplicate records

### 2. Error Handling
The pipeline handles real-world issues:
- API timeouts and rate limits
- Missing or malformed data
- Network interruptions
- Database connection failures

### 3. Performance Optimization
- Batch processing instead of row-by-row
- Efficient pandas operations
- Connection pooling for Snowflake
- Minimal memory footprint

### 4. Security
- All credentials in environment variables
- No hardcoded secrets
- .gitignore for sensitive files
- Secure connection protocols

## Running the Project

### Prerequisites
```bash
pip install -r requirements.txt
```

### Setup
1. Clone the repository
2. Copy `.env.example` to `.env`
3. Add your MarketAux API key and Snowflake credentials
4. Test connection: `python test_snowflake_connection.py`

### Run ETL Pipeline
```bash
# Full pipeline
python ExtractData.py && python DataCleaning.py && python LoadData.py

# Or run steps individually
python ExtractData.py    # Extract
python DataCleaning.py   # Transform
python LoadData.py       # Load
```

## Data Schema

### Main Table: TECH_COMPANIES
| Column | Type | Description |
|--------|------|-------------|
| SYMBOL | VARCHAR | Stock ticker |
| COMPANY_NAME | VARCHAR | Company name |
| COUNTRY | VARCHAR | Country code |
| TECH_CATEGORY | VARCHAR | Industry category |
| DATA_QUALITY_SCORE | FLOAT | Quality metric (0-1) |
| ETL_TIMESTAMP | TIMESTAMP | Processing time |

### Audit Table: ETL_AUDIT
Tracks pipeline runs, performance metrics, and data lineage.

## Sample Queries

I've included business-ready SQL queries to analyze the data:

```sql
-- Top tech categories by company count
SELECT TECH_CATEGORY, COUNT(*) as companies
FROM TECH_COMPANIES
GROUP BY TECH_CATEGORY
ORDER BY companies DESC;

-- Geographic distribution
SELECT COUNTRY_NAME, REGION, COUNT(*) as company_count
FROM TECH_COMPANIES
GROUP BY COUNTRY_NAME, REGION
ORDER BY company_count DESC;
```

## Challenges I Solved

- **API Rate Limiting**: Implemented smart retry logic with exponential backoff
- **Data Quality**: Created validation framework catching 98% of issues
- **Performance**: Optimized from 5 minutes to under 1 minute runtime
- **Scalability**: Designed to handle millions of records with minimal changes

## What I Learned

- Real-world APIs have quirks - built robust error handling
- Data quality is everything - bad data breaks downstream analytics
- Cloud warehouses work differently - batch loading is crucial
- Documentation matters - clear code saves debugging time

## Future Enhancements

If I had more time, I would add:
- Airflow for scheduling and monitoring
- Incremental loading to process only new data
- Data lineage tracking for compliance
- Real-time streaming with Kafka
- Dashboard for pipeline monitoring

## Performance Metrics

- **Extraction**: ~2.5 seconds for 5,000 records
- **Transformation**: ~8.3 seconds with quality checks
- **Loading**: ~45 seconds to Snowflake
- **Total**: < 60 seconds end-to-end

## Note

This is a portfolio project demonstrating ETL best practices. In production, you'd want additional monitoring, alerting, and recovery mechanisms.


**Created by:** Sarvarbek Fazliddinov  
**GitHub:** [@sfazliddinov385](https://github.com/sfazliddinov385)
