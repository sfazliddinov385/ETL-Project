# 🚀 ETL Pipeline: Tech Companies & News Analytics

**Author:** Sarvarbek Fazliddinov  
**GitHub:** [sfazliddinov385](https://github.com/sfazliddinov385)  
**Project Type:** ETL + API Integration + Data Engineering  
**Tech Stack:** Python, MarketAux API, pandas, Snowflake, dotenv

---

## 📌 Project Summary

This project demonstrates an end-to-end **ETL pipeline** that extracts real-time data about tech companies and financial news using the [MarketAux API](https://www.marketaux.com/), transforms the data using Python and `pandas`, and loads the results into a **Snowflake** cloud data warehouse. The pipeline is designed to be modular, scalable, and business-ready.

---

## 🎯 Business Use Case

> **"How can we identify trending tech companies and analyze financial news sentiment for better investment or strategy planning?"**

This pipeline answers that by:

- ✅ Extracting live company & news data via MarketAux  
- 🔧 Transforming and cleaning the data in Python  
- ☁️ Loading the final structured data into **Snowflake** for enterprise-grade querying  

---

## ⚙️ Tech Stack

- **Python 3.10+**
- **MarketAux API** – real-time market & news data
- **pandas** – data transformation and analysis
- **Snowflake** – cloud data warehouse
- **dotenv** – securely manage API keys and credentials

---

## 🗂️ Project Structure

```
ETL-Project/
├── ExtractData.py               # Extracts tech company & news data from MarketAux API
├── DataCleaning.py              # Cleans and transforms raw API data using pandas
├── LoadData.py                  # Loads the transformed data into Snowflake warehouse
│
├── test_snowflake_connection.py # Script to test Snowflake connection securely
├── .env.example                 # Template for required environment variables (no secrets)
├── .gitignore                   # Excludes sensitive files like .env and CSVs from GitHub
├── requirements.txt             # Python dependencies needed to run the pipeline
├── README.md                    # Project documentation (this file)
└── cleaned_tech_companies.csv   # Example of cleaned output (excluded in actual GitHub)
```




## 🚀 How to Run the Project

1. **Clone the Repository**
   ```bash
   git clone https://github.com/sfazliddinov385/ETL-Project.git
   cd ETL-Project


## 🔐 Environment Variables

Create a `.env` file in the root directory using the structure below.  
This file is **excluded** from GitHub using `.gitignore`.

```env
# MarketAux API Credentials
API_KEY=your_marketaux_api_key
BASE_URL=https://api.marketaux.com/v1

# Snowflake Credentials
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_WAREHOUSE=your_warehouse

```
## 📊Snowflake SQL Queries

These queries go beyond technical validation — they support real decision-making for stakeholders, including analysts, investors, and business strategists.

```sql
-- 🚀 Identify Fastest Growing Tech Topics
-- Measures which companies are gaining momentum over the past 7 days
SELECT company_name, COUNT(*) AS mentions_last_week
FROM news_articles
WHERE published_at >= DATEADD(day, -7, CURRENT_DATE())
GROUP BY company_name
ORDER BY mentions_last_week DESC
LIMIT 10;

-- 📉 Detect Companies with Declining Sentiment
-- Helps stakeholders identify at-risk companies based on sentiment drop
SELECT company_name,
       ROUND(AVG(CASE WHEN published_at >= DATEADD(day, -7, CURRENT_DATE()) THEN sentiment_score END), 2) AS sentiment_week,
       ROUND(AVG(CASE WHEN published_at < DATEADD(day, -7, CURRENT_DATE()) THEN sentiment_score END), 2) AS sentiment_prev
FROM news_articles
GROUP BY company_name
HAVING sentiment_week < sentiment_prev
ORDER BY sentiment_week ASC;

-- 🏆 Top 5 Countries with Most Positive Sentiment
-- Reveals which regions have favorable media attention
SELECT country, ROUND(AVG(sentiment_score), 2) AS avg_sentiment
FROM tech_companies AS tc
JOIN news_articles AS na
ON tc.company_name = na.company_name
GROUP BY country
ORDER BY avg_sentiment DESC
LIMIT 5;

-- 🔥 Most Discussed Sectors
-- Lets executives know which industry sectors are most newsworthy
SELECT sector, COUNT(*) AS total_mentions
FROM tech_companies AS tc
JOIN news_articles AS na
ON tc.company_name = na.company_name
GROUP BY sector
ORDER BY total_mentions DESC;

-- ⏳ Sentiment Trend Over Time (Last 14 Days)
-- Useful for trend analysis and building time-series visualizations
SELECT DATE(published_at) AS date,
       ROUND(AVG(sentiment_score), 2) AS avg_sentiment
FROM news_articles
WHERE published_at >= DATEADD(day, -14, CURRENT_DATE())
GROUP BY DATE(published_at)
ORDER BY date;



