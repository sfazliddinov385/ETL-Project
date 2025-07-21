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

## 📊 Sample Snowflake SQL Queries

```sql
-- 1. View top 10 trending tech companies by number of news mentions
SELECT name, symbol, COUNT(*) AS total_mentions
FROM tech_companies
JOIN news_articles ON tech_companies.symbol = news_articles.symbol
GROUP BY name, symbol
ORDER BY total_mentions DESC
LIMIT 10;

-- 2. Sentiment distribution across all financial news
SELECT sentiment, COUNT(*) AS sentiment_count
FROM news_articles
GROUP BY sentiment
ORDER BY sentiment_count DESC;

-- 3. Get latest headlines for a specific company (e.g., Apple)
SELECT title, published_at
FROM news_articles
WHERE symbol = 'AAPL'
ORDER BY published_at DESC
LIMIT 5;

-- 4. Average sentiment score for each tech company
SELECT symbol, AVG(sentiment_score) AS avg_sentiment
FROM news_articles
GROUP BY symbol
ORDER BY avg_sentiment DESC;

-- 5. Count of articles published per day (last 7 days)
SELECT TO_DATE(published_at) AS publish_date, COUNT(*) AS article_count
FROM news_articles
WHERE published_at >= DATEADD(DAY, -7, CURRENT_DATE)
GROUP BY publish_date
ORDER BY publish_date;

-- 6. Top authors contributing the most financial news
SELECT author, COUNT(*) AS article_count
FROM news_articles
WHERE author IS NOT NULL AND author <> ''
GROUP BY author
ORDER BY article_count DESC
LIMIT 5;

