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

# ❄️ Snowflake Integration

This ETL pipeline ends by loading the cleaned tech company and financial news data into **Snowflake**, a powerful cloud-based data warehouse. This enables fast querying, secure storage, and seamless scaling.

---

## 🚀 Key Features
- 🔐 **Secure connection** using `snowflake.connector` and environment variables
- ⚡ **Fast data ingestion** through Python-based batch loading
- 🧩 **Modular structure** via `LoadData.py` for reusability and clarity
- 🧠 **Optimized for analytics**, BI dashboards, and potential ML pipelines

---

## 🧱 Snowflake Table Structure

| Table Name       | Description                                 |
|------------------|---------------------------------------------|
| `tech_companies` | Basic info like symbol, name, sector        |
| `news_articles`  | Title, summary, source, date, sentiment     |

---

## 💎 Why Use Snowflake?

- ☁️ Fully managed cloud data warehouse  
- 📈 Auto-scaling performance with concurrency  
- 🔍 Native SQL support for fast analytics  
- 🔄 Time Travel & Fail-safe data recovery  
- 🔐 Role-based access control for security

---

## 📊 Sample Snowflake SQL Queries

Here are a few powerful queries to showcase your project’s data value:

```sql
-- 🔍 Most Mentioned Tech Companies
SELECT company_name, COUNT(*) AS mentions
FROM news_articles
GROUP BY company_name
ORDER BY mentions DESC
LIMIT 10;

-- 😊 Average Sentiment Per Company
SELECT company_name, ROUND(AVG(sentiment_score), 2) AS avg_sentiment
FROM news_articles
GROUP BY company_name
ORDER BY avg_sentiment DESC;

-- 📅 Daily News Volume Trend
SELECT DATE(published_at) AS date, COUNT(*) AS total_articles
FROM news_articles
GROUP BY DATE(published_at)
ORDER BY date;


