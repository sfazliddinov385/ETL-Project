# 🚀 ETL Pipeline: Tech Companies & News Analytics

**Author:** Sarvarbek Fazliddinov  
**GitHub:** [sfazliddinov385](https://github.com/sfazliddinov385)  
**Project Type:** ETL + API Integration + Data Engineering  
**Tech Stack:** Python, MarketAux API, pandas, Snowflake, dotenv

---

## 📌 Project Summary

This project demonstrates an end-to-end **ETL pipeline** that extracts real-time data about tech companies and financial news using the [MarketAux API](https://www.marketaux.com/), transforms the data using Python and `pandas`, and loads the results into a **Snowflake cloud data warehouse**. The pipeline is designed to be modular, scalable, and business-ready.

---

## 🎯 Business Use Case

> **"How can we identify trending tech companies and analyze financial news sentiment for better investment or strategy planning?"**

This pipeline answers that by:
- Extracting live company & news data via MarketAux
- Transforming and cleaning the data in Python
- Loading the final structured data into Snowflake for enterprise-grade storage and querying

---

## ⚙️ Tech Stack

- **Python 3.10+**
- **MarketAux API** – real-time market & news data
- **pandas** – transformation and cleansing
- **Snowflake** – cloud data warehouse
- **dotenv** – manage API keys and credentials securely
- *(Optional)* Apache Airflow – for future automation

---

## 🗂️ Project Structure

ETL-Project/
│
├── ExtractData.py # Extract companies & news from MarketAux API
├── DataCleaning.py # Clean and prepare the extracted data
├── LoadData.py # Load transformed data into Snowflake
│
├── test_snowflake_connection.py # Verify Snowflake connectivity
├── .env.example # Sample environment variable file (no secrets)
├── .gitignore # Ensure secrets & logs are not committed
├── requirements.txt # All Python dependencies
├── README.md # This documentation
└── cleaned_tech_companies.csv # (Ignored in GitHub) Sample output file


---

## 🔐 Environment Variables

Use a `.env` file in your root directory to store sensitive variables. This file is ignored by GitHub via `.gitignore`.

```env
API_KEY=your_marketaux_api_key
BASE_URL=https://api.marketaux.com/v1

SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_WAREHOUSE=your_warehouse
