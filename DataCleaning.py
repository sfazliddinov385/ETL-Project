import pandas as pd
import numpy as np
from datetime import datetime
import logging
import hashlib
import re

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class DataCleaner:
    """Class to handle all data cleaning and transformation operations"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.raw_data = None
        self.cleaned_data = None
        
    def load_raw_data(self, file_path='marketaux_tech_companies_list.csv'):
        """Load raw data from CSV file"""
        try:
            self.logger.info(f"Loading raw data from {file_path}")
            self.raw_data = pd.read_csv(file_path)
            self.logger.info(f"Loaded {len(self.raw_data)} records")
            return self.raw_data
        except Exception as e:
            self.logger.error(f"Error loading data: {str(e)}")
            raise
    
    def clean_company_names(self, df):
        """Clean and standardize company names"""
        self.logger.info("Cleaning company names...")
        
        def clean_name(name):
            if pd.isna(name):
                return ''
            
            # Remove extra quotes
            name = str(name).strip('"').strip("'")
            
            # Remove extra whitespace
            name = ' '.join(name.split())
            
            # Standardize common abbreviations
            replacements = {
                'Corp.': 'Corporation',
                'Inc.': 'Incorporated',
                'Co.': 'Company',
                'Ltd.': 'Limited',
                'Plc': 'PLC',
                '&': 'and'
            }
            
            for old, new in replacements.items():
                name = name.replace(old, new)
            
            return name.strip()
        
        df['company_name_clean'] = df['Company Name'].apply(clean_name)
        return df
    
    def extract_exchange_info(self, df):
        """Extract exchange and ticker information from symbol"""
        self.logger.info("Extracting exchange information...")
        
        def extract_exchange(symbol):
            if pd.isna(symbol) or '.' not in str(symbol):
                return 'UNKNOWN'
            parts = str(symbol).split('.')
            if len(parts) >= 2:
                return parts[-1]
            return 'UNKNOWN'
        
        def extract_ticker(symbol):
            if pd.isna(symbol):
                return ''
            if '.' in str(symbol):
                return str(symbol).split('.')[0]
            return str(symbol)
        
        df['exchange_code'] = df['Symbol'].apply(extract_exchange)
        df['ticker'] = df['Symbol'].apply(extract_ticker)
        
        # Map exchange codes to full names
        exchange_mapping = {
            'SZ': 'Shenzhen Stock Exchange',
            'SS': 'Shanghai Stock Exchange',
            'KS': 'Korea Stock Exchange',
            'T': 'Tokyo Stock Exchange',
            'HK': 'Hong Kong Stock Exchange',
            'L': 'London Stock Exchange',
            'PA': 'Paris Stock Exchange',
            'DE': 'Frankfurt Stock Exchange',
            'MC': 'Madrid Stock Exchange',
            'MI': 'Milan Stock Exchange',
            'AS': 'Amsterdam Stock Exchange',
            'BR': 'Brussels Stock Exchange',
            'CO': 'Copenhagen Stock Exchange',
            'HE': 'Helsinki Stock Exchange',
            'ST': 'Stockholm Stock Exchange',
            'OL': 'Oslo Stock Exchange',
            'SW': 'Swiss Exchange',
            'VI': 'Vienna Stock Exchange',
            'PR': 'Prague Stock Exchange',
            'WA': 'Warsaw Stock Exchange',
            'AT': 'Athens Stock Exchange',
            'IS': 'Istanbul Stock Exchange',
            'TA': 'Tel Aviv Stock Exchange',
            'JK': 'Jakarta Stock Exchange',
            'BK': 'Bangkok Stock Exchange',
            'KL': 'Kuala Lumpur Stock Exchange',
            'SI': 'Singapore Stock Exchange',
            'AX': 'Australian Stock Exchange',
            'NZ': 'New Zealand Stock Exchange',
            'TO': 'Toronto Stock Exchange',
            'V': 'Vancouver Stock Exchange',
            'MX': 'Mexico Stock Exchange',
            'SA': 'Sao Paulo Stock Exchange',
            'BA': 'Buenos Aires Stock Exchange'
        }
        
        df['exchange_name'] = df['exchange_code'].map(exchange_mapping).fillna('Other Exchange')
        return df
    
    def standardize_countries(self, df):
        """Standardize country codes and add country names"""
        self.logger.info("Standardizing country information...")
        
        # Fix common country code issues
        country_fixes = {
            'CZ': 'CN',  # Czech Republic -> China (based on context of Chinese companies)
            'UK': 'GB',  # United Kingdom standard code
            'USA': 'US'
        }
        
        df['country_clean'] = df['Country'].apply(
            lambda x: country_fixes.get(str(x).upper().strip(), str(x).upper().strip()) 
            if pd.notna(x) else 'UNKNOWN'
        )
        
        # Map country codes to full names
        country_mapping = {
            'CN': 'China',
            'US': 'United States',
            'KR': 'South Korea',
            'JP': 'Japan',
            'TW': 'Taiwan',
            'HK': 'Hong Kong',
            'SG': 'Singapore',
            'IN': 'India',
            'AU': 'Australia',
            'NZ': 'New Zealand',
            'DE': 'Germany',
            'GB': 'United Kingdom',
            'FR': 'France',
            'IT': 'Italy',
            'ES': 'Spain',
            'NL': 'Netherlands',
            'BE': 'Belgium',
            'CH': 'Switzerland',
            'SE': 'Sweden',
            'NO': 'Norway',
            'DK': 'Denmark',
            'FI': 'Finland',
            'AT': 'Austria',
            'PL': 'Poland',
            'CZ': 'Czech Republic',
            'GR': 'Greece',
            'TR': 'Turkey',
            'IL': 'Israel',
            'CA': 'Canada',
            'MX': 'Mexico',
            'BR': 'Brazil',
            'AR': 'Argentina',
            'ID': 'Indonesia',
            'TH': 'Thailand',
            'MY': 'Malaysia',
            'PH': 'Philippines',
            'VN': 'Vietnam'
        }
        
        df['country_name'] = df['country_clean'].map(country_mapping).fillna('Other')
        
        # Add regions
        region_mapping = {
            'CN': 'Asia-Pacific', 'KR': 'Asia-Pacific', 'JP': 'Asia-Pacific',
            'TW': 'Asia-Pacific', 'HK': 'Asia-Pacific', 'SG': 'Asia-Pacific',
            'IN': 'Asia-Pacific', 'AU': 'Asia-Pacific', 'NZ': 'Asia-Pacific',
            'ID': 'Asia-Pacific', 'TH': 'Asia-Pacific', 'MY': 'Asia-Pacific',
            'PH': 'Asia-Pacific', 'VN': 'Asia-Pacific',
            'US': 'North America', 'CA': 'North America',
            'MX': 'Latin America', 'BR': 'Latin America', 'AR': 'Latin America',
            'DE': 'Europe', 'GB': 'Europe', 'FR': 'Europe', 'IT': 'Europe',
            'ES': 'Europe', 'NL': 'Europe', 'BE': 'Europe', 'CH': 'Europe',
            'SE': 'Europe', 'NO': 'Europe', 'DK': 'Europe', 'FI': 'Europe',
            'AT': 'Europe', 'PL': 'Europe', 'CZ': 'Europe', 'GR': 'Europe',
            'TR': 'Middle East', 'IL': 'Middle East'
        }
        
        df['region'] = df['country_clean'].map(region_mapping).fillna('Other')
        return df
    
    def categorize_companies(self, df):
        """Categorize tech companies based on name patterns"""
        self.logger.info("Categorizing companies by tech sector...")
        
        def categorize(name):
            if pd.isna(name):
                return 'Other Technology'
            
            name_lower = str(name).lower()
            
            # Define category patterns
            categories = {
                'Software': [
                    'software', 'systems', 'solutions', 'application', 'platform',
                    'cloud', 'saas', 'digital', 'cyber', 'information technology'
                ],
                'Hardware': [
                    'semiconductor', 'electronics', 'electric', 'components', 
                    'devices', 'hardware', 'manufacturing', 'equipment'
                ],
                'Telecommunications': [
                    'telecom', 'communications', 'wireless', 'mobile', 'network',
                    'broadband', '5g', 'fiber'
                ],
                'Internet Services': [
                    'internet', 'online', 'web', 'portal', 'e-commerce',
                    'digital media', 'social'
                ],
                'AI & Data': [
                    'artificial intelligence', 'ai', 'machine learning', 'data',
                    'analytics', 'big data', 'intelligence', 'algorithm'
                ],
                'Gaming & Entertainment': [
                    'game', 'gaming', 'entertainment', 'interactive', 'media',
                    'animation', 'virtual'
                ],
                'Fintech': [
                    'fintech', 'payment', 'financial technology', 'blockchain',
                    'crypto', 'digital payment', 'banking technology'
                ],
                'Biotech & HealthTech': [
                    'biotech', 'bioinformatics', 'medical technology', 'health tech',
                    'healthcare technology', 'pharma tech', 'life sciences'
                ],
                'Industrial Tech': [
                    'industrial', 'automation', 'robotics', 'iot', 'smart',
                    'control systems', 'sensors'
                ],
                'CleanTech': [
                    'renewable', 'solar', 'battery', 'energy storage', 'clean tech',
                    'green technology', 'sustainable'
                ]
            }
            
            # Check each category
            for category, keywords in categories.items():
                if any(keyword in name_lower for keyword in keywords):
                    return category
            
            # Check for generic tech indicators
            if any(word in name_lower for word in ['technology', 'tech', 'it ']):
                return 'General Technology'
            
            return 'Other Technology'
        
        df['tech_category'] = df['company_name_clean'].apply(categorize)
        return df
    
    def add_data_quality_metrics(self, df):
        """Add data quality flags and scores"""
        self.logger.info("Adding data quality metrics...")
        
        # Check for valid data
        df['has_valid_symbol'] = df['Symbol'].notna() & (df['Symbol'] != '')
        df['has_valid_name'] = df['Company Name'].notna() & (df['Company Name'] != '')
        df['has_valid_country'] = df['country_clean'].notna() & (df['country_clean'] != 'UNKNOWN')
        df['has_valid_industry'] = df['Industry'].notna() & (df['Industry'] != '')
        
        # Calculate data quality score (0-1)
        df['data_quality_score'] = (
            df['has_valid_symbol'].astype(int) * 0.3 +
            df['has_valid_name'].astype(int) * 0.3 +
            df['has_valid_country'].astype(int) * 0.2 +
            df['has_valid_industry'].astype(int) * 0.2
        )
        
        # Add data completeness flag
        df['is_complete_record'] = df['data_quality_score'] == 1.0
        
        return df
    
    def add_metadata(self, df):
        """Add ETL metadata"""
        self.logger.info("Adding metadata...")
        
        # Add timestamps
        df['etl_timestamp'] = datetime.now()
        df['etl_date'] = datetime.now().date()
        df['source_system'] = 'MARKETAUX_API'
        df['source_file'] = 'marketaux_tech_companies_list.csv'
        
        # Generate record hash for change detection
        def generate_hash(row):
            hash_input = f"{row['Symbol']}_{row['Company Name']}_{row['Country']}_{row['Industry']}"
            return hashlib.md5(hash_input.encode()).hexdigest()
        
        df['record_hash'] = df.apply(generate_hash, axis=1)
        
        return df
    
    def remove_duplicates(self, df):
        """Remove duplicate records"""
        self.logger.info("Removing duplicates...")
        
        # Remove duplicates based on Symbol
        initial_count = len(df)
        df = df.drop_duplicates(subset=['Symbol'], keep='first')
        removed_count = initial_count - len(df)
        
        if removed_count > 0:
            self.logger.warning(f"Removed {removed_count} duplicate records")
        
        return df
    
    def handle_missing_values(self, df):
        """Handle missing values appropriately"""
        self.logger.info("Handling missing values...")
        
        # Fill missing values with appropriate defaults
        df['Company Name'] = df['Company Name'].fillna('Unknown Company')
        df['Industry'] = df['Industry'].fillna('Technology')
        df['Country'] = df['Country'].fillna('Unknown')
        
        return df
    
    def clean_all(self):
        """Run all cleaning operations"""
        if self.raw_data is None:
            raise ValueError("No data loaded. Run load_raw_data() first.")
        
        self.logger.info("Starting complete data cleaning process...")
        
        # Create a copy to preserve original
        df = self.raw_data.copy()
        
        # Run all cleaning steps
        df = self.handle_missing_values(df)
        df = self.clean_company_names(df)
        df = self.extract_exchange_info(df)
        df = self.standardize_countries(df)
        df = self.categorize_companies(df)
        df = self.add_data_quality_metrics(df)
        df = self.add_metadata(df)
        df = self.remove_duplicates(df)
        
        self.cleaned_data = df
        
        # Log summary
        self.logger.info(f"Cleaning complete. Shape: {df.shape}")
        self.logger.info(f"Columns: {list(df.columns)}")
        self.logger.info(f"Data quality average: {df['data_quality_score'].mean():.2%}")
        
        return df
    
    def save_cleaned_data(self, output_path='cleaned_tech_companies.csv'):
        """Save cleaned data to CSV"""
        if self.cleaned_data is None:
            raise ValueError("No cleaned data to save. Run clean_all() first.")
        
        self.logger.info(f"Saving cleaned data to {output_path}")
        self.cleaned_data.to_csv(output_path, index=False)
        self.logger.info(f"Saved {len(self.cleaned_data)} records")
    
    def get_cleaning_summary(self):
        """Get summary statistics of the cleaning process"""
        if self.cleaned_data is None:
            return {"status": "No data has been cleaned yet."}
        
        df = self.cleaned_data
        
        summary = {
            'total_records': len(df),
            'unique_companies': df['Symbol'].nunique(),
            'countries': df['country_clean'].nunique(),
            'exchanges': df['exchange_code'].nunique(),
            'tech_categories': df['tech_category'].nunique(),
            'avg_data_quality': df['data_quality_score'].mean(),
            'complete_records': df['is_complete_record'].sum(),
            'records_by_region': df['region'].value_counts().to_dict(),
            'records_by_category': df['tech_category'].value_counts().head(10).to_dict()
        }
        
        return summary


def main():
    """Main function to run data cleaning"""
    cleaner = DataCleaner()
    
    # Load data
    cleaner.load_raw_data('marketaux_tech_companies_list.csv')
    
    # Clean data
    cleaned_df = cleaner.clean_all()
    
    # Save cleaned data
    cleaner.save_cleaned_data()
    
    # Print summary
    summary = cleaner.get_cleaning_summary()
    print("\nCleaning Summary:")
    for key, value in summary.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()