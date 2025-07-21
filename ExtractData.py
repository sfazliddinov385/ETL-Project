import requests
import csv
import time
from datetime import datetime, timedelta
from collections import defaultdict
import json
import os
from dotenv import load_dotenv

# API Configuration
API_KEY = os.getenv("API_KEY")
BASE_URL = os.getenv("BASE_URL")

def fetch_tech_entities():
    """Fetch technology companies from MarketAux entity search"""
    print("Fetching technology companies from MarketAux...")
    
    all_entities = []
    page = 1
    
    # Fetch entities from Technology industry
    while True:
        params = {
            'api_token': API_KEY,
            'industries': 'Technology',
            'types': 'equity',  # Focus on stocks/companies
            'page': page
        }
        
        try:
            response = requests.get(f"{BASE_URL}/entity/search", params=params)
            response.raise_for_status()
            data = response.json()
            
            if 'data' in data and data['data']:
                all_entities.extend(data['data'])
                print(f"Page {page}: Found {len(data['data'])} entities (Total: {len(all_entities)})")
                
                # Check if there are more pages
                if len(data['data']) < 50:  # API limit is 50 per page
                    break
                    
                page += 1
                time.sleep(0.5)  # Rate limiting
            else:
                break
                
        except requests.exceptions.RequestException as e:
            print(f"Error fetching entities: {e}")
            break
    
    # Also fetch from other tech-related industries
    tech_related_industries = ['Communication Services', 'Consumer Cyclical']
    
    for industry in tech_related_industries:
        print(f"\nFetching {industry} companies...")
        page = 1
        
        while True:
            params = {
                'api_token': API_KEY,
                'industries': industry,
                'types': 'equity',
                'page': page
            }
            
            try:
                response = requests.get(f"{BASE_URL}/entity/search", params=params)
                response.raise_for_status()
                data = response.json()
                
                if 'data' in data and data['data']:
                    # Filter for tech-related companies
                    tech_companies = [e for e in data['data'] if is_tech_company(e)]
                    all_entities.extend(tech_companies)
                    print(f"Page {page}: Found {len(tech_companies)} tech companies")
                    
                    if len(data['data']) < 50:
                        break
                        
                    page += 1
                    time.sleep(0.5)
                else:
                    break
                    
            except requests.exceptions.RequestException as e:
                print(f"Error fetching {industry} entities: {e}")
                break
    
    return all_entities

def is_tech_company(entity):
    """Determine if a company is tech-related based on name"""
    tech_keywords = [
        'tech', 'software', 'computer', 'digital', 'cloud', 'cyber', 'data',
        'internet', 'online', 'mobile', 'app', 'semiconductor', 'chip',
        'electronic', 'telecom', 'communication', 'network', 'ai', 'robotics',
        'automation', 'platform', 'saas', 'fintech', 'biotech', 'gaming'
    ]
    
    name_lower = entity.get('name', '').lower()
    return any(keyword in name_lower for keyword in tech_keywords)

def fetch_news_for_entities(entities, limit_per_entity=5):
    """Fetch recent news for entities to get more context"""
    print("\nFetching news data for entities...")
    
    entity_news = {}
    symbols = [e['symbol'] for e in entities[:20]]  # Sample first 20 entities
    
    # Fetch news for multiple symbols at once
    for i in range(0, len(symbols), 5):  # Process 5 symbols at a time
        batch = symbols[i:i+5]
        symbols_str = ','.join(batch)
        
        params = {
            'api_token': API_KEY,
            'symbols': symbols_str,
            'filter_entities': 'true',
            'limit': 10,
            'published_after': (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        }
        
        try:
            response = requests.get(f"{BASE_URL}/news/all", params=params)
            response.raise_for_status()
            data = response.json()
            
            if 'data' in data:
                for article in data['data']:
                    for entity in article.get('entities', []):
                        symbol = entity['symbol']
                        if symbol not in entity_news:
                            entity_news[symbol] = {
                                'sentiment_scores': [],
                                'match_scores': [],
                                'article_count': 0,
                                'recent_headline': article['title']
                            }
                        
                        entity_news[symbol]['sentiment_scores'].append(entity['sentiment_score'])
                        entity_news[symbol]['match_scores'].append(entity['match_score'])
                        entity_news[symbol]['article_count'] += 1
            
            time.sleep(0.5)  # Rate limiting
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching news for {symbols_str}: {e}")
    
    return entity_news

def fetch_trending_tech():
    """Fetch trending tech companies"""
    print("\nFetching trending tech companies...")
    
    params = {
        'api_token': API_KEY,
        'industries': 'Technology',
        'min_doc_count': 5,
        'published_after': (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
        'sort': 'total_documents',
        'limit': 100
    }
    
    try:
        response = requests.get(f"{BASE_URL}/entity/trending/aggregation", params=params)
        response.raise_for_status()
        data = response.json()
        
        if 'data' in data:
            return data['data']
            
    except requests.exceptions.RequestException as e:
        print(f"Error fetching trending entities: {e}")
    
    return []

def save_to_csv(entities, entity_news, trending):
    """Save all data to CSV files"""
    
    # 1. Master CSV with all tech companies
    print("\nSaving data to CSV files...")
    
    with open('marketaux_tech_companies_master.csv', 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        
        # Header
        writer.writerow(['MARKETAUX TECH COMPANIES EXTRACTION REPORT'])
        writer.writerow(['Generated on:', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
        writer.writerow(['Data Source:', 'MarketAux Financial API'])
        writer.writerow([])
        
        # Summary
        writer.writerow(['SUMMARY'])
        writer.writerow(['Total Tech Companies Found:', len(entities)])
        writer.writerow(['Companies with News Data:', len(entity_news)])
        writer.writerow(['Trending Companies:', len(trending)])
        writer.writerow([])
        
        # Main data table
        writer.writerow(['COMPANY DATA'])
        writer.writerow([
            'Rank', 'Symbol', 'Company Name', 'Industry', 'Exchange', 
            'Exchange Country', 'Type', 'News Articles (7 days)', 
            'Avg Sentiment', 'Avg Match Score', 'Recent Headline'
        ])
        
        # Sort entities by symbol
        sorted_entities = sorted(entities, key=lambda x: x.get('symbol', ''))
        
        for i, entity in enumerate(sorted_entities, 1):
            symbol = entity.get('symbol', '')
            
            # Get news data if available
            news_data = entity_news.get(symbol, {})
            article_count = news_data.get('article_count', 0)
            
            # Calculate averages
            if news_data and news_data.get('sentiment_scores'):
                avg_sentiment = sum(news_data['sentiment_scores']) / len(news_data['sentiment_scores'])
                avg_match = sum(news_data['match_scores']) / len(news_data['match_scores'])
                recent_headline = news_data.get('recent_headline', 'N/A')
            else:
                avg_sentiment = 'N/A'
                avg_match = 'N/A'
                recent_headline = 'N/A'
            
            writer.writerow([
                i,
                symbol,
                entity.get('name', ''),
                entity.get('industry', ''),
                entity.get('exchange', 'N/A'),
                entity.get('country', ''),
                entity.get('type', ''),
                article_count,
                f"{avg_sentiment:.4f}" if isinstance(avg_sentiment, float) else avg_sentiment,
                f"{avg_match:.4f}" if isinstance(avg_match, float) else avg_match,
                recent_headline
            ])
    
    # 2. Trending companies CSV
    with open('marketaux_trending_tech.csv', 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        
        writer.writerow(['TRENDING TECH COMPANIES (LAST 7 DAYS)'])
        writer.writerow(['Generated on:', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
        writer.writerow([])
        
        writer.writerow([
            'Rank', 'Symbol', 'Total Articles', 'Average Sentiment', 'Trending Score'
        ])
        
        for i, company in enumerate(trending[:50], 1):  # Top 50 trending
            writer.writerow([
                i,
                company.get('key', ''),
                company.get('total_documents', 0),
                f"{company.get('sentiment_avg', 0):.4f}",
                f"{company.get('score', 0):.4f}"
            ])
    
    # 3. Simple list for easy import
    with open('marketaux_tech_companies_list.csv', 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(['Symbol', 'Company Name', 'Industry', 'Country'])
        
        for entity in sorted_entities:
            writer.writerow([
                entity.get('symbol', ''),
                entity.get('name', ''),
                entity.get('industry', ''),
                entity.get('country', '')
            ])
    
    print("\nFiles created:")
    print("- marketaux_tech_companies_master.csv (comprehensive report)")
    print("- marketaux_trending_tech.csv (trending companies)")
    print("- marketaux_tech_companies_list.csv (simple list)")

def main():
    """Main function to extract tech companies from MarketAux"""
    print("Starting MarketAux Tech Companies Extractor...")
    print("=" * 60)
    
    # 1. Fetch tech entities
    entities = fetch_tech_entities()
    print(f"\nTotal entities found: {len(entities)}")
    
    # Remove duplicates based on symbol
    unique_entities = {}
    for entity in entities:
        symbol = entity.get('symbol', '')
        if symbol and symbol not in unique_entities:
            unique_entities[symbol] = entity
    
    entities = list(unique_entities.values())
    print(f"Unique tech companies: {len(entities)}")
    
    # 2. Fetch news data for sample of entities
    entity_news = {}
    if len(entities) > 0:
        entity_news = fetch_news_for_entities(entities)
    
    # 3. Fetch trending tech companies
    trending = fetch_trending_tech()
    print(f"Trending tech companies found: {len(trending)}")
    
    # 4. Save to CSV files
    save_to_csv(entities, entity_news, trending)
    
    # Display summary
    print("\n" + "=" * 60)
    print("EXTRACTION COMPLETE!")
    print(f"Total unique tech companies: {len(entities)}")
    
    if len(entities) >= 100:
        print("✓ Successfully found 100+ tech companies!")
    else:
        print(f"Found {len(entities)} tech companies (target was 100+)")
    
    # Show sample companies
    print("\nSample of tech companies found:")
    for i, entity in enumerate(entities[:20], 1):
        print(f"{i}. {entity.get('symbol', 'N/A')} - {entity.get('name', 'N/A')}")
    
    if len(entities) > 20:
        print(f"\n... and {len(entities) - 20} more companies")
    
    # Show top trending if available
    if trending:
        print("\nTop 5 Trending Tech Companies (by mentions):")
        for i, company in enumerate(trending[:5], 1):
            print(f"{i}. {company.get('key', 'N/A')} - {company.get('total_documents', 0)} articles")

if __name__ == "__main__":
    main()
    