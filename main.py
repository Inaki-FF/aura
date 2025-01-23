import sqlite3
import pandas as pd
from prefect import task, flow
from bs4 import BeautifulSoup
import os
import re
from openai import OpenAI
from typing import List, Dict
import json
import time

# Constants
DATABASE_PATH = os.path.join("gold", "financial_data.db")
EXTRACTION_PROMPT = """
Please analyze this financial document and extract the following information in JSON format:

{
    "document_info": {
        "company_name": "string",
        "fiscal_year": "YYYY",
        "document_type": "10-K"
    },
    "income_statement": {
        "revenue": number,
        "operating_income": number,
        "net_income": number
    },
    "balance_sheet": {
        "total_assets": number,
        "total_liabilities": number,
        "total_equity": number
    },
    "cash_flow": {
        "operating_cash_flow": number,
        "investing_cash_flow": number,
        "financing_cash_flow": number
    }
}

Please ensure all numbers are in millions of USD and use consistent accounting standards.
"""

@task(name="Initialize Database")
def init_database() -> None:
    """Initialize SQLite database with required tables."""
    # First, remove existing database file if it exists
    if os.path.exists(DATABASE_PATH):
        os.remove(DATABASE_PATH)
    
    conn = sqlite3.connect(DATABASE_PATH)
    
    try:
        # Create documents table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_name TEXT NOT NULL,
                fiscal_year TEXT NOT NULL,
                document_type TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create income statements table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS income_statements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id INTEGER NOT NULL,
                revenue REAL,
                operating_income REAL,
                net_income REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (document_id) REFERENCES documents(id)
            )
        """)
        
        # Create balance sheets table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS balance_sheets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id INTEGER NOT NULL,
                total_assets REAL,
                total_liabilities REAL,
                total_equity REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (document_id) REFERENCES documents(id)
            )
        """)
        
        # Create cash flows table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cash_flows (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id INTEGER NOT NULL,
                operating_cash_flow REAL,
                investing_cash_flow REAL,
                financing_cash_flow REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (document_id) REFERENCES documents(id)
            )
        """)
        
        # Enable foreign key support
        conn.execute("PRAGMA foreign_keys = ON")
        
        conn.commit()
    finally:
        conn.close()

@task(name="Read and parse HTML file")
def read_and_parse_html(file_path: str) -> str:
    with open(file_path, "r", encoding="utf-8") as file:
        return file.read()

@task(name="Extract features with regex and XBRL tags")
def extract_features(html_content: str) -> pd.DataFrame:
    soup = BeautifulSoup(html_content, "html.parser")
    data = []

    # Process numeric tags
    for tag in soup.find_all("ix:nonfraction"):
        try:
            value = re.sub(r",", "", tag.text.strip())
            value = float(value) if re.match(r"^-?\d+(\.\d+)?$", value) else None
            if value is not None:
                data.append({
                    "tag": "nonfraction",
                    "name": tag.get("name", "Unknown"),
                    "value": value,
                    "unit": tag.get("unitRef", "Unknown"),
                    "decimals": tag.get("decimals", "Unknown")
                })
        except Exception as e:
            print(f"Error processing nonfraction tag: {e}")

    # Process non-numeric tags
    for tag in soup.find_all("ix:nonnumeric"):
        try:
            data.append({
                "tag": "nonnumeric",
                "name": tag.get("name", "Unknown"),
                "value": tag.text.strip(),
                "unit": "N/A",
                "decimals": "N/A"
            })
        except Exception as e:
            print(f"Error processing nonnumeric tag: {e}")

    return pd.DataFrame(data)

@task(name="Create Financial Assistant")
def create_financial_assistant(file_path: str) -> Dict:
    client = OpenAI()
    
    message_file = client.files.create(
        file=open(file_path, "rb"),
        purpose="assistants"
    )
    
    assistant = client.beta.assistants.create(
        name="Financial Analyst",
        instructions="You are a financial analyst expert in extracting and analyzing financial data from 10-K reports. Always respond with properly formatted JSON.",
        model="gpt-4o",
        tools=[{"type": "file_search"}],
        response_format={"type": "json_object"}
    )
    
    thread = client.beta.threads.create(
        messages=[{
            "role": "user",
            "content": EXTRACTION_PROMPT,
            "attachments": [
                {"file_id": message_file.id, "tools": [{"type": "file_search"}]}
            ]
        }]
    )
    
    return {
        "assistant_id": assistant.id,
        "thread_id": thread.id,
        "file_id": message_file.id
    }

@task(name="Extract Financial Data")
def extract_financial_data(assistant_info: Dict) -> Dict:
    client = OpenAI()
    
    run = client.beta.threads.runs.create(
        thread_id=assistant_info["thread_id"],
        assistant_id=assistant_info["assistant_id"]
    )
    
    while run.status in ["queued", "in_progress"]:
        time.sleep(2)
        run = client.beta.threads.runs.retrieve(
            thread_id=assistant_info["thread_id"],
            run_id=run.id
        )
    
    messages = client.beta.threads.messages.list(
        thread_id=assistant_info["thread_id"]
    )
    
    # Cleanup
    client.beta.assistants.delete(assistant_id=assistant_info["assistant_id"])
    client.files.delete(file_id=assistant_info["file_id"])
    
    default_data = {
        "document_info": {"company_name": "Unknown", "fiscal_year": "2023", "document_type": "10-K"},
        "income_statement": {"revenue": 0, "operating_income": 0, "net_income": 0},
        "balance_sheet": {"total_assets": 0, "total_liabilities": 0, "total_equity": 0},
        "cash_flow": {"operating_cash_flow": 0, "investing_cash_flow": 0, "financing_cash_flow": 0}
    }
    
    for message in messages.data:
        if message.role == "assistant":
            try:
                return json.loads(message.content[0].text.value)
            except Exception as e:
                print(f"Error parsing response: {e}")
                return default_data
    
    return default_data

@task(name="Save Financial Data")
def save_financial_data(results: Dict) -> None:
    """Save financial data to both SQLite and Parquet formats."""
    # Save to SQLite
    conn = sqlite3.connect(DATABASE_PATH)
    try:
        # Enable foreign key support
        conn.execute("PRAGMA foreign_keys = ON")
        
        for filename, data in results.items():
            doc_info = data["document_info"]
            try:
                # Insert document info and get the ID
                cursor = conn.execute("""
                    INSERT INTO documents (company_name, fiscal_year, document_type)
                    VALUES (?, ?, ?)
                """, (doc_info["company_name"], doc_info["fiscal_year"], doc_info["document_type"]))
                document_id = cursor.lastrowid
                
                # Insert income statement data
                conn.execute("""
                    INSERT INTO income_statements 
                    (document_id, revenue, operating_income, net_income)
                    VALUES (?, ?, ?, ?)
                """, (document_id, 
                      data["income_statement"]["revenue"],
                      data["income_statement"]["operating_income"],
                      data["income_statement"]["net_income"]))
                
                # Insert balance sheet data
                conn.execute("""
                    INSERT INTO balance_sheets 
                    (document_id, total_assets, total_liabilities, total_equity)
                    VALUES (?, ?, ?, ?)
                """, (document_id,
                      data["balance_sheet"]["total_assets"],
                      data["balance_sheet"]["total_liabilities"],
                      data["balance_sheet"]["total_equity"]))
                
                # Insert cash flow data
                conn.execute("""
                    INSERT INTO cash_flows 
                    (document_id, operating_cash_flow, investing_cash_flow, financing_cash_flow)
                    VALUES (?, ?, ?, ?)
                """, (document_id,
                      data["cash_flow"]["operating_cash_flow"],
                      data["cash_flow"]["investing_cash_flow"],
                      data["cash_flow"]["financing_cash_flow"]))
                
                # Commit after each document is processed successfully
                conn.commit()
                
            except sqlite3.Error as e:
                print(f"Error saving data for {filename}: {e}")
                conn.rollback()
            except Exception as e:
                print(f"Unexpected error processing {filename}: {e}")
                conn.rollback()
        
        conn.commit()
    finally:
        conn.close()

    # Save to Parquet
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    output_dir = "gold"
    
    for category in ['income_statements', 'balance_sheets', 'cash_flows']:
        os.makedirs(os.path.join(output_dir, category), exist_ok=True)
        
        data = []
        for document, doc_data in results.items():
            data.append({
                "company": doc_data["document_info"]["company_name"],
                "fiscal_year": doc_data["document_info"]["fiscal_year"],
                **doc_data[category.rstrip('s')]  # Remove 's' to match dict keys
            })
            
        if data:
            pd.DataFrame(data).to_parquet(
                os.path.join(output_dir, category, f"{category}_{timestamp}.parquet"),
                compression='snappy'
            )

@task(name="Run Analysis Queries")
def run_analysis_queries() -> None:
    """Run financial analysis queries and save results to output.txt"""
    conn = sqlite3.connect(DATABASE_PATH)
    output_file = "output.txt"
    
    queries = {
        "1. Year-over-Year Revenue Growth": """
            WITH revenue_data AS (
                SELECT 
                    d.fiscal_year,
                    i.revenue,
                    LAG(i.revenue) OVER (ORDER BY d.fiscal_year) as prev_revenue
                FROM documents d
                JOIN income_statements i ON d.id = i.document_id
            )
            SELECT 
                fiscal_year,
                ROUND(((revenue - prev_revenue) / prev_revenue * 100), 2) as revenue_growth_percent
            FROM revenue_data
            WHERE prev_revenue IS NOT NULL
            ORDER BY fiscal_year
        """,
        
        "2. Net Margin by Year": """
            SELECT 
                d.fiscal_year,
                ROUND((i.net_income / i.revenue * 100), 2) as net_margin_percent
            FROM documents d
            JOIN income_statements i ON d.id = i.document_id
            ORDER BY d.fiscal_year
        """,
        
        "3. Assets vs Liabilities YoY Change": """
            WITH balance_data AS (
                SELECT 
                    d.fiscal_year,
                    b.total_assets,
                    b.total_liabilities,
                    LAG(b.total_assets) OVER (ORDER BY d.fiscal_year) as prev_assets,
                    LAG(b.total_liabilities) OVER (ORDER BY d.fiscal_year) as prev_liabilities
                FROM documents d
                JOIN balance_sheets b ON d.id = b.document_id
            )
            SELECT 
                fiscal_year,
                ROUND(((total_assets - prev_assets) / prev_assets * 100), 2) as assets_change_percent,
                ROUND(((total_liabilities - prev_liabilities) / prev_liabilities * 100), 2) as liabilities_change_percent
            FROM balance_data
            WHERE prev_assets IS NOT NULL
            ORDER BY fiscal_year
        """,
        
        "4. Operating Cash Flow Trend": """
            SELECT 
                d.fiscal_year,
                c.operating_cash_flow,
                ROUND((c.operating_cash_flow / i.revenue * 100), 2) as ocf_to_revenue_percent
            FROM documents d
            JOIN cash_flows c ON d.id = c.document_id
            JOIN income_statements i ON d.id = i.document_id
            ORDER BY d.fiscal_year
        """,
        
        "5. Liquidity Indicator": """
            SELECT 
                d.fiscal_year,
                ROUND((b.total_assets - b.total_liabilities) / b.total_liabilities * 100, 2) as liquidity_ratio_percent
            FROM documents d
            JOIN balance_sheets b ON d.id = b.document_id
            ORDER BY d.fiscal_year
        """
    }
    
    try:
        with open(output_file, 'w') as f:
            f.write("Financial Analysis Report\n")
            f.write("=======================\n\n")
            
            for title, query in queries.items():
                f.write(f"{title}\n")
                f.write("-" * len(title) + "\n")
                
                df = pd.read_sql_query(query, conn)
                f.write(df.to_string())
                f.write("\n\n")
                
            # Add summary statistics
            f.write("Summary Statistics\n")
            f.write("=================\n")
            summary_query = """
                SELECT 
                    ROUND(AVG(i.revenue/1000), 2) as avg_revenue_billions,
                    ROUND(AVG(i.net_income/1000), 2) as avg_net_income_billions,
                    ROUND(AVG(c.operating_cash_flow/1000), 2) as avg_operating_cash_flow_billions
                FROM documents d
                JOIN income_statements i ON d.id = i.document_id
                JOIN cash_flows c ON d.id = c.document_id
            """
            df = pd.read_sql_query(summary_query, conn)
            f.write(df.to_string())
            
    except Exception as e:
        print(f"Error running analysis queries: {str(e)}")
        raise
        
    finally:
        conn.close()

@flow(name="Financial Data Extraction")
def main_flow(file_paths: List[str]) -> Dict:
    """Main flow for financial data extraction."""
    
    # Initialize database first
    init_database()
    
    # Process each file
    results = {}
    for file_path in file_paths:
        print(f"Processing {file_path}...")
        
        # Create assistant and extract data
        assistant_info = create_financial_assistant(file_path)
        financial_data = extract_financial_data(assistant_info)
        
        # Store results
        results[os.path.basename(file_path)] = financial_data
    
    # Save results
    save_financial_data(results)
    
    # Run analysis queries
    run_analysis_queries()
    
    return results

if __name__ == "__main__":
    os.makedirs("gold", exist_ok=True)
    
    file_paths = [
        "./bronze/aapl-20200926.pdf",
        "./bronze/aapl-20210925.pdf",
        "./bronze/aapl-20220924.pdf"
    ]
    
    try:
        results = main_flow(file_paths)
        
        # Save results to JSON for debugging
        output_path = os.path.join("gold", "financial_data_results.json")
        with open(output_path, "w") as f:
            json.dump(results, f, indent=4)
            
        print(f"Processing completed. Results saved to {output_path}")
        
        # Print summary
        print("\nProcessing Summary:")
        for file_name, data in results.items():
            fiscal_year = data['document_info']['fiscal_year']
            print(f"{'✓' if data else '✗'} {file_name} (FY{fiscal_year})")
                
    except Exception as e:
        print(f"Error during execution: {str(e)}")
        raise