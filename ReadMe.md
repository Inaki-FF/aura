# Financial Document Analyzer - User Manual

## Overview
This application processes financial documents (10-K reports) to extract and analyze financial data using OpenAI's GPT model. The system stores the processed data in both SQLite database and Parquet formats, and generates analytical reports.

## Prerequisites
- Python 3.10 or later
- Docker (optional)
- OpenAI API key

## Project Structure
```
financial-analyzer/
├── main.py              # Main application code
├── requirements.txt     # Python dependencies
├── Dockerfile          # Docker configuration
├── docker-compose.yml  # Docker Compose configuration
├── bronze/             # Input directory for raw financial documents
├── gold/              # Output directory for processed data
│   ├── income_statements/
│   ├── balance_sheets/
│   └── cash_flows/
└── data/              # Docker volume mount points
    ├── input/         # Input files when running with Docker
    └── output/        # Output files when running with Docker
```

## Installation

### Method 1: Local Installation
1. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Unix/macOS
   # or
   .\venv\Scripts\activate  # On Windows
   ```

2. Install required packages:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up your OpenAI API key:
   ```bash
   export OPENAI_API_KEY="your-api-key-here"  # Unix/macOS
   # or
   set OPENAI_API_KEY="your-api-key-here"     # Windows
   ```

### Method 2: Docker Installation
1. Build the Docker image:
   ```bash
   docker-compose build
   ```

2. Create a `.env` file with your OpenAI API key:
   ```
   OPENAI_API_KEY=your-api-key-here
   ```

## Usage

### Running Locally
1. Place your financial documents (PDFs) in the `bronze` directory.

2. Run the application:
   ```bash
   python main.py
   ```

### Running with Docker
1. Place your financial documents (PDFs) in the `data/input` directory.

2. Run the application:
   ```bash
   docker-compose up
   ```

## Input Requirements
- Input files should be PDF documents of 10-K financial reports
- Files should be named in the format: `company-YYYYMMDD.pdf`
- Place input files in the appropriate directory:
  - `bronze/` for local execution
  - `data/input/` for Docker execution

## Output
The application generates several types of output:

1. SQLite Database (`gold/financial_data.db`):
   - Documents metadata
   - Income statements
   - Balance sheets
   - Cash flows

2. Parquet Files (`gold/` directory):
   - Income statements
   - Balance sheets
   - Cash flows

3. Analysis Report (`output.txt`):
   - Companies overview
   - Profitability analysis
   - Balance sheet ratios
   - Cash flow analysis
   - Summary statistics

## Database Schema

### Documents Table
```sql
CREATE TABLE documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_name TEXT NOT NULL,
    fiscal_year TEXT NOT NULL,
    document_type TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

### Income Statements Table
```sql
CREATE TABLE income_statements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id INTEGER NOT NULL,
    revenue REAL,
    operating_income REAL,
    net_income REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (document_id) REFERENCES documents(id)
)
```

### Balance Sheets Table
```sql
CREATE TABLE balance_sheets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id INTEGER NOT NULL,
    total_assets REAL,
    total_liabilities REAL,
    total_equity REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (document_id) REFERENCES documents(id)
)
```

### Cash Flows Table
```sql
CREATE TABLE cash_flows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id INTEGER NOT NULL,
    operating_cash_flow REAL,
    investing_cash_flow REAL,
    financing_cash_flow REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (document_id) REFERENCES documents(id)
)
```

## Troubleshooting

### Common Issues

1. OpenAI API Key Error:
   ```
   Error: OPENAI_API_KEY environment variable is not set
   ```
   Solution: Make sure to set the OPENAI_API_KEY environment variable.

2. File Not Found Error:
   ```
   FileNotFoundError: [Errno 2] No such file or directory
   ```
   Solution: Ensure your input files are in the correct directory (bronze/ for local execution or data/input/ for Docker).

3. SQLite Database Error:
   ```
   sqlite3.OperationalError: no such table
   ```
   Solution: Delete the existing database file and let the application create a new one.

### Getting Help
If you encounter any issues not covered in this manual, please:
1. Check the application logs for detailed error messages
2. Verify that all prerequisites are properly installed
3. Ensure your input files meet the required format
4. Check that you have sufficient disk space for output files

## Maintenance
- Regularly backup the SQLite database
- Monitor disk space usage in the output directories
- Keep the OpenAI API key secure and updated
- Update Python dependencies as needed

## Notes
- The application requires an active internet connection to use the OpenAI API
- Processing times may vary depending on the size and complexity of input documents
- All financial figures are stored in millions of USD