# Environ Policies Data Processing

A Python application for processing environmental policy and company data with automatic cleaning, validation, and database loading capabilities.

## Features

- **Stateful Data Processing**: Each DataProcessor instance handles one file
- **Automatic Data Cleaning**: Smart cleaning based on data type (companies/policies)
- **Pydantic Model Validation**: Validates data against defined models
- **SQL Database Loading**: Load cleaned data directly to PostgreSQL with native array support
- **Configurable Settings**: Environment-based configuration
- **Docker Support**: Easy PostgreSQL setup with Docker

## Quick Start

### 1. Install Dependencies
```bash
uv install
```

### 2. Start PostgreSQL Database
Using Docker Compose (recommended):
```bash
docker-compose up -d postgres
```

### 3. Configure Environment Variables
Create a `.env` file in the project root with these contents:
```env
# Database Configuration for PostgreSQL Docker Container
DB_HOST=localhost
DB_PORT=5432
DB_NAME=environ_policies
DB_USER=postgres
DB_PASSWORD=postgres123

# Database Schema and Table Names
DB_SCHEMA=public
COMPANIES_TABLE=companies
POLICIES_TABLE=policies

# Connection Pool Settings
DB_POOL_SIZE=5
DB_MAX_OVERFLOW=10
```

### 4. Run Data Processing
```bash
python src/main.py
```

### Database Schema
The PostgreSQL container automatically creates these tables:

**Companies Table:**
- `id` (INTEGER PRIMARY KEY)
- `name` (VARCHAR(255) NOT NULL)
- `operating_jurisdiction` (VARCHAR(255) NOT NULL)
- `last_login` (TIMESTAMP WITH TIME ZONE)
- `sector` (VARCHAR(255) NOT NULL)

**Policies Table:**
- `id` (VARCHAR(255) PRIMARY KEY)
- `name` (TEXT NOT NULL)
- `published_date` (DATE)
- `description` (TEXT)
- `geography` (VARCHAR(255) NOT NULL)
- `source_url` (TEXT)
- `topics` (TEXT[]) - PostgreSQL array for efficient list storage
- `sectors` (TEXT[]) - PostgreSQL array for efficient list storage
- `status` (VARCHAR(50))
- `updated_date` (TIMESTAMP WITH TIME ZONE)
```

## Usage

`main.py` gives an example usage of the files

## Data Models

The application validates data against Pydantic models:

- **Company**: id, name, operating_jurisdiction, last_login, sector
- **Policy**: id, name, published_date, description, geography, source_url, topics, sectors, status, updated_datetime

## Project Structure

```
environ-policies/
├── src/
│   ├── data/               # Raw data files
│   ├── models/             # Pydantic data models
│   ├── data_processor.py   # Main processing class
│   ├── settings.py         # Configuration settings
│   └── main.py            # Application entry point
├── Dockerfile.postgres    # PostgreSQL Docker image
├── docker-compose.yml     # Docker orchestration
├── init.sql              # Database initialization
├── pyproject.toml        # Dependencies
└── README.md             # This file
```

## Configuration

All database and application settings are managed through `src/settings.py` using Pydantic Settings, which automatically reads from a `.env` file if present.

### Available Settings:
- `DB_HOST`: Database host (default: localhost)
- `DB_PORT`: Database port (default: 5432)
- `DB_NAME`: Database name (default: environ_policies)
- `DB_USER`: Database user (default: postgres)
- `DB_PASSWORD`: Database password
- `DB_SCHEMA`: Database schema (default: public)
- `COMPANIES_TABLE`: Companies table name (default: companies)
- `POLICIES_TABLE`: Policies table name (default: policies)
