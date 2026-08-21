# FAANG Stock ETL Pipeline with Airflow \& PySpark

[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8.1%2B-017CEE?logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5.0-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Power BI](https://img.shields.io/badge/Power%20BI-Analytics-F2C811?logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)

## 📊 Overview

This project implements an automated ETL (Extract, Transform, Load) pipeline for FAANG stock data using Apache Airflow, PySpark, and PostgreSQL. The pipeline fetches historical stock data for major tech companies (Apple, Amazon, Google, Meta, Netflix), performs transformations using PySpark, and loads the processed data into a PostgreSQL database for analysis and visualization in Power BI.

## 🚀 Features

* **Automated Data Extraction**: Fetches 20 years of historical stock data from Yahoo Finance
* **PySpark Transformations**: Calculates key financial metrics including:

  * Daily returns and price ranges
  * Moving averages (14-day)
  * Volatility (7-day rolling standard deviation)
  * Volume analysis (7-day average and ratio)
  * Relative performance vs sector average
  * Cumulative returns
* **Airflow Orchestration**: Daily scheduled ETL runs with retry logic and monitoring
* **PostgreSQL Storage**: Structured data storage with optimized schema
* **Power BI Integration**: Automatic dataset refresh after each successful ETL run
* **Docker Deployment**: Containerized setup for consistent development and production environments

## 📁 Project Structure

```
FAANG\_Stock\_ETL/
├── dags/
│   ├── stocks\_etl\_dag.py          # Main Airflow DAG definition
│   └── scripts/
│       ├── stock\_etl.py           # Core ETL logic (extract, transform, load)
│       └── powerbi\_refresh.py     # Power BI dataset refresh handler
├── config/
│   └── airflow.cfg                 # Airflow configuration
├── jars/
│   └── postgresql-42.7.12.jar     # PostgreSQL JDBC driver
├── logs/                           # Airflow logs (auto-generated)
├── docker-compose.yaml             # Docker services definition
├── Dockerfile                      # Custom Airflow image with Java \& PySpark
├── requirements.txt                # Python dependencies
├── .env                            # Environment variables
└── README.md                       # This file
```

## 🛠️ Technology Stack

|Component|Technology|Purpose|
|-|-|-|
|**Orchestration**|Apache Airflow 2.8.1+|Workflow scheduling \& monitoring|
|**Data Processing**|PySpark 3.5.0|Distributed data transformations|
|**Data Source**|Yahoo Finance (yfinance)|Historical stock data|
|**Database**|PostgreSQL 16|Data storage|
|**Visualization**|Power BI|Reporting \& analytics|
|**Containerization**|Docker \& Docker Compose|Deployment \& environment management|

## 📊 Data Schema

### Stock ETL Table

|Column|Data Type|Description|
|-|-|-|
|`date`|TIMESTAMP|Trading date|
|`company`|VARCHAR(50)|Company name|
|`open\_price`|DOUBLE PRECISION|Opening price|
|`high\_price`|DOUBLE PRECISION|Daily high|
|`low\_price`|DOUBLE PRECISION|Daily low|
|`close\_price`|DOUBLE PRECISION|Closing price|
|`volume`|DOUBLE PRECISION|Trading volume|
|`previous\_close`|DOUBLE PRECISION|Previous day's close|
|`daily\_return\_pct`|DOUBLE PRECISION|Daily return %|
|`daily\_range`|DOUBLE PRECISION|High - Low|
|`daily\_range\_pct`|DOUBLE PRECISION|Range as %|
|`gap\_pct`|DOUBLE PRECISION|Gap % from previous close|
|`mov\_avg\_14d`|DOUBLE PRECISION|14-day moving average|
|`volatility\_7d`|DOUBLE PRECISION|7-day volatility|
|`avg\_volume\_7d`|DOUBLE PRECISION|7-day avg volume|
|`volume\_ratio`|DOUBLE PRECISION|Volume / 7-day avg|
|`sector\_avg\_return`|DOUBLE PRECISION|Sector average return|
|`relative\_performance`|DOUBLE PRECISION|Performance vs sector|
|`cumulative\_return`|DOUBLE PRECISION|Return since inception|
|`created\_at`|TIMESTAMP|Record creation timestamp|

## 🔧 Installation \& Setup

### Prerequisites

* Docker Desktop (Windows/Mac) or Docker Engine (Linux)
* Python 3.10+
* 8GB+ RAM recommended
* 20GB+ free disk space

### Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/yourusername/FAANG\_Stock\_ETL.git
cd FAANG\_Stock\_ETL

# 2. Start the services

docker compose build
docker compose up -d

# 3. Wait for initialization (30-60 seconds)
# 4. Access Airflow UI at http://localhost:8081 (airflow/airflow)
```

### Environment Configuration

Create a `.env` file in the project root:

```env
# Airflow Configuration
AIRFLOW\_UID=50000
AIRFLOW\_IMAGE\_NAME=apache/airflow:3.3.0
FERNET\_KEY=46BKJoQYlPPOexq0OhDZnIlNepKFfl87tCO3dNf2wjM=

# PostgreSQL Credentials
POSTGRES\_USER=airflow
POSTGRES\_PASSWORD=airflow
POSTGRES\_DB=airflow

```

## 📈 ETL Process Details

### 1\. Extract

* Fetches data from Yahoo Finance using the `yfinance` library
* Handles multiple tickers with proper error handling
* Data includes: Open, High, Low, Close, Volume, Dividends, Stock Splits

### 2\. Transform (PySpark)

* **Daily Metrics**: Returns, price ranges, gaps
* **Rolling Statistics**: 14-day MA, 7-day volatility
* **Volume Analysis**: 7-day average volume, volume ratio
* **Relative Performance**: Sector average, performance vs sector
* **Cumulative Metrics**: Returns since first trading day

### 3\. Load

* Inserts data into PostgreSQL using batch processing
* Creates table if not exists with optimized schema
* Uses `psycopg2` for efficient bulk inserts

## 🗓️ Airflow DAG

The pipeline runs daily at 8:00 AM with the following tasks:

```python
1. run\_stock\_etl        # Executes ETL pipeline
2. verify\_load\_task     # Validates data loaded
3. cleanup\_task         # Cleans up old logs
```

## 📊 Power BI Integration

### Setup Power BI Refresh

A Limitation was found on this step due to limitations on my student account, but I will this here in case someone else can do this step.

1. **Register an App in Azure AD**:

   * Go to Azure Portal → Azure Active Directory → App registrations
   * Create a new registration
   * Note: Client ID, Tenant ID
   * Create a Client Secret
2. **Grant Permissions**:

   * API Permissions → Add → Power BI Service → Dataset.ReadWrite.All
   * Grant admin consent
3. **Configure in Airflow**:

```bash
docker exec -it faang\_stock\_etl-airflow-scheduler-1 airflow variables set POWERBI\_TENANT\_ID "your-tenant-id"
docker exec -it faang\_stock\_etl-airflow-scheduler-1 airflow variables set POWERBI\_CLIENT\_ID "your-client-id"
docker exec -it faang\_stock\_etl-airflow-scheduler-1 airflow variables set POWERBI\_CLIENT\_SECRET "your-client-secret"
docker exec -it faang\_stock\_etl-airflow-scheduler-1 airflow variables set POWERBI\_WORKSPACE\_ID "your-workspace-id"
docker exec -it faang\_stock\_etl-airflow-scheduler-1 airflow variables set POWERBI\_DATASET\_ID "your-dataset-id"
```

## 🐳 Docker Commands

```bash
# Start services
docker compose up -d

# View logs
docker compose logs -f

# Stop services
docker compose down

# Rebuild after changes
docker compose build
docker compose up -d

# Execute CLI commands
docker exec -it faang\_stock\_etl-airflow-scheduler-1 airflow dags list
docker exec -it faang\_stock\_etl-airflow-scheduler-1 airflow dags trigger stock\_etl\_dag
```

## 🔍 Monitoring \& Debugging

### Check DAG Status

```bash
docker exec -it faang\_stock\_etl-airflow-scheduler-1 airflow dags list
docker exec -it faang\_stock\_etl-airflow-scheduler-1 airflow dags state stock\_etl\_dag
```

### View Task Logs

```bash
docker compose logs airflow-worker --tail 100
```

### Validate Data

```bash
docker exec -it faang\_stock\_etl-postgres-1 psql -U airflow -d mydb -c "SELECT COUNT(\*) FROM stock\_etl;"
```

### Manual ETL Run

```bash
docker exec -it faang\_stock\_etl-airflow-scheduler-1 airflow dags trigger stock\_etl\_dag
```

## 🧪 Testing

### Run ETL Locally

```bash
cd scripts
python stock\_etl.py
```

### Test PySpark Session

```bash
docker exec -it faang\_stock\_etl-airflow-scheduler-1 python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local\[\*]').appName('test').getOrCreate()
print('✅ Spark works!')
spark.stop()
"
```

### Test PostgreSQL Connection

```bash
docker exec -it faang\_stock\_etl-airflow-scheduler-1 python -c "
import psycopg2
conn = psycopg2.connect(host='postgres', port=5432, database='mydb', user='airflow', password='airflow')
print('✅ PostgreSQL connection successful!')
conn.close()
"
```

## 🚧 Troubleshooting

### Common Issues \& Solutions

|Issue|Solution|
|-|-|
|**DAG import timeout**|Move heavy imports inside functions; Increase `dagbag\_import\_timeout`|
|**Java not found**|Ensure Dockerfile installs OpenJDK 17|
|**Port conflicts**|Change ports in docker-compose.yaml (5432/8080)|
|**Memory issues**|Allocate more memory to Docker in settings|

## 📝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 🙏 Acknowledgments

* [Apache Airflow](https://airflow.apache.org/) for workflow orchestration
* [PySpark](https://spark.apache.org/docs/latest/api/python/) for data processing
* [yfinance](https://github.com/ranaroussi/yfinance) for Yahoo Finance API
* [Power BI](https://powerbi.microsoft.com/) for visualization
