
# Corteva Weather Data Analysis

A comprehensive data engineering solution for ingesting, analyzing, and serving weather data through a REST API. This project demonstrates data modeling, ETL processes, statistical analysis, and API development using Python, PostgreSQL, and Flask with SQLAlchemy.

## Overview

This project solves a weather data analysis challenge by:

- Ingesting weather data from multiple weather stations (1985-2014)
- Calculating yearly statistics for each station
- Providing REST API endpoints to access the data
- Implementing proper data validation and duplicate handling

The weather data comes from stations in Nebraska, Iowa, Illinois, Indiana, and Ohio, containing daily records of maximum/minimum temperatures and precipitation.

## Features

- **Data Ingestion**: Robust ETL pipeline with duplicate detection and logging
- **Statistical Analysis**: Automated calculation of yearly weather statistics
- **REST API**: Flask-based API with SQLAlchemy ORM, filtering and pagination
- **Data Validation**: Handles missing values (-9999) appropriately
- **Database Design**: Optimized PostgreSQL schema with proper constraints

## Technology Stack

- **Database**: PostgreSQL
- **Backend**: Python, Flask
- **ORM**: SQLAlchemy with Flask-SQLAlchemy
- **Data Processing**: pandas, numpy
- **Testing**: pytest
- **Other**: tqdm (progress bars), python-dotenv (environment management)

## Database Schema

### Weather Data Table
```

CREATE TABLE weather_data (
id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
station_id VARCHAR(11) NOT NULL,
date DATE NOT NULL,
max_temp NUMERIC(5,2),
min_temp NUMERIC(5,2),
precipitation NUMERIC(6,2),
CONSTRAINT unique_date_station UNIQUE (date, station_id)
);

```

### Statistics Data Table
```

CREATE TABLE statistics_data (
stat_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
station_id VARCHAR(11) NOT NULL,
year INT NOT NULL,
avg_maximum_temperature NUMERIC(5,2) NOT NULL,
avg_minimum_temperature NUMERIC(5,2) NOT NULL,
total_precipitation NUMERIC(6,2) NOT NULL,
CONSTRAINT unique_statistics_data UNIQUE (station_id, year)
);

```

## Project Structure

```

corteva-weather-data-analysis/
├── app/ # Flask application package
│ ├── **init**.py # Flask app factory
│ ├── config.py # Configuration settings
│ ├── db.py # Database initialization
│ ├── models.py # SQLAlchemy data models
│ └── views.py # API route definitions
├── data/ # Data directory
│ └── wx_data/ # Weather data files
├── Data_Ingestion.py # Weather data ingestion script
├── statistics.py # Statistics calculation script
├── run.py # Flask app runner
├── requirements.txt # Python dependencies
├── README.md # Project documentation
├── weather_data_log.log # Ingestion logs
└── weather_data_statistics_log.log # Statistics logs

```

## Setup Instructions

### Prerequisites
- Python 3.8+
- PostgreSQL
- Git

### Installation

1. **Clone the repository**
```

git clone
cd corteva-weather-data-analysis

```

2. **Install dependencies**
```

pip install -r requirements.txt

```

3. **Database Setup**
- Create a PostgreSQL database named `corteva_weather`
- Run the SQL schema commands to create tables
- Default connection: `postgresql://postgres:postgres@localhost:5432/corteva_weather`

4. **Environment Configuration**
Create a `.env` file to override default database credentials:
```

DATABASE_URL=postgresql://username:password@localhost/corteva_weather

```

## Flask Application Architecture

### Application Factory Pattern
The Flask app uses the application factory pattern in `app/__init__.py`:
```

def create_app():
app = Flask(**name**)
app.config["SQLALCHEMY_DATABASE_URI"] = "postgresql://postgres:postgres@localhost:5432/corteva_weather"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db.init_app(app)
app.register_blueprint(api_bp, url_prefix="/api")
return app

```

### Data Models
SQLAlchemy models are defined in `app/models.py`:

**WeatherData Model**:
- Maps to `weather_data` table
- Fields: id, station_id, date, max_temp, min_temp, precipitation

**Statistics Model**:
- Maps to `statistics_data` table
- Fields: stat_id, station_id, year, avg_maximum_temperature, avg_minimum_temperature, total_precipitation

### Configuration
Database configuration is handled in `app/config.py` with environment variable support:
```

class Config:
SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", "postgresql://user:password@localhost/corteva_weather")
SQLALCHEMY_TRACK_MODIFICATIONS = False

```

## Usage

### Data Ingestion

Run the weather data ingestion script:
```

python Data_Ingestion.py

```

**Features:**
- Processes all `.txt` files in the `data/wx_data/` directory
- Converts temperatures from tenths of degrees to degrees Celsius
- Converts precipitation from tenths of millimeters to centimeters
- Handles missing values (-9999) by setting them to NULL
- Prevents duplicates using `ON CONFLICT` clause
- Provides detailed logging with start/end times and record counts

### Statistics Calculation

Generate yearly statistics for each weather station:
```

python statistics.py

```

**Calculations:**
- Average maximum temperature (°C)
- Average minimum temperature (°C)
- Total accumulated precipitation (cm)
- Ignores missing data in calculations
- Uses NULL for statistics that cannot be calculated

### REST API

Start the Flask development server:
```

python run.py

```

The API will be available at `http://localhost:5000`

## API Endpoints

### Weather Data
- **GET** `/api/weather/`
  - Returns paginated weather data ordered by date and station ID
  - Query parameters:
    - `page` (default: 1, min: 1)
    - `per` (default: 50, min: 1, max: 500)
  - Response: JSON array of weather records with id, station_id, date, max_temp, min_temp, precipitation

### Statistics Data
- **GET** `/api/weather/stats`
  - Returns paginated weather statistics ordered by year and station ID
  - Query parameters:
    - `page` (default: 1, min: 1)
    - `per` (default: 50, min: 1, max: 500)
  - Response: JSON array of statistics with id, station_id, year, avg_maximum_temperature, avg_minimum_temperature, total_precipitation

### Pagination
Both endpoints support pagination with automatic bounds checking:
- Page numbers are automatically adjusted to minimum value of 1
- Per-page results are bounded between 1 and 500 records
- Offset calculation: `(page - 1) * per_page`

## Performance Metrics

Based on the log files:
- **Data Ingestion**: Successfully processed 1,729,957 records in approximately 7 minutes
- **Statistics Calculation**: Completed in approximately 4 seconds
- **Duplicate Handling**: Built-in duplicate detection prevents data duplication on re-runs

## Data Quality

- **Missing Values**: Properly handled using NULL values in database
- **Data Validation**: Converts raw measurements to appropriate units
- **Constraints**: Database constraints ensure data integrity
- **Logging**: Comprehensive logging for monitoring and debugging

## Testing

Run the test suite:
```

pytest

```

## API Usage Examples

### Get first 10 weather records
```

curl "http://localhost:5000/api/weather/?page=1&per=10"

```

### Get weather statistics for page 2
```

curl "http://localhost:5000/api/weather/stats?page=2&per=25"

```

## Deployment Considerations

For production deployment:

### Database Configuration
- Use environment variables for database credentials
- Consider connection pooling for high-traffic scenarios
- Implement database migrations for schema changes

### Application Server
- Use production WSGI server like Gunicorn instead of Flask development server
- Configure proper logging levels
- Implement health check endpoints

### Cloud Deployment on AWS
- **Database**: Amazon RDS for PostgreSQL
- **API Hosting**: AWS ECS/Fargate or EC2 with Application Load Balancer
- **Scheduled Jobs**: AWS Lambda or ECS Scheduled Tasks for data ingestion
- **Storage**: S3 for raw data files
- **Monitoring**: CloudWatch for logging and metrics

## Development

### Running in Development Mode
```

export FLASK_ENV=development
python run.py

```

### Database Migrations
If using Flask-Migrate:
```

flask db init
flask db migrate -m "Initial migration"
flask db upgrade

```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is part of a coding exercise for Corteva.
