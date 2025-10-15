# Data Combiner

This project makes the ingestion and combination of data from a wide variety of similar APIs as simple as writing JSON, while still allowing the flexibility to pull from anywhere. 

## Sources Structure

```
sources
├── apis # Contains information about live APIs data would be pulled from
│   └── example.com.json
├── datasets # Contains folders for datasets that will not update frequently
│   └── census
│       ├── structure.json
│       └── 2020census.csv
├── imports # Contains information about bulk data downloads, like a live version of the datasets folder
│   └── data.gov.json
```

### APIs JSON

Each JSON file in ``/apis`` contains information about how to call the specific API, as well as how to interpret the APIs responses:
```json
{
  "endpoint": "https://api.example.com/data", # REQUIRED: The full endpoint of the API (including query parameters)
  "headers": { # Optional: Headers to include in every request, bearer tokens can be generated (see example)
    "browser-agent": "mozilla", # Example header
    "authorization": # An array of strings/HTTP requests can be passed to any string field to generate auth tokens 
      ["Bearer ", {
        "endpoint": "https://auth.example.com/token", # REQUIRED: Endpoint for auth token generation
        "headers": { # Optional: Headers for the auth token request
          "Content-Type": "application/json",
          "Accept": "application/json"
        },
        "body": # Optional: Body for the auth token request. Note the use of ``env[secret]`` to use environment variables  
            "{\"client_id\": \"env[client-id]\",\"client_secret\": \"env[client-secret]\", \"grant_type\": \"client_credentials\"}",
        "method": "POST", # Optional: Defaults to GET
        "token_key": "credentials.token" # Optional: If excluded, the entire response is treated as the auth token
        }
   ]
  },
  "body" : "filter=True" # Optional: The body for each request
  "column_map": { # REQUIRED: Maps columns in the database to parameters from the response. Transformations can be applied before insertion
    "id": "record_id",
    "name": "full_name",
    "creation_date": { 
      "key": "created_at", # REQUIRED: The key to be transformed
      "transform": { # REQUIRED: Describes the transformation
        "type": "date_format", # REQUIRED: Specifies the transformation
        "from": "YYYY-MM-DDTHH:mm:ssZ",
        "to": "YYYY-MM-DD"
      }
    },
    "item_price": {
      "key": "price",
      "transform": {
        "type": "multiply",
        "factor": 100
      }
    }
  },
  "pagination": { # Optional: If not spec/ified, only one HTTP request is made. If included, only enough parameters to properly describe pagination are required. See [docs/pagination](docspagination) for more details.
    "next_page_url": "links.next", # The key for the next page - Pagination will stop if this key is not found
    "total_records_key": "meta.total", # The key for the total number of records the API will return - Pagination will stop when this number is reached
    "skip_records_param": "offset", # The query parameter that specifies the number of records to skip. Will increment by batch_size until total_records_key is reached
    "batch_size": 100,
    "page_num_param": "page" # The query parameter that specifies a specific page. Will increment until total_records_key is reached
  },
  "rate_limit": { # REQUIRED: Describes the rate limit of the API
    "requests_per_minute": 60,
    "retry_after_header": "Retry-After"
  }
}
```

### Datasets structure

``/datasets`` contains data that needs to be imported from a file. Can be csv, tsv, or xlsx. Each sub folder represents a single data source and must include a ``structure.json`` file describing the format of the data:
```json
{
  "file_names": ["2020census.csv"], # Optional: Defaults to all other files in the folder
  "file_patterns": ["*census.csv"], # Optional: Combines with file_names based on a regex pattern
  "separator": ",", # Optional: For CSV/TSV files - Defaults to "\t" for TSVs, and "," for CSVs/txt 
  "sheet": "sheet1", # Optional: For XLSX files - Which sheet to import from
  "file_encoding": "utf-8", # Optional: Specifies file encoding
  "has_header": true, # Optional: Defaults to false
  "null_values": ["", "N/A", "null"], # Optional: Specifies strings to replace will null
  "column_map": { # REQUIRED: Maps columns from the database to the dataset. Allows for transformation. Can be an array (ie ["id","name","birthdate"]) to use indices instead of column names. Must be an array if has_header is false.
    "id": "record_id",
    "name": "full_name",
    "birth_date": {
      "column": "date_of_birth", # REQUIRED: Specifies which column to transform. OPTIONAL if column_map is an array. 
      "transform": { # REQUIRED: Describes the transformation
        "type": "date_format", # REQUIRED: Specifies the transformation
        "from": "MM/DD/YYYY",
        "to": "YYYY-MM-DD"
      }
    },
    "income": {
      "column": "annual_income",
      "transform": {
        "type": "multiply",
        "factor": 100
      }
    },
    "state": "state_code"
  }
}
```

### Imports structure

Each JSON file in ``/imports`` contains information about how to download and interpret a file. It's like a mix of ``/apis`` and ``/datasets``, since it can pull live data in the form of a dataset.
```json
{
  "endpoint": "https://data.gov/download", # REQUIRED: The URL to download the bulk data file
  "headers": { # Optional: Headers to include in the request
    "browser-agent": "data-combiner", # Example header
    "authorization": "authorization": # An array of strings/HTTP requests can be passed to any string field to generate auth tokens 
      ["Bearer ", {
        "endpoint": "https://auth.example.com/token", # REQUIRED: Endpoint for auth token generation
        "headers": { # Optional: Headers for the auth token request
          "Content-Type": "application/json",
          "Accept": "application/json"
        },
        "body": # Optional: Body for the auth token request. Note the use of ``env[secret]`` to use environment variables  
            "{\"client_id\": \"env[client-id]\",\"client_secret\": \"env[client-secret]\", \"grant_type\": \"client_credentials\"}",
        "method": "POST", # Optional: Defaults to GET
        "token_key": "credentials.token" # Optional: If excluded, the entire response is treated as the auth token
        }
   ]
  },
  "separator": ",", # Optional: The delimiter used in the file (e.g., "," for CSV, "\t" for TSV),
  "sheet": "sheet1", # Optional: For XLSX files - Which sheet to import from
  "file_encoding": "utf-8", # Optional: Specifies the file encoding (default is "utf-8")
  "has_header": true, # Optional: Indicates if the file includes a header row (default is false)
  "null_values": ["", "N/A", "null"], # Optional: Strings to treat as null values
  "column_map": { # REQUIRED: Maps columns in the file to database columns, with optional transformations
    "id": "record_id", # Maps the "id" column in the file to "record_id" in the database
    "name": "full_name", # Maps the "name" column in the file to "full_name" in the database
    "birth_date": { 
      "column": "date_of_birth", # REQUIRED: Specifies the column to transform
      "transform": { # REQUIRED: Describes the transformation
        "type": "date_format", # REQUIRED: Specifies the transformation type
        "from": "MM/DD/YYYY", # REQUIRED: The input date format
        "to": "YYYY-MM-DD" # REQUIRED: The output date format
      }
    },
    "income": {
      "column": "annual_income", # REQUIRED: Specifies the column to transform
      "transform": { # REQUIRED: Describes the transformation
        "type": "multiply", # REQUIRED: Specifies the transformation type
        "factor": 100 # REQUIRED: The factor to multiply by
      }
    },
    "state": "state_code" # Maps the "state" column in the file to "state_code" in the database
  }
}
```

## Installation

1. Clone the repository:
   ```
   git clone <repository-url>
   cd data-combiner
   ```

2. Create a virtual environment:
   ```
   python -m venv .venv
   ```

3. Activate the virtual environment:
   - On Windows:
     ```
     .venv\Scripts\activate
     ```
   - On macOS/Linux:
     ```
     source .venv/bin/activate
     ```

4. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

## Development Environment

This project uses Docker Compose to provide an isolated MariaDB database for development and testing.

### Prerequisites
- Docker and Docker Compose installed on your system

### Starting the Test Database

1. Start the MariaDB container:
   ```bash
   docker-compose up -d
   ```

2. Verify the database is running:
   ```bash
   docker-compose ps
   ```

3. View logs if needed:
   ```bash
   docker-compose logs -f mariadb
   ```

### Database Configuration

Database connection settings are stored in `.env` file:
- **Host:** localhost
- **Port:** 3306
- **Database:** data_combiner_test
- **User:** testuser
- **Password:** testpassword

### Managing the Test Database

- **Stop the database:**
  ```bash
  docker-compose stop
  ```

- **Restart the database:**
  ```bash
  docker-compose restart
  ```

- **Reset the database (removes all data):**
  ```bash
  docker-compose down -v
  docker-compose up -d
  ```

- **Access the database CLI:**
  ```bash
  docker-compose exec mariadb mysql -u testuser -ptestpassword data_combiner_test
  ```

### Initialization Scripts

SQL initialization scripts can be placed in `tests/db/init/` directory. They will run automatically when the container is first created. See `tests/db/init/01-init.sql` for an example.

## Testing

Run tests with pytest:

```bash
# Install test dependencies
pip install -r requirements.txt

# Start the test database
docker-compose up -d

# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/test_database.py
```

## Usage

To run the application, execute the following command:
```
python src/main.py
```

## Contributing

Feel free to submit issues or pull requests for improvements or bug fixes.