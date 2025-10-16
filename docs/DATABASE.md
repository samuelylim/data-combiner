# Database Implementation

## Overview

The data-combiner project includes a complete database backend with support for:
- Dynamic schema generation based on source configurations
- Intelligent upsert operations (INSERT or UPDATE)
- Source tracking and citations
- Configurable unique keys for record identification

## Architecture

### Database Schema

The application creates three main tables in a configurable schema (default: `data_combiner`):

#### 1. `sources` Table
Stores metadata about each data source:
- `id`: Auto-incrementing primary key
- `name`: Unique name of the source
- `source_type`: Type of source ('api', 'dataset', 'import')
- `config_path`: Path to the source configuration file
- `unique_keys`: JSON array of column names used to determine record uniqueness
- `created_at`, `updated_at`: Timestamps

#### 2. `data` Table
Stores the actual data records with dynamic columns:
- `id`: Auto-incrementing primary key
- `created_at`, `updated_at`: Timestamps
- **Dynamic columns**: All columns from `column_map` definitions across all sources

#### 3. `citations` Table
Links data records to their sources (many-to-many):
- `id`: Auto-incrementing primary key
- `data_id`: Foreign key to `data.id`
- `source_id`: Foreign key to `sources.id`
- `created_at`: Timestamp
- Unique index on (`data_id`, `source_id`) to prevent duplicate citations

## Configuration

### Environment Variables

Create a `.env` file based on `.env.example`:

```bash
# Database connection
DB_HOST=localhost
DB_PORT=3306
DB_NAME=data_combiner_test
DB_USER=testuser
DB_PASSWORD=testpassword

# Application schema name
APP_SCHEMA=data_combiner
```

### Source Configuration

Each source configuration (API, dataset, import) can include a `unique_keys` field:

```json
{
  "endpoint": "https://api.example.com/data",
  "column_map": {
    "business_name": "name",
    "license_number": "license_num",
    "address": "street_address"
  },
  "unique_keys": ["license_number"]
}
```

**How `unique_keys` works:**
- Defines which columns determine if a record is "the same" as an existing record
- If a record with matching unique key values exists, it will be **updated**
- If no match exists, a new record will be **inserted**
- If not specified, all non-null columns in the record are used

## Use Cases

### Example 1: Merging Incomplete Data

**Scenario:** A dataset has business names and license numbers, but an API provides addresses for the same businesses.

**Dataset source:**
```json
{
  "column_map": {
    "business_name": "name",
    "license_number": "license_num"
  },
  "unique_keys": ["license_number"]
}
```

**API source:**
```json
{
  "column_map": {
    "license_number": "license_num",
    "address": "street_address"
  },
  "unique_keys": ["license_number"]
}
```

**Result:** Records with the same license number are merged, with the API filling in missing address information.

### Example 2: Tracking Multiple Sources

The `citations` table tracks which sources contributed to each record:

```sql
-- Find all sources that provided data about record #123
SELECT s.name, s.source_type, c.created_at
FROM citations c
JOIN sources s ON c.source_id = s.id
WHERE c.data_id = 123;
```

### Example 3: Composite Unique Keys

Use multiple columns to identify unique records:

```json
{
  "column_map": {
    "first_name": "fname",
    "last_name": "lname",
    "date_of_birth": "dob",
    "city": "city"
  },
  "unique_keys": ["first_name", "last_name", "date_of_birth"]
}
```

### `DatabaseManager`

Main class for database operations.

#### `initialize_schema(columns: List[str], force_recreate: bool = False)`
Initialize the database schema with required tables.

#### `register_source(source_name: str, source_type: str, config_path: Optional[str], unique_keys: Optional[List[str]]) -> int`
Register a data source or retrieve its ID if already exists.

#### `upsert_record(record: Dict[str, Any], source_name: str, unique_keys: Optional[List[str]]) -> int`
Insert or update a data record and create a citation.

#### `get_schema_info() -> Dict[str, Any]`
Get information about the current database schema.

## Future Enhancements

Potential improvements:
- Add indexes on frequently queried columns
- Support for soft deletes
- Record versioning/history tracking
- Batch insert optimization for large datasets
- Support for other database backends (PostgreSQL, SQLite)
