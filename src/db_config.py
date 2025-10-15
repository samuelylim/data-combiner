"""Database configuration and connection management."""
import os
from typing import Optional
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Load environment variables from .env file
load_dotenv()


class DatabaseConfig:
    """Database configuration class."""
    
    def __init__(self):
        self.host = os.getenv('DB_HOST', 'localhost')
        self.port = int(os.getenv('DB_PORT', 3306))
        self.database = os.getenv('DB_NAME', 'data_combiner_test')
        self.user = os.getenv('DB_USER', 'testuser')
        self.password = os.getenv('DB_PASSWORD', 'testpassword')

def get_sqlalchemy_engine():
    """Create a SQLAlchemy engine."""
    config = DatabaseConfig()
    return create_engine(f"mysql+pymysql://{config.user}:{config.password}@{config.host}:{config.port}/{config.database}")
