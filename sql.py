import os
import logging
from dotenv import load_dotenv
import databases
import sqlalchemy
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Load the DATABASE_URL from environment variables

# Explicitly load the .env file from the current project directory
ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../.env")
load_dotenv(dotenv_path=ENV_PATH)

DATABASE_URL = os.getenv("DATABASE_URI")

if not DATABASE_URL:
    logger.error("DATABASE_URI not found in environment variables.")
    raise ValueError("DATABASE_URI not found in environment variables. Ensure it is set in the .env file.")


# Initialize the database connection and metadata
try:
    database = databases.Database(DATABASE_URL)
    metadata = sqlalchemy.MetaData()
    logger.info("Database connection initialized successfully.")
except Exception as e:
    logger.error("Failed to initialize database connection: %s", e)
    raise

# Define rca_results table
rca_results = sqlalchemy.Table(
    "rca_results",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column("filename", sqlalchemy.String, nullable=False),
    sqlalchemy.Column("anomaly", sqlalchemy.Boolean, nullable=False),
    sqlalchemy.Column("details", sqlalchemy.Text, nullable=True),
)

# Insert anomaly result into rca_results table
async def insert_rca_result(rca_result):
    query = rca_results.insert().values(
        filename=rca_result["filename"],
        anomaly=rca_result["anomaly"],
        details=rca_result.get("details", "")
    )
    await database.execute(query)

# Create async SQLAlchemy engine and session for PostgreSQL
ASYNC_DATABASE_URL = DATABASE_URL.replace('postgresql://', 'postgresql+asyncpg://')
engine = create_async_engine(ASYNC_DATABASE_URL, echo=False, future=True)
async_session = sessionmaker(
    engine, expire_on_commit=False, class_=AsyncSession
)

# Helper function to initialize database
async def connect_to_database():
    """
    Connects to the database when the application starts.
    """
    try:
        await database.connect()
        logger.info("Successfully connected to the database.")
    except Exception as e:
        logger.error("Error connecting to the database: %s", e)
        raise

# Helper function to disconnect database
async def disconnect_from_database():
    """
    Disconnects from the database when the application shuts down.
    """
    try:
        await database.disconnect()
        logger.info("Successfully disconnected from the database.")
    except Exception as e:
        logger.error("Error disconnecting from the database: %s", e)
        raise