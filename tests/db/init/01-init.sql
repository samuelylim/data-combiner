-- ============================================================================
-- Database Schema Initialization
-- ============================================================================
-- This script defines the database schema that should be consistent across
-- all environments (dev, test, staging, production).
-- 
-- IMPORTANT: Only include schema definitions here, NOT environment-specific
-- test data. Test data should go in separate seed files.
-- ============================================================================

USE data_combiner_test;

-- ============================================================================
-- APPLICATION SCHEMA
-- ============================================================================
-- The application will create its own schema with the name defined in APP_SCHEMA
-- environment variable (default: 'data_combiner'). This script just ensures
-- the base database exists. The application's DatabaseManager will handle
-- creating the actual tables: sources, data, and citations.

-- Note: The application tables will be created dynamically based on the
-- column_map configurations in your source files. Run main.py to initialize
-- the schema with all required columns.
