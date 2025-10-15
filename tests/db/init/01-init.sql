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
-- SCHEMA DEFINITIONS
-- ============================================================================
-- Define your tables, indexes, and constraints here
-- These should be the same across all environments

-- Example: Create a sample table
-- Uncomment and modify as needed for your project

-- CREATE TABLE IF NOT EXISTS data_sources (
--     id INT AUTO_INCREMENT PRIMARY KEY,
--     name VARCHAR(255) NOT NULL,
--     source_type ENUM('api', 'dataset', 'import') NOT NULL,
--     config_path VARCHAR(500),
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
--     INDEX idx_source_type (source_type),
--     INDEX idx_created_at (created_at)
-- );

-- CREATE TABLE IF NOT EXISTS data_records (
--     id INT AUTO_INCREMENT PRIMARY KEY,
--     source_id INT,
--     data JSON,
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--     FOREIGN KEY (source_id) REFERENCES data_sources(id) ON DELETE CASCADE,
--     INDEX idx_source_id (source_id),
--     INDEX idx_created_at (created_at)
-- );
