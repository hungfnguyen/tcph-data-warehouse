-- Create metabase database and user

-- Create metabase database for Metabase application data
CREATE DATABASE metabase;

-- Create read-only user for Metabase to access your DW
CREATE USER hungfnguyen WITH PASSWORD 'readonly123';
GRANT CONNECT ON DATABASE tcph_dw TO hungfnguyen;
GRANT USAGE ON SCHEMA public TO hungfnguyen;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO hungfnguyen;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO hungfnguyen;

-- Grant future tables (for new tables created by dbt)
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO hungfnguyen;