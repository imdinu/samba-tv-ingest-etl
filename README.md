# samba-tv-ingest-etl

Snowflake SQL scripts to ingest Samba TV data from an S3 bucket and apply transformations to apply sessionization and obtain reach and frequency

The following files are part of the project:

- `aws-s3-integration.sql`: Sets up the AWS S3 integration for Snowflake.
- `ingest-ddl.sql`: Defines the data structures for ingesting Samba TV data.
- `ingest-backfill.sql`: Contains SQL commands to backfill data into the Samba TV raw tables.
- `etl-ddl-backfill.sql`: Contains the DDL for the ETL process, including task control tables.
- `etl-dag.sql`: Defines the ETL workflow using tasks and dependencies.
