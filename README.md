# Flights_ELT
## ELT Pipeline: API to SQL Server with SQLAlchemy & Stored Procedure

A simple demonstration of an **ELT pipeline** that:

- Extracts data from a public REST API.
- Loads data by executing a stored procedure which handles upserts into a SQL Server db.
- Uses a stored procedure to handle transformation.
- Implements SQLAlchemy, logging and retry logic
