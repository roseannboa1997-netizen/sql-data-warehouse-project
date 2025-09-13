/*
===================================================================
Create Database and Schemas
===================================================================
Script Purpose:
    This scripts creates a new database name 'DataWarehouse' after checking if it already exists.
    If database exists, it is dropped and recreated. Additionally, the script set up three schemas 
    within the database: 'bronze', 'silver' and 'gold'

WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists.
    All data in the database will permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this scripts
*/

Use Master;
GO

-- Drop and Recreate the 'DataWarehouse' database
IF EXISTS (SELECT * FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN 
    ALTER DATABASE Warehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
END;
GO

-- Create Database 'DataWarehouse' database
CREATE DATABASE DataWarehouse;

Use DataWarehouse;

-- Create Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
