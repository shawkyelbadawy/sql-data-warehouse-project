/*
	=============================
	Create Database and Schemas
	=============================
	Script purpose:
		This script creates a database named 'DataWarehouse', And setup a three schemas
		within the database: 'bronze', 'silver' and 'gold'.
*/

--Create database
USE master;
GO
CREATE DATABASE DataWarehouse;
GO
USE DataWarehouse;


-- Create schemas 
CREATE SCHEMA bronze;
GO 
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
