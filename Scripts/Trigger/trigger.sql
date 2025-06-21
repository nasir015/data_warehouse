/*
This script implements a centralized audit logging system for the bronze.crm_cust_info table in the DataWarehouse database. The process involves creating an audit_log table that records all INSERT, UPDATE, and DELETE operations performed on the bronze.crm_cust_info table. A trigger is set up to capture these operations and store them in the audit log for tracking changes to customer data.
*/


/* 0. Cleanup: Drop audit log tables if they exist */
DROP TABLE IF EXISTS bronze.audit_log;
DROP TABLE IF EXISTS silver.audit_log;
GO

USE DataWarehouse;
GO

/* 1. Create audit log tables with JSON row data */
CREATE TABLE bronze.audit_log (
    LogID         INT IDENTITY(1,1) PRIMARY KEY,
    TableName     SYSNAME,
    OperationType VARCHAR(10),
    RecordPK      VARCHAR(100),
    LogMessage    VARCHAR(255),
    LogDate       DATETIME DEFAULT GETDATE(),
    LogData       NVARCHAR(MAX)
);
GO

CREATE TABLE silver.audit_log (
    LogID         INT IDENTITY(1,1) PRIMARY KEY,
    TableName     SYSNAME,
    OperationType VARCHAR(10),
    RecordPK      VARCHAR(100),
    LogMessage    VARCHAR(255),
    LogDate       DATETIME DEFAULT GETDATE(),
    LogData       NVARCHAR(MAX)
);
GO

/* ----------- BRONZE TRIGGERS (6 tables) ----------- */

/* bronze.crm_cust_info */
CREATE OR ALTER TRIGGER trg_audit_bronze_crm_cust_info ON bronze.crm_cust_info
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    -- INSERT
    IF EXISTS (SELECT * FROM inserted) AND NOT EXISTS (SELECT * FROM deleted)
        INSERT INTO bronze.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'crm_cust_info', 'INSERT', CAST(i.cst_key AS VARCHAR(100)),
            'New customer added. Cust_key = ' + CAST(i.cst_key AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM inserted i2 WHERE i2.cst_key = i.cst_key FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM inserted i;
    -- UPDATE
    IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
        INSERT INTO bronze.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'crm_cust_info', 'UPDATE', CAST(i.cst_key AS VARCHAR(100)),
            'Customer updated. Cust_key = ' + CAST(i.cst_key AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM inserted i2 WHERE i2.cst_key = i.cst_key FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM inserted i;
    -- DELETE
    IF EXISTS (SELECT * FROM deleted) AND NOT EXISTS (SELECT * FROM inserted)
        INSERT INTO bronze.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'crm_cust_info', 'DELETE', CAST(d.cst_key AS VARCHAR(100)),
            'Customer deleted. Cust_key = ' + CAST(d.cst_key AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM deleted d2 WHERE d2.cst_key = d.cst_key FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM deleted d;
END
GO

/* bronze.crm_prd_info */
CREATE OR ALTER TRIGGER trg_audit_bronze_crm_prd_info ON bronze.crm_prd_info
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT * FROM inserted) AND NOT EXISTS (SELECT * FROM deleted)
        INSERT INTO bronze.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'crm_prd_info', 'INSERT', CAST(i.prd_key AS VARCHAR(100)),
            'New product added. Product Key = ' + CAST(i.prd_key AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM inserted i2 WHERE i2.prd_key = i.prd_key FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM inserted i;
    IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
        INSERT INTO bronze.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'crm_prd_info', 'UPDATE', CAST(i.prd_key AS VARCHAR(100)),
            'Product updated. Product Key = ' + CAST(i.prd_key AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM inserted i2 WHERE i2.prd_key = i.prd_key FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM inserted i;
    IF EXISTS (SELECT * FROM deleted) AND NOT EXISTS (SELECT * FROM inserted)
        INSERT INTO bronze.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'crm_prd_info', 'DELETE', CAST(d.prd_key AS VARCHAR(100)),
            'Product deleted. Product Key = ' + CAST(d.prd_key AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM deleted d2 WHERE d2.prd_key = d.prd_key FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM deleted d;
END
GO

/* bronze.crm_sales_details */
CREATE OR ALTER TRIGGER trg_audit_bronze_crm_sales_details ON bronze.crm_sales_details
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT * FROM inserted) AND NOT EXISTS (SELECT * FROM deleted)
        INSERT INTO bronze.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'crm_sales_details', 'INSERT', CAST(i.sls_prd_key AS VARCHAR(100)),
            'New sales info added. Sales product key = ' + CAST(i.sls_prd_key AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM inserted i2 WHERE i2.sls_prd_key = i.sls_prd_key FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM inserted i;
    IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
        INSERT INTO bronze.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'crm_sales_details', 'UPDATE', CAST(i.sls_prd_key AS VARCHAR(100)),
            'Sales info updated. Sales product key = ' + CAST(i.sls_prd_key AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM inserted i2 WHERE i2.sls_prd_key = i.sls_prd_key FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM inserted i;
    IF EXISTS (SELECT * FROM deleted) AND NOT EXISTS (SELECT * FROM inserted)
        INSERT INTO bronze.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'crm_sales_details', 'DELETE', CAST(d.sls_prd_key AS VARCHAR(100)),
            'Sales info deleted. Sales product key = ' + CAST(d.sls_prd_key AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM deleted d2 WHERE d2.sls_prd_key = d.sls_prd_key FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM deleted d;
END
GO

/* bronze.erp_cust_az12 */
CREATE OR ALTER TRIGGER trg_audit_bronze_erp_cust_az12 ON bronze.erp_cust_az12
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT * FROM inserted) AND NOT EXISTS (SELECT * FROM deleted)
        INSERT INTO bronze.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'erp_cust_az12', 'INSERT', CAST(i.cid AS VARCHAR(100)),
            'New customer added. Cust_ID = ' + CAST(i.cid AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM inserted i2 WHERE i2.cid = i.cid FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM inserted i;
    IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
        INSERT INTO bronze.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'erp_cust_az12', 'UPDATE', CAST(i.cid AS VARCHAR(100)),
            'Customer updated. Cust_ID = ' + CAST(i.cid AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM inserted i2 WHERE i2.cid = i.cid FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM inserted i;
    IF EXISTS (SELECT * FROM deleted) AND NOT EXISTS (SELECT * FROM inserted)
        INSERT INTO bronze.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'erp_cust_az12', 'DELETE', CAST(d.cid AS VARCHAR(100)),
            'Customer deleted. Cust_ID = ' + CAST(d.cid AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM deleted d2 WHERE d2.cid = d.cid FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM deleted d;
END
GO

/* bronze.erp_loc_a101 */
CREATE OR ALTER TRIGGER trg_audit_bronze_erp_loc_a101 ON bronze.erp_loc_a101
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT * FROM inserted) AND NOT EXISTS (SELECT * FROM deleted)
        INSERT INTO bronze.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'erp_loc_a101', 'INSERT', CAST(i.cid AS VARCHAR(100)),
            'New location added. Cust_ID = ' + CAST(i.cid AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM inserted i2 WHERE i2.cid = i.cid FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM inserted i;
    IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
        INSERT INTO bronze.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'erp_loc_a101', 'UPDATE', CAST(i.cid AS VARCHAR(100)),
            'Location updated. Cust_ID = ' + CAST(i.cid AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM inserted i2 WHERE i2.cid = i.cid FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM inserted i;
    IF EXISTS (SELECT * FROM deleted) AND NOT EXISTS (SELECT * FROM inserted)
        INSERT INTO bronze.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'erp_loc_a101', 'DELETE', CAST(d.cid AS VARCHAR(100)),
            'Location deleted. Cust_ID = ' + CAST(d.cid AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM deleted d2 WHERE d2.cid = d.cid FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM deleted d;
END
GO

/* bronze.erp_px_cat_g1v2 */
CREATE OR ALTER TRIGGER trg_audit_bronze_erp_px_cat_g1v2 ON bronze.erp_px_cat_g1v2
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT * FROM inserted) AND NOT EXISTS (SELECT * FROM deleted)
        INSERT INTO bronze.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'erp_px_cat_g1v2', 'INSERT', CAST(i.id AS VARCHAR(100)),
            'New product added. id = ' + CAST(i.id AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM inserted i2 WHERE i2.id = i.id FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM inserted i;
    IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
        INSERT INTO bronze.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'erp_px_cat_g1v2', 'UPDATE', CAST(i.id AS VARCHAR(100)),
            'Product updated. id = ' + CAST(i.id AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM inserted i2 WHERE i2.id = i.id FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM inserted i;
    IF EXISTS (SELECT * FROM deleted) AND NOT EXISTS (SELECT * FROM inserted)
        INSERT INTO bronze.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'erp_px_cat_g1v2', 'DELETE', CAST(d.id AS VARCHAR(100)),
            'Product deleted. id = ' + CAST(d.id AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM deleted d2 WHERE d2.id = d.id FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM deleted d;
END
GO

/* ----------- SILVER TRIGGERS (6 tables) ----------- */

/* silver.crm_cust_info */
CREATE OR ALTER TRIGGER trg_audit_silver_crm_cust_info ON silver.crm_cust_info
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT * FROM inserted) AND NOT EXISTS (SELECT * FROM deleted)
        INSERT INTO silver.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'crm_cust_info', 'INSERT', CAST(i.cst_key AS VARCHAR(100)),
            'New customer info added. Customer key = ' + CAST(i.cst_key AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM inserted i2 WHERE i2.cst_key = i.cst_key FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM inserted i;
    IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
        INSERT INTO silver.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'crm_cust_info', 'UPDATE', CAST(i.cst_key AS VARCHAR(100)),
            'Customer info updated. Customer key = ' + CAST(i.cst_key AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM inserted i2 WHERE i2.cst_key = i.cst_key FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM inserted i;
    IF EXISTS (SELECT * FROM deleted) AND NOT EXISTS (SELECT * FROM inserted)
        INSERT INTO silver.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'crm_cust_info', 'DELETE', CAST(d.cst_key AS VARCHAR(100)),
            'Customer info deleted. Customer key = ' + CAST(d.cst_key AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM deleted d2 WHERE d2.cst_key = d.cst_key FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM deleted d;
END
GO

/* silver.crm_prd_info */
CREATE OR ALTER TRIGGER trg_audit_silver_crm_prd_info ON silver.crm_prd_info
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT * FROM inserted) AND NOT EXISTS (SELECT * FROM deleted)
        INSERT INTO silver.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'crm_prd_info', 'INSERT', CAST(i.prd_key AS VARCHAR(100)),
            'New product info added. Product key = ' + CAST(i.prd_key AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM inserted i2 WHERE i2.prd_key = i.prd_key FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM inserted i;
    IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
        INSERT INTO silver.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'crm_prd_info', 'UPDATE', CAST(i.prd_key AS VARCHAR(100)),
            'Product info updated. Product key = ' + CAST(i.prd_key AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM inserted i2 WHERE i2.prd_key = i.prd_key FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM inserted i;
    IF EXISTS (SELECT * FROM deleted) AND NOT EXISTS (SELECT * FROM inserted)
        INSERT INTO silver.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'crm_prd_info', 'DELETE', CAST(d.prd_key AS VARCHAR(100)),
            'Product info deleted. Product key = ' + CAST(d.prd_key AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM deleted d2 WHERE d2.prd_key = d.prd_key FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM deleted d;
END
GO

/* silver.crm_sales_details */
CREATE OR ALTER TRIGGER trg_audit_silver_crm_sales_details ON silver.crm_sales_details
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT * FROM inserted) AND NOT EXISTS (SELECT * FROM deleted)
        INSERT INTO silver.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'crm_sales_details', 'INSERT', CAST(i.sls_prd_key AS VARCHAR(100)),
            'New sales info added. Product sales key = ' + CAST(i.sls_prd_key AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM inserted i2 WHERE i2.sls_prd_key = i.sls_prd_key FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM inserted i;
    IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
        INSERT INTO silver.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'crm_sales_details', 'UPDATE', CAST(i.sls_prd_key AS VARCHAR(100)),
            'Sales info updated. Product sales key = ' + CAST(i.sls_prd_key AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM inserted i2 WHERE i2.sls_prd_key = i.sls_prd_key FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM inserted i;
    IF EXISTS (SELECT * FROM deleted) AND NOT EXISTS (SELECT * FROM inserted)
        INSERT INTO silver.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'crm_sales_details', 'DELETE', CAST(d.sls_prd_key AS VARCHAR(100)),
            'Sales info deleted. Product sales key = ' + CAST(d.sls_prd_key AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM deleted d2 WHERE d2.sls_prd_key = d.sls_prd_key FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM deleted d;
END
GO

/* silver.erp_cust_az12 */
CREATE OR ALTER TRIGGER trg_audit_silver_erp_cust_az12 ON silver.erp_cust_az12
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT * FROM inserted) AND NOT EXISTS (SELECT * FROM deleted)
        INSERT INTO silver.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'erp_cust_az12', 'INSERT', CAST(i.cid AS VARCHAR(100)),
            'New customer info added. Customer ID = ' + CAST(i.cid AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM inserted i2 WHERE i2.cid = i.cid FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM inserted i;
    IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
        INSERT INTO silver.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'erp_cust_az12', 'UPDATE', CAST(i.cid AS VARCHAR(100)),
            'Customer info updated. Customer ID = ' + CAST(i.cid AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM inserted i2 WHERE i2.cid = i.cid FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM inserted i;
    IF EXISTS (SELECT * FROM deleted) AND NOT EXISTS (SELECT * FROM inserted)
        INSERT INTO silver.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'erp_cust_az12', 'DELETE', CAST(d.cid AS VARCHAR(100)),
            'Customer info deleted. Customer ID = ' + CAST(d.cid AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM deleted d2 WHERE d2.cid = d.cid FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM deleted d;
END
GO

/* silver.erp_loc_a101 */
CREATE OR ALTER TRIGGER trg_audit_silver_erp_loc_a101 ON silver.erp_loc_a101
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT * FROM inserted) AND NOT EXISTS (SELECT * FROM deleted)
        INSERT INTO silver.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'erp_loc_a101', 'INSERT', CAST(i.cid AS VARCHAR(100)),
            'New location info added. Customer ID = ' + CAST(i.cid AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM inserted i2 WHERE i2.cid = i.cid FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM inserted i;
    IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
        INSERT INTO silver.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'erp_loc_a101', 'UPDATE', CAST(i.cid AS VARCHAR(100)),
            'Location info updated. Customer ID = ' + CAST(i.cid AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM inserted i2 WHERE i2.cid = i.cid FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM inserted i;
    IF EXISTS (SELECT * FROM deleted) AND NOT EXISTS (SELECT * FROM inserted)
        INSERT INTO silver.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'erp_loc_a101', 'DELETE', CAST(d.cid AS VARCHAR(100)),
            'Location info deleted. Customer ID = ' + CAST(d.cid AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM deleted d2 WHERE d2.cid = d.cid FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM deleted d;
END
GO

/* silver.erp_px_cat_g1v2 */
CREATE OR ALTER TRIGGER trg_audit_silver_erp_px_cat_g1v2 ON silver.erp_px_cat_g1v2
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT * FROM inserted) AND NOT EXISTS (SELECT * FROM deleted)
        INSERT INTO silver.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'erp_px_cat_g1v2', 'INSERT', CAST(i.id AS VARCHAR(100)),
            'New product info added. Product ID = ' + CAST(i.id AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM inserted i2 WHERE i2.id = i.id FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM inserted i;
    IF EXISTS (SELECT * FROM inserted) AND EXISTS (SELECT * FROM deleted)
        INSERT INTO silver.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'erp_px_cat_g1v2', 'UPDATE', CAST(i.id AS VARCHAR(100)),
            'Product info updated. Product ID = ' + CAST(i.id AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM inserted i2 WHERE i2.id = i.id FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM inserted i;
    IF EXISTS (SELECT * FROM deleted) AND NOT EXISTS (SELECT * FROM inserted)
        INSERT INTO silver.audit_log
            (TableName, OperationType, RecordPK, LogMessage, LogDate, LogData)
        SELECT 'erp_px_cat_g1v2', 'DELETE', CAST(d.id AS VARCHAR(100)),
            'Product info deleted. Product ID = ' + CAST(d.id AS VARCHAR(100)), GETDATE(),
            (SELECT * FROM deleted d2 WHERE d2.id = d.id FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM deleted d;
END
GO