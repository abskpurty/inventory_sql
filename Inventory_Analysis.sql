USE inventory;

CREATE TABLE SalesByBrands(
    Brand VARCHAR(35),
    Description VARCHAR(35),
    Price DOUBLE,
    Size VARCHAR(20),
    Volume VARCHAR(10),
    Classification INT,
    PurchasePrice DOUBLE,
    VendorNumber INT,
    VendorName VARCHAR(40)
);

LOAD DATA INFILE '2017PurchasePricesDec.tsv'
INTO TABLE SalesByBrands
FIELDS TERMINATED BY '\t'
IGNORE 1 ROWS;

SHOW VARIABLES LIKE "secure_file_priv";

DROP TABLE IF EXISTS inventory_end;

SELECT * FROM SalesByBrands
WHERE Brand = 90025;

CREATE TABLE inventory_begin(
    InventoryID VARCHAR(35),
    Store INT,
    City VARCHAR(35),
    Brand INT,
    Description VARCHAR(35),
    Size VARCHAR(20),
    Onhand INT,
    Price DOUBLE,
    StartDate DATE
);

LOAD DATA INFILE 'BegInvFINAL12312016.tsv'
INTO TABLE inventory_begin
FIELDS TERMINATED BY '\t'
IGNORE 1 ROWS;

SELECT * FROM inventory_begin
LIMIT 5;

CREATE TABLE inventory_end(
    InventoryID VARCHAR(35),
    Store INT,
    City VARCHAR(35),
    Brand INT,
    Description VARCHAR(35),
    Size VARCHAR(20),
    Onhand INT,
    Price DOUBLE,
    EndDate DATE
);

LOAD DATA INFILE 'EndInvFINAL12312016.tsv'
INTO TABLE inventory_end
FIELDS TERMINATED BY '\t'
IGNORE 1 ROWS;

CREATE TABLE InvoicePurchases(
    VendorNum INT,
    VendorName VARCHAR(35),
    InvoiceDate VARCHAR(20),
    PONumber INT,
    PODate VARCHAR(20),
    PayDate VARCHAR(20),
    Quantity INT,
    PayAmount DOUBLE,
    Freight DOUBLE,
    Approval VARCHAR(20)
);

LOAD DATA INFILE 'InvoicePurchases12312016.tsv'
INTO TABLE InvoicePurchases
FIELDS TERMINATED BY '\t'
IGNORE 1 ROWS;

CREATE TABLE Purchases(
    InventoryID VARCHAR(35),
    Store INT,
    Brand INT,
    Description VARCHAR(35),
    Size VARCHAR(20),
    VendorNumber INT,
    VendorName VARCHAR(40),
    PONumber INT,
    PODate DATE,
    ReceivingDate DATE,
    InvoiceDate DATE,
    PayDate DATE,
    PurchasePrice DOUBLE,
    Quantity INT,
    PayAmount DOUBLE,
    Classification INT
);

LOAD DATA INFILE 'PurchasesFINAL12312016.tsv'
INTO TABLE Purchases
FIELDS TERMINATED BY '\t'
IGNORE 1 ROWS;

SELECT COUNT(*) FROM Purchases;

CREATE TABLE Sales(
    InventoryID VARCHAR(35),
    Store INT,
    Brand INT,
    Description VARCHAR(35),
    Size VARCHAR(20),
    SalesQuantity INT,
    SalesTotal DOUBLE,
    SalesPrice DOUBLE,
    SalesDate VARCHAR(15),
    Volume VARCHAR(15),
    Classification INT,
    ExciseTax DOUBLE,
    VendorNumber INT,
    VendorName VARCHAR(40)
);

LOAD DATA INFILE 'SalesFINAL12312016.tsv'
INTO TABLE Sales
FIELDS TERMINATED BY '\t'
IGNORE 1 ROWS;

SELECT COUNT(*) FROM Sales;

SELECT * FROM Purchases
LIMIT 2;

SELECT COUNT(DISTINCT VendorNumber) FROM SalesByBrands;

SELECT DISTINCT VendorNum FROM InvoicePurchases
ORDER BY VendorNum DESC;

SELECT COUNT(*) FROM InvoicePurchases
WHERE (PayDate, InvoiceDate, PONumber, VendorNum) 
NOT IN (SELECT PayDate, InvoiceDate, PONumber, VendorNumber FROM Purchases);

SELECT COUNT(*) FROM InvoicePurchases
WHERE (PONumber, VendorNum) 
NOT IN (SELECT PONumber, VendorNumber FROM Purchases);

SELECT *
FROM InvoicePurchases ip
LEFT JOIN Purchases p
ON ip.PayDate = p.PayDate
AND ip.InvoiceDate = p.InvoiceDate
AND ip.PONumber = p.PONumber
AND ip.VendorNum = p.VendorNumber
WHERE p.PayDate IS NULL;

SELECT COUNT(*) FROM InvoicePurchases;

SELECT SUM(Quantity) FROM Purchases
WHERE PONumber = 8106
GROUP BY PONumber;

SELECT * FROM inventory_end e
LEFT JOIN inventory_begin b
USING(InventoryID)
WHERE b.Store IS NULL AND e.Store = 81;

SELECT * FROM inventory_end
WHERE Store > 79;

SELECT InventoryID, sp - ss AS Q FROM
(SELECT InventoryID, SUM(Quantity) AS sp FROM Purchases GROUP BY InventoryID) AS p
JOIN
(SELECT InventoryID, SUM(SalesQuantity) AS ss FROM Sales GROUP BY InventoryID) AS s
USING(InventoryID);

SELECT InventoryID FROM Purchases WHERE InventoryID = '1_HARDERSFIELD_10021';

SELECT DISTINCT InventoryID, SUM(Onhand) 
FROM inventory_end 
WHERE InventoryID = '1_HARDERSFIELD_10021'
GROUP BY InventoryID;

SELECT COUNT(DISTINCT InventoryID) FROM inventory_begin;

SET SQL_SAFE_UPDATES = 0;
UPDATE Sales SET SalesDate = REPLACE(SalesDate, '/', '-');
UPDATE Sales SET SalesDate = STR_TO_DATE(SalesDate, '%m-%d-%Y');
SET SQL_SAFE_UPDATES = 1;
ALTER TABLE Sales MODIFY SalesDate DATE;

DESCRIBE Sales;

SELECT COUNT(DISTINCT City) FROM inventory_begin;

SELECT COUNT(DISTINCT Brand) FROM inventory_begin; 
SELECT COUNT(DISTINCT Brand) FROM Purchases;       
SELECT COUNT(DISTINCT Brand) FROM Sales;           

SET @maxdate = (SELECT MAX(SalesDate) FROM Sales);
SET @mindate = (SELECT MIN(SalesDate) FROM Sales);

SELECT @maxdate;
SELECT @mindate;

CREATE TABLE purchase_ AS
SELECT * FROM Purchases WHERE ReceivingDate BETWEEN @mindate AND @maxdate;

CREATE TABLE invoicepurchases_ AS
SELECT * FROM InvoicePurchases WHERE PONumber IN (SELECT PONumber FROM purchase_);

SELECT Brand, ROUND(SUM(POWER(PurchasePrice - MeanPrice, 2)),3) / COUNT(*) AS Variance
FROM (
    SELECT Brand, PurchasePrice, AVG(PurchasePrice) OVER (PARTITION BY Brand) AS MeanPrice
    FROM purchase_
) AS Subquery
GROUP BY Brand;

SELECT Store, Brand, ROUND(SUM(POWER(SalesPrice - MeanPrice, 2)) / COUNT(*),3) AS Variance
FROM (
    SELECT Store, Brand, SalesPrice, AVG(SalesPrice) OVER (PARTITION BY Store,Brand) AS MeanPrice
    FROM Sales
) AS Subquery
GROUP BY Store, Brand;

SELECT Store, Brand, SalesPrice, SalesDate 
FROM Sales 
WHERE Brand = 62 
ORDER BY SalesDate;

SELECT Brand, ROUND(SUM(SalesTotal),2) AS Revenue
FROM Sales
GROUP BY Brand
ORDER BY Revenue DESC;

SELECT Store, ROUND(SUM(SalesTotal),2) AS Revenue
FROM Sales
GROUP BY Store
ORDER BY Revenue DESC;

SELECT DAYOFWEEK(SalesDate), SUM(SalesQuantity), SUM(SalesTotal)
FROM Sales
GROUP BY DAYOFWEEK(SalesDate);

CREATE TEMPORARY TABLE StockOut
SELECT *,
SUM(Quantity) OVER (PARTITION BY InventoryID ORDER BY Date, Pref ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS CurrentStock
FROM(
    (SELECT InventoryID, Onhand AS Quantity, StartDate AS Date,
