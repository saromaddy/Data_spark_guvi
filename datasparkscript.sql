create database Data_spark;
use data_spark;
select * from customer;

# Demographic distribution

SELECT
    Gender,
    COUNT(*) AS Total_Customers,
    AVG(Age) AS Average_Age,
    City,
    State_x AS State,
    Country_x AS Country,
    Continent
FROM
    MergedData
GROUP BY
    Gender, City, State_x, Country_x, Continent
ORDER BY
    Total_Customers DESC;
    
#customer purchase patterns

SELECT
    Customer_ID,
    COUNT(DISTINCT `Order Number`) AS Purchase_Frequency,
    AVG(`Unit Price USD` * Quantity) AS Average_Order_Value,
    GROUP_CONCAT(DISTINCT `Product Name`) AS Preferred_Products
FROM
    MergedData
GROUP BY
    Customer_ID;

#overall Sales performance

SELECT
    MONTH(`Order Date`) AS Month,
    YEAR(`Order Date`) AS Year,
    SUM(`Unit Price USD` * Quantity) AS Total_Sales
FROM
    MergedData
GROUP BY
    Year, Month
ORDER BY
    Year, Month;

# Sales by Product
SELECT
    `Product Name`,
    SUM(Quantity) AS Total_Quantity_Sold,
    SUM(`Unit Price USD` * Quantity) AS Total_Revenue
FROM
    MergedData
GROUP BY
    `Product Name`
ORDER BY
    Total_Revenue DESC;

#sales by store
SELECT
    StoreKey,
    SUM(`Unit Price USD` * Quantity) AS Total_Sales,
    AVG(`Square Meters`) AS Average_Store_Size
FROM
    MergedData
GROUP BY
    StoreKey
ORDER BY
    Total_Sales DESC;

#sales by currency

SELECT
    `Currency Code`,
    SUM(`Unit Price USD` * Quantity) AS Total_Sales,
    AVG(`Unit Cost USD`) AS Average_Cost,
    AVG(`Unit Price USD`) AS Average_Price
FROM
    MergedData
GROUP BY
    `Currency Code`
ORDER BY
    Total_Sales DESC;
    
# sales seasonality analysis

SELECT
    DATE_FORMAT(`Order Date`, '%Y-%m') AS Month,
    SUM(`Unit Price USD` * Quantity) AS Total_Sales
FROM
    MergedData
GROUP BY
    Month
ORDER BY
    Month;