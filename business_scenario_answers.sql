  -- TASK 1
  SELECT country_code, COUNT(*) AS total_no_stores
  FROM dim_store_details
  WHERE NOT (country_code = 'GB' AND store_type = 'Web Portal')
  GROUP BY country_code
  ORDER BY total_no_stores DESC;

-----------------------------------------------------------------------------
--SELECT * FROM orders_table
-----------------------------------------------------------------------------
  --TASK 2
  SELECT locality, COUNT(*) AS total_no_stores
  FROM dim_store_details
  GROUP BY locality
  ORDER BY total_no_stores DESC
  LIMIT 7;
------------------------------------------------------------------------------
  --TASK 3
  SELECT dim_date_times.month, ROUND(SUM(dim_products.product_price * orders_table.product_quantity)::numeric,2) AS total_sales
  FROM orders_table
  JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
  JOIN dim_products ON orders_table.product_code = dim_products.product_code
  GROUP BY dim_date_times.month
  ORDER BY total_sales DESC
  LIMIT 6;
-------------------------------------------------------------------------------
--TASK 4
  SELECT
      CASE
          WHEN dim_store_details.store_type = 'Web Portal' THEN 'Web'
          ELSE 'Offline'
      END AS location,
      COUNT(*) AS numbers_of_sales,
      SUM(orders_table.product_quantity) AS product_quantity_count
  FROM orders_table     
      INNER JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
  GROUP BY location;
-------------------------------------------------------------------------------
--Task 5
  WITH StoreSales AS (
      SELECT ds.store_type,
          SUM(ot.product_quantity * dp.product_price) AS total_sales
      FROM orders_table ot
          JOIN dim_products dp ON ot.product_code = dp.product_code
          JOIN dim_store_details ds ON ot.store_code = ds.store_code
      GROUP BY ds.store_type
  )
  SELECT store_type,
      ROUND(SUM(total_sales)::numeric, 2) AS total_sales,
      ROUND((SUM(total_sales)::numeric * 100.0) / SUM(SUM(total_sales)::numeric) OVER (), 2) AS percentage_total
  FROM
      StoreSales
  GROUP BY
      store_type
  ORDER BY
      total_sales DESC;

  SELECT
dsd.store_type,
ROUND(CAST(SUM(ord.product_quantity * dp.product_price) as NUMERIC), 2)
AS total_sales,
ROUND(COUNT( * ) / CAST((SELECT COUNT( * ) FROM orders_table) AS NUMERIC), 2) * 100 as "percentage_total(%)"
FROM
orders_table ord
LEFT JOIN
dim_store_details dsd
ON
ord.store_code = dsd.store_code
LEFT JOIN
dim_products dp
ON
dp.product_code = ord.product_code
GROUP BY
dsd.store_type
ORDER BY
total_sales DESC
  -------------------------------------------------------------------------------
  --TASK 6
  WITH MonthlySales AS (
      SELECT
          ddt.year AS year,
          ddt.month AS month,
          ROUND(SUM(ot.product_quantity * dp.product_price)::numeric, 2) AS total_sales
      FROM orders_table ot 
          JOIN dim_products dp ON ot.product_code = dp.product_code
          JOIN dim_date_times ddt ON ot.date_uuid = ddt.date_uuid
      GROUP BY year, month
  )
  SELECT total_sales, year, month
  FROM MonthlySales
  ORDER BY total_sales DESC
  LIMIT 10;
----------------------------------------------------------------------------------
  --TASK 7
  SELECT country_code, SUM(staff_numbers) AS total_staff_numbers
  FROM dim_store_details
  GROUP BY country_code
  ORDER BY total_staff_numbers DESC;
----------------------------------------------------------------------------------
  --Task 8
  SELECT  store_type, country_code, ROUND(SUM(orders_table.product_quantity * dim_products.product_price)::numeric,2) AS total_sales
  FROM orders_table
  JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
  JOIN dim_products ON orders_table.product_code = dim_products.product_code
  WHERE country_code = 'DE'
  GROUP BY store_type, country_code
  ORDER BY total_sales ASC;
-----------------------------------------------------------------------------------
--TASK 9
WITH SalesCTE AS (
    SELECT
        ddt.year,
        (ddt.year || '-' || ddt.month || '-' || ddt.day || ' ' || ddt.timestamp)::timestamp with time zone AS sale_time,
        LEAD((ddt.year || '-' || ddt.month || '-' || ddt.day || ' ' || ddt.timestamp)::timestamp with time zone) 
            OVER (PARTITION BY ddt.year ORDER BY (ddt.year || '-' || ddt.month || '-' || ddt.day || ' ' || ddt.timestamp)::timestamp with time zone) AS next_sale_time
    FROM
        orders_table ot
    JOIN
        dim_date_times ddt ON ot.date_uuid = ddt.date_uuid
)

SELECT
    year,
    AVG(next_sale_time - sale_time) AS actual_time_taken
FROM
    SalesCTE
WHERE
    next_sale_time IS NOT NULL
GROUP BY
    year
ORDER BY
    actual_time_taken DESC
LIMIT 
	5; 
--done