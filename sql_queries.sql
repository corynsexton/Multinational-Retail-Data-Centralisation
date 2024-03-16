-- 1. How many stores does the business have and in which countries?
SELECT 	country_code, 
		COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores DESC;

-- 2. Which locations currently have the most stores?
SELECT 	locality, 
		COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 7;

-- 3. Which months produced the largest amount of sales?
SELECT 	ROUND(SUM(orders_table.product_quantity * dim_products.product_price_£)::numeric, 2) AS total_sales_£,
		dim_date_times.month
FROM orders_table
JOIN dim_date_times
 	ON dim_date_times.date_uuid = orders_table.date_uuid
JOIN dim_products
	ON 	dim_products.product_code = orders_table.product_code
GROUP BY dim_date_times.month
ORDER BY total_sales_£ DESC
LIMIT 6;

-- 4. How many online and offline sales?
SELECT 	COUNT(*) AS number_of_sales,
		SUM(orders_table.product_quantity) AS product_quantity_count,
	CASE
		WHEN dim_store_details.store_type = 'Web Portal'
		THEN 'Web'
		ELSE 'Offline'
		END AS location
FROM orders_table
JOIN dim_store_details
	ON dim_store_details.store_code = orders_table.store_code
GROUP BY location
ORDER BY location DESC;

-- 5. What percentage of sales come through each type of store?
SELECT 	ds.store_type,
		ROUND(SUM(ot.product_quantity * dp.product_price_£)::numeric, 2) AS "total_sales (£)",
		CAST((SUM(ot.product_quantity * dp.product_price_£) / 
    		SUM(SUM(ot.product_quantity * dp.product_price_£)) OVER ()) * 100 AS NUMERIC(10, 2)) AS "percentage_total (%)"
FROM orders_table AS ot
JOIN dim_store_details AS ds
	ON ds.store_code = ot.store_code
JOIN dim_products AS dp
	ON dp.product_code = ot.product_code
GROUP BY store_type
ORDER BY "total_sales (£)" DESC;

-- 6. Which month in each year produced the highest cost of sales?
SELECT	ROUND(SUM(ot.product_quantity * dp.product_price_£)::numeric, 2) AS "total_sales (£)",
		dt.year,
		dt.month
FROM dim_date_times dt
JOIN orders_table ot
	ON ot.date_uuid = dt.date_uuid
JOIN dim_products dp
	ON dp.product_code = ot.product_code
GROUP BY dt.year, dt.month
ORDER BY "total_sales (£)" DESC;

-- 7. What is the staff headcount?
-- Ops team would like to know overall staff numbers in each location around the world.
SELECT 	country_code,
		SUM(staff_numbers) AS total_staff_numbers
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;

-- 8. Which German store type is selling the most?
SELECT 	ds.country_code,
		ds.store_type,
		ROUND(SUM(ot.product_quantity * dp.product_price_£)::numeric, 2) AS "total_sales (£)"
FROM dim_store_details ds
JOIN orders_table ot
	ON ot.store_code = ds.store_code
JOIN dim_products dp
	ON dp.product_code = ot.product_code
WHERE country_code = 'DE'
GROUP BY ds.store_type,
		 ds.country_code
ORDER BY "total_sales (£)" DESC;

-- 9. How quickly is the company making sales?
-- Determine the average time taken between each sale grouped by year
WITH correct_timestamp AS(
	SELECT 	year,
			month,
			day,
			timestamp::time AS timestamp,
			(year || '-' || LPAD(month::TEXT, 2, '0') || '-' || LPAD(day::TEXT, 2, '0') || ' ' || timestamp::time)::timestamp AS date_time
	FROM dim_date_times
	ORDER BY date_time
),

next_sale AS(
	SELECT	year,
			date_time,
			LEAD(date_time, 1) OVER (
				PARTITION BY year
				ORDER BY date_time
			) AS next_sale,
			LEAD(date_time, 1) OVER (
				PARTITION BY year
				ORDER BY date_time
			) - date_time AS time_difference
	FROM correct_timestamp
	),

time_difference AS(
SELECT	year,
		AVG(time_difference) AS actual_time_difference
FROM next_sale
	GROUP BY year
	)
	
SELECT	year,
		'"Hours: ' || EXTRACT(HOUR FROM actual_time_difference) || '", ' || 
		'"Minutes: ' || EXTRACT(MINUTE FROM actual_time_difference) || '", ' ||
		'"Seconds: ' || ROUND(EXTRACT(SECOND FROM actual_time_difference))  || '", ' ||
		'"Milliseconds: ' || EXTRACT(MILLISECOND FROM actual_time_difference) || '"' AS actual_time_taken
FROM time_difference
GROUP BY year, 
		 actual_time_difference
ORDER BY actual_time_difference;