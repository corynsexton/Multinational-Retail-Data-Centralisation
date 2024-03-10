-- Alter order data column types
ALTER TABLE orders_table
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
	ALTER COLUMN card_number TYPE VARCHAR(19),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN product_quantity TYPE SMALLINT

-- Alter user data column types
ALTER TABLE dim_users
	ALTER COLUMN first_name TYPE VARCHAR(255),
	ALTER COLUMN last_name TYPE VARCHAR(255),
	ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::date,
	ALTER COLUMN country_code TYPE VARCHAR(2),
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
	ALTER COLUMN join_date TYPE DATE USING join_date::date

-- -- Alter store data column types
ALTER TABLE dim_store_details
	ALTER COLUMN longitude TYPE FLOAT,
	ALTER COLUMN locality TYPE VARCHAR(255),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint,
	ALTER COLUMN opening_date TYPE DATE USING opening_date::date,
	ALTER COLUMN store_type TYPE VARCHAR(255),
	ALTER COLUMN latitude TYPE FLOAT,
	ALTER COLUMN country_code TYPE VARCHAR(2),
	ALTER COLUMN continent TYPE VARCHAR(255);

--Alter products data column types and categorise weight column
ALTER TABLE dim_products
	ADD COLUMN weight_class VARCHAR(14)

UPDATE dim_products
	SET weight_class = 
		CASE
			WHEN weight_kg < 2 THEN 'Light'
			WHEN weight_kg >= 2 AND weight_kg < 40 THEN 'Mid_Sized'
			WHEN weight_kg >= 40 AND weight_kg < 140 THEN 'Heavy'
			WHEN weight_kg >= 140 THEN 'Truck_Required'
		END

ALTER TABLE dim_products
	RENAME removed TO still_available

ALTER TABLE dim_products
	ALTER COLUMN product_price_£ TYPE FLOAT,
	ALTER COLUMN weight_kg TYPE FLOAT,
	ALTER COLUMN "EAN" TYPE VARCHAR(17),
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN date_added TYPE DATE USING date_added::date,
	ALTER COLUMN uuid TYPE UUID USING uuid::uuid

ALTER TABLE dim_products
	ALTER COLUMN still_available TYPE bool USING 
	CASE 
		WHEN still_available = 'Removed'THEN FALSE
		WHEN still_available = 'Still_available' THEN TRUE
	END

-- Alter sales time data column types
ALTER TABLE dim_date_times
	ALTER COLUMN month TYPE VARCHAR(2),
	ALTER COLUMN year TYPE VARCHAR(4),
	ALTER COLUMN day TYPE VARCHAR(2),
	ALTER COLUMN time_period TYPE VARCHAR(10),
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid

-- Alter card data column types
ALTER TABLE dim_card_details
	ALTER COLUMN card_number TYPE VARCHAR(19),
	ALTER COLUMN expiry_date TYPE VARCHAR(5),
	ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::date


-- ADD PRIMARY KEYS
ALTER TABLE dim_users
	ADD PRIMARY KEY (user_uuid);

ALTER TABLE dim_store_details
	ADD PRIMARY KEY (store_code);
	
ALTER TABLE dim_products
	ADD PRIMARY KEY (product_code);
	
ALTER TABLE dim_date_times
	ADD PRIMARY KEY (date_uuid);
	
ALTER TABLE dim_card_details
	ADD PRIMARY KEY (card_number);


-- ADD FOREIGN KEYS
ALTER TABLE orders_table
	ADD CONSTRAINT orders_table_user_uuid_fkey
	FOREIGN KEY (user_uuid)
	REFERENCES dim_users(user_uuid);

ALTER TABLE orders_table
	ADD CONSTRAINT orders_table_store_code_fkey
	FOREIGN KEY (store_code)
	REFERENCES dim_store_details(store_code);
	
ALTER TABLE orders_table
	ADD CONSTRAINT orders_table_product_code_fkey
	FOREIGN KEY (product_code)
	REFERENCES dim_products(product_code);
	
ALTER TABLE orders_table
	ADD CONSTRAINT orders_table_date_uuid_fkey
	FOREIGN KEY (date_uuid)
	REFERENCES dim_date_times(date_uuid);
	
ALTER TABLE orders_table
	ADD CONSTRAINT orders_table_card_number_fkey
	FOREIGN KEY (card_number)
	REFERENCES dim_card_details(card_number);