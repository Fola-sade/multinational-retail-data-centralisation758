ALTER TABLE orders_table
  ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid,
  ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid,
  ALTER COLUMN card_number TYPE VARCHAR(19),
  ALTER COLUMN store_code TYPE VARCHAR(12),
  ALTER COLUMN product_code TYPE VARCHAR(11),
  ALTER COLUMN product_quantity TYPE SMALLINT;

SELECT MAX(LENGTH(date_uuid)) AS max_length FROM dim_date_times

SELECT MAX(LENGTH(CAST(day AS TEXT))) AS max_length FROM dim_date_times 

SELECT * FROM dim_store_details


--Task 2
ALTER TABLE dim_users
  ALTER COLUMN first_name TYPE VARCHAR(255),
  ALTER COLUMN last_name TYPE VARCHAR(255),
  ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE,
  ALTER COLUMN country_code TYPE VARCHAR(2),
  ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid,
  ALTER COLUMN join_date TYPE DATE USING join_date::DATE;


--Task 3
ALTER TABLE dim_store_details
  ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision,
  ALTER COLUMN locality TYPE VARCHAR(255),
  ALTER COLUMN store_code TYPE VARCHAR(12),
  ALTER COLUMN staff_numbers TYPE SMALLINT,
  ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
  ALTER COLUMN store_type TYPE VARCHAR(255),
  ALTER COLUMN store_type DROP NOT NULL,
  ALTER COLUMN latitude TYPE FLOAT USING latitude::double precision,
  ALTER COLUMN country_code TYPE VARCHAR(2),
  ALTER COLUMN continent TYPE VARCHAR(255);

  SELECT * FROM dim_store_details 
  WHERE locality IS NULL;

  UPDATE dim_store_details
  SET locality = 'N/A'
  WHERE locality IS NULL;

--Task 4
  -- Removing pound sign from product_price in dim_products
  UPDATE dim_products SET product_price = REPLACE(product_price, '£', '');

  ALTER TABLE dim_products
  ADD weight_class VARCHAR(50);

  UPDATE dim_products
  SET weight_class = CASE
    WHEN weight < 2 THEN 'Light' 
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    WHEN weight >= 140 THEN 'Truck_Required'
  END;

  SELECT * FROM dim_date_times


--Task 5
  ALTER TABLE dim_products
  RENAME COLUMN removed TO still_available;


  SELECT MAX(LENGTH(product_code)) AS max_length FROM dim_products

  ALTER TABLE dim_products
     ALTER COLUMN product_price TYPE FLOAT USING product_price::double precision,
     ALTER COLUMN uuid TYPE uuid USING uuid::uuid,
     ALTER COLUMN "EAN" TYPE VARCHAR(50),
     ALTER COLUMN product_code TYPE VARCHAR(50), 
     ALTER COLUMN date_added TYPE DATE; 

  ALTER TABLE dim_products 
	ALTER still_available TYPE bool 
		USING CASE WHEN still_available='Removed' THEN FALSE ELSE TRUE END;
		
  DELETE FROM dim_products WHERE product_name = 'LB3D71C025';
  DELETE FROM dim_products WHERE product_name = 'VLPCU81M30';


--Task 6
  ALTER TABLE dim_date_times
     ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid,
     ALTER COLUMN month TYPE VARCHAR(15), --2
	 ALTER COLUMN year TYPE VARCHAR(15), --4
     ALTER COLUMN day TYPE VARCHAR(15), --2
	 ALTER COLUMN time_period TYPE VARCHAR(15);

--Task 7

  SELECT MAX(LENGTH(expiry_date)) AS max_length FROM dim_card_details
  SELECT MAX(LENGTH(CAST(date_payment_confirmed AS TEXT))) AS max_length FROM dim_card_details 

 --card_number = 19
 --expiry_date = 5
 --date_payment_confirmed = 19
  ALTER TABLE dim_card_details
     ALTER COLUMN card_number TYPE VARCHAR(19),
	 ALTER COLUMN expiry_date TYPE VARCHAR(5),
	 ALTER COLUMN date_payment_confirmed TYPE VARCHAR(19);

--Task 8
--SELECT * FROM orders_table

 -- SELECT orders_table.product_quantity, orders_table.card_number, dim_card_details.card_number FROM orders_table JOIN dim_products ON orders_table.user_uuid = dim_products.uuid;
  SELECT constraint_name
  FROM information_schema.table_constraints
  WHERE table_name = 'orders_table' AND constraint_type = 'FORIEIGN KEY';
  --drop PK
  ALTER TABLE dim_users DROP CONSTRAINT dim_users_pkey;
  SELECT * FROM dim_users
  --change column name
  ALTER TABLE dim_users
 RENAME COLUMN uuid TO user_uuid;
  -- Adding primary keys to each of the tables prefixed with dim

  ALTER TABLE dim_card_details ADD PRIMARY KEY (card_number);
  ALTER TABLE dim_date_times ADD PRIMARY KEY(date_uuid);
  ALTER TABLE dim_products ADD PRIMARY KEY(product_code);
  ALTER TABLE dim_store_details ADD PRIMARY KEY(store_code); 
  ALTER TABLE dim_users ADD PRIMARY KEY(user_uuid); --user_uuid

--Task 9

SELECT * FROM dim_card_details

  ALTER TABLE orders_table
  ADD CONSTRAINT card_number_fk
  FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);

  ALTER TABLE orders_table
  ADD CONSTRAINT date_uuid_fk
  FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);

  ALTER TABLE orders_table
  ADD CONSTRAINT product_code_fk
  FOREIGN KEY (product_code) REFERENCES dim_products(product_code);

  ALTER TABLE orders_table
  ADD CONSTRAINT store_code_fk
  FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);

  ALTER TABLE orders_table
  ADD CONSTRAINT user_uuid_fk
  FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);

  --ERD for table has to be selected when you right-click the fact table. Doing this you'll visualize the cardinality

  --debugging dim_store_details 
  SELECT *
  FROM dim_card_details
  WHERE card_number = '213163034758051';

  INSERT INTO dim_card_details (card_number, expiry_date, card_provider, date_payment_confirmed)
  VALUES ('4560485762943720000', '09/26', 'Diners Club / Carte Blanche', '2015-11-25 00:00:00');

  SELECT COUNT(*) AS missing_card_numbers
  FROM orders_table o
  WHERE NOT EXISTS (
      SELECT 1
      FROM dim_card_details d
      WHERE d.card_number = o.card_number
);

  