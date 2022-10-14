-- Example DWH

DROP TABLE IF EXISTS dim_transaction;
CREATE TABLE dim_transaction (
	id_transaction INT NOT NULL,
	id_customer INT NOT NULL,
	name_customer VARCHAR(255),
	gender_customer VARCHAR(20),
	country_customer VARCHAR(50),
	birthdate_customer DATE NOT NULL
	date_transaction DATE NOT NULL,
	Type VARCHAR(40),
	product_transaction VARCHAR(255),
	amount_transaction INT
	);

-- DROP TABLE IF EXISTS fact_order_items; 
-- CREATE TABLE fact_order_items (
-- 	order_item_id INT NOT NULL ,
-- 	order_id INT NOT NULL,
-- 	product_id INT NOT NULL,
-- 	order_item_quantity INT,
-- 	product_discount INT,
-- 	product_subdiscount INT,
-- 	product_price INT NOT NULL,
-- 	product_subprice INT NOT NULL
-- );