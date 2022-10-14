SELECT
	BT.id_transaction,
	BT.id_customer,
	BC.name_customer,
	BC.birthdate_customer,
	BC.gender_customer,
	BC.country_customer,
	BT.date_transaction,
	BP."Type",
	BT.product_transaction,
	BT.amount_transaction
FROM
	bigdata_transaction BT
	LEFT JOIN bigdata_customer BC ON BC.id_customer = BT.id_customer
	LEFT JOIN bigdata_product BP ON BP."Type" = BT.product_transaction