USE customer_db;

SELECT * FROM customer_activity_hive;

SELECT activity_type, COUNT(*)
FROM customer_activity_hive
GROUP BY activity_type;

SELECT city, COUNT(*)
FROM customer_activity_hive
GROUP BY city;

SELECT product, activity_type
FROM customer_activity_hive;

SELECT customer_id, name, city, product
FROM customer_activity_hive
WHERE activity_type = 'PURCHASE';
