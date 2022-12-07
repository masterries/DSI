CREATE TEMPORARY TABLE users AS

SELECT 251 AS user_id
	,'Bill' AS first_name
	,'Gates' AS last_name
	,'Co-Chair... blogger' AS summary
	,'us:91' AS region_id
	,131 AS industry_id
	,57817532 AS photo_id;

CREATE TEMPORARY TABLE regions AS

SELECT 'us:91' AS id
	,'Greater Seattle Area' AS region_name;

CREATE TEMPORARY TABLE industries AS

SELECT 131 AS id
	,'Philantropy' AS industry_name;

CREATE TEMPORARY TABLE positions AS (
	SELECT 458 AS id
	,251 AS user_id
	,'Co-chair' AS job_title
	,'Bill & Melinda Gates' AS organization

UNION ALL
	
	SELECT 457 AS id
	,251 AS user_id
	,'Co-founder, Chairman' AS job_title
	,'Microsoft' AS organization
	);

CREATE TEMPORARY TABLE education AS (
	SELECT 807 AS id
	,251 AS user_id
	,'Harvard University' AS school_name
	,1973 AS start
	,1975 AS "end"

UNION ALL
	
	SELECT 806 AS id
	,251 AS user_id
	,'Lakeside School, Seattle' AS school_name
	,NULL AS start
	,NULL AS "end"
	);

CREATE TEMPORARY TABLE contact_info AS (
	SELECT 155                  AS id
	,251                        AS user_id
	,'blog'                     AS type
	,'http://thegatesnotes.com' AS url

UNION ALL
	
	SELECT 156 AS id
	,251 AS user_id
	,'twitter' AS type
	,'http://twitter.com/BillGates' AS url
	);

SELECT *
FROM users
JOIN regions ON regions.id = users.region_id
JOIN industries ON industries.id = users.industry_id
JOIN positions ON positions.user_id = users.user_id
JOIN education ON education.user_id = users.user_id
JOIN contact_info ON contact_info.user_id = users.user_id;

