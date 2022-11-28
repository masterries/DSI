-- JSON import with copy, is often easier with Python, but for
-- simple JSON without embedded newlines in the JSON values, this is good enough.
-- http://adpgtech.blogspot.com/2014/09/importing-json-data.html
-- DROP TABLE IF EXISTS jtracks CASCADE;

CREATE TABLE IF NOT EXISTS jtracks(id SERIAL, body JSONB);
\copy jtracks(body)	FROM 'library.json' WITH CSV QUOTE E '\x01' DELIMITER E '\x02';


SELECT * FROM jtracks LIMIT 5;

SELECT pg_typeof(body)
FROM jtracks LIMIT 1;

SELECT body->>'name'
FROM jtracks LIMIT 5;

-- Could we use parenthesis and cast to convert to text?
SELECT pg_typeof(body->'name')
FROM jtracks LIMIT 1;

SELECT pg_typeof(body->'name'::TEXT)
FROM jtracks LIMIT 1;

SELECT pg_typeof(body->'name')::TEXT
FROM jtracks LIMIT 1;

-- This is how to cast correctly
SELECT pg_typeof((body->'name')::TEXT)
FROM jtracks LIMIT 1;

-- Yes we could, but why even try?
SELECT pg_typeof(body->>'name')
FROM jtracks LIMIT 1;

SELECT MAX((body->>'count')::INT)
FROM jtracks;

-- Difference between -> and ->>
select body->'name' name1, body->>'name' name2 from jtracks limit 1;

SELECT body->>'name' as name, body->>'count' as "count"
FROM jtracks
ORDER BY (body->>'count')::INT DESC;

-- Yes you need the cast even though it is an integer in the JSON :(
-- One is jsonb the other is 'text'
SELECT pg_typeof(body->'count'), pg_typeof(body->>'count')
FROM jtracks LIMIT 1;

SELECT pg_typeof((body->>'count')::int)
FROM jtracks LIMIT 1;

-- Look into JSON for a value
SELECT COUNT(*)
FROM jtracks
WHERE body->>'name' = 'Summer Nights';

SELECT COUNT(*)
FROM jtracks
WHERE body->>'artist' = 'AC/DC';

-- Ask if the body contains a key/value pair
SELECT COUNT(*)
FROM jtracks
WHERE body @>'{"artist": "AC/DC"}';

SELECT COUNT(*)
FROM jtracks
WHERE body @> ('{"artist": "AC/DC"}'::jsonb);

-- Adding something to the JSONB column
UPDATE jtracks
SET body = body || '{"is_favorite": "yes"}'
WHERE (body->'count')::INT > 200;

-- Should see some with and without "favorite"
SELECT body
FROM jtracks
WHERE (body->'count')::INT > 160 LIMIT 5;

-- Use the ? operator to check if the tag is present
SELECT COUNT(*)
FROM jtracks
WHERE body ? 'is_favorite';

-- https://bitnine.net/blog-postgresql/postgresql-internals-jsonb-type-and-its-indexes/
-- Lets throw some bulk into the table
INSERT INTO jtracks (body)
SELECT ('{ "type": "Neon", "series": "24 Hours of Lemons", "number": ' || generate_series(1000, 5000) || '}')::jsonb;

-- Prepare three indexes...
DROP INDEX idx_jtracks_btree;

DROP INDEX idx_jtracks_gin;

DROP INDEX idx_jtracks_gin_path_ops;

CREATE INDEX idx_jtracks_btree ON jtracks USING BTREE ((body->>'name'));

CREATE INDEX idx_jtracks_gin ON jtracks USING gin (body);

CREATE INDEX idx_jtracks_gin_path_ops ON jtracks USING gin (body jsonb_path_ops);

-- Might need to wait a little while while PostgreSQL catches up :)
-- See which query uses which index
EXPLAIN SELECT COUNT(*) FROM jtracks WHERE body->>'artist' = 'Queen';

EXPLAIN SELECT COUNT(*) FROM jtracks WHERE body->>'name' = 'Summer Nights';

EXPLAIN SELECT COUNT(*) FROM jtracks WHERE body ? 'favorite';

EXPLAIN SELECT COUNT(*) FROM jtracks WHERE body @>'{"name": "Summer Nights"}';

EXPLAIN SELECT COUNT(*) FROM jtracks WHERE body @>'{"artist": "Queen"}';

EXPLAIN SELECT COUNT(*) FROM jtracks WHERE body @>'{"name": "Folsom Prison Blues", "artist": "Johnny Cash"}';

CREATE INDEX idx_tracks_name ON tracks (name);


-- Now let's take a look at something more interesting :-)

-- Semijoins
SELECT jtracks.body->>'name' FROM jtracks WHERE jtracks.body->>'name' IN (
		SELECT tracks.name
		FROM tracks
		);

-- Inner JOINS
SELECT jtracks.body->>'name', tracks.* as name FROM jtracks
join tracks on tracks.name = jtracks.body->>'name';

-- As well as left JOINS!
SELECT jtracks.body->>'name' as name FROM jtracks
left join tracks on tracks.name = jtracks.body->>'name';


SELECT jtracks.body->>'artist' artist
	,avg((jtracks.body->>'rating')::INT)
	,count(*)
FROM jtracks
GROUP BY artist
ORDER BY count(*) DESC;

-- https://stackoverflow.com/questions/30074452/how-to-update-a-jsonb-columns-field-in-postgresql
-- Updating a numeric field in JSONB
-- Failure and then success :)
SELECT (body->'count') + 1
FROM jtracks LIMIT 1;

SELECT (body->'count')::INT + 1
FROM jtracks LIMIT 1;

SELECT (body->>'count')::INT
FROM jtracks
WHERE body->>'name' = 'Summer Nights';

SELECT ((body->>'count')::INT + 1)
FROM jtracks
WHERE body->>'name' = 'Summer Nights';

UPDATE jtracks
SET body = jsonb_set(body, '{ count }', ((body->>'count')::INT + 1)::TEXT::jsonb)
WHERE body->>'name' = 'Summer Nights';


select * from tracks;