--
-- Drop existing table
--
DROP TABLE IF EXISTS foo;

--
-- Create new table foo
--
CREATE TABLE foo(type INTEGER, prod VARCHAR, quantity NUMERIC);

--
-- Insert some values
--
INSERT INTO foo VALUES
  (1, 'Table', 100),
  (2, 'Chair', 250),
  (3, 'Bed', 300);

--
-- Select query with grouping sets
--
SELECT type, prod, sum(quantity) s_quant
FROM
(
  SELECT type, prod, quantity
  FROM foo F1
  LIMIT 3
) F2 GROUP BY GROUPING SETS((type, prod), (prod)) ORDER BY type, s_quant;


--
-- Case for partitioned table
--

--
-- Create new table bar
--
CREATE TABLE pfoo(type INTEGER, prod VARCHAR, quantity NUMERIC)
DISTRIBUTED RANDOMLY
PARTITION BY RANGE (quantity) (
	partition "1" start (0) end (100),
	partition "2" start (100) end (200),
	partition "3" start (200) end (300),
	partition "4" start (300) end (400)
);

--
-- Insert some values
--
INSERT INTO pfoo VALUES
  (1, 'Table', 100),
  (2, 'Chair', 250),
  (3, 'Bed', 300);

--
-- Turn off GPORCA
--
set optimizer to off;

--
-- Select query with grouping sets
--
SELECT type, prod, sum(quantity) s_quant
FROM (
	SELECT * FROM pfoo
) AS t
GROUP BY GROUPING SETS((type), (prod))
ORDER BY type, s_quant;

--
-- Reset settings
--
reset optimizer;
