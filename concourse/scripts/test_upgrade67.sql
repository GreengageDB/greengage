CREATE  TABLE table1 (
    i1 int default 0,
    i2 int,
    n1 numeric default 0,
    n2 numeric,
    t1 text)
 WITH (
    APPENDONLY=TRUE,
    BLOCKSIZE=131072,
    ORIENTATION=COLUMN,
    CHECKSUM=TRUE,
    COMPRESSTYPE=ZLIB,
    COMPRESSLEVEL=3
) DISTRIBUTED BY (i1);

INSERT INTO table1
SELECT 
  g, g, g, g, g
FROM generate_series(1,100) g;

CREATE  TABLE test (
    i1 int default 0,
    i2 int,
    n1 numeric default 0,
    n2 numeric,
    t1 text)
 DISTRIBUTED BY (i1);

 CREATE  TABLE table_p (
  i1 int default 0,
  i2 int,
  n1 numeric default 0,
  n2 numeric,
  t1 text,
  t2 text)
 WITH (
  APPENDONLY=TRUE,
  BLOCKSIZE=131072,
  ORIENTATION=COLUMN,
  CHECKSUM=TRUE,
  COMPRESSLEVEL=3
) DISTRIBUTED BY (i2)
PARTITION BY RANGE (i1) (
  START (1) INCLUSIVE END (100),
  START (100) INCLUSIVE END (200),
  START (200) INCLUSIVE END (300),
  START (300) INCLUSIVE END (400),
  START (400) INCLUSIVE END (500)
);

CREATE SEQUENCE seq1
        START WITH 0
        INCREMENT BY 1
        NO MAXVALUE
        MINVALUE 0
        CACHE 1;

SELECT pg_catalog.setval('public.seq1', 150, true);

CREATE READABLE EXTERNAL TABLE table_ext (
        id numeric,
        name character varying(255),
        searchbase character varying(255)
) LOCATION (
        'gpfdist://10.64.13.204:8081/out-id5_ch00.csv'
)
FORMAT 'TEXT' (delimiter E'|' null E'' escape E'\\')
ENCODING 'UTF8';