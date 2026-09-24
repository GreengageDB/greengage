CREATE EXTENSION arrowflight;

SELECT arrowflight_build_info() <> '' AS build_info_available;

SELECT fdwname
FROM pg_foreign_data_wrapper
WHERE fdwname = 'flightsql_fdw'
ORDER BY fdwname;
