-- Load the module in every new backend of the test database, the
-- qExecs' too: the qExecs send their data only where it is loaded, and
-- LOAD reaches only the backends running now. A setting of the database
-- reaches every instance, and each test runs in a new session.
ALTER DATABASE :"DBNAME" SET session_preload_libraries = 'test_explain_hooks';
