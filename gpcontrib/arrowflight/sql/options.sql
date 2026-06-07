CREATE SERVER arrowflight_fdw_options_srv
FOREIGN DATA WRAPPER arrowflight_fdw
OPTIONS (
    host 'localhost',
    port '8815',
    write_mode 'staging',
    batch_rows '8',
    max_batch_bytes '1024',
    timeout_ms '1000',
    retry_count '0',
    retry_backoff_ms '10'
);

CREATE SERVER arrowflight_fdw_bad_auth_no_tls_srv
FOREIGN DATA WRAPPER arrowflight_fdw
OPTIONS (
    host 'localhost',
    port '8815',
    auth_token_file '/tmp/token'
);

CREATE SERVER arrowflight_fdw_bad_cert_only_srv
FOREIGN DATA WRAPPER arrowflight_fdw
OPTIONS (
    host 'localhost',
    port '8815',
    tls 'true',
    tls_client_cert_file '/tmp/client.pem'
);

CREATE FOREIGN TABLE arrowflight_fdw_bad_url (
    id int4
)
SERVER arrowflight_fdw_options_srv
OPTIONS (
    url 'http://localhost:8815/dataset/bad_url'
);

CREATE FOREIGN TABLE arrowflight_fdw_bad_empty_path (
    id int4
)
SERVER arrowflight_fdw_options_srv
OPTIONS (
    path ''
);

CREATE FOREIGN TABLE arrowflight_fdw_bad_retry (
    id int4
)
SERVER arrowflight_fdw_options_srv
OPTIONS (
    path 'dataset/bad_retry',
    retry_count '11'
);

CREATE FOREIGN TABLE arrowflight_fdw_bad_timeout (
    id int4
)
SERVER arrowflight_fdw_options_srv
OPTIONS (
    path 'dataset/bad_timeout',
    timeout_ms '-2'
);

CREATE FOREIGN TABLE arrowflight_fdw_bad_batch_rows (
    id int4
)
SERVER arrowflight_fdw_options_srv
OPTIONS (
    path 'dataset/bad_batch_rows',
    batch_rows '0'
);

CREATE FOREIGN TABLE arrowflight_fdw_bad_max_batch_bytes (
    id int4
)
SERVER arrowflight_fdw_options_srv
OPTIONS (
    path 'dataset/bad_max_batch_bytes',
    max_batch_bytes '-1'
);

CREATE FOREIGN TABLE arrowflight_fdw_bad_projection (
    id int4
)
SERVER arrowflight_fdw_options_srv
OPTIONS (
    url 'arrowflight://localhost:8815/dataset/bad_projection?projection_pushdown=require'
);

CREATE FOREIGN TABLE arrowflight_fdw_bad_security_table_option (
    id int4
)
SERVER arrowflight_fdw_options_srv
OPTIONS (
    path 'dataset/bad_security_table_option',
    auth_token_file '/tmp/token'
);

CREATE FOREIGN TABLE arrowflight_fdw_dist_key_options (
    id int4 OPTIONS (insert_dist_by_key 'true', insert_dist_by_key_weight '0'),
    label text
)
SERVER arrowflight_fdw_options_srv
OPTIONS (
    path 'dataset/dist_key_options'
);

CREATE FOREIGN TABLE arrowflight_fdw_bad_dist_key_weight (
    id int4 OPTIONS (insert_dist_by_key_weight '-1')
)
SERVER arrowflight_fdw_options_srv
OPTIONS (
    path 'dataset/bad_dist_key_weight'
);

CREATE FOREIGN TABLE arrowflight_fdw_bad_dist_key_table_option (
    id int4
)
SERVER arrowflight_fdw_options_srv
OPTIONS (
    path 'dataset/bad_dist_key_table_option',
    insert_dist_by_key 'true'
);

DROP FOREIGN TABLE arrowflight_fdw_dist_key_options;
DROP SERVER arrowflight_fdw_options_srv;
