SELECT current_database(), inet_server_port();

select * from files

select * from chunks

SELECT file_id, chunk_index, chunk_id, node_url, chunk_size
FROM chunks
WHERE file_id = 2
ORDER BY chunk_index;

CREATE TABLE IF NOT EXISTS chunk_replicas (
    id SERIAL PRIMARY KEY,
    chunk_id VARCHAR(255) NOT NULL,
    node_url VARCHAR(255) NOT NULL,
    UNIQUE (chunk_id, node_url)
);
SELECT * FROM chunk_replicas;


SELECT
    c.chunk_index,
    c.chunk_id,
    r.node_url
FROM chunks c
JOIN chunk_replicas r
    ON c.chunk_id = r.chunk_id
WHERE c.file_id = 6
ORDER BY c.chunk_index, r.node_url;


ALTER TABLE chunks
ADD COLUMN IF NOT EXISTS checksum VARCHAR(64);

SELECT
    column_name,
    data_type
FROM information_schema.columns
WHERE table_name = 'chunks'
ORDER BY ordinal_position;


SELECT
    file_id,
    chunk_index,
    chunk_id,
    chunk_size,
    checksum,
    node_url
FROM chunks
WHERE checksum IS NOT NULL
ORDER BY file_id, chunk_index;

SELECT
    chunk_index,
    chunk_id,
    checksum,
    node_url
FROM chunks
WHERE file_id = 7
ORDER BY chunk_index;

SELECT
    chunk_index,
    checksum
FROM chunks
WHERE file_id = 7
  AND chunk_index = 0;

UPDATE files
SET filename = 'corruption_test.bin'
WHERE id = 7;

SELECT checksum
FROM chunks
WHERE file_id = 7
AND chunk_index = 0;

SELECT
    r.chunk_id,
    r.node_url
FROM chunk_replicas r
WHERE r.chunk_id = '7_0_afb21830fed44bd29f8cdf828507af05'
ORDER BY r.node_url;

SELECT
    c.chunk_id,
    c.node_url AS primary_node,
    r.node_url AS replica_node
FROM chunks c
LEFT JOIN chunk_replicas r
    ON c.chunk_id = r.chunk_id
WHERE c.file_id = 7
ORDER BY c.chunk_index, r.node_url;

INSERT INTO chunk_replicas (chunk_id, node_url)
VALUES (
    '7_0_afb21830fed44bd29f8cdf828507af05',
    'http://127.0.0.1:8002'
)
ON CONFLICT (chunk_id, node_url) DO NOTHING;

SELECT chunk_id, node_url
FROM chunk_replicas
WHERE chunk_id = '7_0_afb21830fed44bd29f8cdf828507af05';

SELECT id, filename, file_size, total_chunks
FROM files
ORDER BY id DESC
LIMIT 1;

SELECT
    c.chunk_index,
    c.chunk_id,
    c.node_url
FROM chunks c
WHERE c.file_id = (
    SELECT MAX(id) FROM files
)
ORDER BY c.chunk_index;

SELECT *
FROM files
WHERE filename = 'rollback_test.bin';