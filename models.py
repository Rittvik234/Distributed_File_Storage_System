from database import get_connection


def create_tables():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id SERIAL PRIMARY KEY,
            filename VARCHAR(255) NOT NULL,
            file_size BIGINT NOT NULL,
            total_chunks INT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS chunks (
            id SERIAL PRIMARY KEY,
            file_id INT REFERENCES files(id) ON DELETE CASCADE,
            chunk_index INT NOT NULL,
            chunk_id VARCHAR(255) NOT NULL,
            node_url VARCHAR(255) NOT NULL,
            chunk_size BIGINT NOT NULL
        );
    """)

    conn.commit()

    cursor.close()
    conn.close()

    print("Tables created successfully!")