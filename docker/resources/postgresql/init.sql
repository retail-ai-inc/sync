-- Create databases (no "IF NOT EXISTS" in standard Postgres SQL)

-- Switch to source_db
\connect source_db

-- Create table users in source_db
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100)
);


\c source_db
SELECT * FROM pg_create_logical_replication_slot('sync_slot', 'pgoutput');

-- Create publication for logical replication
CREATE PUBLICATION mypub FOR TABLE users;
ALTER TABLE users REPLICA IDENTITY FULL;
insert into users (id, name, email) values (1, 'John', 'John@mail' );