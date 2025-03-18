-- Create source_db and target_db in MySQL
CREATE DATABASE IF NOT EXISTS source_db;

-- Use source_db
USE source_db;

-- Create table users
CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100)
);
insert into users (id, name, email) values (1, 'John', 'John@mail' );
ALTER TABLE users MODIFY COLUMN id INT NOT NULL AUTO_INCREMENT;

CREATE TABLE IF NOT EXISTS orders (
    id INT PRIMARY KEY,
    user_id INT,
    product VARCHAR(100),
    quantity INT,
    order_date DATETIME
);

