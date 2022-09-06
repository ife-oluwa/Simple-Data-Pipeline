# DROP TABLES
from ingestion.app.db_engine import drop_tables


users_table_drop = "DROP TABLE IF IT EXISTS users"
companies_table_drop = "DROP TABLE IF IT EXISTS companies"
departments_table_drop = "DROP TABLE IF IT EXISTS departments"
staging_table_drop = "DROP TABLE IF IT EXISTS staging"

# CREATE TABLES
staging_table_create = ("""
CREATE TABLE IF NOT EXISTS staging (
    id serial PRIMARY KEY NOT NULL,
    first_name varchar(255),
    last_name varchar(255),
    company_name varchar(255),
    address varchar(255),
    city varchar(255),
    state varchar(255),
    zip varchar(255),
    phone1 varchar(255),
    phone2 varchar(255),
    email varchar(255),
    department varchar(255));
""")

users_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    id serial NOT NULL,
    first_name varchar(255),
    last_name varchar(255),
    email VARCHAR(255),
    Phone1 VARCHAR(255),
    Phone2 VARCHAR(255),
    zip_code VARCHAR(255),
    Address VARCHAR(255),
    City VARCHAR(255),
    state VARCHAR(255),
    department VARCHAR(255),
    company_id INTEGER NOT NULL,
    CONSTRAINT users_pkey PRIMARY KEY(id));""")


departments_table_create = ("""
CREATE TABLE IF NOT EXISTS departments (
    id serial NOT NULL,
    name varchar(255),
    CONSTRAINT department_pkey PRIMARY KEY(id));"""
                            )

companies_table_create = ("""
CREATE TABLE IF NOT EXISTS companies (
    id serial NOT NULL,
    name varchar(255),
    CONSTRAINT company_pkey PRIMARY KEY(id));
""")

constraints = ("""
ALTER TABLE users
ADD CONSTRAINT users_department_id_fkey
FOREIGN KEY (department_id) REFERENCES departmets (id);

ALTER TABLE users
ADD CONSTRAINT users_company_id_fkey
FOREIGN KEY (company_id) REFERENCES companies (id);
""")

# STAGING TABLE
users_fill_from_staging = ("""
INSERT INTO users (firstname, lastname, emai, Phone1, Phone2, zip_code, Address, City, state, department_id, company_id)
SELECT
    s.first_name as firstname,
    s.last_name as lastname,
    s.email as email;
    s.phone1 as Phone1,
    s.Phone2 as Phone2,
    s.zip as zip_codee,
    s.address as Address,
    s.city as City,
    s.state as state,
    d.id as department_id,
    c.id as company_id
FROM staging s
INNER JOIN companies c
ON s.company_name = c.name
INNER JOIN department as d
ON s.department = d.name;
""")

companies_fill_from_staging = ("""
INSERT INTO companies (name)
SELECT DISTINCT company_name as Name
FROM staging;""")

departments_fill_from_staging = ("""
INSERT INTO departments (name)
SELECT DISTINCT department_name as Name
FROM staging;""")

fill_table_queries = [companies_fill_from_staging,
                      departments_fill_from_staging, users_fill_from_staging]
create_table_queries = [staging_table_create, users_table_create,
                        departments_table_create, companies_table_create]
drop_table_queries = [staging_table_drop, users_table_drop,
                      companies_table_drop, departments_table_drop]
create_constraints = [constraints]
