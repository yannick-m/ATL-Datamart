SELECT * FROM public.nyc_raw LIMIT 100;

CREATE TABLE public.dim_taxi_vendor (
    vendorid INTEGER PRIMARY KEY,
    vendor_name VARCHAR(255)
);

CREATE TABLE public.dim_payment_type (
    payment_type_id INTEGER PRIMARY KEY,
    payment_description VARCHAR(255)
);

CREATE TABLE public.dim_rate_code (
    ratecodeid INTEGER PRIMARY KEY,
    rate_description VARCHAR(255)
);

CREATE TABLE public.dim_store_and_fwd_flag (
    store_and_fwd_flag_id INTEGER PRIMARY KEY,
    flag_description VARCHAR(255)
);

CREATE TABLE public.fact_taxi_trip (
    trip_id SERIAL PRIMARY KEY,
    vendorid INTEGER REFERENCES public.dim_taxi_vendor(vendorid),
    pickup_datetime TIMESTAMP WITHOUT TIME ZONE,
    dropoff_datetime TIMESTAMP WITHOUT TIME ZONE,
    passenger_count DOUBLE PRECISION,
    trip_distance DOUBLE PRECISION,
    ratecodeid INTEGER REFERENCES public.dim_rate_code(ratecodeid),
    store_and_fwd_flag INTEGER REFERENCES public.dim_store_and_fwd_flag(store_and_fwd_flag_id),
    pulocationid DOUBLE PRECISION,
    dolocationid DOUBLE PRECISION,
    payment_type INTEGER REFERENCES public.dim_payment_type(payment_type_id),
    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    airport_fee DOUBLE PRECISION
);

-- Insertion dans dim_taxi_vendor
-- Connexion à la base de données source (nyc_warehouse)
CREATE EXTENSION IF NOT EXISTS dblink;
SELECT dblink_connect('nyc_warehouse_connection', 'host=localhost dbname=nyc_warehouse user=postgres password=admin');

-- Insertion des données dans dim_taxi_vendor
INSERT INTO data_smart.public.dim_taxi_vendor (vendorid, vendor_name)
SELECT DISTINCT
    vendorid,
    CASE vendorid
        WHEN 1 THEN 'Creative Mobile Technologies, LLC'
        WHEN 2 THEN 'VeriFone Inc.'
        ELSE 'Unknown'
    END AS vendor_name
FROM dblink('nyc_warehouse_connection', 'SELECT vendorid FROM public.nyc_raw') AS t(vendorid INTEGER);

-- Fermeture de la connexion dblink
SELECT dblink_disconnect('nyc_warehouse_connection');

-- Insertion dans dim_taxi_vendor
-- Connexion à la base de données source (nyc_warehouse)
CREATE EXTENSION IF NOT EXISTS dblink;
SELECT dblink_connect('nyc_warehouse_connection', 'host=localhost dbname=nyc_warehouse user=postgres password=admin');

-- Insertion des données dans dim_rate_code
INSERT INTO data_smart.public.dim_rate_code (ratecodeid, rate_description)
SELECT DISTINCT
    ratecodeid,
    CASE ratecodeid
        WHEN 1 THEN 'Standard rate'
        WHEN 2 THEN 'JFK' 
        WHEN 3 THEN 'Newark'
        WHEN 4 THEN 'Nassau or Westchester'
        WHEN 5 THEN 'Negotiated fare'
        WHEN 6 THEN 'Group ride'
        ELSE 'Unknown'
    END AS rate_description
FROM dblink('nyc_warehouse_connection', 'SELECT ratecodeid FROM public.nyc_raw WHERE ratecodeid IS NOT NULL') AS t(ratecodeid INTEGER);

-- Fermeture de la connexion dblink
SELECT dblink_disconnect('nyc_warehouse_connection');

-- Connexion à la base de données source (nyc_warehouse)
CREATE EXTENSION IF NOT EXISTS dblink;
SELECT dblink_connect('nyc_warehouse_connection', 'host=localhost dbname=nyc_warehouse user=postgres password=admin');

-- Insertion dans dim_payment_type
INSERT INTO public.dim_payment_type (payment_type_id, payment_description)
SELECT DISTINCT payment_type AS payment_type_id,
       CASE payment_type
            WHEN 1 THEN 'Credit card'
            WHEN 2 THEN 'Cash'
            WHEN 3 THEN 'No charge'
            WHEN 4 THEN 'Dispute'
            WHEN 5 THEN 'Unknown'
            WHEN 6 THEN 'Voided trip'
            ELSE 'Unknown'
       END AS payment_description
FROM dblink('nyc_warehouse_connection', 'SELECT payment_type FROM public.nyc_raw WHERE payment_type IS NOT NULL') AS t(payment_type INTEGER);

-- Fermeture de la connexion dblink
SELECT dblink_disconnect('nyc_warehouse_connection');

-- Connexion à la base de données source (nyc_warehouse)
CREATE EXTENSION IF NOT EXISTS dblink;
SELECT dblink_connect('nyc_warehouse_connection', 'host=localhost dbname=nyc_warehouse user=postgres password=admin');

-- Insertion dans dim_store_and_fwd_flag
INSERT INTO public.dim_store_and_fwd_flag (store_and_fwd_flag_id, flag_description)
SELECT DISTINCT 
       CASE store_and_fwd_flag
            WHEN 'Y' THEN 1
            WHEN 'N' THEN 0
            ELSE -1
       END AS store_and_fwd_flag_id,
       CASE store_and_fwd_flag
            WHEN 'Y' THEN 'Store and forward trip'
            WHEN 'N' THEN 'Not a store and forward trip'
            ELSE 'Unknown'
       END AS flag_description
FROM dblink('nyc_warehouse_connection', 'SELECT store_and_fwd_flag FROM public.nyc_raw WHERE store_and_fwd_flag IS NOT NULL') AS t(store_and_fwd_flag text);

-- Fermeture de la connexion dblink
SELECT dblink_disconnect('nyc_warehouse_connection');


-- Connexion à la base de données source (nyc_warehouse)
CREATE EXTENSION IF NOT EXISTS dblink;
SELECT dblink_connect('nyc_warehouse_connection', 'host=localhost dbname=nyc_warehouse user=postgres password=admin');

-- Insertion dans fact_taxi_trip
INSERT INTO data_smart.public.fact_taxi_trip (
    vendorid,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeid,
    store_and_fwd_flag,
    pulocationid,
    dolocationid,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee
)
SELECT
    vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeid,
    CASE WHEN store_and_fwd_flag = 'Y' THEN 1 ELSE 0 END AS store_and_fwd_flag,
    pulocationid,
    dolocationid,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee
FROM dblink('nyc_warehouse_connection', 'SELECT vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee FROM public.nyc_raw WHERE payment_type IS NOT NULL') AS t(
    vendorid INTEGER,
    tpep_pickup_datetime TIMESTAMP WITHOUT TIME ZONE,
    tpep_dropoff_datetime TIMESTAMP WITHOUT TIME ZONE,
    passenger_count DOUBLE PRECISION,
    trip_distance DOUBLE PRECISION,
    ratecodeid INTEGER,
    store_and_fwd_flag TEXT,
    pulocationid INTEGER,
    dolocationid INTEGER,
    payment_type BIGINT,
    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    airport_fee DOUBLE PRECISION
);

-- Fermeture de la connexion dblink
SELECT dblink_disconnect('nyc_warehouse_connection');

SELECT COUNT(*)
FROM public.fact_taxi_trip
WHERE passenger_count = 0;

SELECT COUNT(*) 
FROM public.fact_taxi_trip
WHERE fare_amount <= 0
   OR tip_amount < 0
   OR tolls_amount < 0
   OR total_amount <= 0;