## Database and Table Creation

```sql
CREATE DATABASE etl_test_db;

CREATE TABLE TaxiTrips (
    Id INT IDENTITY PRIMARY KEY,
    tpep_pickup_datetime DATETIME NOT NULL,
    tpep_dropoff_datetime DATETIME NOT NULL,
    passenger_count INT NOT NULL,
    trip_distance FLOAT NOT NULL,
    store_and_fwd_flag NVARCHAR(3) NOT NULL,
    PULocationID INT NOT NULL,
    DOLocationID INT NOT NULL,
    fare_amount DECIMAL NOT NULL,
    tip_amount DECIMAL NOT NULL
);
```
And then I noticed that I need to alter my table a bit
```sql
ALTER TABLE TaxiTrips
ALTER COLUMN fare_amount DECIMAL(18, 2);

ALTER TABLE TaxiTrips
ALTER COLUMN tip_amount DECIMAL(18, 2);
```
Number of rows - 29283

## 10GB CSV

I implemented an async batch processing mechanism to handle large datasets efficiently, ensuring each batch is read,
validated, transformed, and inserted into the database independently. 
To optimize performance for a 10GB CSV file, I would introduce parallel processing for batches,
leveraging asynchronous operations like Task.WhenAll

To enhance scalability and maintainability, I would refactor the project to incorporate dependency injection (DI) for better control over class dependencies and lifecycle management.
