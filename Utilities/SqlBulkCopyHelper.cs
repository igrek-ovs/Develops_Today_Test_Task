using Microsoft.Data.SqlClient;

namespace Utilities;

public class SqlBulkCopyHelper
{
    public static void ConfigureColumnMappings(SqlBulkCopy bulkCopy)
    {
        bulkCopy.ColumnMappings.Add("tpep_pickup_datetime", "tpep_pickup_datetime");
        bulkCopy.ColumnMappings.Add("tpep_dropoff_datetime", "tpep_dropoff_datetime");
        bulkCopy.ColumnMappings.Add("passenger_count", "passenger_count");
        bulkCopy.ColumnMappings.Add("trip_distance", "trip_distance");
        bulkCopy.ColumnMappings.Add("store_and_fwd_flag", "store_and_fwd_flag");
        bulkCopy.ColumnMappings.Add("PULocationID", "PULocationID");
        bulkCopy.ColumnMappings.Add("DOLocationID", "DOLocationID");
        bulkCopy.ColumnMappings.Add("fare_amount", "fare_amount");
        bulkCopy.ColumnMappings.Add("tip_amount", "tip_amount");
    }
}