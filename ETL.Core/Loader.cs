using System.Data;
using Microsoft.Data.SqlClient;
using Utilities;

namespace ETL.Core;

public static class Loader
{
    public static async Task BulkInsert(DataTable table, string tableName, string connectionString)
    {
        await using (var connection = new SqlConnection(connectionString))
        {
            connection.Open();
            using (var bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.DestinationTableName = tableName;
                
                SqlBulkCopyHelper.ConfigureColumnMappings(bulkCopy);
                
                await bulkCopy.WriteToServerAsync(table);
            }
        }
    }
}