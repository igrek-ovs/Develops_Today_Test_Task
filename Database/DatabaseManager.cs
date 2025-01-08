using System.Data;
using Microsoft.Data.SqlClient;

namespace Database;

public class DatabaseManager
{
    private readonly string _connectionString;

    public DatabaseManager(string connectionString)
    {
        _connectionString = connectionString;
    }

    public async Task ExecuteNonQueryAsync(string query, Dictionary<string, object>? parameters = null)
    {
        await using (var connection = new SqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            await using (var command = new SqlCommand(query, connection))
            {
                if (parameters != null)
                {
                    foreach (var param in parameters)
                    {
                        command.Parameters.AddWithValue(param.Key, param.Value);
                    }
                }

                await command.ExecuteNonQueryAsync();
            }
        }
    }

    public async Task<DataTable> ExecuteQueryAsync(string query, Dictionary<string, object>? parameters = null)
    {
        var dataTable = new DataTable();
        await using (var connection = new SqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            await using (var command = new SqlCommand(query, connection))
            {
                if (parameters != null)
                {
                    foreach (var param in parameters)
                    {
                        command.Parameters.AddWithValue(param.Key, param.Value);
                    }
                }

                await using (var reader = command.ExecuteReader())
                {
                    dataTable.Load(reader);
                }
            }
        }

        return dataTable;
    }
}