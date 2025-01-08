using System.Data;
using Database;

namespace ETL.Core;

public class MenuHandler
{
    private readonly DatabaseManager _dbManager;

    public MenuHandler(string connectionString)
    {
        _dbManager = new DatabaseManager(connectionString);
    }

    public async Task RunAsync()
    {
        while (true)
        {
            Console.WriteLine("Choose an option:");
            Console.WriteLine("1. Find PULocationId with the highest average tip_amount.");
            Console.WriteLine("2. Find the top 100 longest fares by trip_distance.");
            Console.WriteLine("3. Find the top 100 longest fares by time spent traveling.");
            Console.WriteLine("4. Search trips by PULocationId.");
            Console.WriteLine("5. Count records.");
            Console.WriteLine("6. Delete all records.");
            Console.WriteLine("7. Exit.");
            Console.Write("Enter your choice: ");

            string choice = Console.ReadLine();
            Console.WriteLine();

            switch (choice)
            {
                case "1":
                    await FindHighestAverageTip();
                    break;
                case "2":
                    await FindTop100LongestByDistance();
                    break;
                case "3":
                    await FindTop100LongestByTime();
                    break;
                case "4":
                    await SearchByPuLocationId();
                    break;
                case "5":
                    await CoutntRecordsAsync();
                    break;
                case "6":
                    await DeleteAllRecordsAsync();
                    break;
                case "7":
                    Console.WriteLine("Exiting...");
                    return;
                default:
                    Console.WriteLine("Invalid choice. Please try again.");
                    break;
            }

            Console.WriteLine();
        }
    }

    private async Task CoutntRecordsAsync()
    {
        string query = @"
            SELECT COUNT(*) AS RecordCount FROM TaxiTrips;
        ";

        var result = await _dbManager.ExecuteQueryAsync(query);

        Console.WriteLine($"Count of records in TaxiTrips: {result.Rows[0]["RecordCount"]}");
    }
    
    private async Task DeleteAllRecordsAsync()
    {
        string deleteQuery = "DELETE FROM TaxiTrips;";

        try
        {
            await _dbManager.ExecuteNonQueryAsync(deleteQuery);
            Console.WriteLine("All records have been deleted from the table.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred while deleting records: {ex.Message}");
        }
    }
    
    private async Task FindHighestAverageTip()
    {
        string query = @"
            SELECT TOP 1 PULocationID, AVG(tip_amount) AS AvgTip
            FROM TaxiTrips
            GROUP BY PULocationID
            ORDER BY AvgTip DESC;
        ";

        var result = await _dbManager.ExecuteQueryAsync(query);

        Console.WriteLine("PULocationId with the highest average tip:");
        foreach (DataRow row in result.Rows)
        {
            Console.WriteLine($"PULocationID: {row["PULocationID"]}, Average Tip: {row["AvgTip"]}");
        }
    }

    private async Task FindTop100LongestByDistance()
    {
        string query = @"
            SELECT TOP 100 *
            FROM TaxiTrips
            ORDER BY trip_distance DESC;
        ";

        var result = await _dbManager.ExecuteQueryAsync(query);

        Console.WriteLine("Top 100 longest fares by distance:");
        foreach (DataRow row in result.Rows)
        {
            Console.WriteLine($"Trip Distance: {row["trip_distance"]}, PULocationID: {row["PULocationID"]}, DOLocationID: {row["DOLocationID"]}");
        }
    }

    private async Task FindTop100LongestByTime()
    {
        string query = @"
            SELECT TOP 100 *, DATEDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) AS DurationMinutes
            FROM TaxiTrips
            ORDER BY DurationMinutes DESC;
        ";

        var result = await _dbManager.ExecuteQueryAsync(query);

        Console.WriteLine("Top 100 longest fares by time:");
        foreach (DataRow row in result.Rows)
        {
            Console.WriteLine($"Duration (minutes): {row["DurationMinutes"]}, PULocationID: {row["PULocationID"]}, DOLocationID: {row["DOLocationID"]}");
        }
    }

    private async Task SearchByPuLocationId()
    {
        Console.Write("Enter PULocationID to search: ");
        string input = Console.ReadLine();

        if (!int.TryParse(input, out int puLocationId))
        {
            Console.WriteLine("Invalid input. Please enter a valid number.");
            return;
        }

        string query = @"
            SELECT *
            FROM TaxiTrips
            WHERE PULocationID = @PULocationID;
        ";

        var parameters = new Dictionary<string, object>
        {
            { "@PULocationID", puLocationId }
        };

        var result = await _dbManager.ExecuteQueryAsync(query, parameters);

        Console.WriteLine($"Trips with PULocationID = {puLocationId}:");
        foreach (DataRow row in result.Rows)
        {
            Console.WriteLine($"Pickup: {row["tpep_pickup_datetime"]}, Dropoff: {row["tpep_dropoff_datetime"]}, Distance: {row["trip_distance"]}, Tip: {row["tip_amount"]}");
        }
    }
}