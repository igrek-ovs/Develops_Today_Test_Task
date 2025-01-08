using System.Data;
using ETL.Core;
using ETL.Core.Models;

public class BatchProcessor
{
    private readonly string _connectionString;
    private readonly string _csvFilePath;
    private readonly string _duplicateCsvFilePath;
    private readonly int _batchSize;

    public BatchProcessor(string connectionString, string csvFilePath, string duplicateCsvFilePath, int batchSize)
    {
        _connectionString = connectionString;
        _csvFilePath = csvFilePath;
        _duplicateCsvFilePath = duplicateCsvFilePath;
        _batchSize = batchSize;
    }

    public async Task ProcessAllAsync()
    {
        var seenKeys = new HashSet<string>();
        var uniqueRecords = new List<TaxiTripRecord>();
        var duplicates = new List<TaxiTripRecord>();

        var records = Extractor.ExtractDataAsync<TaxiTripRecord>(_csvFilePath);

        await foreach (var record in records)
        {
            var key = $"{record.TpepPickupDatetime}_{record.TpepDropoffDatetime}_{record.PassengerCount}";

            if (!seenKeys.Add(key))
            {
                duplicates.Add(record);
            }
            else
            {
                uniqueRecords.Add(record);
            }
        }

        Console.WriteLine($"Total unique records: {uniqueRecords.Count}");
        Console.WriteLine($"Total duplicates found: {duplicates.Count}");

        Transformer.ExportDuplicates(duplicates, _duplicateCsvFilePath);

        await ProcessUniqueRecordsInBatchesAsync(uniqueRecords);
    }

    private async Task ProcessUniqueRecordsInBatchesAsync(List<TaxiTripRecord> uniqueRecords)
    {
        var batch = new List<TaxiTripRecord>();

        foreach (var record in uniqueRecords)
        {
            batch.Add(record);

            if (batch.Count >= _batchSize)
            {
                await ProcessBatchAsync(batch);
                batch.Clear();
            }
        }

        if (batch.Any())
        {
            await ProcessBatchAsync(batch);
        }
    }

    private async Task ProcessBatchAsync(List<TaxiTripRecord> batch)
    {
        var validRecords = Transformer.FilterInvalidRecords(batch, IsValidTaxiTrip);

        var transformedRecords = validRecords.Select(record =>
        {
            Transformer.TransformFlags(record, r => r.StoreAndFwdFlag, (r, newFlag) => r.StoreAndFwdFlag = newFlag);

            Transformer.ConvertDates(record, r => r.TpepPickupDatetime, (r, utcDate) => r.TpepPickupDatetime = utcDate.ToString("o"));
            Transformer.ConvertDates(record, r => r.TpepDropoffDatetime, (r, utcDate) => r.TpepDropoffDatetime = utcDate.ToString("o"));

            return record;
        }).ToList();

        var dataTable = CreateDataTable(transformedRecords);
        await Loader.BulkInsert(dataTable, "TaxiTrips", _connectionString);

        Console.WriteLine($"Processed batch of {transformedRecords.Count} records.");
    }

    private static DataTable CreateDataTable(IEnumerable<TaxiTripRecord> records)
    {
        var table = new DataTable();
        table.Columns.Add("tpep_pickup_datetime", typeof(DateTime));
        table.Columns.Add("tpep_dropoff_datetime", typeof(DateTime));
        table.Columns.Add("passenger_count", typeof(int));
        table.Columns.Add("trip_distance", typeof(float));
        table.Columns.Add("store_and_fwd_flag", typeof(string));
        table.Columns.Add("PULocationID", typeof(int));
        table.Columns.Add("DOLocationID", typeof(int));
        table.Columns.Add("fare_amount", typeof(decimal));
        table.Columns.Add("tip_amount", typeof(decimal));

        foreach (var record in records)
        {
            table.Rows.Add(
                DateTime.Parse(record.TpepPickupDatetime),
                DateTime.Parse(record.TpepDropoffDatetime),
                record.PassengerCount,
                record.TripDistance,
                record.StoreAndFwdFlag,
                record.PuLocationId,
                record.DoLocationId,
                record.FareAmount,
                record.TipAmount
            );
        }

        return table;
    }

    private static bool IsValidTaxiTrip(TaxiTripRecord record)
    {
        return !string.IsNullOrWhiteSpace(record.TpepPickupDatetime) &&
               !string.IsNullOrWhiteSpace(record.TpepDropoffDatetime) &&
               !string.IsNullOrWhiteSpace(record.StoreAndFwdFlag) &&
               record.StoreAndFwdFlag is "N" or "Y" &&
               int.TryParse(record.PassengerCount, out var passengerCount) && passengerCount > 0 &&
               float.TryParse(record.TripDistance, out var tripDistance) && tripDistance >= 0 &&
               decimal.TryParse(record.FareAmount, out var fareAmount) && fareAmount >= 0 &&
               decimal.TryParse(record.TipAmount, out var tipAmount) && tipAmount >= 0 &&
               int.TryParse(record.DoLocationId, out var doLocationId) && doLocationId >= 0 &&
               int.TryParse(record.PuLocationId, out var puLocationId) && puLocationId >= 0;
    }
}
