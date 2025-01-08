using ETL.Core;
using Microsoft.Extensions.Configuration;

namespace Develops_Today_Test_Task;

class Program
{
    static async Task Main(string[] args)
    {
        var config = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();

        var etlSettings = config.GetSection("ETLSettings");
        string csvFilePath = etlSettings["CsvFilePath"];
        string connectionString = etlSettings["ConnectionString"];
        string duplicateCsvFilePath = etlSettings["DuplicateCsvFilePath"];
        int batchSize = int.Parse(etlSettings["BatchSize"]);
        
        var batchProcessor = new BatchProcessor(connectionString, csvFilePath, duplicateCsvFilePath, batchSize);
        await batchProcessor.ProcessAllAsync();

        var menuHandler = new MenuHandler(connectionString);
        await menuHandler.RunAsync();
    }
}