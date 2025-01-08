using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;

namespace ETL.Core;

public static class Extractor
{
    public static async IAsyncEnumerable<T> ExtractDataAsync<T>(string csvFilePath, CsvConfiguration? configuration = null)
    {
        configuration ??= new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            TrimOptions = TrimOptions.Trim,
            IgnoreBlankLines = true,
            HeaderValidated = null,
            MissingFieldFound = null
        };

        using (var reader = new StreamReader(csvFilePath))
        using (var csv = new CsvReader(reader, configuration))
        {
            await foreach (var record in csv.GetRecordsAsync<T>())
            {
                yield return record;
            }
        }
    }
}