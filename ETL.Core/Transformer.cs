using System.Globalization;
using CsvHelper;

namespace ETL.Core;

public static class Transformer
{
    public static T TransformFlags<T>(T record, Func<T, string> flagSelector, Action<T, string> flagUpdater)
    {
        var flag = flagSelector(record);
        if (flag == "N")
            flagUpdater(record, "No");
        else if (flag == "Y")
            flagUpdater(record, "Yes");

        return record;
    }

    public static T ConvertDates<T>(T record, Func<T, string> dateSelector, Action<T, DateTime> dateUpdater)
    {
        var dateStr = dateSelector(record);
        if (DateTime.TryParse(dateStr, out var date))
        {
            var estTimeZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
            var utcDate = TimeZoneInfo.ConvertTimeToUtc(date, estTimeZone);
            dateUpdater(record, utcDate);
        }

        return record;
    }
    
    public static IEnumerable<T> FilterInvalidRecords<T>(IEnumerable<T> records, Func<T, bool> isValid)
    {
        return records.Where(isValid);
    }

    public static void ExportDuplicates<T>(IEnumerable<T> duplicates, string filePath)
    {
        using (var writer = new StreamWriter(filePath))
        using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
        {
            csv.WriteRecords(duplicates);
        }
    }
}