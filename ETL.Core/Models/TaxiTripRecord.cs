using CsvHelper.Configuration.Attributes;

namespace ETL.Core.Models;

public class TaxiTripRecord
{
    [Name("tpep_pickup_datetime")] public string TpepPickupDatetime { get; set; }
    [Name("tpep_dropoff_datetime")] public string TpepDropoffDatetime { get; set; }
    [Name("passenger_count")] public string PassengerCount { get; set; }
    [Name("trip_distance")] public string TripDistance { get; set; }
    [Name("store_and_fwd_flag")] public string StoreAndFwdFlag { get; set; }
    [Name("PULocationID")] public string PuLocationId { get; set; }
    [Name("DOLocationID")] public string DoLocationId { get; set; }
    [Name("fare_amount")] public string FareAmount { get; set; }
    [Name("tip_amount")] public string TipAmount { get; set; }
}