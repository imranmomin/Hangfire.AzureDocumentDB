using System;
using System.Globalization;

namespace Hangfire.AzureDocumentDB.Helper
{
    internal static class TimeHelper
    {
        public static int ToEpoch(this DateTime date)
        {
            if (date.Equals(DateTime.MinValue)) return int.MinValue;
            DateTime epochDateTime = new DateTime(1970, 1, 1);
            TimeSpan epochTimeSpan = date - epochDateTime;
            return (int)epochTimeSpan.TotalSeconds;
        }

        public static string ToEpoch(this string s)
        {
            return DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTime date)
                ? date.ToEpoch().ToString(CultureInfo.InvariantCulture)
                : s;
        }
    }

}