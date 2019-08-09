using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Common.Extensions
{
    public static class TimeExtensions
    {
        private readonly static DateTime epochStart = new DateTime(1970, 1, 1);
        public static long ToEpochMilliseconds(this DateTime dateTime)
        {
            TimeSpan delta = dateTime - epochStart;

            return (long)delta.TotalMilliseconds;
        }

        public static DateTime ofEpochMilli(this DateTime dateTime, long milliseconds)
        {
            return DateTimeOffset.FromUnixTimeMilliseconds(milliseconds).DateTime;
        }
    }
}
