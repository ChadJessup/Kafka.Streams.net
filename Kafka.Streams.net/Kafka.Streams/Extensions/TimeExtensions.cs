namespace System
{
    public static class TimeExtensions
    {
        public static DateTime GetNewest(this DateTime dateTime, DateTime other)
            => dateTime.Ticks >= other.Ticks
                ? dateTime
                : other;

        public static DateTime GetOldest(this DateTime dateTime, DateTime other)
            => dateTime <= other
                ? dateTime
                : other;

        private readonly static DateTime epochStart = new DateTime(1970, 1, 1);
        public static long ToEpochMilliseconds(this DateTime dateTime)
        {
            TimeSpan delta = dateTime - epochStart;

            return (long)delta.TotalMilliseconds;
        }

        public static DateTime OfEpochMilli(this DateTime dateTime, long milliseconds)
        {
            return DateTimeOffset.FromUnixTimeMilliseconds(milliseconds).DateTime;
        }
    }
}
