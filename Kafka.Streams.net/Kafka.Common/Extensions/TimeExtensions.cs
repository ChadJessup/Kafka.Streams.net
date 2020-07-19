namespace System
{
    public static class TimeExtensions
    {
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
