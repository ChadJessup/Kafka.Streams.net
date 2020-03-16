﻿namespace System
{
    public static class TimeExtensions
    {
        public static DateTime GetNewest(this DateTime dateTime, DateTime other)
            => dateTime.Ticks >= other.Ticks
                ? dateTime
                : other;
    }
}
