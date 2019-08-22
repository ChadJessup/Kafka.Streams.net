using System;
using System.Text.RegularExpressions;

namespace Kafka.Common.Extensions
{
    public static class StringHelpers
    {
        public static string[] Split(this string input, string pattern, RegexOptions options = RegexOptions.None, TimeSpan? matchTimeout = null)
        {
            matchTimeout ??= Regex.InfiniteMatchTimeout;

            return Regex.Split(input, pattern, options, matchTimeout.Value);
        }
    }
}
