using Kafka.Streams.KStream.Internals;
using System;
using System.IO;
using System.Threading;
using Xunit;

namespace Kafka.Streams.Tests
{
    internal class TestUtils
    {
        internal static void waitForCondition(Func<bool> condition, TimeSpan timeout, string errorMessage)
        {
            Assert.True(SpinWait.SpinUntil(condition, timeout), errorMessage);
        }

        public static string GetTempDirectory()
        {
            return Path.GetTempPath();
        }
    }
}