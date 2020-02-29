using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.IO;
using System.Threading;
using Xunit;

namespace Kafka.Streams.Tests
{
    internal static class TestUtils
    {
        internal static void waitForCondition(
            Func<bool> condition,
            string errorMessage)
            => waitForCondition(condition, TimeSpan.FromSeconds(20.0), errorMessage);

        internal static void waitForCondition(
            Func<bool> condition,
            TimeSpan? timeout,
            string errorMessage)
        {
            if (!timeout.HasValue)
            {
                timeout = TimeSpan.FromSeconds(20.0);
            }

            Assert.True(SpinWait.SpinUntil(condition, timeout.Value), errorMessage);
        }

        public static StreamsBuilder GetStreamsBuilder(StreamsConfig config)
        {
            var services = new ServiceCollection().AddSingleton(config);
            return new StreamsBuilder(services);
        }

        public static string GetTempDirectory()
        {
            return Path.GetTempPath();
        }
    }
}