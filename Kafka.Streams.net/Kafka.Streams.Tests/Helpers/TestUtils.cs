using Confluent.Kafka;
using Kafka.Streams.Clients;
using Kafka.Streams.Clients.Consumers;
using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Tasks;
using Kafka.Streams.Threads.Stream;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.IO;
using System.Threading;
using Xunit;

namespace Kafka.Streams.Tests.Helpers
{
    internal static class TestUtils
    {
        internal static void waitForCondition(
            Func<bool> condition,
            string errorMessage)
            => waitForCondition(condition, TimeSpan.FromSeconds(20.0), errorMessage);

        internal static void waitForCondition(
            Func<bool> condition,
            long timeoutMs,
            string errorMessage)
            => waitForCondition(condition, TimeSpan.FromMilliseconds(timeoutMs), errorMessage);

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
        public static StreamsBuilder GetStreamsBuilder(IServiceCollection services)
        {
            if (services is null)
            {
                throw new ArgumentNullException(nameof(services));
            }

            return new StreamsBuilder(services);
        }

        public static StreamsBuilder GetStreamsBuilder(StreamsConfig config)
        {
            if (config is null)
            {
                throw new ArgumentNullException(nameof(config));
            }

            var services = new ServiceCollection().AddSingleton(config);

            return new StreamsBuilder(services);
        }

        public static IStreamThread CreateStreamThread(
            StreamsBuilder streamsBuilder,
            string clientId = "testClientId",
            bool eosEnabled = false)
        {
            if (eosEnabled)
            {
                // clientSupplier.SetApplicationIdForProducer(applicationId);
            }

            //clientSupplier.SetClusterForAdminClient(createCluster());

            var thread = streamsBuilder.Services.GetRequiredService<IStreamThread>();

            return thread;
        }

        public static string GetTempDirectory()
        {
            return Path.GetTempPath();
        }

        internal static Mock<ITaskManager> GetMockTaskManagerCommit(int commits)
        {
            var mockTaskManager = GetMockTaskManager();
            mockTaskManager
                .SetupSequence(tm => tm.commitAll())
                .Returns(commits);

            return mockTaskManager;
        }

        internal static Mock<ITaskManager> GetMockTaskManager()
        {
            var mockTaskManager = new Mock<ITaskManager> { DefaultValue = DefaultValue.Mock };
            mockTaskManager.SetupAllProperties();

            return mockTaskManager;
        }

        internal static ILogger<T> GetMockLogger<T>()
        {
            return new Mock<ILogger<T>>().Object;
        }

        internal static T GetService<T>(ServiceProvider services)
            => services.GetRequiredService<T>();

        internal static Mock<IKafkaClientSupplier> GetMockClientSupplier(
            MockConsumer<byte[], byte[]> mockConsumer,
            MockRestoreConsumer mockRestoreConsumer)
        {
            var mockClientSupplier = new Mock<IKafkaClientSupplier>();
            mockClientSupplier
                .Setup(cs => cs.GetConsumer(It.IsAny<ConsumerConfig>(), It.IsAny<IConsumerRebalanceListener>()))
                    .Returns(mockConsumer);

            mockClientSupplier
                .Setup(cs => cs.GetRestoreConsumer(It.IsAny<RestoreConsumerConfig>()))
                    .Returns(mockRestoreConsumer);

            mockClientSupplier.SetupAllProperties();

            return mockClientSupplier;
        }

        internal static object GetTempDirectory(string v)
        {
            throw new NotImplementedException();
        }
    }
}
