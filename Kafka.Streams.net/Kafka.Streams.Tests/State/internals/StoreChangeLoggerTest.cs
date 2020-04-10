//using Confluent.Kafka;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.State;
//using Kafka.Streams.State.Internals;
//using System;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    public class StoreChangeLoggerTest
//    {

//        private readonly string topic = "topic";

//        private Dictionary<int, ValueAndTimestamp<string>> logged = new HashMap<>();
//        private Dictionary<int, Headers> loggedHeaders = new HashMap<>();

//        // private InternalMockProcessorContext context = new InternalMockProcessorContext(
//        //     StateSerdes.WithBuiltinTypes(topic, int, string),
//        //     new RecordCollectorImpl(
//        //         "StoreChangeLoggerTest",
//        //         new LogContext("StoreChangeLoggerTest "),
//        //         new DefaultProductionExceptionHandler(),
//        //         new Metrics().sensor("skipped-records"))
//        //     {
//        // 
//        //     public void Send<K1, V1>(
//        //         string topic,
//        //         K1 key,
//        //         V1 value,
//        //         Headers headers,
//        //         int partition,
//        //         long timestamp,
//        //         ISerializer<K1> keySerializer,
//        //         ISerializer<V1> valueSerializer)
//        // {
//        //     logged.Put((int)key, ValueAndTimestamp.Make((string)value, timestamp));
//        //     loggedHeaders.Put((int)key, headers);
//        // }

//        public void Send<K1, V1>(
//            string topic,
//            K1 key,
//            V1 value,
//            Headers headers,
//            long timestamp,
//            ISerializer<K1> keySerializer,
//            ISerializer<V1> valueSerializer,
//            IStreamPartitioner<K1, V1> partitioner)
//        {
//            throw new InvalidOperationException();
//        }
//    }

//    private StoreChangeLogger<int, string> changeLogger =
//        new StoreChangeLogger<int, string>(topic, context, StateSerdes.WithBuiltinTypes(topic, int, string));

//    [Fact]
//    public void TestAddRemove()
//    {
//        context.setTime(1);
//        changeLogger.logChange(0, "zero");
//        context.setTime(5);
//        changeLogger.logChange(1, "one");
//        changeLogger.logChange(2, "two");
//        changeLogger.logChange(3, "three", 42L);

//        Assert.Equal(ValueAndTimestamp.Make("zero", 1L), logged.Get(0));
//        Assert.Equal(ValueAndTimestamp.Make("one", 5L), logged.Get(1));
//        Assert.Equal(ValueAndTimestamp.Make("two", 5L), logged.Get(2));
//        Assert.Equal(ValueAndTimestamp.Make("three", 42L), logged.Get(3));

//        changeLogger.logChange(0, null);
//        Assert.Null(logged.Get(0));
//    }

//    [Fact]
//    public void ShouldNotSendRecordHeadersToChangelogTopic()
//    {
//        context.Headers.Add(new Header("key", "value".getBytes()));
//        changeLogger.logChange(0, "zero");
//        changeLogger.logChange(0, "zero", 42L);

//        Assert.Null(loggedHeaders.Get(0));
//    }
//}
