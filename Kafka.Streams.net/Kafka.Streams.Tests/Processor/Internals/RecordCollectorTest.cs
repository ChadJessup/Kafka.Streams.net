using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Errors;
using Kafka.Streams.KStream;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Temporary;
using Kafka.Streams.Tests.Errors;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class RecordCollectorTest
    {

        private LogContext logContext = new LogContext("test ");

        private List<PartitionInfo> infos = Arrays.asList(
            new PartitionInfo("topic1", 0, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
            new PartitionInfo("topic1", 1, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>()),
            new PartitionInfo("topic1", 2, Node.noNode(), System.Array.Empty<Node>(), System.Array.Empty<Node>())
        );

        private Cluster cluster = new Cluster(
            "cluster",
            Collections.singletonList(Node.noNode()),
            infos,
            Collections.emptySet<string>(),
            Collections.emptySet<string>());

        private ISerializer<byte[]> byteArraySerializer = Serdes.ByteArray().Serializer;
        private ISerializer<string> stringSerializer = Serdes.String().Serializer;

        private IStreamPartitioner<string, object> streamPartitioner = (topic, key, value, numPartitions) =>
        {
            return int.parseInt(key) % numPartitions;
        };

        [Fact]
        public void TestSpecificPartition()
        {
            RecordCollector collector = new RecordCollector(
                "RecordCollectorTest-TestSpecificPartition",
                //new LogContext("RecordCollectorTest-TestSpecificPartition "),
                new DefaultProductionExceptionHandler());
            collector.Init(new MockProducer<>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer));

            Headers headers = new Headers(new Header[] { new Header("key", "value".GetBytes()) });

            collector.Send("topic1", "999", "0", null, DateTime.MinValue, null, stringSerializer, stringSerializer);
            collector.Send("topic1", "999", "0", null, DateTime.MinValue, null, stringSerializer, stringSerializer);
            collector.Send("topic1", "999", "0", null, DateTime.MinValue, null, stringSerializer, stringSerializer);

            collector.Send("topic1", "999", "0", headers, DateTime.MinValue + TimeSpan.FromSeconds(1), null, stringSerializer, stringSerializer);
            collector.Send("topic1", "999", "0", headers, DateTime.MinValue + TimeSpan.FromSeconds(1), null, stringSerializer, stringSerializer);

            collector.Send("topic1", "999", "0", headers, DateTime.MinValue + TimeSpan.FromSeconds(2), null, stringSerializer, stringSerializer);

            Dictionary<TopicPartition, long> offsets = collector.offsets;

            Assert.Equal(2L, offsets[new TopicPartition("topic1", 0)]);
            Assert.Equal(1L, offsets[new TopicPartition("topic1", 1)]);
            Assert.Equal(0L, offsets[new TopicPartition("topic1", 2)]);

            // ignore StreamPartitioner
            collector.Send("topic1", "999", "0", null, DateTime.MinValue + TimeSpan.FromSeconds(0), stringSerializer, stringSerializer, null);
            collector.Send("topic1", "999", "0", null, DateTime.MinValue + TimeSpan.FromSeconds(1), stringSerializer, stringSerializer, null);
            collector.Send("topic1", "999", "0", headers, DateTime.MinValue + TimeSpan.FromSeconds(2), stringSerializer, stringSerializer, null);

            Assert.Equal(3L, offsets[new TopicPartition("topic1", 0)]);
            Assert.Equal(2L, offsets[new TopicPartition("topic1", 1)]);
            Assert.Equal(1L, offsets[new TopicPartition("topic1", 2)]);
        }

        [Fact]
        public void TestStreamPartitioner()
        {

            RecordCollector collector = new RecordCollector(
                "RecordCollectorTest-TestStreamPartitioner",
                //new LogContext("RecordCollectorTest-TestStreamPartitioner "),
                new DefaultProductionExceptionHandler()
                //new Metrics().sensor("skipped-records")
            );
            collector.Init(new MockProducer<>(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer));

            Headers headers = new Headers(new Header[] { new RecordHeader("key", "value".GetBytes()) });

            collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
            collector.send("topic1", "9", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
            collector.send("topic1", "27", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
            collector.send("topic1", "81", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
            collector.send("topic1", "243", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

            collector.send("topic1", "28", "0", headers, null, stringSerializer, stringSerializer, streamPartitioner);
            collector.send("topic1", "82", "0", headers, null, stringSerializer, stringSerializer, streamPartitioner);
            collector.send("topic1", "244", "0", headers, null, stringSerializer, stringSerializer, streamPartitioner);

            collector.send("topic1", "245", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

            Dictionary<TopicPartition, long> offsets = collector.offsets();

            Assert.Equal((long)4L, offsets.Get(new TopicPartition("topic1", 0)));
            Assert.Equal((long)2L, offsets.Get(new TopicPartition("topic1", 1)));
            Assert.Equal((long)0L, offsets.Get(new TopicPartition("topic1", 2)));
        }


        [Fact]// (expected = StreamsException)
        public void ShouldThrowStreamsExceptionOnAnyExceptionButProducerFencedException()
        {
            RecordCollector collector = new RecordCollector(
                "test",
                logContext,
                new DefaultProductionExceptionHandler(),
                //new Metrics().sensor("skipped-records"));
            //            collector.Init(new MockProducer(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer)
            //            {
            //
            //
            //            public Future<RecordMetadata> send(ProducerRecord record, Callback callback)
            //            {
            //                throw new KafkaException();
            //            }
            //        });

            collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner));
        }


        [Fact]
        public void ShouldThrowStreamsExceptionOnSubsequentCallIfASendFailsWithDefaultExceptionHandler()
        {
            RecordCollector collector = new RecordCollector(
                "test",
                //logContext,
                new DefaultProductionExceptionHandler(),
                //new Metrics().sensor("skipped-records"));
            //          collector.Init(new MockProducer(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer)
            //          {
            //
            //
            //          public Future<RecordMetadata> send(ProducerRecord record, Callback callback)
            //          {
            //              callback.onCompletion(null, new Exception());
            //              return null;
            //          }
            //      });

            collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner));

            try
            {
                collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
                Assert.True(false, "Should have thrown StreamsException");
            }
            catch (StreamsException expected) { /* ok */ }
        }


        [Fact]
        public void ShouldNotThrowStreamsExceptionOnSubsequentCallIfASendFailsWithContinueExceptionHandler()
        {
            RecordCollector collector = new RecordCollector(
                "test",
                //logContext,
                new AlwaysContinueProductionExceptionHandler());
                //new Metrics().sensor("skipped-records"));
            //            collector.Init(new MockProducer(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer)
            //            {
            //
            //
            //            public Future<RecordMetadata> send(ProducerRecord record, Callback callback)
            //            {
            //                callback.onCompletion(null, new Exception());
            //                return null;
            //            }
            //        });

            collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

            collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
        }


        [Fact]
        public void ShouldRecordSkippedMetricAndLogWarningIfSendFailsWithContinueExceptionHandler()
        {
            //Metrics metrics = new Metrics();
            //Sensor sensor = metrics.sensor("skipped-records");
            //LogCaptureAppender logCaptureAppender = LogCaptureAppender.CreateAndRegister();
            //MetricName metricName = new MetricName("Name", "group", "description", Collections.emptyMap());
            //sensor.Add(metricName, new WindowedSum());
            RecordCollector collector = new RecordCollector(
                "test",
                //  logContext,
                new AlwaysContinueProductionExceptionHandler());
                //sensor);
            //            collector.Init(new MockProducer(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer)
            //            {
            //
            //
            //            public Future<RecordMetadata> send(ProducerRecord record, Callback callback)
            //            {
            //                callback.onCompletion(null, new Exception());
            //                return null;
            //            }
            //        });
            collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
            Assert.Equal(1.0, metrics.metrics().Get(metricName).metricValue());
            Assert.True(logCaptureAppender.getMessages().Contains("test Error sending records topic=[topic1] and partition=[0]; The exception handler chose to CONTINUE processing in spite of this error. Enable TRACE logging to view failed messages key and value."));
            LogCaptureAppender.unregister(logCaptureAppender);
        }


        [Fact]
        public void ShouldThrowStreamsExceptionOnFlushIfASendFailedWithDefaultExceptionHandler()
        {
            RecordCollector collector = new RecordCollector(
                "test",
                //logContext,
                new DefaultProductionExceptionHandler());
                //new Metrics().sensor("skipped-records"));
            //            collector.Init(new MockProducer(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer)
            //            {
            //
            //
            //            public Future<RecordMetadata> send(ProducerRecord record, Callback callback)
            //            {
            //                callback.onCompletion(null, new Exception());
            //                return null;
            //            }
            //        });

            collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

            try
            {
                collector.Flush();
                Assert.True(false, "Should have thrown StreamsException");
            }
            catch (StreamsException expected) { /* ok */ }
        }


        [Fact]
        public void ShouldNotThrowStreamsExceptionOnFlushIfASendFailedWithContinueExceptionHandler()
        {
            RecordCollector collector = new RecordCollector(
                "test",
                //logContext,
                new AlwaysContinueProductionExceptionHandler());
                //new Metrics().sensor("skipped-records"));
            //            collector.Init(new MockProducer(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer)
            //            {
            //
            //
            //            public Future<RecordMetadata> send(ProducerRecord record, Callback callback)
            //            {
            //                callback.onCompletion(null, new Exception());
            //                return null;
            //            }
            //        });

            collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

            collector.Flush();
        }


        [Fact]
        public void ShouldThrowStreamsExceptionOnCloseIfASendFailedWithDefaultExceptionHandler()
        {
            RecordCollector collector = new RecordCollector(
                "test",
                //logContext,
                new DefaultProductionExceptionHandler());
                //new Metrics().sensor("skipped-records"));
            //    collector.Init(new MockProducer(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer)
            //    {
            //
            //
            //    public Future<RecordMetadata> send(ProducerRecord record, Callback callback)
            //    {
            //        callback.onCompletion(null, new Exception());
            //        return null;
            //    }
            //});

            collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

            try
            {
                collector.Close();
                Assert.True(false, "Should have thrown StreamsException");
            }
            catch (StreamsException expected) { /* ok */ }
        }


        [Fact]
        public void ShouldNotThrowStreamsExceptionOnCloseIfASendFailedWithContinueExceptionHandler()
        {
            RecordCollector collector = new RecordCollector(
                "test",
                //logContext,
                new AlwaysContinueProductionExceptionHandler());
                //new Metrics().sensor("skipped-records"));
            //            collector.Init(new MockProducer(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer)
            //            {
            //
            //
            //            public Future<RecordMetadata> send(ProducerRecord record, Callback callback)
            //            {
            //                callback.onCompletion(null, new Exception());
            //                return null;
            //            }
            //        });

            collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);

            collector.Close();
        }


        [Fact]// (expected = StreamsException)
        public void ShouldThrowIfTopicIsUnknownWithDefaultExceptionHandler()
        {
            RecordCollector collector = new RecordCollector(
                "test",
                //logContext,
                new DefaultProductionExceptionHandler());
                //new Metrics().sensor("skipped-records"));
            //    collector.Init(new MockProducer(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer)
            //    {
            //
            //
            //    public List<PartitionInfo> partitionsFor(string topic)
            //    {
            //        return Collections.emptyList();
            //    }
            //
            //});
            collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
        }


        [Fact]// (expected = StreamsException)
        public void ShouldThrowIfTopicIsUnknownWithContinueExceptionHandler()
        {
            RecordCollector collector = new RecordCollector(
                "test",
                //logContext,
                new AlwaysContinueProductionExceptionHandler());
                //new Metrics().sensor("skipped-records"));
            //            collector.Init(new MockProducer(cluster, true, new DefaultPartitioner(), byteArraySerializer, byteArraySerializer)
            //            {
            //
            //
            //            public List<PartitionInfo> partitionsFor(string topic)
            //            {
            //                return Collections.emptyList();
            //            }
            //
            //        });
            collector.send("topic1", "3", "0", null, null, stringSerializer, stringSerializer, streamPartitioner);
        }

        [Fact]
        public void TestRecordHeaderPassThroughSerializer()
        {
            CustomStringSerializer keySerializer = new CustomStringSerializer();
            CustomStringSerializer valueSerializer = new CustomStringSerializer();
            keySerializer.Configure(Collections.emptyMap(), true);

            RecordCollector collector = new RecordCollector(
                    "test",
                    //logContext,
                    new DefaultProductionExceptionHandler()
                    //new Metrics().sensor("skipped-records")
            );
            MockProducer<byte[], byte[]> mockProducer = new MockProducer<>(cluster, true, new DefaultPartitioner(),
                    byteArraySerializer, byteArraySerializer);
            collector.Init(mockProducer);

            collector.send("topic1", "3", "0", new Headers(), null, keySerializer, valueSerializer, streamPartitioner);

            List<ProducerRecord<byte[], byte[]>> recordHistory = mockProducer.history();
            foreach (ProducerRecord<byte[], byte[]> sentRecord in recordHistory)
            {
                Headers headers = sentRecord.Headers;
                Assert.Equal(2, headers.ToArray().Length);
                Assert.Equal(new RecordHeader("key", "key".GetBytes()), headers.lastHeader("key"));
                Assert.Equal(new RecordHeader("value", "value".GetBytes()), headers.lastHeader("value"));
            }
        }

        [Fact]
        public void TestShouldNotThrowNPEOnCloseIfProducerIsNotInitialized()
        {
            RecordCollector collector = new RecordCollector(
                    "NoNPE",
                    //logContext,
                    new DefaultProductionExceptionHandler()
                    //new Metrics().sensor("skipped-records")
            );

            collector.Close();
        }

        //    private static class CustomStringSerializer : Serdes.String().Serializer
        //    {
        //
        //        private bool isKey;
        //
        //        private CustomStringSerializer()
        //        {
        //        }
        //
        //
        //        public void Configure(Dictionary<string, ?> configs, bool isKey)
        //        {
        //            this.isKey = isKey;
        //            base.configure(configs, isKey);
        //        }
        //
        //
        //        public byte[] Serialize(string topic, Headers headers, string data)
        //        {
        //            if (isKey)
        //            {
        //                headers.Add(new RecordHeader("key", "key".getBytes()));
        //            }
        //            else
        //            {
        //                headers.Add(new RecordHeader("value", "value".getBytes()));
        //            }
        //            return serialize(topic, data);
        //        }
        //    }
        //}
    }
}
