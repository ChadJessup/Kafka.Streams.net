using Confluent.Kafka;
using Kafka.Streams.Errors;
using Kafka.Streams.KStream;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class SinkNodeTest
    {
        private ISerializer<byte[]> anySerializer = Serdes.ByteArray().Serializer;
        private StateSerdes<Bytes, Bytes> anyStateSerde = StateSerdes.WithBuiltinTypes("anyName", Bytes, Bytes);
        private IRecordCollector recordCollector = new RecordCollectorImpl(
            null,
            new LogContext("sinknode-test "),
            new DefaultProductionExceptionHandler());

        private InternalMockProcessorContext context = new InternalMockProcessorContext(
            anyStateSerde,
            recordCollector
        );

        private SinkNode<byte[], byte[]> sink = new SinkNode<byte[], byte[]>("anyNodeName",
                new StaticTopicNameExtractor<>("any-output-topic"), anySerializer, anySerializer, null);

        // Used to verify that the correct exceptions are thrown if the compiler checks are bypassed

        private SinkNode<object, object> illTypedSink = (SinkNode<object, object>)sink;


        public void Before()
        {
            recordCollector.Init(new MockProducer<>(true, anySerializer, anySerializer));
            sink.Init(context);
        }

        [Fact]
        public void ShouldThrowStreamsExceptionOnInputRecordWithInvalidTimestamp()
        {
            Bytes anyKey = new Bytes("any key".getBytes());
            Bytes anyValue = new Bytes("any value".getBytes());

            // When/Then
            context.setTime(-1); // ensures a negative timestamp is set for the record we send next
            try
            {
                illTypedSink.Process(anyKey, anyValue);
                Assert.True(false, "Should have thrown StreamsException");
            }
            catch (StreamsException ignored)
            {
                // expected
            }
        }

        [Fact]
        public void ShouldThrowStreamsExceptionOnKeyValueTypeSerializerMismatch()
        {
            string keyOfDifferentTypeThanSerializer = "key with different type";
            string valueOfDifferentTypeThanSerializer = "value with different type";

            // When/Then
            context.setTime(0);
            try
            {
                illTypedSink.Process(keyOfDifferentTypeThanSerializer, valueOfDifferentTypeThanSerializer);
                Assert.True(false, "Should have thrown StreamsException");
            }
            catch (StreamsException e)
            {
                Assert.Equal(e.getCause(), instanceOf(ClassCastException));
            }
        }

        [Fact]
        public void ShouldHandleNullKeysWhenThrowingStreamsExceptionOnKeyValueTypeSerializerMismatch()
        {
            string invalidValueToTriggerSerializerMismatch = "";

            // When/Then
            context.setTime(1);
            try
            {
                illTypedSink.Process(null, invalidValueToTriggerSerializerMismatch);
                Assert.True(false, "Should have thrown StreamsException");
            }
            catch (StreamsException e)
            {
                Assert.Equal(e.getCause(), instanceOf(ClassCastException));
                Assert.Equal(e.ToString(), containsString("unknown because key is null"));
            }
        }

        [Fact]
        public void ShouldHandleNullValuesWhenThrowingStreamsExceptionOnKeyValueTypeSerializerMismatch()
        {
            string invalidKeyToTriggerSerializerMismatch = "";

            // When/Then
            context.setTime(1);
            try
            {
                illTypedSink.Process(invalidKeyToTriggerSerializerMismatch, null);
                Assert.True(false, "Should have thrown StreamsException");
            }
            catch (StreamsException e)
            {
                Assert.Equal(e.getCause(), instanceOf(ClassCastException));
                Assert.Equal(e.ToString(), containsString("unknown because value is null"));
            }
        }
    }
}
