using Confluent.Kafka;
using Xunit;
using System;

namespace Kafka.Streams.Tests.State
{
    public class StateSerdesTest
    {

        [Xunit.Fact]// (expected = NullPointerException)
        public void shouldThrowIfTopicNameIsNullForBuiltinTypes()
        {
            StateSerdes.withBuiltinTypes(null, byte[], byte[]);
        }

        [Xunit.Fact]// (expected = NullPointerException)
        public void shouldThrowIfKeyClassIsNullForBuiltinTypes()
        {
            StateSerdes.withBuiltinTypes("anyName", null, byte[]);
        }

        [Xunit.Fact]// (expected = NullPointerException)
        public void shouldThrowIfValueClassIsNullForBuiltinTypes()
        {
            StateSerdes.withBuiltinTypes("anyName", byte[], null);
        }

        [Xunit.Fact]
        public void shouldReturnSerdesForBuiltInKeyAndValueTypesForBuiltinTypes()
        {
            Class[] supportedBuildInTypes = new Class[] {
            string,
            Short,
            int,
            long,
            Float,
            Double,
            byte[],
            ByteBuffer,
            Bytes
        };

            foreach (Class keyClass in supportedBuildInTypes)
            {
                foreach (Class valueClass in supportedBuildInTypes)
                {
                    Assert.assertNotNull(StateSerdes.withBuiltinTypes("anyName", keyClass, valueClass));
                }
            }
        }

        [Xunit.Fact]// (expected = IllegalArgumentException)
        public void shouldThrowForUnknownKeyTypeForBuiltinTypes()
        {
            StateSerdes.withBuiltinTypes("anyName", Class, byte[]);
        }

        [Xunit.Fact]// (expected = IllegalArgumentException)
        public void shouldThrowForUnknownValueTypeForBuiltinTypes()
        {
            StateSerdes.withBuiltinTypes("anyName", byte[], Class);
        }

        [Xunit.Fact]// (expected = NullPointerException)
        public void shouldThrowIfTopicNameIsNull()
        {
            new StateSerdes<>(null, Serdes.ByteArray(), Serdes.ByteArray());
        }

        [Xunit.Fact]// (expected = NullPointerException)
        public void shouldThrowIfKeyClassIsNull()
        {
            new StateSerdes<>("anyName", null, Serdes.ByteArray());
        }

        [Xunit.Fact]// (expected = NullPointerException)
        public void shouldThrowIfValueClassIsNull()
        {
            new StateSerdes<>("anyName", Serdes.ByteArray(), null);
        }

        [Xunit.Fact]
        public void shouldThrowIfIncompatibleSerdeForValue()
        {// throws ClassNotFoundException
            Class myClass = Class.forName("java.lang.string");
            StateSerdes<object, object> stateSerdes = new StateSerdes<object, object>("anyName", Serdes.serdeFrom(myClass), Serdes.serdeFrom(myClass));
            int myInt = 123;
            Exception e = assertThrows(StreamsException, () => stateSerdes.rawValue(myInt));
            Assert.Equal(
                e.getMessage(),
                equalTo(
                    "A serializer (org.apache.kafka.common.serialization.StringSerializer) " +
                    "is not compatible to the actual value type (value type: java.lang.int). " +
                    "Change the default Serdes in StreamConfig or provide correct Serdes via method parameters."));
        }

        [Xunit.Fact]
        public void shouldSkipValueAndTimestampeInformationForErrorOnTimestampAndValueSerialization()
        {// throws ClassNotFoundException
            Class myClass = Class.forName("java.lang.string");
            StateSerdes<object, object> stateSerdes =
                new StateSerdes<object, object>("anyName", Serdes.serdeFrom(myClass), new ValueAndTimestampSerde(Serdes.serdeFrom(myClass)));
            int myInt = 123;
            Exception e = assertThrows(StreamsException, () => stateSerdes.rawValue(ValueAndTimestamp.make(myInt, 0L)));
            Assert.Equal(
                e.getMessage(),
                equalTo(
                    "A serializer (org.apache.kafka.common.serialization.StringSerializer) " +
                        "is not compatible to the actual value type (value type: java.lang.int). " +
                        "Change the default Serdes in StreamConfig or provide correct Serdes via method parameters."));
        }

        [Xunit.Fact]
        public void shouldThrowIfIncompatibleSerdeForKey()
        {// throws ClassNotFoundException
            Class myClass = Class.forName("java.lang.string");
            StateSerdes<object, object> stateSerdes = new StateSerdes<object, object>("anyName", Serdes.serdeFrom(myClass), Serdes.serdeFrom(myClass));
            int myInt = 123;
            Exception e = assertThrows(StreamsException, () => stateSerdes.rawKey(myInt));
            Assert.Equal(
                e.getMessage(),
                equalTo(
                    "A serializer (org.apache.kafka.common.serialization.StringSerializer) " +
                        "is not compatible to the actual key type (key type: java.lang.int). " +
                        "Change the default Serdes in StreamConfig or provide correct Serdes via method parameters."));
        }

    }
}
