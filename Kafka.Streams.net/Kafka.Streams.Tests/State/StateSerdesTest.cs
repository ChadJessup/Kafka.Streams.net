//using Kafka.Streams.Errors;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.State;
//using System;
//using Xunit;

//namespace Kafka.Streams.Tests.State
//{
//    public class StateSerdesTest
//    {
//        [Fact]// (expected = NullReferenceException)
//        public void ShouldThrowIfTopicNameIsNullForBuiltinTypes()
//        {
//            StateSerdes.WithBuiltinTypes(null, byte[], byte[]);
//        }

//        [Fact]// (expected = NullReferenceException)
//        public void ShouldThrowIfKeyClassIsNullForBuiltinTypes()
//        {
//            StateSerdes.WithBuiltinTypes("anyName", null, byte[]);
//        }

//        [Fact]// (expected = NullReferenceException)
//        public void ShouldThrowIfValueClassIsNullForBuiltinTypes()
//        {
//            StateSerdes.WithBuiltinTypes("anyName", byte[], null);
//        }

//        [Fact]
//        public void ShouldReturnSerdesForBuiltInKeyAndValueTypesForBuiltinTypes()
//        {
//            Type[] supportedBuildInTypes = new Type[]
//            {
//                typeof(string),
//                typeof(short),
//                typeof(int),
//                typeof(long),
//                typeof(float),
//                typeof(double),
//                typeof(byte[]),
//                typeof(ByteBuffer),
//                typeof(Bytes),
//            };

//            foreach (var keyClass in supportedBuildInTypes)
//            {
//                foreach (var valueClass in supportedBuildInTypes)
//                {
//                    Assert.NotNull(StateSerdes.WithBuiltinTypes("anyName", keyClass, valueClass));
//                }
//            }
//        }

//        [Fact]// (expected = ArgumentException)
//        public void ShouldThrowForUnknownKeyTypeForBuiltinTypes()
//        {
//            StateSerdes.WithBuiltinTypes("anyName", Class, byte[]);
//        }

//        [Fact]// (expected = ArgumentException)
//        public void ShouldThrowForUnknownValueTypeForBuiltinTypes()
//        {
//            StateSerdes.WithBuiltinTypes("anyName", byte[], Class);
//        }

//        [Fact]// (expected = NullReferenceException)
//        public void ShouldThrowIfTopicNameIsNull()
//        {
//            new StateSerdes<>(null, Serdes.ByteArray(), Serdes.ByteArray());
//        }

//        [Fact]// (expected = NullReferenceException)
//        public void ShouldThrowIfKeyClassIsNull()
//        {
//            new StateSerdes<>("anyName", null, Serdes.ByteArray());
//        }

//        [Fact]// (expected = NullReferenceException)
//        public void ShouldThrowIfValueClassIsNull()
//        {
//            new StateSerdes<>("anyName", Serdes.ByteArray(), null);
//        }

//        [Fact]
//        public void ShouldThrowIfIncompatibleSerdeForValue()
//        {
//            // throws ClassNotFoundException
//            // Class myClass = Class.forName("java.lang.string");
//            StateSerdes<object, object> stateSerdes = new StateSerdes<object, object>("anyName", Serdes.SerdeFrom(myClass), Serdes.SerdeFrom(myClass));
//            int myInt = 123;
//            Exception e = Assert.Throws<StreamsException>(() => stateSerdes.RawValue(myInt));
//            Assert.Equal(
//                e.ToString(),
//                equalTo(
//                    "A serializer (org.apache.kafka.common.serialization.Serdes.String().Serializer) " +
//                    "is not compatible to the actual value type (value type: java.lang.int). " +
//                    "Change the default Serdes in StreamConfig or provide correct Serdes via method parameters."));
//        }

//        [Fact]
//        public void ShouldSkipValueAndTimestampeInformationForErrorOnTimestampAndValueSerialization()
//        {// throws ClassNotFoundException
//            var myClass = Type.GetType("System.String");
//            StateSerdes<object, object> stateSerdes =
//                new StateSerdes<object, object>("anyName", Serdes.SerdeFrom(myClass), new ValueAndTimestampSerde(Serdes.SerdeFrom(myClass)));
//            int myInt = 123;
//            Exception e = Assert.Throws<StreamsException>(() => stateSerdes.RawValue(ValueAndTimestamp.Make(myInt, 0L)));
//            Assert.Equal(
//                e.ToString(),
//                equalTo(
//                    "A serializer (org.apache.kafka.common.serialization.Serdes.String().Serializer) " +
//                        "is not compatible to the actual value type (value type: java.lang.int). " +
//                        "Change the default Serdes in StreamConfig or provide correct Serdes via method parameters."));
//        }

//        [Fact]
//        public void ShouldThrowIfIncompatibleSerdeForKey()
//        {// throws ClassNotFoundException
//         //            Class myClass = Class.forName("java.lang.string");
//            StateSerdes<object, object> stateSerdes = new StateSerdes<object, object>(
//                "anyName", Serdes.SerdeFrom(myClass),
//                Serdes.SerdeFrom(myClass));
//            int myInt = 123;
//            Exception e = Assert.Throws(StreamsException, () => stateSerdes.rawKey(myInt));
//            Assert.Equal(
//                e.ToString(),
//                equalTo(
//                    "A serializer (org.apache.kafka.common.serialization.Serdes.String().Serializer) " +
//                        "is not compatible to the actual key type (key type: java.lang.int). " +
//                        "Change the default Serdes in StreamConfig or provide correct Serdes via method parameters."));
//        }
//    }
//}
