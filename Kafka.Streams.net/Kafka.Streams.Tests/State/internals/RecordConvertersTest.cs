//using Confluent.Kafka;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.State.Interfaces;
//using System;
//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */












//    public class RecordConvertersTest
//    {

//        private IRecordConverter timestampedValueConverter = rawValueToTimestampedValue();

//        [Xunit.Fact]
//        public void ShouldPreserveNullValueOnConversion()
//        {
//            ConsumeResult<byte[], byte[]> nullValueRecord = new ConsumeResult<byte[], byte[]>("", 0, 0L, System.Array.Empty<byte>(), null);
//            Assert.Null(timestampedValueConverter.convert(nullValueRecord).Value);
//        }

//        [Xunit.Fact]
//        public void ShouldAddTimestampToValueOnConversionWhenValueIsNotNull()
//        {
//            long timestamp = 10L;
//            byte[] value = new byte[1];
//            ConsumeResult<byte[], byte[]> inputRecord = new ConsumeResult<byte[], byte[]>(
//                    "topic", 1, 0, timestamp, TimestampType.CreateTime, 0L, 0, 0, System.Array.Empty<byte>(), value);
//            byte[] expectedValue = new ByteBuffer().Allocate(9).putLong(timestamp).put(value).array();
//            byte[] actualValue = timestampedValueConverter.Convert(inputRecord).Value;
//            Array.Equals(expectedValue, actualValue);
//        }
//    }
//}
///*






//*

//*





//*/












