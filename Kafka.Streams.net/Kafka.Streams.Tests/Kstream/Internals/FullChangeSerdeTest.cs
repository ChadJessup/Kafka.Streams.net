//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Internals;
//using Xunit;

//namespace Kafka.Streams.Tests.Kstream.Internals
//{
//    public class FullChangeSerdeTest
//    {
//        private FullChangeSerde<string> serde = FullChangeSerde.Wrap(Serdes.String());

//        [Fact]
//        public void shouldRoundTripNull()
//        {
//            Assert.Equal(serde.serializeParts(null, null), nullValue());
//            Assert.Equal(FullChangeSerde.mergeChangeArraysIntoSingleLegacyFormattedArray(null), nullValue());
//            Assert.Equal(FullChangeSerde.decomposeLegacyFormattedArrayIntoChangeArrays(null), nullValue());
//            Assert.Equal(serde.deserializeParts(null, null), nullValue());
//        }


//        [Fact]
//        public void shouldRoundTripNullChange()
//        {
//            Assert.Equal(
//                serde.serializeParts(null, new Change<string>(null, null)),
//                new Change<byte[]>(null, null)
//            );

//            Assert.Equal(
//                serde.deserializeParts(null, new Change<string>(null, null)),
//                new Change<string>(null, null)
//            );

//            byte[] legacyFormat = FullChangeSerde.mergeChangeArraysIntoSingleLegacyFormattedArray(new Change<string>(null, null));
//            Assert.Equal(
//                FullChangeSerde.decomposeLegacyFormattedArrayIntoChangeArrays(legacyFormat),
//                new Change<byte[]>(null, null)
//            );
//        }

//        [Fact]
//        public void shouldRoundTripOldNull()
//        {
//            Change<byte[]> serialized = serde.serializeParts(null, new Change<string>("new", null));
//            byte[] legacyFormat = FullChangeSerde.mergeChangeArraysIntoSingleLegacyFormattedArray(serialized);
//            Change<byte[]> decomposedLegacyFormat = FullChangeSerde.decomposeLegacyFormattedArrayIntoChangeArrays(legacyFormat);
//            Assert.Equal(
//                serde.deserializeParts(null, decomposedLegacyFormat),
//                new Change<string>("new", null)
//            );
//        }

//        [Fact]
//        public void shouldRoundTripNewNull()
//        {
//            Change<byte[]> serialized = serde.serializeParts(null, new Change<string>(null, "old"));
//            byte[] legacyFormat = FullChangeSerde.mergeChangeArraysIntoSingleLegacyFormattedArray(serialized);
//            Change<byte[]> decomposedLegacyFormat = FullChangeSerde.decomposeLegacyFormattedArrayIntoChangeArrays(legacyFormat);
//            Assert.Equal(
//                serde.deserializeParts(null, decomposedLegacyFormat),
//                new Change<string>(null, "old")
//            );
//        }

//        [Fact]
//        public void shouldRoundTripChange()
//        {
//            IChange<byte[]> serialized = serde.SerializeParts(null, new Change<string>("new", "old"));
//            byte[] legacyFormat = FullChangeSerde.mergeChangeArraysIntoSingleLegacyFormattedArray(serialized);
//            IChange<byte[]> decomposedLegacyFormat = FullChangeSerde.decomposeLegacyFormattedArrayIntoChangeArrays(legacyFormat);
//            Assert.Equal(
//                serde.DeserializeParts(null, decomposedLegacyFormat),
//                new Change<string>("new", "old")
//            );
//        }
//    }
//}
