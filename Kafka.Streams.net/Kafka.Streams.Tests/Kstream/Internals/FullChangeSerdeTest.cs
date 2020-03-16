//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Internals;

//namespace Kafka.Streams.KStream.Internals
//{

//    public class FullChangeSerdeTest
//    {
//        private FullChangeSerde<string> serde = FullChangeSerde.wrap(Serdes.String());

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
//                serde.serializeParts(null, new Change<>(null, null)),
//                (new Change<byte[]>(null, null))
//            );

//            Assert.Equal(
//                serde.deserializeParts(null, new Change<>(null, null)),
//                (new Change<string>(null, null))
//            );

//            byte[] legacyFormat = FullChangeSerde.mergeChangeArraysIntoSingleLegacyFormattedArray(new Change<>(null, null));
//            Assert.Equal(
//                FullChangeSerde.decomposeLegacyFormattedArrayIntoChangeArrays(legacyFormat),
//                (new Change<byte[]>(null, null))
//            );
//        }

//        [Fact]
//        public void shouldRoundTripOldNull()
//        {
//            Change<byte[]> serialized = serde.serializeParts(null, new Change<>("new", null));
//            byte[] legacyFormat = FullChangeSerde.mergeChangeArraysIntoSingleLegacyFormattedArray(serialized);
//            Change<byte[]> decomposedLegacyFormat = FullChangeSerde.decomposeLegacyFormattedArrayIntoChangeArrays(legacyFormat);
//            Assert.Equal(
//                serde.deserializeParts(null, decomposedLegacyFormat),
//                (new Change<>("new", null))
//            );
//        }

//        [Fact]
//        public void shouldRoundTripNewNull()
//        {
//            Change<byte[]> serialized = serde.serializeParts(null, new Change<>(null, "old"));
//            byte[] legacyFormat = FullChangeSerde.mergeChangeArraysIntoSingleLegacyFormattedArray(serialized);
//            Change<byte[]> decomposedLegacyFormat = FullChangeSerde.decomposeLegacyFormattedArrayIntoChangeArrays(legacyFormat);
//            Assert.Equal(
//                serde.deserializeParts(null, decomposedLegacyFormat),
//                (new Change<>(null, "old"))
//            );
//        }

//        [Fact]
//        public void shouldRoundTripChange()
//        {
//            Change<byte[]> serialized = serde.serializeParts(null, new Change<>("new", "old"));
//            byte[] legacyFormat = FullChangeSerde.mergeChangeArraysIntoSingleLegacyFormattedArray(serialized);
//            Change<byte[]> decomposedLegacyFormat = FullChangeSerde.decomposeLegacyFormattedArrayIntoChangeArrays(legacyFormat);
//            Assert.Equal(
//                serde.deserializeParts(null, decomposedLegacyFormat),
//                (new Change<>("new", "old"))
//            );
//        }
//    }
//}
