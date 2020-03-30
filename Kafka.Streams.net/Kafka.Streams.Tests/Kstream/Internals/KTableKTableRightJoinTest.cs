namespace Kafka.Streams.Tests.Kstream.Internals
{
}
///*
//
//
//
//
//
//
// *
//
// *
//
//
//
//
//
// */
//using Kafka.Streams.Kafka.Streams;

//namespace Kafka.Streams.KStream.Internals
//{














//    public class KTableKTableRightJoinTest
//    {
//        [Fact]
//        public void shouldLogAndMeterSkippedRecordsDueToNullLeftKey()
//        {
//            var builder = new StreamsBuilder();


//            Processor<string, Change<string>> join = new KTableKTableRightJoin<>(
//                (IKTable<string, string, string>)builder.Table("left", Consumed.with(Serdes.String(), Serdes.String())),
//                (IKTable<string, string, string>)builder.Table("right", Consumed.with(Serdes.String(), Serdes.String())),
//                null
//            ).get();

//            var context = new MockProcessorContext();
//            context.setRecordMetadata("left", -1, -2, null, -3);
//            join.init(context);
//            LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
//            join.process(null, new Change<>("new", "old"));
//            LogCaptureAppender.unregister(appender);

//            Assert.Equal(1.0, getMetricByName(context.metrics().metrics(), "skipped-records-total", "stream-metrics").metricValue());
//            Assert.Equal(appender.getMessages(), asItem("Skipping record due to null key. change=[(new<-old)] topic=[left] partition=[-1] offset=[-2]"));
//        }
//    }
//}
