using Kafka.Streams.Kafka.Streams;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{
    public class KTableKTableRightJoinTest
    {
        [Fact]
        public void shouldLogAndMeterSkippedRecordsDueToNullLeftKey()
        {
            var builder = new StreamsBuilder();


            Processor<string, Change<string>> join = new KTableKTableRightJoin<>(
                (IKTable<string, string, string>)builder.Table("left", Consumed.With(Serdes.String(), Serdes.String())),
                (IKTable<string, string, string>)builder.Table("right", Consumed.With(Serdes.String(), Serdes.String())),
                null
            ).Get();

            var context = new MockProcessorContext();
            context.SetRecordMetadata("left", -1, -2, null, -3);
            join.Init(context);
            LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
            join.Process(null, new Change<string>("new", "old"));
            LogCaptureAppender.unregister(appender);

            Assert.Equal(1.0, getMetricByName(context.metrics().metrics(), "skipped-records-total", "stream-metrics").metricValue());
            Assert.Equal(appender.getMessages(), asItem("Skipping record due to null key. change=[(new<-old)] topic=[left] partition=[-1] offset=[-2]"));
        }
    }
}
