/*






 *

 *





 */




















public class ProcessorContextTest {
    private ProcessorContext context;

    
    public void Prepare() {
        StreamsConfig streamsConfig = mock(StreamsConfig);
        expect(streamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG)).andReturn("add-id");
        expect(streamsConfig.defaultValueSerde()).andReturn(Serdes.ByteArray());
        expect(streamsConfig.defaultKeySerde()).andReturn(Serdes.ByteArray());
        replay(streamsConfig);

        context = new ProcessorContextImpl(
            mock(TaskId),
            mock(StreamTask),
            streamsConfig,
            mock(RecordCollector),
            mock(ProcessorStateManager),
            mock(StreamsMetricsImpl),
            mock(ThreadCache)
        );
    }

    [Xunit.Fact]
    public void ShouldNotAllowToScheduleZeroMillisecondPunctuation() {
        try {
            context.schedule(Duration.ofMillis(0L), null, null);
            Assert.True(false, "Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            Assert.Equal(expected.getMessage(), ("The minimum supported scheduling interval is 1 millisecond."));
        }
    }

    [Xunit.Fact]
    public void ShouldNotAllowToScheduleSubMillisecondPunctuation() {
        try {
            context.schedule(Duration.ofNanos(999_999L), null, null);
            Assert.True(false, "Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            Assert.Equal(expected.getMessage(), ("The minimum supported scheduling interval is 1 millisecond."));
        }
    }
}
