
namespace Kafka.Streams.Processors
{




    /**
     * Controls what notion of time is used for punctuation scheduled via {@link IProcessorContext#schedule(TimeSpan, PunctuationType, Punctuator)} schedule}:
     * <ul>
     *   <li>STREAM_TIME - uses "stream time", which is advanced by the processing of messages
     *   in accordance with the timestamp as extracted by the {@link ITimestampExtractor} in use.
     *   <b>NOTE:</b> Only advanced if messages arrive</li>
     *   <li>WALL_CLOCK_TIME - uses system time (the wall-clock time),
     *   which is advanced at the polling interval ({@link org.apache.kafka.streams.StreamsConfig#POLL_MS_CONFIG})
     *   independent of whether new messages arrive. <b>NOTE:</b> This is best effort only as its granularity is limited
     *   by how long an iteration of the processing loop takes to complete</li>
     * </ul>
     */
    public enum PunctuationType
    {

        STREAM_TIME,
        WALL_CLOCK_TIME,
    }
}
