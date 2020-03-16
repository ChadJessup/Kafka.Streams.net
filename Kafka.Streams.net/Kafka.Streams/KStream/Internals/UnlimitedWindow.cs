using NodaTime;

namespace Kafka.Streams.KStream.Internals
{
    /**
     * {@link UnlimitedWindow} is an "infinite" large window with a fixed (inclusive) start time.
     * All windows of the same {@link org.apache.kafka.streams.kstream.UnlimitedWindows window specification} will have the
     * same start time.
     * To make the window size "infinite" end time is set to {@link long#MaxValue}.
     * <p>
     * For time semantics, see {@link org.apache.kafka.streams.processor.ITimestampExtractor ITimestampExtractor}.
     *
     * @see TimeWindow
     * @see SessionWindow
     * @see org.apache.kafka.streams.kstream.UnlimitedWindows
     * @see org.apache.kafka.streams.processor.ITimestampExtractor
     */
    public class UnlimitedWindow : Window
    {
        /**
         * Create a new window for the given start time (inclusive).
         *
         * @param startMs the start timestamp of the window (inclusive)
         * @throws ArgumentException if {@code start} is negative
         */
        public UnlimitedWindow(Instant start)
            : base(start, Duration.MaxValue)
        {
        }

        public UnlimitedWindow(long start)
            : this(Instant.FromUnixTimeMilliseconds(start))
        {
        }

        /**
         * Returns {@code true} if the given window is of the same type, because all unlimited windows overlap with each
         * other due to their infinite size.
         *
         * @param other another window
         * @return {@code true}
         * @throws ArgumentException if the {@code other} window has a different type than {@code this} window
         */

        public override bool Overlap(Window other)
        {
            if (GetType() != other.GetType())
            {
                throw new System.ArgumentException("Cannot compare windows of different type. Other window has type "
                    + other.GetType() + ".");
            }

            return true;
        }
    }
}
