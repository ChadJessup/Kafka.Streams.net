namespace Kafka.Streams.KStream.Internals
{
    /**
     * A {@link TimeWindow} covers a half-open time interval with its start timestamp as an inclusive boundary and its end
     * timestamp as exclusive boundary.
     * It is a fixed size window, i.e., all instances (of a single {@link org.apache.kafka.streams.kstream.TimeWindows
     * window specification}) will have the same size.
     * <p>
     * For time semantics, see {@link org.apache.kafka.streams.processor.ITimestampExtractor ITimestampExtractor}.
     *
     * @see SessionWindow
     * @see UnlimitedWindow
     * @see org.apache.kafka.streams.kstream.TimeWindows
     * @see org.apache.kafka.streams.processor.ITimestampExtractor
     */
    public class TimeWindow : Window
    {


        /**
         * Create a new window for the given start time (inclusive) and end time (exclusive).
         *
         * @param startMs the start timestamp of the window (inclusive)
         * @param endMs   the end timestamp of the window (exclusive)
         * @throws ArgumentException if {@code startMs} is negative or if {@code endMs} is smaller than or equal to
         * {@code startMs}
         */
        public TimeWindow(long startMs, long endMs)
            : base(startMs, endMs)
        {
            if (startMs == endMs)
            {
                throw new System.ArgumentException("Window endMs must be greater than window startMs.");
            }
        }

        /**
         * Check if the given window overlaps with this window.
         *
         * @param other another window
         * @return {@code true} if {@code other} overlaps with this window&mdash;{@code false} otherwise
         * @throws ArgumentException if the {@code other} window has a different type than {@code this} window
         */

        public override bool Overlap(Window other)
        {
            if (GetType() != other.GetType())
            {
                throw new System.ArgumentException("Cannot compare windows of different type. Other window has type "
                    + other.GetType() + ".");
            }
            TimeWindow otherWindow = (TimeWindow)other;
            return startMs < otherWindow.endMs && otherWindow.startMs < endMs;
        }
    }
}
