using System;
using System.Collections.Generic;
using Kafka.Streams.Internals;
using Kafka.Streams.KStream.Internals;

namespace Kafka.Streams.KStream
{
    /**
     * The fixed-size time-based window specifications used for aggregations.
     * <p>
     * The semantics of time-based aggregation windows are: Every T1 (advance) milliseconds, compute the aggregate total for
     * T2 (size) milliseconds.
     * <ul>
     *     <li> If {@code advance < size} a hopping windows is defined:<br />
     *          it discretize a stream into overlapping windows, which implies that a record maybe contained in one and or
     *          more "adjacent" windows.</li>
     *     <li> If {@code advance == size} a tumbling window is defined:<br />
     *          it discretize a stream into non-overlapping windows, which implies that a record is only ever contained in
     *          one and only one tumbling window.</li>
     * </ul>
     * Thus, the specified {@link TimeWindow}s are aligned to the epoch.
     * Aligned to the epoch means, that the first window starts at timestamp zero.
     * For example, hopping windows with size of 5000ms and advance of 3000ms, have window boundaries
     * [0;5000),[3000;8000),... and not [1000;6000),[4000;9000),... or even something "random" like [1452;6452),[4452;9452),...
     * <p>
     * For time semantics, see {@link TimestampExtractor}.
     *
     * @see SessionWindows
     * @see UnlimitedWindows
     * @see JoinWindows
     * @see KGroupedStream#windowedBy(Windows)
     * @see TimestampExtractor
     */
    public class TimeWindows : Windows<TimeWindow>
    {
        private readonly TimeSpan maintainDuration;

        /** The size of the windows in milliseconds. */
        public TimeSpan size { get; }

        /**
         * The size of the window's advance interval in milliseconds, i.e., by how much a window moves forward relative to
         * the previous one.
         */
        public TimeSpan Advance { get; }

        private readonly TimeSpan grace;

        public TimeWindows(
            TimeSpan size,
            TimeSpan advance,
            TimeSpan grace,
            TimeSpan maintainDuration,
            int segments)
            : base(segments)
        {
            this.size = size;
            this.Advance = advance;
            this.grace = grace;
            this.maintainDuration = maintainDuration;
        }

        public TimeWindows(
            TimeSpan size,
            TimeSpan advance,
            TimeSpan grace,
            TimeSpan maintainDuration)
            : this(
                  size,
                  advance,
                  grace,
                  maintainDuration,
                  0)
        {
        }

        /**
         * Return a window definition with the given window size, and with the advance interval being equal to the window
         * size.
         * The time interval represented by the N-th window is: {@code [N * size, N * size + size)}.
         * <p>
         * This provides the semantics of tumbling windows, which are fixed-sized, gap-less, non-overlapping windows.
         * Tumbling windows are a special case of hopping windows with {@code advance == size}.
         *
         * @param size The size of the window
         * @return a new window definition with default maintain duration of 1 day
         * @throws ArgumentException if the specified window size is zero or negative or can't be represented as {@code long milliseconds}
         */
        public static TimeWindows Of(TimeSpan size)// throws ArgumentException
        {
            return new TimeWindows(size, size, TimeSpan.MaxValue, TimeSpan.FromDays(1.0));
        }

        /**
         * Return a window definition with the original size, but advance ("hop") the window by the given interval, which
         * specifies by how much a window moves forward relative to the previous one.
         * The time interval represented by the N-th window is: {@code [N * advance, N * advance + size)}.
         * <p>
         * This provides the semantics of hopping windows, which are fixed-sized, overlapping windows.
         *
         * @param advance The advance interval ("hop") of the window, with the requirement that {@code 0 < advance.toMillis() <= sizeMs}.
         * @return a new window definition with default maintain duration of 1 day
         * @throws ArgumentException if the advance interval is negative, zero, or larger than the window size
         */
        [Obsolete]
        public TimeWindows AdvanceBy(TimeSpan advance)
        {
            return new TimeWindows(
                this.size,
                advance,
                this.grace,
                this.maintainDuration,
                this.segments);
        }

        public override Dictionary<DateTime, TimeWindow> WindowsFor(DateTime timestamp)
        {
            var calculatedStart = timestamp - this.size + this.Advance;

            DateTime windowStart = new DateTime((calculatedStart.Ticks / this.Advance.Ticks) * this.Advance.Ticks, DateTimeKind.Utc);

            Dictionary<DateTime, TimeWindow> windows = new Dictionary<DateTime, TimeWindow>();

            while (windowStart <= timestamp)
            {
                TimeWindow window = new TimeWindow(windowStart, windowStart + this.size);
                windows.Put(windowStart, window);
                windowStart += this.Advance;
            }

            return windows;
        }

        public override TimeSpan Size()
        {
            return this.size;
        }

        /**
         * Reject late events that arrive more than {@code millisAfterWindowEnd}
         * after the end of its window.
         *
         * Lateness is defined as (stream_time - record_timestamp).
         *
         * @param afterWindowEnd The grace period to admit late-arriving events to a window.
         * @return this updated builder
         * @throws ArgumentException if {@code afterWindowEnd} is negative or can't be represented as {@code long milliseconds}
         */
        public TimeWindows Grace(TimeSpan afterWindowEnd) // throws ArgumentException
        {
            string msgPrefix = ApiUtils.PrepareMillisCheckFailMsgPrefix(afterWindowEnd, "afterWindowEnd");
            var validatedAfterWindowEnd = ApiUtils.ValidateMillisecondDuration(afterWindowEnd, msgPrefix);
            if (validatedAfterWindowEnd < TimeSpan.FromMilliseconds(0))
            {
                throw new ArgumentException("Grace period must not be negative.");
            }

            return new TimeWindows(
                this.size,
                this.Advance,
                validatedAfterWindowEnd,
                this.maintainDuration,
                this.segments);
        }

        public TimeSpan GracePeriod()
        {
            // NOTE: in the future, when we remove maintainMs,
            // we should default the grace period to 24h to maintain the default behavior,
            // or we can default to (24h - size) if you want to be super accurate.
            return this.grace.TotalMilliseconds != -1
                ? this.grace
                : this.maintainDuration - this.size;
        }

        /**
         * {@inheritDoc}
         * <p>
         * For {@code TimeWindows} the maintain duration is at least as small as the window size.
         *
         * @return the window maintain duration
         * @deprecated since 2.1. Use {@link Materialized#retention} instead.
         */
        public long maintainMs()
        {
            return Math.Max((long)this.maintainDuration.TotalMilliseconds, (long)this.size.TotalMilliseconds);
        }

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || this.GetType() != o.GetType())
            {
                return false;
            }
            TimeWindows that = (TimeWindows)o;
            return this.maintainDuration == that.maintainDuration &&
                this.segments == that.segments &&
                this.size == that.size &&
                this.Advance == that.Advance &&
                this.grace == that.grace;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(
                this.maintainDuration,
                this.segments,
                this.size,
                this.Advance,
                this.grace);
        }

        public override string ToString()
        {
            return "TimeWindows{" +
                "maintainDurationMs=" + this.maintainDuration.TotalMilliseconds +
                ", sizeMs=" + this.size.TotalMilliseconds +
                ", advanceMs=" + this.Advance.TotalMilliseconds +
                ", graceMs=" + this.grace.TotalMilliseconds +
                ", segments=" + this.segments +
                '}';
        }
    }
}
