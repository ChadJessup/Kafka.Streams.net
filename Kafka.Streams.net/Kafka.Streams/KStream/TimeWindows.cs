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
        private long maintainDurationMs;

        /** The size of the windows in milliseconds. */
        public long sizeMs;

        /**
         * The size of the window's advance interval in milliseconds, i.e., by how much a window moves forward relative to
         * the previous one.
         */
        public long advanceMs;
        private long graceMs;

        private TimeWindows(long sizeMs, long advanceMs, long graceMs, long maintainDurationMs)
        {
            this.sizeMs = sizeMs;
            this.advanceMs = advanceMs;
            this.graceMs = graceMs;
            this.maintainDurationMs = maintainDurationMs;
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
        public static TimeWindows of(TimeSpan size)// throws ArgumentException
        {
            String msgPrefix = ApiUtils.PrepareMillisCheckFailMsgPrefix(size, "size");
            return of(ApiUtils.ValidateMillisecondDuration(size, msgPrefix));
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
        public TimeWindows advanceBy(TimeSpan advance)
        {
            String msgPrefix = ApiUtils.PrepareMillisCheckFailMsgPrefix(advance, "advance");
            return advanceBy(ApiUtils.ValidateMillisecondDuration(advance, msgPrefix));
        }

        public Dictionary<long, TimeWindow> WindowsFor(long timestamp)
        {
            long windowStart = (Math.Max(0, timestamp - sizeMs + advanceMs) / advanceMs) * advanceMs;
            Dictionary<long, TimeWindow> windows = new Dictionary<long, TimeWindow>();
            while (windowStart <= timestamp)
            {
                TimeWindow window = new TimeWindow(windowStart, windowStart + sizeMs);
                windows.Put(windowStart, window);
                windowStart += advanceMs;
            }
            return windows;
        }

        public long Size()
        {
            return sizeMs;
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
        public TimeWindows grace(TimeSpan afterWindowEnd) // throws ArgumentException
        {
            String msgPrefix = ApiUtils.PrepareMillisCheckFailMsgPrefix(afterWindowEnd, "afterWindowEnd");
            var afterWindowEndMs = ApiUtils.ValidateMillisecondDuration(afterWindowEnd, msgPrefix);
            if (afterWindowEndMs < 0)
            {
                throw new ArgumentException("Grace period must not be negative.");
            }

            return new TimeWindows(sizeMs, advanceMs, afterWindowEndMs, maintainDurationMs, segments);
        }

        public long gracePeriodMs()
        {
            // NOTE: in the future, when we remove maintainMs,
            // we should default the grace period to 24h to maintain the default behavior,
            // or we can default to (24h - size) if you want to be super accurate.
            return graceMs != -1 ? graceMs : maintainMs() - size();
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
            return Math.Max(maintainDurationMs, sizeMs);
        }

        public bool Equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }
            TimeWindows that = (TimeWindows)o;
            return maintainDurationMs == that.maintainDurationMs &&
                segments == that.segments &&
                sizeMs == that.sizeMs &&
                advanceMs == that.advanceMs &&
                graceMs == that.graceMs;
        }

        public int GetHashCode()
        {
            return HashCode.Combine(
                maintainDurationMs,
                segments,
                sizeMs,
                advanceMs,
                graceMs);
        }

        public string ToString()
        {
            return "TimeWindows{" +
                "maintainDurationMs=" + maintainDurationMs +
                ", sizeMs=" + sizeMs +
                ", advanceMs=" + advanceMs +
                ", graceMs=" + graceMs +
                ", segments=" + segments +
                '}';
        }
    }
}
