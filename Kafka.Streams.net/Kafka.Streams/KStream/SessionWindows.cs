using Kafka.Streams.Internals;
using NodaTime;
using System;

namespace Kafka.Streams.KStream
{
    /**
     * A session based window specification used for aggregating events into sessions.
     * <p>
     * Sessions represent a period of activity separated by a defined gap of inactivity.
     * Any events processed that fall within the inactivity gap of any existing sessions are merged into the existing sessions.
     * If the event falls outside of the session gap then a new session will be created.
     * <p>
     * For example, if we have a session gap of 5 and the following data arrives:
     * <pre>
     * +--------------------------------------+
     * |    key    |    value    |    time    |
     * +-----------+-------------+------------+
     * |    A      |     1       |     10     |
     * +-----------+-------------+------------+
     * |    A      |     2       |     12     |
     * +-----------+-------------+------------+
     * |    A      |     3       |     20     |
     * +-----------+-------------+------------+
     * </pre>
     * We'd have 2 sessions for key A.
     * One starting from time 10 and ending at time 12 and another starting and ending at time 20.
     * The length of the session is driven by the timestamps of the data within the session.
     * Thus, session windows are no fixed-size windows (c.f. {@link TimeWindows} and {@link JoinWindows}).
     * <p>
     * If we then received another record:
     * <pre>
     * +--------------------------------------+
     * |    key    |    value    |    time    |
     * +-----------+-------------+------------+
     * |    A      |     4       |     16     |
     * +-----------+-------------+------------+
     * </pre>
     * The previous 2 sessions would be merged into a single session with start time 10 and end time 20.
     * The aggregate value for this session would be the result of aggregating all 4 values.
     * <p>
     * For time semantics, see {@link TimestampExtractor}.
     *
     * @see TimeWindows
     * @see UnlimitedWindows
     * @see JoinWindows
     * @see KGroupedStream#windowedBy(SessionWindows)
     * @see TimestampExtractor
     */
    public class SessionWindows
    {
        private static readonly TimeSpan DEFAULT_RETENTION_MS = TimeSpan.FromDays(1.0);
        private TimeSpan gap;
        private TimeSpan maintainDuration;
        private TimeSpan graceWindow;

        private SessionWindows(TimeSpan gap, TimeSpan maintainDuration, TimeSpan graceWindow)
        {
            this.gap = gap;
            this.maintainDuration = maintainDuration;
            this.graceWindow = graceWindow;
        }

        /**
         * Create a new window specification with the specified inactivity gap in milliseconds.
         *
         * @param inactivityGapMs the gap of inactivity between sessions in milliseconds
         * @return a new window specification with default maintain duration of 1 day
         *
         * @throws ArgumentException if {@code inactivityGapMs} is zero or negative
         * @deprecated Use {@link #with(Duration)} instead.
         */
        public static SessionWindows with(TimeSpan inactivityGap)
        {
            if (inactivityGap.TotalMilliseconds <= 0)
            {
                throw new ArgumentException("Gap time (inactivityGap) cannot be zero or negative.");
            }

            return new SessionWindows(inactivityGap, DEFAULT_RETENTION_MS, TimeSpan.FromMilliseconds(-1));
        }

        /**
         * Create a new window specification with the specified inactivity gap.
         *
         * @param inactivityGap the gap of inactivity between sessions
         * @return a new window specification with default maintain duration of 1 day
         *
         * @throws ArgumentException if {@code inactivityGap} is zero or negative or can't be represented as {@code long milliseconds}
         */
        public static SessionWindows with(Duration inactivityGap)
        {
            String msgPrefix = ApiUtils.prepareMillisCheckFailMsgPrefix(inactivityGap, "inactivityGap");
            return with(ApiUtils.validateMillisecondDuration(inactivityGap.ToTimeSpan(), msgPrefix));
        }

        /**
         * Set the window maintain duration (retention time) in milliseconds.
         * This retention time is a guaranteed <i>lower bound</i> for how long a window will be maintained.
         *
         * @return itself
         * @throws ArgumentException if {@code durationMs} is smaller than window gap
         *
         * @deprecated since 2.1. Use {@link Materialized#retention}
         *             or directly configure the retention in a store supplier and use
         *             {@link Materialized#as(SessionBytesStoreSupplier)}.
         */
        public SessionWindows until(TimeSpan duration)// throws ArgumentException
        {
            if (duration < gap)
            {
                throw new ArgumentException("Window retention time (durationMs) cannot be smaller than window gap.");
            }

            return new SessionWindows(gap, duration, graceWindow);
        }

        /**
         * Reject late events that arrive more than {@code afterWindowEnd}
         * after the end of its window.
         *
         * Note that new events may change the boundaries of session windows, so aggressive
         * close times can lead to surprising results in which a too-late event is rejected and then
         * a subsequent event moves the window boundary forward.
         *
         * @param afterWindowEnd The grace period to admit late-arriving events to a window.
         * @return this updated builder
         * @throws ArgumentException if the {@code afterWindowEnd} is negative of can't be represented as {@code long milliseconds}
         */
        public SessionWindows grace(Duration afterWindowEnd)// throws ArgumentException
        {
            var msgPrefix = ApiUtils.prepareMillisCheckFailMsgPrefix(afterWindowEnd, "afterWindowEnd");
            var afterWindow = ApiUtils.validateMillisecondDuration(afterWindowEnd.ToTimeSpan(), msgPrefix);

            if (afterWindow.TotalMilliseconds < 0)
            {
                throw new ArgumentException("Grace period must not be negative.");
            }

            return new SessionWindows(
                gap,
                maintainDuration,
                afterWindow);
        }

        public TimeSpan gracePeriod()
        {
            // NOTE: in the future, when we remove maintainMs,
            // we should default the grace period to 24h to maintain the default behavior,
            // or we can default to (24h - gapMs) if you want to be super accurate.
            return graceWindow != TimeSpan.FromMilliseconds(-1)
                ? graceWindow
                : maintain() - inactivityGap();
        }

        /**
         * Return the specified gap for the session windows in milliseconds.
         *
         * @return the inactivity gap of the specified windows
         */
        public TimeSpan inactivityGap()
        {
            return this.gap;
        }

        /**
         * Return the window maintain duration (retention time) in milliseconds.
         * <p>
         * For {@code SessionWindows} the maintain duration is at least as small as the window gap.
         *
         * @return the window maintain duration
         * @deprecated since 2.1. Use {@link Materialized#retention} instead.
         */
        public TimeSpan maintain()
        {
            if (maintainDuration > gap)
            {
                return maintainDuration;
            }

            return gap;
        }


        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }

            if (o == null || GetType() != o.GetType())
            {
                return false;
            }

            SessionWindows that = (SessionWindows)o;
            return gap == that.gap &&
                maintainDuration == that.maintainDuration &&
                graceWindow == that.graceWindow;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(gap, maintainDuration, graceWindow);
        }

        public override string ToString()
        {
            return "SessionWindows{" +
                $"gapMs={gap}" +
                $", maintainDurationMs={maintainDuration}" +
                $", graceMs={graceWindow}" +
                '}';
        }
    }
}
