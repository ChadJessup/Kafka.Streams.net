using Kafka.Streams.Internals;
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
     * The aggregate value for this session would be the result of aggregating All 4 values.
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
        private readonly TimeSpan gap;
        private readonly TimeSpan maintainDuration;
        private readonly TimeSpan graceWindow;

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
         * @deprecated Use {@link #with(TimeSpan)} instead.
         */
        public static SessionWindows With(TimeSpan inactivityGap)
        {
            if (inactivityGap.TotalMilliseconds <= 0)
            {
                throw new ArgumentException("Gap time (inactivityGap) cannot be zero or negative.");
            }

            return new SessionWindows(inactivityGap, DEFAULT_RETENTION_MS, TimeSpan.FromMilliseconds(-1));
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
        public SessionWindows Until(TimeSpan duration)
        {
            if (duration < this.gap)
            {
                throw new ArgumentException("Window retention time (durationMs) cannot be smaller than window gap.");
            }

            return new SessionWindows(this.gap, duration, this.graceWindow);
        }

        /**
         * Reject late events that arrive more than {@code afterWindowEnd}
         * after the end of its window.
         *
         * Note that new events may change the boundaries of session windows, so aggressive
         * Close times can lead to surprising results in which a too-late event is rejected and then
         * a subsequent event moves the window boundary forward.
         *
         * @param afterWindowEnd The grace period to admit late-arriving events to a window.
         * @return this updated builder
         * @throws ArgumentException if the {@code afterWindowEnd} is negative of can't be represented as {@code long milliseconds}
         */
        public SessionWindows Grace(TimeSpan afterWindowEnd)// throws ArgumentException
        {
            var msgPrefix = ApiUtils.PrepareMillisCheckFailMsgPrefix(afterWindowEnd, "afterWindowEnd");
            var afterWindow = ApiUtils.ValidateMillisecondDuration(afterWindowEnd, msgPrefix);

            if (afterWindow.TotalMilliseconds < 0)
            {
                throw new ArgumentException("Grace period must not be negative.");
            }

            return new SessionWindows(
                this.gap,
                this.maintainDuration,
                afterWindow);
        }

        public TimeSpan GracePeriod()
        {
            // NOTE: in the future, when we remove maintainMs,
            // we should default the grace period to 24h to maintain the default behavior,
            // or we can default to (24h - gapMs) if you want to be super accurate.
            return this.graceWindow != TimeSpan.FromMilliseconds(-1)
                ? this.graceWindow
                : this.Maintain() - this.InactivityGap();
        }

        /**
         * Return the specified gap for the session windows in milliseconds.
         *
         * @return the inactivity gap of the specified windows
         */
        public TimeSpan InactivityGap()
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
        public TimeSpan Maintain()
        {
            if (this.maintainDuration > this.gap)
            {
                return this.maintainDuration;
            }

            return this.gap;
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

            SessionWindows that = (SessionWindows)o;
            return this.gap == that.gap &&
                this.maintainDuration == that.maintainDuration &&
                this.graceWindow == that.graceWindow;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(this.gap, this.maintainDuration, this.graceWindow);
        }

        public override string ToString()
        {
            return "SessionWindows{" +
                $"gapMs={this.gap}" +
                $", maintainDurationMs={this.maintainDuration}" +
                $", graceMs={this.graceWindow}" +
                '}';
        }
    }
}
