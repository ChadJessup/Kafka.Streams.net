using Kafka.Streams.Nodes;

namespace Kafka.Streams.Processors.Internals
{
    public class PunctuationSchedule : Stamped<IProcessorNode>
    {
        private readonly long interval;
        public IPunctuator punctuator { get; }
        public bool isCancelled { get; private set; } = false;

        // this Cancellable will be re-pointed at the successor schedule in next()
        public RepointableCancellable cancellable { get; }

        public PunctuationSchedule(
            IProcessorNode node,
            long time,
            long interval,
            IPunctuator punctuator)
            : this(node, time, interval, punctuator, new RepointableCancellable())
        {
            cancellable.setSchedule(this);
        }

        public PunctuationSchedule(
            IProcessorNode node,
            long time,
            long interval,
            IPunctuator punctuator,
            RepointableCancellable cancellable)
            : base(node, time)
        {
            this.interval = interval;
            this.punctuator = punctuator;
            this.cancellable = cancellable;
        }

        public IProcessorNode node()
        {
            return value;
        }

        public void markCancelled()
        {
            isCancelled = true;
        }

        public PunctuationSchedule next(long currTimestamp)
        {
            var nextPunctuationTime = timestamp + interval;
            if (currTimestamp >= nextPunctuationTime)
            {
                // we missed one ore more punctuations
                // avoid scheduling a new punctuations immediately, this can happen:
                // - when using STREAM_TIME punctuation and there was a gap i.e., no data was
                //   received for at least 2*interval
                // - when using WALL_CLOCK_TIME and there was a gap i.e., punctuation was delayed for at least 2*interval (GC pause, overload, ...)
                var intervalsMissed = (currTimestamp - timestamp) / interval;
                nextPunctuationTime = timestamp + (intervalsMissed + 1) * interval;
            }

            var nextSchedule = new PunctuationSchedule(value, nextPunctuationTime, interval, punctuator, cancellable);

            cancellable.setSchedule(nextSchedule);

            return nextSchedule;
        }

        public override bool Equals(object other)
        {
            return base.Equals(other);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}
