using Kafka.Streams.Nodes;

namespace Kafka.Streams.Processors.Internals
{
    public class PunctuationSchedule : Stamped<ProcessorNode>
    {
        private readonly long interval;
        public Punctuator punctuator { get; }
        public bool isCancelled { get; private set; } = false;

        // this Cancellable will be re-pointed at the successor schedule in next()
        public RepointableCancellable cancellable { get; }

        public PunctuationSchedule(
            ProcessorNode node,
            long time,
            long interval,
            Punctuator punctuator)
            : this(node, time, interval, punctuator, new RepointableCancellable())
        {
            cancellable.setSchedule(this);
        }

        public PunctuationSchedule(
            ProcessorNode node,
            long time,
            long interval,
            Punctuator punctuator,
            RepointableCancellable cancellable)
            : base(node, time)
        {
            this.interval = interval;
            this.punctuator = punctuator;
            this.cancellable = cancellable;
        }

        public ProcessorNode node()
        {
            return value;
        }

        public void markCancelled()
        {
            isCancelled = true;
        }

        public PunctuationSchedule next(long currTimestamp)
        {
            long nextPunctuationTime = timestamp + interval;
            if (currTimestamp >= nextPunctuationTime)
            {
                // we missed one ore more punctuations
                // avoid scheduling a new punctuations immediately, this can happen:
                // - when using STREAM_TIME punctuation and there was a gap i.e., no data was
                //   received for at least 2*interval
                // - when using WALL_CLOCK_TIME and there was a gap i.e., punctuation was delayed for at least 2*interval (GC pause, overload, ...)
                long intervalsMissed = (currTimestamp - timestamp) / interval;
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