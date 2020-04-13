using System;
using Kafka.Streams.Nodes;

namespace Kafka.Streams.Processors.Internals
{
    public class PunctuationSchedule : Stamped<IProcessorNode>
    {
        private readonly TimeSpan interval;
        public IPunctuator punctuator { get; }
        public bool isCancelled { get; private set; } = false;

        // this Cancellable will be re-pointed at the successor schedule in next()
        public RepointableCancellable cancellable { get; }

        public PunctuationSchedule(
            IProcessorNode node,
            DateTime time,
            TimeSpan interval,
            IPunctuator punctuator)
            : this(node, time, interval, punctuator, new RepointableCancellable())
        {
            this.cancellable.SetSchedule(this);
        }

        public PunctuationSchedule(
            IProcessorNode node,
            DateTime time,
            TimeSpan interval,
            IPunctuator punctuator,
            RepointableCancellable cancellable)
            : base(node, time)
        {
            this.interval = interval;
            this.punctuator = punctuator;
            this.cancellable = cancellable;
        }

        public IProcessorNode Node()
        {
            return this.value;
        }

        public void MarkCancelled()
        {
            this.isCancelled = true;
        }

        public PunctuationSchedule Next(DateTime currTimestamp)
        {
            var nextPunctuationTime = this.timestamp + this.interval;
            if (currTimestamp >= nextPunctuationTime)
            {
                // we missed one ore more punctuations
                // avoid scheduling a new punctuations immediately, this can happen:
                // - when using STREAM_TIME punctuation and there was a gap i.e., no data was
                //   received for at least 2*interval
                // - when using WALL_CLOCK_TIME and there was a gap i.e., punctuation was delayed for at least 2*interval (GC pause, overload, ...)
                var intervalsMissed = (currTimestamp - this.timestamp) / this.interval;
                nextPunctuationTime = this.timestamp + (intervalsMissed + 1) * this.interval;
            }

            var nextSchedule = new PunctuationSchedule(this.value, nextPunctuationTime, this.interval, this.punctuator, this.cancellable);

            this.cancellable.SetSchedule(nextSchedule);

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
