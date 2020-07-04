using System;

namespace Kafka.Streams.Processors.Internals
{
    public class WrappedPunctuator : IPunctuator
    {
        private readonly Action<DateTime> punctuator;

        public WrappedPunctuator(Action<DateTime> punctuator)
            => this.punctuator = punctuator;

        public void Punctuate(DateTime timestamp)
            => this.punctuator(timestamp);
    }
}
