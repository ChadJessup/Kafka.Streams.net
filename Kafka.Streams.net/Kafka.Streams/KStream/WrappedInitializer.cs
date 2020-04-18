using Kafka.Streams.KStream.Interfaces;
using System;

namespace Kafka.Streams.KStream
{
    public class WrappedInitializer<VA> : IInitializer<VA>
    {
        private readonly Initializer<VA> initializer;

        public WrappedInitializer(Initializer<VA> initializer)
            => this.initializer = initializer;

        public VA Apply()
            => this.initializer();
    }
}
