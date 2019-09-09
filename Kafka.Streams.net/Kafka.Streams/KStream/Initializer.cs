using Kafka.Streams.KStream.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.KStream
{
    public class Initializer<VA> : IInitializer<VA>
    {
        private readonly Func<VA> initializer;

        public Initializer(Func<VA> initializer)
            => this.initializer = initializer;

        public VA apply()
            => this.initializer();
    }
}
