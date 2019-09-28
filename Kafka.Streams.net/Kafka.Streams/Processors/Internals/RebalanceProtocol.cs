using System;

namespace Kafka.Streams.Processor.Internals
{
    public class RebalanceProtocol
    {
        public static RebalanceProtocol EAGER = new RebalanceProtocol(0);
        public static RebalanceProtocol COOPERATIVE = new RebalanceProtocol(1);

        private readonly byte id;

        public RebalanceProtocol(byte id)
            => this.id = id;

        public static RebalanceProtocol forId(byte id)
        {
            return id switch
            {
                0 => RebalanceProtocol.EAGER,
                1 => RebalanceProtocol.COOPERATIVE,
                _ => throw new ArgumentException("Unknown rebalance protocol id: " + id),
            };
        }
    }
}
