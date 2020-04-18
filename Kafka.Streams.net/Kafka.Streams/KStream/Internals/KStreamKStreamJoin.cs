using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors;
using Microsoft.Extensions.Logging;
using System;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamKStreamJoin<K, R, V1, V2> : IProcessorSupplier<K, V1>
    {
        private static readonly ILogger LOG = new LoggerFactory().CreateLogger<KStreamKStreamJoin<K, R, V1, V2>>();
        private readonly KafkaStreamsContext context;
        private readonly string otherWindowName;
        private readonly TimeSpan joinBeforeMs;
        private readonly TimeSpan joinAfterMs;

        private readonly ValueJoiner<V1, V2, R> joiner;
        private readonly bool outer;

        public KStreamKStreamJoin(
            KafkaStreamsContext context,
            string otherWindowName,
            TimeSpan joinBeforeMs,
            TimeSpan joinAfterMs,
            ValueJoiner<V1, V2, R> joiner,
            bool outer)
        {
            this.context = context;
            this.otherWindowName = otherWindowName;
            this.joinBeforeMs = joinBeforeMs;
            this.joinAfterMs = joinAfterMs;
            this.joiner = joiner;
            this.outer = outer;
        }

        public IKeyValueProcessor<K, V1> Get()
        {
            return new KStreamKStreamJoinProcessor<K, V1, V2, R>(
                context: this.context,
                outer: this.outer,
                joiner: this.joiner,
                joinBeforeMs: this.joinBeforeMs,
                joinAfterMs: this.joinAfterMs);
        }

        IKeyValueProcessor IProcessorSupplier.Get()
            => this.Get();
    }
}
