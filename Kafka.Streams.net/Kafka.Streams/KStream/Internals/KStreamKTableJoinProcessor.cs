using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamKTableJoinProcessor<K1, K2, V1, V2, R> : AbstractProcessor<K1, V1>
    {
        private readonly ILogger<KStreamKTableJoinProcessor<K1, K2, V1, V2, R>> logger;
        private readonly string storeName;

        private readonly IKTableValueGetter<K2, V2> valueGetter;
        private readonly IKeyValueMapper<K1, V1, K2> keyMapper;
        private readonly IValueJoiner<V1, V2, R> joiner;
        private readonly bool leftJoin;

        public KStreamKTableJoinProcessor(
            ILogger<KStreamKTableJoinProcessor<K1, K2, V1, V2, R>> logger,
            string storeName,
            IKTableValueGetter<K2, V2> valueGetter,
            IKeyValueMapper<K1, V1, K2> keyMapper,
            IValueJoiner<V1, V2, R> joiner,
            bool leftJoin)
        {
            this.logger = logger;
            this.storeName = storeName;

            this.valueGetter = valueGetter;
            this.keyMapper = keyMapper;
            this.joiner = joiner;
            this.leftJoin = leftJoin;
        }

        public override void Init(IProcessorContext context)
        {
            base.Init(context);

            valueGetter.init(context, this.storeName);
        }

        public override void Process(K1 key, V1 value)
        {
            // we do join iff keys are equal, thus, if key is null we cannot join and just ignore the record
            // If {@code keyMapper} returns {@code null} it implies there is no match,
            // so ignore unless it is a left join
            //
            // we also ignore the record if value is null, because in a key-value data model a null-value indicates
            // an empty message (ie, there is nothing to be joined) -- this contrast SQL NULL semantics
            // furthermore, on left/outer joins 'null' in ValueJoiner#apply() indicates a missing record --
            // thus, to be consistent and to avoid ambiguous null semantics, null values are ignored
            if (key == null || value == null)
            {
                logger.LogWarning(
                    $"Skipping record due to null key or value. key=[{key}] " +
                    $"value=[{value}] topic=[{context.Topic}] partition=[{context.partition}] " +
                    $"offset=[{context.offset}]");
            }
            else
            {
                K2 mappedKey = keyMapper.Apply(key, value);
                V2 value2 = valueGetter.get(mappedKey).value;

                if (leftJoin || value2 != null)
                {
                    context.forward(key, joiner.apply(value, value2));
                }
            }
        }

        public override void Close()
        {
            valueGetter.close();
        }
    }
}
