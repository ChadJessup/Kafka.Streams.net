using Kafka.Streams.Errors;
using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableMapProcessor<K, V> : AbstractProcessor<K, Change<V>>
    {

        /**
         * @throws StreamsException if key is null
         */

        public override void Process(K key, Change<V> change)
        {
            // the original key should never be null
            if (key == null)
            {
                throw new StreamsException("Record key for the grouping KTable should not be null.");
            }

            // if the value is null, we do not need to forward its selected key-value further
            //KeyValue<K, V> newPair = change.newValue == null ? null : mapper.apply(key, change.newValue);
            //KeyValue<K, V> oldPair = change.oldValue == null ? null : mapper.apply(key, change.oldValue);

            // if the selected repartition key or value is null, skip
            // forward oldPair first, to be consistent with reduce and aggregate
//            if (oldPair != null && oldPair.key != null && oldPair.value != null)
            {
                //context.forward(oldPair.key, new Change<K>(null, oldPair.value));
            }

            //if (newPair != null && newPair.key != null && newPair.value != null)
            //{
            //    context.forward(newPair.key, new Change<K>(newPair.value, null));
            //}

        }
    }
}