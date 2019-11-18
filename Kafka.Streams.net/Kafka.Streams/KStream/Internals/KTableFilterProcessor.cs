
//using Kafka.Streams.Processors;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.State;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableFilterProcessor<K, V> : AbstractProcessor<K, Change<V>>
//    {
//        private ITimestampedKeyValueStore<K, V> store;
//        private TimestampedTupleForwarder<K, V> tupleForwarder;

//        public void init(IProcessorContext context)
//        {
//            base.init(context);
//            if (queryableName != null)
//            {
//                store = (ITimestampedKeyValueStore<K, V>)context.getStateStore(queryableName);
//                tupleForwarder = new TimestampedTupleForwarder<K, V>(
//                    store,
//                    context,
//                    new TimestampedCacheFlushListener<K, V>(context),
//                    sendOldValues);
//            }
//        }


//        public void process(K key, Change<V> change)
//        {
//            V newValue = computeValue(key, change.newValue);
//            V oldValue = sendOldValues ? computeValue(key, change.oldValue) : null;

//            if (sendOldValues && oldValue == null && newValue == null)
//            {
//                return; // unnecessary to forward here.
//            }

//            if (queryableName != null)
//            {
//                store.Add(key, ValueAndTimestamp.make(newValue, context.timestamp()));
//                tupleForwarder.maybeForward(key, newValue, oldValue);
//            }
//            else
//            {

//                context.forward(key, new Change<>(newValue, oldValue));
//            }
//        }
//    }
//}