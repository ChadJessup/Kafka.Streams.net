//using Kafka.Common.Metrics;
//using Kafka.Common.Utils;
//using Kafka.Streams.State.Interfaces;

//namespace Kafka.Streams.State.Internals
//{
//    public class MeteredKeyValueIterator : IKeyValueIterator<K, V>
//    {

//        private IKeyValueIterator<Bytes, byte[]> iter;
//        private Sensor sensor;
//        private long startNs;

//        private MeteredKeyValueIterator(IKeyValueIterator<Bytes, byte[]> iter,
//                                        Sensor sensor)
//        {
//            this.iter = iter;
//            this.sensor = sensor;
//            this.startNs = time.nanoseconds();
//        }


//        public bool hasNext()
//        {
//            return iter.hasNext();
//        }


//        public KeyValue<K, V> next()
//        {
//            KeyValue<Bytes, byte[]> keyValue = iter.next();
//            return KeyValue.pair(
//                serdes.keyFrom(keyValue.key),
//                outerValue(keyValue.value));
//        }


//        public void Remove()
//        {
//            iter.Remove();
//        }


//        public void close()
//        {
//            try
//            {
//                iter.close();
//            }
//            finally
//            {
//                metrics.recordLatency(sensor, startNs, time.nanoseconds());
//            }
//        }


//        public K peekNextKey()
//        {
//            return serdes.keyFrom(iter.peekNextKey()());
//        }
//    }
//}