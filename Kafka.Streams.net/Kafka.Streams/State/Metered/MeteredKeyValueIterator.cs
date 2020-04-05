//using Kafka.Common.Utils;
//using Kafka.Streams.State.Interfaces;

//namespace Kafka.Streams.State.Metered
//{
//    public class MeteredKeyValueIterator<K, V> : IKeyValueIterator<K, V>
//    {

//        private IKeyValueIterator<Bytes, byte[]> iter;
//        private long startNs;

//        private MeteredKeyValueIterator(IKeyValueIterator<Bytes, byte[]> iter)
//        {
//            this.iter = iter;
//            //this.sensor = sensor;
//            this.startNs = time.nanoseconds();
//        }


//        public bool hasNext()
//        {
//            return iter.hasNext();
//        }


//        public KeyValuePair<K, V> next()
//        {
//            KeyValuePair<Bytes, byte[]> keyValue = iter.MoveNext();
//            return KeyValuePair.pair(
//                serdes.keyFrom(keyValue.Key),
//                outerValue(keyValue.Value));
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