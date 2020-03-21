
//using Kafka.Common.Metrics;
//using Kafka.Common.Utils;
//using Kafka.Common.Utils.Interfaces;
//using Kafka.Streams.KStream;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.State.Interfaces;

//namespace Kafka.Streams.State.Metered
//{
//    public class MeteredWindowedKeyValueIterator<K, V> : IKeyValueIterator<Windowed<K>, V>
//    {
//        private IKeyValueIterator<Windowed<Bytes>, byte[]> iter;
//        private Sensor sensor;
//        private IStreamsMetrics metrics;
//        private StateSerdes<K, V> serdes;
//        private long startNs;
//        private ITime time;

//        MeteredWindowedKeyValueIterator(IKeyValueIterator<Windowed<Bytes>, byte[]> iter,
//                                        Sensor sensor,
//                                        IStreamsMetrics metrics,
//                                        StateSerdes<K, V> serdes,
//                                        ITime time)
//        {
//            this.iter = iter;
//            this.sensor = sensor;
//            this.metrics = metrics;
//            this.serdes = serdes;
//            this.startNs = time.nanoseconds();
//            this.time = time;
//        }

//        public override bool hasNext()
//        {
//            return iter.hasNext();
//        }

//        public override KeyValuePair<Windowed<K>, V> next()
//        {
//            KeyValuePair<Windowed<Bytes>, byte[]> next = iter.next();
//            return KeyValuePair.pair(windowedKey(next.key), serdes.valueFrom(next.value));
//        }

//        private Windowed<K> windowedKey(Windowed<Bytes> bytesKey)
//        {
//            K key = serdes.keyFrom(bytesKey.key()());
//            return new Windowed<>(key, bytesKey.window());
//        }

//        public override void Remove()
//        {
//            iter.Remove();
//        }

//        public override void close()
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

//        public override Windowed<K> peekNextKey()
//        {
//            return windowedKey(iter.peekNextKey());
//        }
//    }
//}