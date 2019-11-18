
//using Kafka.Common.Metrics;
//using Kafka.Common.Utils.Interfaces;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.State.Interfaces;

//namespace Kafka.Streams.State.Internals
//{
//    public class MeteredWindowStoreIterator<V> : IWindowStoreIterator<V>
//    {
//        private IWindowStoreIterator<byte[]> iter;
//        private Sensor sensor;
//        private IStreamsMetrics metrics;
//        private StateSerdes<object, V> serdes;
//        private long startNs;
//        private ITime time;

//        MeteredWindowStoreIterator(IWindowStoreIterator<byte[]> iter,
//                                   Sensor sensor,
//                                   IStreamMetrics metrics,
//                                   StateSerdes<?, V> serdes,
//                                   ITime time)
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

//        public override KeyValue<long, V> next()
//        {
//            KeyValue<long, byte[]> next = iter.next();
//            return KeyValue.pair(next.key, serdes.valueFrom(next.value));
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
//                metrics.recordLatency(this.sensor, this.startNs, time.nanoseconds());
//            }
//        }

//        public override long peekNextKey()
//        {
//            return iter.peekNextKey();
//        }
//    }
//}