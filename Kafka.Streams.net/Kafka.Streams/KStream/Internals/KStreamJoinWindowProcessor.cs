
//using Kafka.Streams.Processors;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.State.Interfaces;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KStreamJoinWindowProcessor<K, V> : AbstractProcessor<K, V>
//    {

//        private IWindowStore<K, V> window;

//        public KStreamJoinWindowProcessor(string windowName)
//        {
//            this.windowName = windowName;
//        }

//        public string windowName { get; }


//        public override void init(IProcessorContext context)
//        {
//            base.Init(context);

//            window = (IWindowStore<K, V>)context.getStateStore(windowName);
//        }


//        public override void process(K key, V value)
//        {
//            // if the key is null, we do not need to put the record into window store
//            // since it will never be considered for join operations
//            if (key != null)
//            {
//                context.Forward(key, value);
//                // Every record basically starts a new window. We're using a window store mostly for the retention.
//                window.put(key, value, context.timestamp());
//            }
//        }
//    }
//}
