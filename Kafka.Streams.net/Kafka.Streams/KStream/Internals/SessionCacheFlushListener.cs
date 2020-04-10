
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.Processors.Internals;
//using Kafka.Streams.State.Internals;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class SessionCacheFlushListener<K, V> : ICacheFlushListener<IWindowed<K>, V>
//    {
//        private IInternalProcessorContext<K, V> context;
//        private ProcessorNode<K, V> myNode;

//        SessionCacheFlushListener(IProcessorContext context)
//        {
//            this.context = (IInternalProcessorContext<K, V>)context;
//            myNode = this.context.currentNode();
//        }


//        public void apply(IWindowed<K> key,
//                           V newValue,
//                           V oldValue,
//                           long timestamp)
//        {
//            ProcessorNode prev = context.currentNode();
//            context.setCurrentNode(myNode);
//            try
//            {

//                context.Forward(key, new Change<>(newValue, oldValue), To.All().WithTimestamp(key.window.end()));
//            }
//            finally
//            {

//                context.setCurrentNode(prev);
//            }
//        }
//    }
//}
