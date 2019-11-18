

//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.Processors.Internals;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableTransformValuesGetter<K, V, V1> : IKTableValueGetter<K, V1>
//    {
//        private IKTableValueGetter<K, V> parentGetter;
//        private IValueTransformerWithKey<K, V, V1> valueTransformer;

//        public KTableTransformValuesGetter(
//            IKTableValueGetter<K, V> parentGetter,
//            IValueTransformerWithKey<K, V, V1> valueTransformer)
//        {
//            this.parentGetter = parentGetter = parentGetter ?? throw new ArgumentNullException(nameof(parentGetter));
//            this.valueTransformer = valueTransformer = valueTransformer ?? throw new ArgumentNullException(nameof(valueTransformer));
//        }

//        public void init(IProcessorContext context)
//        {
//            parentGetter.init(context);
//            valueTransformer.init(new ForwardingDisabledProcessorContext(context));
//        }


//        public ValueAndTimestamp<V1> get(K key)
//        {
//            ValueAndTimestamp<V> valueAndTimestamp = parentGetter[key];
//            return ValueAndTimestamp.make(
//                valueTransformer.transform(key, getValueOrNull(valueAndTimestamp)),
//                valueAndTimestamp == null ? UNKNOWN : valueAndTimestamp.timestamp());
//        }


//        public void close()
//        {
//            parentGetter.close();
//            valueTransformer.close();
//        }
//    }
//}
