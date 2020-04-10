

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

//        public void Init(IProcessorContext context)
//        {
//            parentGetter.Init(context);
//            valueTransformer.Init(new ForwardingDisabledProcessorContext(context));
//        }


//        public ValueAndTimestamp<V1> get(K key)
//        {
//            ValueAndTimestamp<V> valueAndTimestamp = parentGetter[key];
//            return ValueAndTimestamp.Make(
//                valueTransformer.transform(key, ValueAndTimestamp.GetValueOrNull(valueAndTimestamp)),
//                valueAndTimestamp == null ? UNKNOWN : valueAndTimestamp.timestamp());
//        }


//        public void Close()
//        {
//            parentGetter.Close();
//            valueTransformer.Close();
//        }
//    }
//}
