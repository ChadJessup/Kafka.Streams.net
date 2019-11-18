
//using Kafka.Streams.Processors;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableTransformValues<K, V, V1> : IKTableProcessorSupplier<K, V, V1>
//    {
//        private KTable<K, object, V> parent;
//        private IValueTransformerWithKeySupplier<K, V, V1> transformerSupplier;
//        private string queryableName;
//        private bool sendOldValues = false;

//        public KTableTransformValues(
//            KTable<K, object, V> parent,
//            IValueTransformerWithKeySupplier<K, V, V1> transformerSupplier,
//            string queryableName)
//        {
//            this.parent = parent ?? throw new ArgumentNullException(nameof(parent));
//            this.transformerSupplier = transformerSupplier = transformerSupplier ?? throw new ArgumentNullException(nameof(transformerSupplier));
//            this.queryableName = queryableName;
//        }

//        public IProcessor<K, Change<V>> get()
//        {
//            return new KTableTransformValuesProcessor(transformerSupplier());
//        }


//        public IKTableValueGetterSupplier<K, V1> view()
//        {
//            if (queryableName != null)
//            {
//                return new KTableMaterializedValueGetterSupplier<K, V1>(queryableName);
//            }

//            return null;
//            //            return new IKTableValueGetterSupplier<K, V1>()
//            //{
//            //             IKTableValueGetterSupplier<K, V> parentValueGetterSupplier = parent.valueGetterSupplier();

//            //            public IKTableValueGetter<K, V1> get()
//            //            {
//            //                return new KTableTransformValuesGetter<K, V, V1>(
//            //                    parentValueGetterSupplier[],
//            //                    transformerSupplier());
//            //            }


//            //            public string[] storeNames()
//            //            {
//            //                return parentValueGetterSupplier.storeNames();
//            //            }
//            //        };
//            //    }


//            //    public void enableSendingOldValues()
//            //    {
//            //        parent.enableSendingOldValues();
//            //        sendOldValues = true;
//            //    }
//        }
//    }
//}