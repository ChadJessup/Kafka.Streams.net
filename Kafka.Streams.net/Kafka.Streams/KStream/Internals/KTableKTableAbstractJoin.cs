
//namespace Kafka.Streams.KStream.Internals
//{
//    public abstract class KTableKTableAbstractJoin<K, R, V1, V2> : IKTableProcessorSupplier<K, V1, R>
//    {
//        private KTable<K, object, V1> table1;
//        private KTable<K, object, V2> table2;
//        protected IKTableValueGetterSupplier<K, V1> valueGetterSupplier1 { get; }
//        protected IKTableValueGetterSupplier<K, V2> valueGetterSupplier2 { get; }
//        IValueJoiner<V1, V2, R> joiner;

//        bool sendOldValues = false;

//        public KTableKTableAbstractJoin(
//            KTable<K, object, V1> table1,
//            KTable<K, object, V2> table2,
//            IValueJoiner<V1, V2, R> joiner)
//        {
//            this.table1 = table1;
//            this.table2 = table2;
//            this.valueGetterSupplier1 = table1.valueGetterSupplier();
//            this.valueGetterSupplier2 = table2.valueGetterSupplier();
//            this.joiner = joiner;
//        }

//        public void enableSendingOldValues()
//        {
//            table1.enableSendingOldValues();
//            table2.enableSendingOldValues();
//            sendOldValues = true;
//        }
//    }
//}
