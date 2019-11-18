

//using Kafka.Streams.Processors.Interfaces;

//namespace Kafka.Streams.KStream.Internals.Suppress
//{
//    public class WindowEndTimeDefinition<K> : ITimeDefinition<K>
//        where K : Windowed<K>
//    {
//        private static WindowEndTimeDefinition<K> INSTANCE = new WindowEndTimeDefinition<K>();

//        private WindowEndTimeDefinition() { }


//        public static WindowEndTimeDefinition<K> instance()
//        {
//            return WindowEndTimeDefinition<K>.INSTANCE;
//        }


//        public long time(IProcessorContext<K, V> context, K key)
//        {
//            return key.window.end();
//        }


//        public TimeDefinitionType type()
//        {
//            return TimeDefinitionType.WINDOW_END_TIME;
//        }
//    }
//}