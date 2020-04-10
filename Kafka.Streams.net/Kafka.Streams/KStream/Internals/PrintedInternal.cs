
//using Kafka.Streams.Processors;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class PrintedInternal<K, V> : Printed<K, V>
//    {
//        public PrintedInternal(Printed<K, V> printed)
//            : base(printed)
//        {
//        }

//        public IProcessorSupplier build(string processorName)
//        {
//            return new KStreamPrint<K, V>(new PrintForeachAction<K, V>(outputStream, mapper, label != null ? label : processorName));
//        }

//        public string Name => processorName;
//    }
//}