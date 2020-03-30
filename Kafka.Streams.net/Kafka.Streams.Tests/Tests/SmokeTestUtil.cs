namespace Kafka.Streams.Tests.Tests
{
}
//using Confluent.Kafka;
//using Xunit;

//namespace Kafka.Streams.Tests.Tools
//{
//    public class SmokeTestUtil
//    {

//        static int END = int.MaxValue;

//        static ProcessorSupplier<object, object> printProcessorSupplier(string topic)
//        {
//            return printProcessorSupplier(topic, "");
//        }

//        static ProcessorSupplier<object, object> printProcessorSupplier(string topic, string name)
//        {
//            return new ProcessorSupplier<object, object>()
//            {


//            public Processor<object, object> get()
//            {
//                return new AbstractProcessor<object, object>()
//                {
//                    private int numRecordsProcessed = 0;


//        public void init(ProcessorContext context)
//        {
//            base.init(context);
//            System.Console.Out.WriteLine("[DEV] initializing processor: topic=" + topic + " taskId=" + context.taskId());
//            numRecordsProcessed = 0;
//        }


//        public void process(object key, object value)
//        {
//            numRecordsProcessed++;
//            if (numRecordsProcessed % 100 == 0)
//            {
//                System.Console.Out.printf("%s: %s%n", name, Instant.now());
//                System.Console.Out.WriteLine("processed " + numRecordsProcessed + " records from topic=" + topic);
//            }
//        }
//    };
//}
//        };
//    }

//    public static class Unwindow<K, V> : KeyValueMapper<Windowed<K>, V, K> {

//    public K apply(Windowed<K> winKey, V value)
//    {
//        return winKey.Key;
//    }
//}

//public static class Agg
//{

//    KeyValueMapper<string, long, KeyValuePair<string, long>> selector()
//    {
//        return new KeyValueMapper<string, long, KeyValuePair<string, long>>()
//        {


//                public KeyValuePair<string, long> apply(string key, long value)
//        {
//            return new KeyValuePair<>(value == null ? null : long.toString(value), 1L);
//        }
//    };
//}

//public Initializer<long> init()
//{
//    return new Initializer<long>()
//    {


//                public long apply()
//    {
//        return 0L;
//    }
//};
//        }

//        Aggregator<string, long, long> adder()
//{
//    return new Aggregator<string, long, long>()
//    {


//                public long apply(string aggKey, long value, long aggregate)
//    {
//        return aggregate + value;
//    }
//};
//        }

//        Aggregator<string, long, long> remover()
//{
//    return new Aggregator<string, long, long>()
//    {


//                public long apply(string aggKey, long value, long aggregate)
//    {
//        return aggregate - value;
//    }
//};
//        }
//    }

//    public static Serde<string> stringSerde = Serdes.String();

//public static Serde<int> intSerde = Serdes.Int();

//static Serde<long> longSerde = Serdes.Long();

//static Serde<Double> doubleSerde = Serdes.Double();

//static File createDir(File parent, string child)
//{
//    File dir = new File(parent, child);

//    dir.mkdir();

//    return dir;
//}

//public static void sleep(long duration)
//{
//    try
//    {
//        Thread.sleep(duration);
//    }
//    catch (Exception ignore) { }
//}

//}
