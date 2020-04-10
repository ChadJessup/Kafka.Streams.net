
using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamPrintProcessor<K, V> : AbstractProcessor<K, V>
    {
        public override void Process(K key, V value)
        {
            //action.apply(key, value);
        }


        public override void Close()
        {
            //if (action is PrintForeachAction)
            //{
            //    ((PrintForeachAction)action).Close();
            //}
        }
    }
}
