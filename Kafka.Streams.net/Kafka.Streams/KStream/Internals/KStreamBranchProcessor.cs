
using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamBranchProcessor<K, V> : AbstractProcessor<K, V>
    {
        public override void Process(K key, V value)
        {
            //for (int i = 0; i < predicates.Length; i++)
            //{
            //    if (predicates[i].test(key, value))
            //    {
            //        // use forward with child here and then break the loop
            //        // so that no record is going to be piped to multiple streams
            //        context.Forward(key, value, To.Child(childNodes[i]));
            //        break;
            //    }
            //}
        }
    }
}
