namespace Kafka.Streams.KStream.Internals
{
    public class GroupedInternal<K, V> : Grouped<K, V>
    {
        public GroupedInternal(Grouped<K, V> grouped)
            : base(grouped)
        {
        }
    }
}
