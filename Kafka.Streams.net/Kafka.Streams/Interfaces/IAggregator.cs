namespace Kafka.Streams.Interfaces
{
    public interface IAggregator<K, V, VA>
    {
        /**
         * Compute a new aggregate from the key and value of a record and the current aggregate of the same key.
         *
         * @param key       the key of the record
         * @param value     the value of the record
         * @param aggregate the current aggregate value
         * @return the new aggregate value
         */
        VA Apply(K key, V value, VA aggregate);
    }
}