namespace Kafka.Streams.KStream.Interfaces
{
    /**
     * The {@code IAggregator} interface for aggregating values of the given key.
     * This is a generalization of {@link Reducer} and allows to have different types for input value and aggregation
     * result.
     * {@code IAggregator} is used in combination with {@link Initializer} that provides an initial aggregation value.
     * <p>
     * {@code IAggregator} can be used to implement aggregation functions like count.

     * @param key type
     * @param input value type
     * @param aggregate value type
     * @see Initializer
     * @see KGroupedStream#aggregate(Initializer, IAggregator)
     * @see KGroupedStream#aggregate(Initializer, IAggregator, Materialized)
     * @see TimeWindowedKStream#aggregate(Initializer, IAggregator)
     * @see TimeWindowedKStream#aggregate(Initializer, IAggregator, Materialized)
     * @see SessionWindowedKStream#aggregate(Initializer, IAggregator, Merger)
     * @see SessionWindowedKStream#aggregate(Initializer, IAggregator, Merger, Materialized)
     * @see Reducer
     */

    public delegate VAggregate Aggregator<in K, in V, VAggregate>(K key, V value, VAggregate aggregate);
    public interface IAggregator<K, V, VAggregate>
    {
        /**
         * Compute a new aggregate from the key and value of a record and the current aggregate of the same key.
         *
         * @param key       the key of the record
         * @param value     the value of the record
         * @param aggregate the current aggregate value
         * @return the new aggregate value
         */
        VAggregate Apply(K key, V value, VAggregate aggregate);
    }
}
