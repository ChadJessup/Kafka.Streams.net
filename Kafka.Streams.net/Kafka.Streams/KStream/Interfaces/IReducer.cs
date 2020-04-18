namespace Kafka.Streams.KStream
{
    /**
     * The {@code Reducer} interface for combining two values of the same type into a new value.
     * In contrast to {@link IAggregator} the result type must be the same as the input type.
     * <p>
     * The provided values can be either original values from input {@link KeyValuePair} pair records or be a previously
     * computed result from {@link Reducer#apply(object, object)}.
     * <p>
     * {@code Reducer} can be used to implement aggregation functions like sum, min, or max.
     *
     * @param value type
     * @see KGroupedStream#reduce(Reducer)
     * @see KGroupedStream#reduce(Reducer, Materialized)
     * @see TimeWindowedKStream#reduce(Reducer)
     * @see TimeWindowedKStream#reduce(Reducer, Materialized)
     * @see SessionWindowedKStream#reduce(Reducer)
     * @see SessionWindowedKStream#reduce(Reducer, Materialized)
     * @see IAggregator
     */
    public interface IReducer<V>
    {
        /**
         * Aggregate the two given values into a single one.
         *
         * @param value1 the first value for the aggregation
         * @param value2 the second value for the aggregation
         * @return the aggregated value
         */
        V Apply(V value1, V value2);
    }
}
