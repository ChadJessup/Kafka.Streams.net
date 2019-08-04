namespace Kafka.Streams.Interfaces
{
    /**
 * The {@code Initializer} interface for creating an initial value in aggregations.
 * {@code Initializer} is used in combination with {@link Aggregator}.
 *
 * @param aggregate value type
 * @see Aggregator
 * @see KGroupedStream#aggregate(Initializer, Aggregator)
 * @see KGroupedStream#aggregate(Initializer, Aggregator, Materialized)
 * @see TimeWindowedKStream#aggregate(Initializer, Aggregator)
 * @see TimeWindowedKStream#aggregate(Initializer, Aggregator, Materialized)
 * @see SessionWindowedKStream#aggregate(Initializer, Aggregator, Merger)
 * @see SessionWindowedKStream#aggregate(Initializer, Aggregator, Merger, Materialized)
 */
    public interface IInitializer<VA>
   
{

        /**
         * Return the initial value for an aggregation.
         *
         * @return the initial value for an aggregation
         */
        VA Apply();
    }
}