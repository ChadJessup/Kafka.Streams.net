
namespace Kafka.Streams.KStream.Interfaces
{
    /**
     * The {@code Initializer} interface for creating an initial value in aggregations.
     * {@code Initializer} is used in combination with {@link IAggregator}.
     *
     * @param aggregate value type
     * @see IAggregator
     * @see KGroupedStream#aggregate(Initializer, IAggregator)
     * @see KGroupedStream#aggregate(Initializer, IAggregator, Materialized)
     * @see TimeWindowedKStream#aggregate(Initializer, IAggregator)
     * @see TimeWindowedKStream#aggregate(Initializer, IAggregator, Materialized)
     * @see SessionWindowedKStream#aggregate(Initializer, IAggregator, Merger)
     * @see SessionWindowedKStream#aggregate(Initializer, IAggregator, Merger, Materialized)
     */
    public delegate VA Initializer<out VA>();

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
