namespace Kafka.Streams.Interfaces
{
    /**
     * The interface for merging aggregate values for {@link SessionWindows} with the given key.
     *
     * @param   key type
     * @param   aggregate value type
     */
    public interface IMerger<K, V>
   
{

        /**
         * Compute a new aggregate from the key and two aggregates.
         *
         * @param aggKey    the key of the record
         * @param aggOne    the first aggregate
         * @param aggTwo    the second aggregate
         * @return          the new aggregate value
         */
        V Apply(K aggKey, V aggOne, V aggTwo);
    }
}