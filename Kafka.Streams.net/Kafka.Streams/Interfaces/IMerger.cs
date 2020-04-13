namespace Kafka.Streams.Interfaces
{
    /**
     * Compute a new aggregate from the key and two aggregates.
     *
     * @param aggKey    the key of the record
     * @param aggOne    the first aggregate
     * @param aggTwo    the second aggregate
     * @return          the new aggregate value
     */
    public delegate V Merger<in K, V>(K aggKey, V aggOne, V aggTwo);
}
