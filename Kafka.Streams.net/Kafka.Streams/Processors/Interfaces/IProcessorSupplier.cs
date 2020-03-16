namespace Kafka.Streams.Processors
{
    /**
     * A processor supplier that can create one or more {@link IProcessor} instances.
     *
     * It is used in {@link Topology} for adding new processor operators, whose generated
     * topology can then be replicated (and thus creating one or more {@link IProcessor} instances)
     * and distributed to multiple stream threads.
     *
     * @param the type of keys
     * @param the type of values
     */
    public interface IProcessorSupplier<K, V>
    {
        /**
         * Return a new {@link IProcessor} instance.
         *
         * @return  a new {@link IProcessor} instance
         */
        IKeyValueProcessor<K, V> get();
    }
}