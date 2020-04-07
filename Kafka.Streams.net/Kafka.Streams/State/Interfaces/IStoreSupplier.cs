namespace Kafka.Streams.State.Interfaces
{
    /**
     * A state store supplier which can create one or more {@link IStateStore} instances.
     *
     * @param State store type
     */
    public interface IStoreSupplier<T>
        where T : IStateStore
    {
        /**
         * Return the name of this state store supplier.
         * This must be a valid Kafka topic name; valid characters are ASCII alphanumerics, '.', '_' and '-'.
         *
         * @return the name of this state store supplier
         */
        string Name { get; }

        /**
         * Return a new {@link IStateStore} instance.
         *
         * @return a new {@link IStateStore} instance of type T
         */
        T Get();

        /**
         * Return a string that is used as the scope for metrics recorded by Metered stores.
         * @return metricsScope
         */
        //string MetricsScope();

        void SetName(string name);
    }
}
