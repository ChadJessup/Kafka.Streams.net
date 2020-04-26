using Kafka.Streams.Processors.Interfaces;

namespace Kafka.Streams.Processors
{
    /**
     * A processor of key-value pair records.
     *
     * @param the type of keys
     * @param the type of values
     */
    public interface IKeyValueProcessor<in K, in V> : IKeyValueProcessor
    {
        /**
         * Process the record with the given key and value.
         *
         * @param key the key for the record
         * @param value the value for the record
         */
        void Process(K key, V value);
        void IKeyValueProcessor.Process(object key, object value)
            => this.Process((K)key, (V)value);
    }

    public interface IKeyValueProcessor
    {
        /**
          * Initialize this processor with the given context. The framework ensures this is called once per processor when the topology
          * that contains it is initialized. When the framework is done with the processor, {@link #Close()} will be called on it; the
          * framework may later re-use the processor by calling {@code #Init()} again.
          * <p>
          * The provided {@link IProcessorContext context} can be used to access topology and record meta data, to
          * {@link IProcessorContext#schedule(TimeSpan, PunctuationType, Punctuator) schedule} a method to be
          * {@link Punctuator#punctuate(long) called periodically} and to access attached {@link IStateStore}s.
          *
          * @param context the context; may not be null
          */

        void Init(IProcessorContext context);

        /**
         * Close this processor and clean up any resources. Be aware that {@code #Close()} is called after an internal cleanup.
         * Thus, it is not possible to write anything to Kafka as underlying clients are already closed. The framework may
         * later re-use this processor by calling {@code #Init()} on it again.
         * <p>
         * Note: Do not Close any streams managed resources, like {@link IStateStore}s here, as they are managed by the library.
         */
        void Close();

        /**
         * Process the record with the given key and value.
         *
         * @param key the key for the record
         * @param value the value for the record
         */
        void Process(object key, object value);
    }
}
