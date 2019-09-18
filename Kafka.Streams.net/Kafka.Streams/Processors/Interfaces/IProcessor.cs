using Kafka.Streams.Processor.Interfaces;

namespace Kafka.Streams.Processor
{
    /**
     * A processor of key-value pair records.
     *
     * @param the type of keys
     * @param the type of values
     */
    public interface IProcessor<K, V>
    {
        /**
         * Initialize this processor with the given context. The framework ensures this is called once per processor when the topology
         * that contains it is initialized. When the framework is done with the processor, {@link #close()} will be called on it; the
         * framework may later re-use the processor by calling {@code #init()} again.
         * <p>
         * The provided {@link IProcessorContext<K, V> context} can be used to access topology and record meta data, to
         * {@link IProcessorContext#schedule(Duration, PunctuationType, Punctuator) schedule} a method to be
         * {@link Punctuator#punctuate(long) called periodically} and to access attached {@link IStateStore}s.
         *
         * @param context the context; may not be null
         */
        void init(IProcessorContext context);

        /**
         * Process the record with the given key and value.
         *
         * @param key the key for the record
         * @param value the value for the record
         */
        void process(K key, V value);

        /**
         * Close this processor and clean up any resources. Be aware that {@code #close()} is called after an internal cleanup.
         * Thus, it is not possible to write anything to Kafka as underlying clients are already closed. The framework may
         * later re-use this processor by calling {@code #init()} on it again.
         * <p>
         * Note: Do not close any streams managed resources, like {@link IStateStore}s here, as they are managed by the library.
         */
        void close();
    }
}