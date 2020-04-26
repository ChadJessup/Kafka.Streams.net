using Kafka.Streams.Processors.Interfaces;

namespace Kafka.Streams.Processors
{
    /**
     * An abstract implementation of {@link IProcessor} that manages the {@link IProcessorContext} instance and provides default no-op
     * implementation of {@link #Close()}.
     *
     * @param the type of keys
     * @param the type of values
     */
    public abstract class AbstractProcessor<K, V> : IKeyValueProcessor<K, V>
    {
        public IProcessorContext Context { get; private set; }

        public virtual void Init(IProcessorContext context)
        {
            this.Context = context;
        }

        /**
         * Close this processor and clean up any resources.
         * <p>
         * This method does nothing by default; if desired, sues should override it with custom functionality.
         * </p>
         */
        public virtual void Close()
        {
            // do nothing
        }

        public abstract void Process(K key, V value);
    }
}
