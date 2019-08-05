using Kafka.Streams.Processor;
using Kafka.Streams.Processor.Interfaces;

namespace Kafka.Streams.Processor
{
    /**
     * An abstract implementation of {@link Processor} that manages the {@link IProcessorContext} instance and provides default no-op
     * implementation of {@link #close()}.
     *
     * @param the type of keys
     * @param the type of values
     */
    public abstract class AbstractProcessor<K, V> : Processor<K, V>
    {
        private IProcessorContext context;

        protected AbstractProcessor()
        {
        }

        public void init(IProcessorContext context)
        {
            this.context = context;
        }

        /**
         * Close this processor and clean up any resources.
         * <p>
         * This method does nothing by default; if desired, sues should override it with custom functionality.
         * </p>
         */

        public void close()
        {
            // do nothing
        }

        public abstract void process(K key, V value);

        /**
         * Get the processor's context set during {@link #init(IProcessorContext) initialization}.
         *
         * @return the processor context; null only when called prior to {@link #init(IProcessorContext) initialization}.
         */
    }
}