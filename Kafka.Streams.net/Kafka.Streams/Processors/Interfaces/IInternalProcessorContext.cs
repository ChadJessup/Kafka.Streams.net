using Kafka.Streams.Processor.Internals;

namespace Kafka.Streams.Processor.Interfaces
{
    /**
     * For internal use so we can update the {@link RecordContext} and current
     * {@link ProcessorNode} when we are forwarding items that have been evicted or flushed from
     * {@link ThreadCache}
     */
    public interface IInternalProcessorContext<K, V> : IProcessorContext<K, V>
    {
        /**
         * Returns the current {@link RecordContext}
         * @return the current {@link RecordContext}
         */
        ProcessorRecordContext recordContext { get; }

        /**
         * @param recordContext the {@link ProcessorRecordContext} for the record about to be processes
         */
        void setRecordContext(ProcessorRecordContext recordContext);

        /**
         * @param currentNode the current {@link ProcessorNode}
         */
        void setCurrentNode(ProcessorNode<K, V> currentNode);

        /**
         * Get the current {@link ProcessorNode}
         */
        ProcessorNode<K, V> currentNode { get; }

        /**
         * Get the thread-global cache
         */
        //ThreadCache getCache();

        /**
         * Mark this context as being initialized
         */
        void initialize();

        /**
         * Mark this context as being uninitialized
         */
        void uninitialize();
    }
}