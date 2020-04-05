using Kafka.Streams.Nodes;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State.Internals;

namespace Kafka.Streams.Processors.Interfaces
{
    /**
     * For internal use so we can update the {@link RecordContext} and current
     * {@link ProcessorNode} when we are forwarding items that have been evicted or flushed from
     * {@link ThreadCache}
     */
    public interface IInternalProcessorContext : IProcessorContext
    {
        /**
         * Returns the current {@link RecordContext}
         * @return the current {@link RecordContext}
         */
        ProcessorRecordContext RecordContext { get; }

        /**
         * @param recordContext the {@link ProcessorRecordContext} for the record about to be processes
         */
        void SetRecordContext(ProcessorRecordContext recordContext);

        /**
         * @param currentNode the current {@link ProcessorNode}
         */
        void SetCurrentNode(IProcessorNode? currentNode);
        /**
         * Get the current {@link ProcessorNode}
         */
        IProcessorNode GetCurrentNode();
        /**
         * Get the thread-global cache
         */
        ThreadCache GetCache();

        /**
         * Mark this context as being initialized
         */
        void Initialize();

        /**
         * Mark this context as being uninitialized
         */
        void Uninitialize();
    }
}
