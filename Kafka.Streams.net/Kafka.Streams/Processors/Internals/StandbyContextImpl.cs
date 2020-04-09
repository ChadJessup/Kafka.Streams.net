using Kafka.Streams.Configs;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using Kafka.Streams.Tasks;
using Microsoft.Extensions.Logging;
using System;

namespace Kafka.Streams.Processors.Internals
{
    public class StandbyContextImpl<K, V> : AbstractProcessorContext<K, V>, ISupplier
    {
        private static readonly IRecordCollector NO_OP_COLLECTOR = new NoOpRecordCollector();
        public StandbyContextImpl(
            ILoggerFactory loggerFactory,
            ILogger<StandbyContextImpl<K, V>> logger,
            TaskId id,
            StreamsConfig config,
            ProcessorStateManager stateMgr)
         : base(
            id,
            config,
            stateMgr,
            new ThreadCache(
                loggerFactory.CreateLogger<ThreadCache>(),//($"stream-thread [{Thread.CurrentThread.Name}] ",
                0))
        {
        }

        public IRecordCollector RecordCollector()
        {
            return NO_OP_COLLECTOR;
        }

        /**
         * @throws InvalidOperationException on every invocation
         */
        public override IStateStore GetStateStore(KafkaStreamsContext context, string name)
        {
            throw new InvalidOperationException("this should not happen: getStateStore() not supported in standby tasks.");
        }

        /**
         * @throws InvalidOperationException on every invocation
         */
        public override string Topic
            => throw new InvalidOperationException("this should not happen: Topic not supported in standby tasks.");

        /**
         * @throws InvalidOperationException on every invocation
         */
        public override int Partition
            => throw new InvalidOperationException("this should not happen: Partition not supported in standby tasks.");


        /**
         * @throws InvalidOperationException on every invocation
         */
        public override long Offset
            => throw new InvalidOperationException("this should not happen: offset() not supported in standby tasks.");

        /**
         * @throws InvalidOperationException on every invocation
         */

        public override long Timestamp
            => throw new InvalidOperationException("this should not happen: timestamp() not supported in standby tasks.");

        /**
         * @throws InvalidOperationException on every invocation
         */

        public void Forward(K key, V value)
        {
            throw new InvalidOperationException("this should not happen: forward() not supported in standby tasks.");
        }

        /**
         * @throws InvalidOperationException on every invocation
         */

        public void Forward(K key, V value, To to)
        {
            throw new InvalidOperationException("this should not happen: forward() not supported in standby tasks.");
        }

        /**
         * @throws InvalidOperationException on every invocation
         */

        [Obsolete]
        public void Forward(K key, V value, int childIndex)
        {
            throw new InvalidOperationException("this should not happen: forward() not supported in standby tasks.");
        }

        /**
         * @throws InvalidOperationException on every invocation
         */

        [Obsolete]
        public void Forward(K key, V value, string childName)
        {
            throw new InvalidOperationException("this should not happen: forward() not supported in standby tasks.");
        }

        /**
         * @throws InvalidOperationException on every invocation
         */

        public override void Commit()
        {
            throw new InvalidOperationException("this should not happen: commit() not supported in standby tasks.");
        }

        /**
         * @throws InvalidOperationException on every invocation
         */

        [Obsolete]
        public ICancellable Schedule(long interval, PunctuationType type, IPunctuator callback)
        {
            throw new InvalidOperationException("this should not happen: schedule() not supported in standby tasks.");
        }

        /**
         * @throws InvalidOperationException on every invocation
         */

        public override ICancellable Schedule(TimeSpan interval, PunctuationType type, IPunctuator callback)
        {
            throw new InvalidOperationException("this should not happen: schedule() not supported in standby tasks.");
        }

        /**
         * @throws InvalidOperationException on every invocation
         */

        public override ProcessorRecordContext RecordContext
            => throw new InvalidOperationException("this should not happen: recordContext not supported in standby tasks.");

        /**
         * @throws InvalidOperationException on every invocation
         */

        public override void SetRecordContext(ProcessorRecordContext recordContext)
        {
            throw new InvalidOperationException("this should not happen: setRecordContext not supported in standby tasks.");
        }

        public override void SetCurrentNode(IProcessorNode currentNode)
        {
            // no-op. can't throw as this is called on commit when the StateStores get flushed.
        }

        /**
         * @throws InvalidOperationException on every invocation
         */
        public override IProcessorNode CurrentNode
            => throw new InvalidOperationException("this should not happen: currentNode not supported in standby tasks.");
    }
}
