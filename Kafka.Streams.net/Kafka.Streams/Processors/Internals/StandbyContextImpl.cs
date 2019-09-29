using Kafka.Streams.Configs;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals.Metrics;
using Kafka.Streams.Processors;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using Kafka.Streams.Tasks;
using System;
using System.Threading;

namespace Kafka.Streams.Processors.Internals
{
    public class StandbyContextImpl<K, V> : AbstractProcessorContext<K, V>, ISupplier
    {
        private static readonly IRecordCollector NO_OP_COLLECTOR = new NoOpRecordCollector();
        public StandbyContextImpl(
            TaskId id,
            StreamsConfig config,
            ProcessorStateManager stateMgr,
            StreamsMetricsImpl metrics)
         : base(
            id,
            config,
            metrics,
            stateMgr,
            new ThreadCache(
                new LogContext(string.Format("stream-thread [%s] ", Thread.CurrentThread.Name)),
                0,
                metrics))
        {
        }


        IStateManager getStateMgr()
        {
            return stateManager;
        }

        public IRecordCollector recordCollector()
        {
            return NO_OP_COLLECTOR;
        }

        /**
         * @throws InvalidOperationException on every invocation
         */
        public override IStateStore getStateStore(string name)
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
        public override int partition
            => throw new InvalidOperationException("this should not happen: partition() not supported in standby tasks.");


        /**
         * @throws InvalidOperationException on every invocation
         */
        public override long offset
            => throw new InvalidOperationException("this should not happen: offset() not supported in standby tasks.");

        /**
         * @throws InvalidOperationException on every invocation
         */

        public override long timestamp
            => throw new InvalidOperationException("this should not happen: timestamp() not supported in standby tasks.");

        /**
         * @throws InvalidOperationException on every invocation
         */

        public void forward(K key, V value)
        {
            throw new InvalidOperationException("this should not happen: forward() not supported in standby tasks.");
        }

        /**
         * @throws InvalidOperationException on every invocation
         */

        public void forward(K key, V value, To to)
        {
            throw new InvalidOperationException("this should not happen: forward() not supported in standby tasks.");
        }

        /**
         * @throws InvalidOperationException on every invocation
         */

        [Obsolete]
        public void forward(K key, V value, int childIndex)
        {
            throw new InvalidOperationException("this should not happen: forward() not supported in standby tasks.");
        }

        /**
         * @throws InvalidOperationException on every invocation
         */

        [Obsolete]
        public void forward(K key, V value, string childName)
        {
            throw new InvalidOperationException("this should not happen: forward() not supported in standby tasks.");
        }

        /**
         * @throws InvalidOperationException on every invocation
         */

        public override void commit()
        {
            throw new InvalidOperationException("this should not happen: commit() not supported in standby tasks.");
        }

        /**
         * @throws InvalidOperationException on every invocation
         */

        [Obsolete]
        public ICancellable schedule(long interval, PunctuationType type, Punctuator callback)
        {
            throw new InvalidOperationException("this should not happen: schedule() not supported in standby tasks.");
        }

        /**
         * @throws InvalidOperationException on every invocation
         */

        public override ICancellable schedule(TimeSpan interval, PunctuationType type, Punctuator callback)
        {
            throw new InvalidOperationException("this should not happen: schedule() not supported in standby tasks.");
        }

        /**
         * @throws InvalidOperationException on every invocation
         */

        public override ProcessorRecordContext recordContext
            => throw new InvalidOperationException("this should not happen: recordContext not supported in standby tasks.");

        /**
         * @throws InvalidOperationException on every invocation
         */

        public override void setRecordContext(ProcessorRecordContext recordContext)
        {
            throw new InvalidOperationException("this should not happen: setRecordContext not supported in standby tasks.");
        }

        public override void setCurrentNode(ProcessorNode currentNode)
        {
            // no-op. can't throw as this is called on commit when the StateStores get flushed.
        }

        /**
         * @throws InvalidOperationException on every invocation
         */
        public override ProcessorNode currentNode
            => throw new InvalidOperationException("this should not happen: currentNode not supported in standby tasks.");
    }
}