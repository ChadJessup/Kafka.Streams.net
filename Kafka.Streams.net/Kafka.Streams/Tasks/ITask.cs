using System.Collections.Generic;
using Confluent.Kafka;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.Threads;

namespace Kafka.Streams.Tasks
{
    public enum TaskState
    {
        CREATED = 0, //(1, 4),         // 0
        RESTORING = 1, //(2, 3, 4),    // 1
        RUNNING = 2, //(3, 4),         // 2
        SUSPENDED = 3,//(1, 4),       // 3
        CLOSED = 4, //(0);             // 4, we allow CLOSED to transit to CREATED to handle corrupted tasks

        //        private Set<Integer> validTransitions = new HashSet<>();
        //
        //    State(Integer... validTransitions)
        //    {
        //        this.validTransitions.addAll(Arrays.asList(validTransitions));
        //    }
        //
        //    public bool isValidTransition(State newState)
        //    {
        //        return validTransitions.contains(newState.ordinal());
        //    }
    }

    public enum TaskType
    {
        ACTIVE,//("ACTIVE"),

        STANDBY,//("STANDBY"),

        GLOBAL,//("GLOBAL"),

        //        public string name;
        //
        //    TaskType(string name)
        //    {
        //        this.name = name;
        //    }
    }

    public interface ITask : IStateMachine<TaskState>
    {
        // this must be negative to distinguish a running active task from other kinds of tasks
        // which may be caught up to the same offsets
        long LatestOffset { get; set; } //= -2L;

        /*
         *
         * <pre>
         *                 +-------------+
         *          +<---- | Created (0) | <----------------------+
         *          |      +-----+-------+                        |
         *          |            |                                |
         *          |            v                                |
         *          |      +-----+-------+                        |
         *          +<---- | Restoring(1)|<---------------+       |
         *          |      +-----+-------+                |       |
         *          |            |                        |       |
         *          |            +--------------------+   |       |
         *          |            |                    |   |       |
         *          |            v                    v   |       |
         *          |      +-----+-------+       +----+---+----+  |
         *          |      | Running (2) | ----> | Suspended(3)|  |    //TODO Suspended(3) could be removed after we've stable on KIP-429
         *          |      +-----+-------+       +------+------+  |
         *          |            |                      |         |
         *          |            |                      |         |
         *          |            v                      |         |
         *          |      +-----+-------+              |         |
         *          +----> | Closing (4) | <------------+         |
         *                 +-----+-------+                        |
         *                       |                                |
         *                       v                                |
         *                 +-----+-------+                        |
         *                 | Closed (5)  | -----------------------+
         *                 +-------------+
         * </pre>
         */

        TaskId Id { get; }

        bool IsActive();

        bool IsClosed();

        /**
         * @throws LockException could happen when multi-threads within the single instance, could retry
         * @throws StreamsException fatal error, should close the thread
         */
        void InitializeIfNeeded();

        /**
         * @throws StreamsException fatal error, should close the thread
         */
        void CompleteRestoration();

        void AddRecords(TopicPartition partition, IEnumerable<ConsumeResult<byte[], byte[]>> records);

        bool CommitNeeded();

        /**
         * @throws StreamsException fatal error, should close the thread
         */
        void PrepareCommit();

        void PostCommit();

        /**
         * @throws TaskMigratedException all the task has been migrated
         * @throws StreamsException fatal error, should close the thread
         */
        void PrepareSuspend();

        void Suspend();
        /**
         *
         * @throws StreamsException fatal error, should close the thread
         */
        void Resume();

        /**
         * Prepare to close a task that we still own and prepare it for committing
         * Throws an exception if this couldn't be done.
         * Must be idempotent.
         *
         * @throws StreamsException fatal error, should close the thread
         */
        Dictionary<TopicPartition, long> PrepareCloseClean();

        /**
         * Must be idempotent.
         */
        void CloseClean(Dictionary<TopicPartition, long> checkpoint);

        /**
         * Prepare to close a task that we may not own. Discard any uncommitted progress and close the task.
         * Never throws an exception, but just makes all attempts to release resources while closing.
         * Must be idempotent.
         */
        void PrepareCloseDirty();

        /**
         * Must be idempotent.
         */
        void CloseDirty();

        /**
         * Revive a closed task to a created one; should never throw an exception
         */
        void Revive();

        IStateStore GetStore(string name);

        HashSet<TopicPartition> InputPartitions();

        /**
         * @return any changelog partitions associated with this task
         */
        IEnumerable<TopicPartition> ChangelogPartitions();

        /**
         * @return the offsets of all the changelog partitions associated with this task,
         *         indicating the current positions of the logged state stores of the task.
         */
        Dictionary<TopicPartition, long> ChangelogOffsets();

        void MarkChangelogAsCorrupted(List<TopicPartition> partitions);

        public Dictionary<TopicPartition, long> PurgeableOffsets()
        {
            return new Dictionary<TopicPartition, long>();
        }

        public Dictionary<TopicPartition, OffsetAndMetadata> CommittableOffsetsAndMetadata()
        {
            return new Dictionary<TopicPartition, OffsetAndMetadata>();
        }

        public void RecordProcessBatchTime(long processBatchTime)
        { }

        public void RecordProcessTimeRatioAndBufferSize(long allTaskProcessMs) { }

        public bool Process(long wallClockTime)
        {
            return false;
        }

        public bool CommitRequested()
        {
            return false;
        }

        public bool MaybePunctuateStreamTime()
        {
            return false;
        }

        public bool MaybePunctuateSystemTime()
        {
            return false;
        }

        string ToString(string v);
    }
}
