using System.Collections.Generic;
using Confluent.Kafka;
using System;
using Microsoft.Extensions.Logging;
using System.Linq;
using Kafka.Streams.Errors;
using Kafka.Streams.Tasks;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.Clients.Consumers;
using Kafka.Streams.Configs;

namespace Kafka.Streams.Processors.Internals
{
    public class StoreChangelogReader : IChangelogReader
    {
        private readonly ILogger<StoreChangelogReader> logger;
        private readonly IConsumer<byte[], byte[]> restoreConsumer;
        private readonly IStateRestoreListener userStateRestoreListener;
        private Dictionary<TopicPartition, TopicPartitionOffset> endOffsets = new Dictionary<TopicPartition, TopicPartitionOffset>();
        private readonly Dictionary<string, List<Partition>> partitionInfo = new Dictionary<string, List<Partition>>();
        private readonly Dictionary<TopicPartition, StateRestorer> stateRestorers = new Dictionary<TopicPartition, StateRestorer>();
        private readonly HashSet<TopicPartition> needsRestoring = new HashSet<TopicPartition>();
        private readonly HashSet<TopicPartition> needsInitializing = new HashSet<TopicPartition>();
        private readonly HashSet<TopicPartition> completedRestorers = new HashSet<TopicPartition>();
        private readonly TimeSpan pollTime;

        public StoreChangelogReader(
            ILogger<StoreChangelogReader> logger,
            StreamsConfig config,
            RestoreConsumer restoreConsumer,
            IStateRestoreListener userStateRestoreListener)
        {
            this.logger = logger;
            this.restoreConsumer = restoreConsumer;
            this.pollTime = TimeSpan.FromMilliseconds(config.PollMs);
            this.userStateRestoreListener = userStateRestoreListener;
        }

        public void Register(StateRestorer restorer)
        {
            restorer = restorer ?? throw new ArgumentNullException(nameof(restorer));

            if (!this.stateRestorers.ContainsKey(restorer.partition))
            {
                restorer.SetUserRestoreListener(this.userStateRestoreListener);
                this.stateRestorers.Add(restorer.partition, restorer);

                this.logger.LogTrace($"Added restorer for changelog {restorer.partition}");
            }

            this.needsInitializing.Add(restorer.partition);
        }

        public List<TopicPartition> Restore(IRestoringTasks active)
        {
            if (this.needsInitializing.Any())
            {
                this.Initialize(active);
            }

            if (!this.needsRestoring.Any())
            {
                this.restoreConsumer.Unsubscribe();

                return this.Completed();
            }

            try
            {
                ConsumeResult<byte[], byte[]> records = this.restoreConsumer.Consume(this.pollTime);

                foreach (TopicPartition partition in this.needsRestoring)
                {
                    StateRestorer restorer = this.stateRestorers[partition];
                    long pos = 0;// processNext(records.records(partition), restorer, endOffsets[partition]);

                    restorer.SetRestoredOffset(pos);

                    if (restorer.HasCompleted(pos, this.endOffsets[partition].Offset))
                    {
                        restorer.RestoreDone();
                        this.endOffsets.Remove(partition);
                        this.completedRestorers.Add(partition);
                    }
                }
            }
            catch (TopicPartitionOffsetException recoverableException)
            {
                this.logger.LogWarning("Restoring StreamTasks failed. Deleting StreamTasks stores to recreate from scratch.", recoverableException);
                var partitions = new HashSet<TopicPartition>(recoverableException.Results.Select(tpo => tpo.TopicPartition));

                foreach (TopicPartition partition in partitions)
                {
                    var task = active.RestoringTaskFor(partition);
                    this.logger.LogInformation("Reinitializing StreamTask {} for changelog {}", task, partition);

                    this.needsInitializing.Remove(partition);
                    this.needsRestoring.Remove(partition);

                    StateRestorer restorer = this.stateRestorers[partition];
                    restorer.SetCheckpointOffset(StateRestorer.NO_CHECKPOINT);
                    // task.ReinitializeStateStoresForPartitions(recoverableException.Results.Select(tpo => tpo.TopicPartition).ToList());
                }

                this.restoreConsumer.SeekToBeginning(partitions);
            }

            this.needsRestoring.RemoveWhere(tp => this.completedRestorers.Contains(tp));

            if (!this.needsRestoring.Any())
            {
                this.restoreConsumer.Unsubscribe();
            }

            return this.Completed();
        }

        private void Initialize(IRestoringTasks active)
        {
            if (this.restoreConsumer.Subscription.Any())
            {
                throw new StreamsException($"Restore consumer should not be subscribed to any topics ({this.restoreConsumer.Subscription.ToJoinedString()})");
            }

            // first refresh the changelog partition information from brokers, since initialize is only called when
            // the needsInitializing map is not empty, meaning we do not know the metadata for some of them yet
            this.RefreshChangelogInfo();

            var initializable = new HashSet<TopicPartition>();
            foreach (TopicPartition topicPartition in this.needsInitializing)
            {
                if (this.HasPartition(topicPartition))
                {
                    initializable.Add(topicPartition);
                }
            }

            // try to Fetch end offsets for the initializable restorers and Remove any partitions
            // where we already have All of the data
            try
            {
                this.endOffsets = this.restoreConsumer.Committed(initializable, TimeSpan.FromSeconds(5.0))
                    .ToDictionary(k => k.TopicPartition, v => v);
            }
            catch (TimeoutException e)
            {
                // if timeout exception gets thrown we just give up this time and retry in the next run loop
                this.logger.LogDebug(e, $"Could not Fetch end offset for {initializable}; will fall back to partition by partition fetching");

                return;
            }

            IEnumerator<TopicPartition> iter = initializable.GetEnumerator();
            while (iter.MoveNext())
            {
                TopicPartition topicPartition = iter.Current;
                long endOffset = this.endOffsets[topicPartition].Offset;

                // offset should not be null; but since the consumer API does not guarantee it
                // we add this check just in case
                StateRestorer restorer = this.stateRestorers[topicPartition];
                if (restorer.Checkpoint() >= endOffset)
                {
                    restorer.SetRestoredOffset(restorer.Checkpoint());
                    // iter.Current.Remove();

                    this.completedRestorers.Add(topicPartition);
                }
                else if (restorer.offsetLimit == 0 || endOffset == 0)
                {
                    restorer.SetRestoredOffset(0);
                    // iter.Remove();
                    this.completedRestorers.Add(topicPartition);
                }
                else
                {
                    restorer.SetEndingOffset(endOffset);
                }

                this.needsInitializing.Remove(topicPartition);
            }

            // set up restorer for those initializable
            if (initializable.Any())
            {
                this.StartRestoration(initializable, active);
            }
        }

        private void StartRestoration(
            HashSet<TopicPartition> initialized,
            IRestoringTasks active)
        {
            this.logger.LogDebug("Start restoring state stores from changelog topics {}", initialized);

            var assignment = new HashSet<TopicPartition>(this.restoreConsumer.Assignment);
            assignment.UnionWith(initialized);

            this.restoreConsumer.Assign(assignment);

            var needsPositionUpdate = new List<StateRestorer>();

            foreach (TopicPartition partition in initialized)
            {
                StateRestorer restorer = this.stateRestorers[partition];
                if (restorer.Checkpoint() != StateRestorer.NO_CHECKPOINT)
                {
                    this.logger.LogTrace($"Found checkpoint {restorer.Checkpoint()} from changelog {partition} for store {restorer.storeName}.");

                    this.restoreConsumer.Seek(new TopicPartitionOffset(partition, restorer.Checkpoint()));

                    this.LogRestoreOffsets(
                        partition,
                        restorer.Checkpoint(),
                        this.endOffsets[partition].Offset);

                    restorer.SetStartingOffset(this.restoreConsumer.Position(partition));
                    restorer.RestoreStarted();
                }
                else
                {
                    this.logger.LogTrace($"Did not find checkpoint from changelog {partition} for store {restorer.storeName}, rewinding to beginning.");

                    this.restoreConsumer.SeekToBeginning(new List<TopicPartition> { partition });
                    needsPositionUpdate.Add(restorer);
                }
            }

            foreach (StateRestorer restorer in needsPositionUpdate)
            {
                TopicPartition partition = restorer.partition;

                // If checkpoint does not exist it means the task was not shutdown gracefully before;
                // and in this case if EOS is turned on we should wipe out the state and re-initialize the task.
                var task = active.RestoringTaskFor(partition);

                if (false)//task.IsEosEnabled())
                {
                    this.logger.LogInformation("No checkpoint found for task {} state store {} changelog {} with EOS turned on. " +
                            "Reinitializing the task and restore its state from the beginning.", task.Id, restorer.storeName, partition);

                    this.needsInitializing.Remove(partition);
                    initialized.Remove(partition);
                    restorer.SetCheckpointOffset(this.restoreConsumer.Position(partition));

                    //task.ReinitializeStateStoresForPartitions(new List<TopicPartition> { partition });
                }
                else
                {
                    this.logger.LogInformation($"Restoring task {task.Id}'s state store {restorer.storeName} from beginning of the changelog {partition} ");

                    long position = this.restoreConsumer.Position(restorer.partition);
                    this.LogRestoreOffsets(
                        restorer.partition,
                        position,
                        this.endOffsets[restorer.partition].Offset);
                    restorer.SetStartingOffset(position);
                    restorer.RestoreStarted();
                }
            }

            this.needsRestoring.UnionWith(initialized);
        }

        private void LogRestoreOffsets(
            TopicPartition partition,
            long startingOffset,
            long endOffset)
        {
            this.logger.LogDebug($"Restoring partition {partition} from offset {startingOffset} to endOffset {endOffset}");
        }

        private List<TopicPartition> Completed()
            => this.completedRestorers.ToList();

        private void RefreshChangelogInfo()
        {
            try
            {
                //partitionInfo.putAll(restoreConsumer..listTopics());
            }
            catch (TimeoutException e)
            {
                this.logger.LogDebug(e, "Could not Fetch topic metadata within the timeout, will retry in the next run loop");
            }
        }

        public Dictionary<TopicPartition, long> GetRestoredOffsets()
        {
            var restoredOffsets = new Dictionary<TopicPartition, long>();

            foreach (KeyValuePair<TopicPartition, StateRestorer> entry in this.stateRestorers)
            {
                StateRestorer restorer = entry.Value;
                if (restorer.IsPersistent())
                {
                    restoredOffsets.Add(entry.Key, restorer.restoredOffset);
                }
            }

            return restoredOffsets;
        }

        public void Reset()
        {
            this.partitionInfo.Clear();
            this.stateRestorers.Clear();
            this.needsRestoring.Clear();
            this.endOffsets.Clear();
            this.needsInitializing.Clear();
            this.completedRestorers.Clear();
        }

        private bool HasPartition(TopicPartition topicPartition)
        {
            List<Partition> partitions = this.partitionInfo[topicPartition.Topic];

            if (partitions == null)
            {
                return false;
            }

            foreach (var partition in partitions)
            {
                if (partition == topicPartition.Partition)
                {
                    return true;
                }
            }

            return false;
        }

        public void Remove(IEnumerable<TopicPartition> enumerable)
        {
            throw new NotImplementedException();
        }
    }
}
