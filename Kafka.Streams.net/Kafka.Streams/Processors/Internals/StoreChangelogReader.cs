using System.Collections.Generic;
using Confluent.Kafka;
using System;
using Microsoft.Extensions.Logging;
using System.Linq;
using Kafka.Streams.Errors;
using Kafka.Streams.Tasks;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.Clients.Consumers;
using Kafka.Streams.Extensions;
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

        public void register(StateRestorer restorer)
        {
            restorer = restorer ?? throw new ArgumentNullException(nameof(restorer));

            if (!stateRestorers.ContainsKey(restorer.partition))
            {
                restorer.setUserRestoreListener(userStateRestoreListener);
                stateRestorers.Add(restorer.partition, restorer);

                this.logger.LogTrace($"Added restorer for changelog {restorer.partition}");
            }

            needsInitializing.Add(restorer.partition);
        }

        public List<TopicPartition> restore(IRestoringTasks active)
        {
            if (needsInitializing.Any())
            {
                initialize(active);
            }

            if (!needsRestoring.Any())
            {
                restoreConsumer.Unsubscribe();

                return completed();
            }

            try
            {
                ConsumeResult<byte[], byte[]> records = restoreConsumer.Consume(pollTime);

                foreach (TopicPartition partition in needsRestoring)
                {
                    StateRestorer restorer = stateRestorers[partition];
                    long pos = 0;// processNext(records.records(partition), restorer, endOffsets[partition]);

                    restorer.setRestoredOffset(pos);

                    if (restorer.hasCompleted(pos, endOffsets[partition].Offset))
                    {
                        restorer.restoreDone();
                        endOffsets.Remove(partition);
                        completedRestorers.Add(partition);
                    }
                }
            }
            catch (TopicPartitionOffsetException recoverableException)
            {
                this.logger.LogWarning("Restoring StreamTasks failed. Deleting StreamTasks stores to recreate from scratch.", recoverableException);
                var partitions = new HashSet<TopicPartition>(recoverableException.Results.Select(tpo => tpo.TopicPartition));

                foreach (TopicPartition partition in partitions)
                {
                    var task = active.restoringTaskFor(partition);
                    logger.LogInformation("Reinitializing StreamTask {} for changelog {}", task, partition);

                    needsInitializing.Remove(partition);
                    needsRestoring.Remove(partition);

                    StateRestorer restorer = stateRestorers[partition];
                    restorer.setCheckpointOffset(StateRestorer.NO_CHECKPOINT);
                    task.reinitializeStateStoresForPartitions(recoverableException.Results.Select(tpo => tpo.TopicPartition).ToList());
                }

                restoreConsumer.SeekToBeginning(partitions);
            }

            needsRestoring.RemoveWhere(tp => completedRestorers.Contains(tp));

            if (!needsRestoring.Any())
            {
                restoreConsumer.Unsubscribe();
            }

            return completed();
        }

        private void initialize(IRestoringTasks active)
        {
            if (restoreConsumer.Subscription.Any())
            {
                throw new StreamsException($"Restore consumer should not be subscribed to any topics ({restoreConsumer.Subscription.ToJoinedString()})");
            }

            // first refresh the changelog partition information from brokers, since initialize is only called when
            // the needsInitializing map is not empty, meaning we do not know the metadata for some of them yet
            refreshChangelogInfo();

            var initializable = new HashSet<TopicPartition>();
            foreach (TopicPartition topicPartition in needsInitializing)
            {
                if (hasPartition(topicPartition))
                {
                    initializable.Add(topicPartition);
                }
            }

            // try to fetch end offsets for the initializable restorers and Remove any partitions
            // where we already have all of the data
            try
            {
                endOffsets = restoreConsumer.Committed(initializable, TimeSpan.FromSeconds(5.0))
                    .ToDictionary(k => k.TopicPartition, v => v);
            }
            catch (TimeoutException e)
            {
                // if timeout exception gets thrown we just give up this time and retry in the next run loop
                logger.LogDebug(e, $"Could not fetch end offset for {initializable}; will fall back to partition by partition fetching");

                return;
            }

            IEnumerator<TopicPartition> iter = initializable.GetEnumerator();
            while (iter.MoveNext())
            {
                TopicPartition topicPartition = iter.Current;
                long endOffset = endOffsets[topicPartition].Offset;

                // offset should not be null; but since the consumer API does not guarantee it
                // we add this check just in case
                StateRestorer restorer = stateRestorers[topicPartition];
                if (restorer.checkpoint() >= endOffset)
                {
                    restorer.setRestoredOffset(restorer.checkpoint());
                    // iter.Current.Remove();

                    completedRestorers.Add(topicPartition);
                }
                else if (restorer.offsetLimit == 0 || endOffset == 0)
                {
                    restorer.setRestoredOffset(0);
                    // iter.Remove();
                    completedRestorers.Add(topicPartition);
                }
                else
                {
                    restorer.setEndingOffset(endOffset);
                }

                needsInitializing.Remove(topicPartition);
            }

            // set up restorer for those initializable
            if (initializable.Any())
            {
                StartRestoration(initializable, active);
            }
        }

        private void StartRestoration(
            HashSet<TopicPartition> initialized,
            IRestoringTasks active)
        {
            logger.LogDebug("Start restoring state stores from changelog topics {}", initialized);

            var assignment = new HashSet<TopicPartition>(restoreConsumer.Assignment);
            assignment.UnionWith(initialized);

            restoreConsumer.Assign(assignment);

            var needsPositionUpdate = new List<StateRestorer>();

            foreach (TopicPartition partition in initialized)
            {
                StateRestorer restorer = stateRestorers[partition];
                if (restorer.checkpoint() != StateRestorer.NO_CHECKPOINT)
                {
                    this.logger.LogTrace($"Found checkpoint {restorer.checkpoint()} from changelog {partition} for store {restorer.storeName}.");

                    this.restoreConsumer.Seek(new TopicPartitionOffset(partition, restorer.checkpoint()));

                    logRestoreOffsets(
                        partition,
                        restorer.checkpoint(),
                        endOffsets[partition].Offset);

                    restorer.setStartingOffset(restoreConsumer.Position(partition));
                    restorer.restoreStarted();
                }
                else
                {
                    this.logger.LogTrace($"Did not find checkpoint from changelog {partition} for store {restorer.storeName}, rewinding to beginning.");

                    restoreConsumer.SeekToBeginning(new List<TopicPartition> { partition });
                    needsPositionUpdate.Add(restorer);
                }
            }

            foreach (StateRestorer restorer in needsPositionUpdate)
            {
                TopicPartition partition = restorer.partition;

                // If checkpoint does not exist it means the task was not shutdown gracefully before;
                // and in this case if EOS is turned on we should wipe out the state and re-initialize the task.
                var task = active.restoringTaskFor(partition);

                if (task.isEosEnabled())
                {
                    this.logger.LogInformation("No checkpoint found for task {} state store {} changelog {} with EOS turned on. " +
                            "Reinitializing the task and restore its state from the beginning.", task.id, restorer.storeName, partition);

                    needsInitializing.Remove(partition);
                    initialized.Remove(partition);
                    restorer.setCheckpointOffset(restoreConsumer.Position(partition));

                    task.reinitializeStateStoresForPartitions(new List<TopicPartition> { partition });
                }
                else
                {
                    this.logger.LogInformation($"Restoring task {task.id}'s state store {restorer.storeName} from beginning of the changelog {partition} ");

                    long position = restoreConsumer.Position(restorer.partition);
                    logRestoreOffsets(
                        restorer.partition,
                        position,
                        endOffsets[restorer.partition].Offset);
                    restorer.setStartingOffset(position);
                    restorer.restoreStarted();
                }
            }

            needsRestoring.UnionWith(initialized);
        }

        private void logRestoreOffsets(
            TopicPartition partition,
            long startingOffset,
            long endOffset)
        {
            this.logger.LogDebug($"Restoring partition {partition} from offset {startingOffset} to endOffset {endOffset}");
        }

        private List<TopicPartition> completed()
            => completedRestorers.ToList();

        private void refreshChangelogInfo()
        {
            try
            {
                //partitionInfo.putAll(restoreConsumer..listTopics());
            }
            catch (TimeoutException e)
            {
                this.logger.LogDebug(e, "Could not fetch topic metadata within the timeout, will retry in the next run loop");
            }
        }

        public Dictionary<TopicPartition, long> restoredOffsets()
        {
            var restoredOffsets = new Dictionary<TopicPartition, long>();

            foreach (KeyValuePair<TopicPartition, StateRestorer> entry in stateRestorers)
            {
                StateRestorer restorer = entry.Value;
                if (restorer.isPersistent())
                {
                    restoredOffsets.Add(entry.Key, restorer.restoredOffset);
                }
            }

            return restoredOffsets;
        }

        public void reset()
        {
            partitionInfo.Clear();
            stateRestorers.Clear();
            needsRestoring.Clear();
            endOffsets.Clear();
            needsInitializing.Clear();
            completedRestorers.Clear();
        }

        private bool hasPartition(TopicPartition topicPartition)
        {
            List<Partition> partitions = partitionInfo[topicPartition.Topic];

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
    }
}