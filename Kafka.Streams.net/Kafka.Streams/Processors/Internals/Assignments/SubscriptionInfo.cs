using System;
using System.Collections.Generic;
using System.Linq;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Tasks;
using Kafka.Streams.Temporary;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.Processors.Internals.Assignments
{
    public class SubscriptionInfo
    {
        private static ILogger<SubscriptionInfo> LOG { get; }// = LoggerFactory.getLogger(SubscriptionInfo.class);

        static int UNKNOWN = -1;
        static int MIN_VERSION_OFFSET_SUM_SUBSCRIPTION = 7;

        public static long UNKNOWN_OFFSET_SUM { get; } = -3L;

        private SubscriptionInfoData data;
        private HashSet<TaskId>? prevTasksCache = null;
        private HashSet<TaskId>? standbyTasksCache = null;
        private Dictionary<TaskId, long>? taskOffsetSumsCache = null;
        private static readonly long LATEST_OFFSET = -2L;

        public SubscriptionInfo()
        {
            // Just statically check to make sure that the generated code always stays in sync with the overall protocol
            int subscriptionInfoLatestVersion = SubscriptionInfoData.SCHEMAS.Length - 1;
            if (subscriptionInfoLatestVersion != StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION)
            {
                throw new InvalidProgramException(
                    "streams/src/main/resources/common/message/SubscriptionInfo.json needs to be updated to match the " +
                        "latest assignment protocol version. SubscriptionInfo only supports up to  ["
                        + subscriptionInfoLatestVersion + "] but needs to support up to [" + StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION + "].");
            }
        }

        private static void validateVersions(int version, int latestSupportedVersion)
        {
            if (latestSupportedVersion == UNKNOWN && (version < 1 || version > 2))
            {
                throw new ArgumentException(
                    "Only versions 1 and 2 are expected to use an UNKNOWN (-1) latest supported version. " +
                        "Got " + version + "."
                );
            }
            else if (latestSupportedVersion != UNKNOWN && (version < 1 || version > latestSupportedVersion))
            {
                throw new ArgumentException(
                    "version must be between 1 and " + latestSupportedVersion + "; was: " + version
                );
            }
        }

        public SubscriptionInfo(
            int version,
            int latestSupportedVersion,
            Guid processId,
            string userEndPoint,
            Dictionary<TaskId, long> taskOffsetSums)
        {
            validateVersions(version, latestSupportedVersion);
            SubscriptionInfoData data = new SubscriptionInfoData();
            data.setVersion(version);
            data.setProcessId(processId);

            if (version >= 2)
            {
                data.setUserEndPoint(
                    userEndPoint == null
                    ? Array.Empty<byte>()
                    : userEndPoint.GetBytes());
            }

            if (version >= 3)
            {
                data.setLatestSupportedVersion(latestSupportedVersion);
            }

            this.data = data;

            if (version >= MIN_VERSION_OFFSET_SUM_SUBSCRIPTION)
            {
                this.setTaskOffsetSumDataFromTaskOffsetSumMap(taskOffsetSums);
            }
            else
            {
                this.setPrevAndStandbySetsFromParsedTaskOffsetSumMap(taskOffsetSums);
            }
        }

        private SubscriptionInfo(SubscriptionInfoData subscriptionInfoData)
        {
            validateVersions(subscriptionInfoData.version(), subscriptionInfoData.latestSupportedVersion());
            this.data = subscriptionInfoData;
        }

        private void setTaskOffsetSumDataFromTaskOffsetSumMap(Dictionary<TaskId, long> taskOffsetSums)
        {
            Dictionary<int, List<PartitionToOffsetSum>> topicGroupIdToPartitionOffsetSum = new Dictionary<int, List<PartitionToOffsetSum>>();
            foreach (var taskEntry in taskOffsetSums)
            {
                TaskId task = taskEntry.Key;
                if (topicGroupIdToPartitionOffsetSum.ContainsKey(task.TopicGroupId))
                {
                    topicGroupIdToPartitionOffsetSum[task.TopicGroupId].Add(
                        new PartitionToOffsetSum()
                            .setPartition(task.Partition)
                            .setOffsetSum(taskEntry.Value));
                }
            }

            this.data.setTaskOffsetSums(topicGroupIdToPartitionOffsetSum
                .Select(t =>
            {
                TaskOffsetSum taskOffsetSum = new TaskOffsetSum();
                taskOffsetSum.SetTopicGroupId(t.Key);
                taskOffsetSum.SetPartitionToOffsetSum(t.Value);
                return taskOffsetSum;
            })
            .ToList());
        }

        private void setPrevAndStandbySetsFromParsedTaskOffsetSumMap(Dictionary<TaskId, long> taskOffsetSums)
        {
            HashSet<TaskId> prevTasks = new HashSet<TaskId>();
            HashSet<TaskId> standbyTasks = new HashSet<TaskId>();

            foreach (var taskOffsetSum in taskOffsetSums)
            {
                if (taskOffsetSum.Value == LATEST_OFFSET)
                {
                    prevTasks.Add(taskOffsetSum.Key);
                }
                else
                {
                    standbyTasks.Add(taskOffsetSum.Key);
                }
            }

            this.data.setPrevTasks(prevTasks.Select(t =>
            {
                TaskId taskId = new TaskId();
                taskId.SetTopicGroupId(t.TopicGroupId);
                taskId.SetPartition(t.Partition);
                return taskId;
            })
            .ToList());

            this.data.setStandbyTasks(standbyTasks.Select(t =>
            {
                TaskId taskId = new TaskId();
                taskId.SetTopicGroupId(t.TopicGroupId);
                taskId.SetPartition(t.Partition);

                return taskId;
            })
            .ToList());
        }

        public int version()
        {
            return this.data.version();
        }

        public int latestSupportedVersion()
        {
            return this.data.latestSupportedVersion();
        }

        public Guid processId()
        {
            return this.data.processId();
        }

        public HashSet<TaskId> prevTasks()
        {
            if (this.prevTasksCache == null)
            {
                if (this.data.version() >= MIN_VERSION_OFFSET_SUM_SUBSCRIPTION)
                {
                    this.prevTasksCache = getActiveTasksFromTaskOffsetSumMap(this.taskOffsetSums());
                }
                else
                {
                    this.prevTasksCache = this.data.prevTasks()
                            .Select(t => new TaskId(t.TopicGroupId, t.Partition))
                            .ToHashSet();
                }
            }
            return this.prevTasksCache;
        }

        public HashSet<TaskId> standbyTasks()
        {
            if (this.standbyTasksCache == null)
            {
                if (this.data.version() >= MIN_VERSION_OFFSET_SUM_SUBSCRIPTION)
                {
                    this.standbyTasksCache = getStandbyTasksFromTaskOffsetSumMap(this.taskOffsetSums());
                }
                else
                {
                    this.standbyTasksCache = this.data.standbyTasks()
                        .Select(t => new TaskId(t.TopicGroupId, t.Partition))
                        .ToHashSet();
                }
            }
            return this.standbyTasksCache;
        }

        public Dictionary<TaskId, long> taskOffsetSums()
        {
            if (this.taskOffsetSumsCache == null)
            {
                this.taskOffsetSumsCache = new Dictionary<TaskId, long>();
                if (this.data.version() >= MIN_VERSION_OFFSET_SUM_SUBSCRIPTION)
                {
                    foreach (TaskOffsetSum taskOffsetSum in this.data.taskOffsetSums())
                    {
                        foreach (PartitionToOffsetSum partitionOffsetSum in taskOffsetSum.partitionToOffsetSum)
                        {
                            this.taskOffsetSumsCache.Put(
                                new TaskId(
                                    taskOffsetSum.topicGroupId(),
                                    partitionOffsetSum.partition()),
                                partitionOffsetSum.offsetSum()
                            );
                        }
                    }
                }
                else
                {
                    this.prevTasks().ForEach(taskId => this.taskOffsetSumsCache.Put(taskId, LATEST_OFFSET));
                    this.standbyTasks().ForEach(taskId => this.taskOffsetSumsCache.Put(taskId, UNKNOWN_OFFSET_SUM));
                }
            }
            return this.taskOffsetSumsCache;
        }

        public string? userEndPoint()
        {
            return this.data.userEndPoint() == null || this.data.userEndPoint().Length == 0
                ? null
                : this.data.userEndPoint();
        }

        public static HashSet<TaskId> getActiveTasksFromTaskOffsetSumMap(Dictionary<TaskId, long> taskOffsetSums)
        {
            return taskOffsetSumMapToTaskSet(taskOffsetSums, true);
        }

        public static HashSet<TaskId> getStandbyTasksFromTaskOffsetSumMap(Dictionary<TaskId, long> taskOffsetSums)
        {
            return taskOffsetSumMapToTaskSet(taskOffsetSums, false);
        }

        private static HashSet<TaskId> taskOffsetSumMapToTaskSet(
            Dictionary<TaskId, long> taskOffsetSums,
            bool getActiveTasks)
        {
            return taskOffsetSums
                .Where(t => getActiveTasks == (t.Value == SubscriptionInfo.LATEST_OFFSET))
                .Select(e => e.Key)
                .ToHashSet();
        }

        /**
         * @throws TaskAssignmentException if method fails to encode the data
         */
        public ByteBuffer encode()
        {
            if (this.data.version() > StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION)
            {
                throw new InvalidOperationException(
                    "Should never try to encode a SubscriptionInfo with version [" +
                        this.data.version() + "] > LATEST_SUPPORTED_VERSION [" + StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION + "]"
                );
            }
            else
            {
                var cache = new ObjectSerializationCache();
                ByteBuffer buffer = new ByteBuffer().Allocate(this.data.Size(cache, (short)this.data.version()));
                ByteBufferAccessor accessor = new ByteBufferAccessor(buffer);
                this.data.Write(accessor, cache, (short)this.data.version());
                buffer.Rewind();
                return buffer;
            }
        }

        /**
         * @throws TaskAssignmentException if method fails to decode the data
         */
        public static SubscriptionInfo Decode(ByteBuffer data)
        {
            data.Rewind();
            int version = data.GetInt();

            if (version > StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION)
            {
                // in this special case, we only rely on the version and latest version,
                int latestSupportedVersion = data.GetInt();
                SubscriptionInfoData subscriptionInfoData = new SubscriptionInfoData();
                subscriptionInfoData.setVersion(version);
                subscriptionInfoData.setLatestSupportedVersion(latestSupportedVersion);
                LOG.LogInformation("Unable to decode subscription data: used version: {}; latest supported version: {}",
                    version,
                    latestSupportedVersion
                );
                return new SubscriptionInfo(subscriptionInfoData);
            }
            else
            {
                data.Rewind();
                ByteBufferAccessor accessor = new ByteBufferAccessor(data);
                SubscriptionInfoData subscriptionInfoData = new SubscriptionInfoData(accessor, (short)version);
                return new SubscriptionInfo(subscriptionInfoData);
            }
        }

        public override int GetHashCode()
        {
            return this.data.GetHashCode();
        }

        public override bool Equals(object o)
        {
            if (o is SubscriptionInfo other)
            {
                return this.data.Equals(other.data);
            }
            else
            {
                return false;
            }
        }

        public override string ToString()
        {
            return this.data.ToString();
        }
    }
}
