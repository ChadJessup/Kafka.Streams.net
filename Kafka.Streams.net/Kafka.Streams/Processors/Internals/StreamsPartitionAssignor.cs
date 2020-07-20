using Confluent.Kafka;
using Kafka.Common;
using Kafka.Common.Interfaces;
using Kafka.Streams.Errors;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Processors.Internals.Assignments;
using Kafka.Streams.State;
using Kafka.Streams.Tasks;
using Kafka.Streams.Temporary;
using Kafka.Streams.Utils;
using Microsoft.Extensions.Logging;
using Priority_Queue;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Streams.Processors.Internals
{

    public class StreamsPartitionAssignor : IConsumerPartitionAssignor
    {
        public enum Error
        {
            NONE = 0,
            INCOMPLETE_SOURCE_TOPIC_METADATA = 1,
            VERSION_PROBING = 2,
        }

        public static Error FromCode(int code)
        {
            return code switch
            {
                0 => Error.NONE,
                1 => Error.INCOMPLETE_SOURCE_TOPIC_METADATA,
                2 => Error.VERSION_PROBING,
                _ => throw new System.ArgumentException("Unknown error code: " + code),
            };
        }

        private ILogger<StreamsPartitionAssignor> log;
        private string logPrefix;

        private class AssignedPartition : IComparable<AssignedPartition>
        {
            public TaskId taskId { get; }
            public TopicPartition Partition { get; }

            public AssignedPartition(TaskId taskId, TopicPartition partition)
            {
                this.taskId = taskId;
                this.Partition = partition;
            }

            public int CompareTo(AssignedPartition that)
            {
                return PARTITION_COMPARATOR.Compare(this.Partition, that.Partition);
            }

            public override bool Equals(object o)
            {
                if (!(o is AssignedPartition))
                {
                    return false;
                }

                AssignedPartition other = (AssignedPartition)o;
                return this.CompareTo(other) == 0;
            }

            public override int GetHashCode()
            {
                // Only partition is important for compareTo, equals and hashCode.
                return this.Partition.GetHashCode();
            }
        }

        private class ClientMetadata
        {
            public HostInfo hostInfo { get; }
            public ClientState state { get; }
            public SortedSet<string> consumers { get; }

            public ClientMetadata(string endPoint)
            {
                // get the host info, or null if no endpoint is configured (ie endPoint == null)
                this.hostInfo = HostInfo.BuildFromEndpoint(endPoint);

                // initialize the consumer memberIds
                this.consumers = new SortedSet<string>();

                // initialize the client state
                this.state = new ClientState();
            }

            public void addConsumer(string consumerMemberId, IEnumerable<TopicPartition> ownedPartitions)
            {
                this.consumers.Add(consumerMemberId);
                this.state.IncrementCapacity();
                this.state.AddOwnedPartitions(ownedPartitions, consumerMemberId);
            }

            public void addPreviousTasksAndOffsetSums(string consumerId, Dictionary<TaskId, long> taskOffsetSums)
            {
                this.state.AddPreviousTasksAndOffsetSums(consumerId, taskOffsetSums);
            }

            public override string ToString()
            {
                return "ClientMetadata{" +
                    "hostInfo=" + this.hostInfo +
                    ", consumers=" + this.consumers +
                    ", state=" + this.state +
                    '}';
            }
        }

        // keep track of any future consumers in a "dummy" Client since we can't decipher their subscription
        private static Guid FUTURE_ID = Guid.NewGuid();

        protected static readonly IComparer<TopicPartition> PARTITION_COMPARATOR = new PartitionComparator();

        private string userEndPoint;
        private AssignmentConfigs assignmentConfigs;

        private ITaskManager taskManager;
        private StreamsMetadataState streamsMetadataState;

        private int assignmentErrorCode;
        private long nextScheduledRebalanceMs;
        private IClock time;

        protected int usedSubscriptionMetadataVersion = StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION;

        private IAdminClient adminClient;
        private InternalTopicManager internalTopicManager;
        private CopartitionedTopicsEnforcer copartitionedTopicsEnforcer;
        private RebalanceProtocol rebalanceProtocol;
        private AssignmentListener assignmentListener;

        private ISupplier<ITaskAssignor> taskAssignorSupplier;

        /**
         * We need to have the PartitionAssignor and its StreamThread to be mutually accessible since the former needs
         * later's cached metadata while sending subscriptions, and the latter needs former's returned assignment when
         * adding tasks.
         *
         * @throws KafkaException if the stream thread is not specified
         */
        public void Configure(Dictionary<string, string?> configs)
        {
            AssignorConfiguration assignorConfiguration = new AssignorConfiguration(configs);

            // this.logPrefix = assignorConfiguration.logPrefix();
            this.usedSubscriptionMetadataVersion = assignorConfiguration
                .configuredMetadataVersion(this.usedSubscriptionMetadataVersion);
            this.taskManager = assignorConfiguration.taskManager();
            this.streamsMetadataState = assignorConfiguration.streamsMetadataState();
            this.assignmentErrorCode = assignorConfiguration.assignmentErrorCode();
            this.nextScheduledRebalanceMs = assignorConfiguration.nextScheduledRebalanceMs();
            this.time = assignorConfiguration.time();
            this.assignmentConfigs = assignorConfiguration.assignmentConfigs();
            partitionGrouper = assignorConfiguration.partitionGrouper();
            this.userEndPoint = assignorConfiguration.userEndPoint();
            this.adminClient = assignorConfiguration.adminClient();
            this.internalTopicManager = assignorConfiguration.internalTopicManager();
            this.copartitionedTopicsEnforcer = assignorConfiguration.copartitionedTopicsEnforcer();
            this.rebalanceProtocol = assignorConfiguration.rebalanceProtocol();
            this.taskAssignorSupplier = assignorConfiguration.taskAssignor;
            this.assignmentListener = assignorConfiguration.assignmentListener();
        }

        public string Name()
        {
            return "stream";
        }

        public List<RebalanceProtocol> supportedProtocols()
        {
            List<RebalanceProtocol> supportedProtocols = new List<RebalanceProtocol>
            {
                RebalanceProtocol.EAGER
            };

            if (this.rebalanceProtocol == RebalanceProtocol.COOPERATIVE)
            {
                supportedProtocols.Add(this.rebalanceProtocol);
            }

            return supportedProtocols;
        }

        public ByteBuffer subscriptionUserData(HashSet<string> topics)
        {
            // Adds the following information to subscription
            // 1. Client Guid (a unique id assigned to an instance of KafkaStreams)
            // 2. Map from task id to its overall lag

            this.handleRebalanceStart(topics);

            return new SubscriptionInfo(
                this.usedSubscriptionMetadataVersion,
                StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION,
                this.taskManager.processId(),
                this.userEndPoint,
                this.taskManager.GetTaskOffsetSums())
                    .encode();
        }

        private Dictionary<string, Assignment> errorAssignment(
            Dictionary<Guid, ClientMetadata> clientsMetadata,
            int errorCode)
        {
            Dictionary<string, Assignment> assignment = new Dictionary<string, Assignment>();
            foreach (ClientMetadata clientMetadata in clientsMetadata.Values)
            {
                foreach (string consumerId in clientMetadata.consumers)
                {
                    assignment.Put(consumerId, new Assignment(
                        new List<TopicPartition>(),
                        new AssignmentInfo(
                            StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION,
                            new List<TaskId>(),
                            new Dictionary<TaskId, HashSet<TopicPartition>>(),
                            new Dictionary<HostInfo, HashSet<TopicPartition>>(),
                            new Dictionary<HostInfo, HashSet<TopicPartition>>(),
                            errorCode)
                        .Encode()));
                }
            }
            return assignment;
        }

        /*
         * This assigns tasks to consumer clients in the following steps.
         *
         * 0. decode the subscriptions to assemble the metadata for each client and check for version probing
         *
         * 1. check all repartition source topics and use internal topic manager to make sure
         *    they have been created with the right number of partitions. Also verify and/or create
         *    any changelog topics with the correct number of partitions.
         *
         * 2. use the partition grouper to generate tasks along with their assigned partitions, then use
         *    the configured TaskAssignor to construct the mapping of tasks to clients.
         *
         * 3. construct the global mapping of host to partitions to enable query routing.
         *
         * 4. within each client, assign tasks to consumer clients.
         */

        public GroupAssignment Assign(Cluster metadata, GroupSubscription groupSubscription)
        {
            Dictionary<string, Subscription> subscriptions = groupSubscription.groupSubscription();

            // ---------------- Step Zero ---------------- //

            // construct the client metadata from the decoded subscription info

            Dictionary<Guid, ClientMetadata> clientMetadataMap = new Dictionary<Guid, ClientMetadata>();
            HashSet<TopicPartition> allOwnedPartitions = new HashSet<TopicPartition>();

            int minReceivedMetadataVersion = StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION;
            int minSupportedMetadataVersion = StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION;

            int futureMetadataVersion = StreamsAssignmentProtocolVersions.UNKNOWN;
            foreach (var entry in subscriptions)
            {
                string consumerId = entry.Key;
                Subscription subscription = entry.Value;
                SubscriptionInfo info = SubscriptionInfo.Decode(subscription.userData);
                int usedVersion = info.version();

                minReceivedMetadataVersion = this.updateMinReceivedVersion(usedVersion, minReceivedMetadataVersion);
                minSupportedMetadataVersion = updateMinSupportedVersion(info.latestSupportedVersion(), minSupportedMetadataVersion);

                Guid processId;
                if (usedVersion > StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION)
                {
                    futureMetadataVersion = usedVersion;
                    processId = FUTURE_ID;
                    if (!clientMetadataMap.ContainsKey(FUTURE_ID))
                    {
                        clientMetadataMap.Put(FUTURE_ID, new ClientMetadata(null));
                    }
                }
                else
                {
                    processId = info.processId();
                }

                ClientMetadata clientMetadata = clientMetadataMap[processId];

                // create the new client metadata if necessary
                if (clientMetadata == null)
                {
                    clientMetadata = new ClientMetadata(info.userEndPoint());
                    clientMetadataMap.Put(info.processId(), clientMetadata);
                }

                // add the consumer and any info in its subscription to the client
                clientMetadata.addConsumer(consumerId, subscription.ownedPartitions);
                allOwnedPartitions.AddRange(subscription.ownedPartitions);
                clientMetadata.addPreviousTasksAndOffsetSums(consumerId, info.taskOffsetSums());
            }

            bool versionProbing =
                this.checkMetadataVersions(minReceivedMetadataVersion, minSupportedMetadataVersion, futureMetadataVersion);

            this.log.LogDebug("Constructed client metadata {} from the member subscriptions.", clientMetadataMap);

            // ---------------- Step One ---------------- //

            // parse the topology to determine the repartition source topics,
            // making sure they are created with the number of partitions as
            // the maximum of the depending sub-topologies source topics' number of partitions
            Dictionary<int, TopicsInfo> topicGroups = this.taskManager.Builder().TopicGroups();

            Dictionary<TopicPartition, PartitionInfo> allRepartitionTopicPartitions;
            try
            {
                allRepartitionTopicPartitions = this.prepareRepartitionTopics(topicGroups, metadata);
            }
            catch (TaskAssignmentException e)
            {
                return new GroupAssignment(
                    errorAssignment(clientMetadataMap,
                        AssignorError.INCOMPLETE_SOURCE_TOPIC_METADATA.code())
                );
            }

            Cluster fullMetadata = metadata.withPartitions(allRepartitionTopicPartitions);

            this.log.LogDebug("Created repartition topics {} from the parsed topology.", allRepartitionTopicPartitions.Values);

            // ---------------- Step Two ---------------- //

            // construct the assignment of tasks to clients

            HashSet<string> allSourceTopics = new HashSet<string>();
            Dictionary<int, HashSet<string>> sourceTopicsByGroup = new Dictionary<int, HashSet<string>>();
            foreach (var entry in topicGroups)
            {
                allSourceTopics.AddRange(entry.Value.sourceTopics);
                sourceTopicsByGroup.Put(entry.Key, entry.Value.sourceTopics);
            }

            // get the tasks as partition groups from the partition grouper
            Dictionary<TaskId, HashSet<TopicPartition>> partitionsForTask =
                partitionGrouper.partitionGroups(sourceTopicsByGroup, fullMetadata);

            HashSet<TaskId> statefulTasks = new HashSet<TaskId>();

            bool probingRebalanceNeeded =
                this.assignTasksToClients(fullMetadata, allSourceTopics, topicGroups, clientMetadataMap, partitionsForTask, statefulTasks);

            // ---------------- Step Three ---------------- //

            // construct the global partition assignment per host map

            Dictionary<HostInfo, HashSet<TopicPartition>> partitionsByHost = new Dictionary<HostInfo, HashSet<TopicPartition>>();
            Dictionary<HostInfo, HashSet<TopicPartition>> standbyPartitionsByHost = new Dictionary<HostInfo, HashSet<TopicPartition>>();
            if (minReceivedMetadataVersion >= 2)
            {
                this.populatePartitionsByHostMaps(partitionsByHost, standbyPartitionsByHost, partitionsForTask, clientMetadataMap);
            }

            this.streamsMetadataState.OnChange(partitionsByHost, standbyPartitionsByHost, fullMetadata);

            // ---------------- Step Four ---------------- //

            // compute the assignment of tasks to threads within each client and build the group assignment

            Dictionary<string, Assignment> assignment = this.computeNewAssignment(
                statefulTasks,
                clientMetadataMap,
                partitionsForTask,
                partitionsByHost,
                standbyPartitionsByHost,
                allOwnedPartitions,
                minReceivedMetadataVersion,
                minSupportedMetadataVersion,
                versionProbing,
                probingRebalanceNeeded);

            return new GroupAssignment(assignment);
        }

        /**
         * Verify the subscription versions are within the expected bounds and check for version probing.
         *
         * @return whether this was a version probing rebalance
         */
        private bool checkMetadataVersions(
            int minReceivedMetadataVersion,
            int minSupportedMetadataVersion,
            int futureMetadataVersion)
        {
            bool versionProbing;

            if (futureMetadataVersion == StreamsAssignmentProtocolVersions.UNKNOWN)
            {
                versionProbing = false;
            }
            else if (minReceivedMetadataVersion >= StreamsAssignmentProtocolVersions.EARLIEST_PROBEABLE_VERSION)
            {
                versionProbing = true;
                this.log.LogInformation("Received a future (version probing) subscription (version: {})."
                             + " Sending assignment back (with supported version {}).",
                    futureMetadataVersion,
                    minSupportedMetadataVersion);

            }
            else
            {
                throw new InvalidOperationException(
                    "Received a future (version probing) subscription (version: " + futureMetadataVersion
                        + ") and an incompatible pre Kafka 2.0 subscription (version: " + minReceivedMetadataVersion
                        + ") at the same time."
                );
            }

            if (minReceivedMetadataVersion < StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION)
            {
                this.log.LogInformation("Downgrade metadata to version {}. Latest supported version is {}.",
                    minReceivedMetadataVersion,
                    StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION);
            }
            if (minSupportedMetadataVersion < StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION)
            {
                this.log.LogInformation("Downgrade latest supported metadata to version {}. Latest supported version is {}.",
                    minSupportedMetadataVersion,
                    StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION);
            }
            return versionProbing;
        }

        /**
         * @return a map of repartition topics and their metadata
         * @throws TaskAssignmentException if there is incomplete source topic metadata due to missing source topic(s)
         */
        private Dictionary<string, InternalTopicConfig> computeRepartitionTopicMetadata(
            Dictionary<int, TopicsInfo> topicGroups,
            Cluster metadata)
        {
            Dictionary<string, InternalTopicConfig> repartitionTopicMetadata = new Dictionary<string, InternalTopicConfig>();
            foreach (TopicsInfo topicsInfo in topicGroups.Values)
            {
                foreach (string topic in topicsInfo.sourceTopics)
                {
                    if (!topicsInfo.repartitionSourceTopics.ContainsKey(topic) &&
                            !metadata.topics().Contains(topic))
                    {
                        this.log.LogError("Source topic {} is missing/unknown during rebalance, please make sure all source topics " +
                                      "have been pre-created before starting the Streams application. Returning error {}",
                                      topic, AssignorError.INCOMPLETE_SOURCE_TOPIC_METADATA.Name);
                        throw new TaskAssignmentException("Missing source topic during assignment.");
                    }
                }
                foreach (InternalTopicConfig topic in topicsInfo.repartitionSourceTopics.Values)
                {
                    repartitionTopicMetadata.Put(topic.Name, topic);
                }
            }

            return repartitionTopicMetadata;
        }

        /**
         * Computes and assembles all repartition topic metadata then creates the topics if necessary.
         *
         * @return map from repartition topic to its partition info
         */
        private Dictionary<TopicPartition, PartitionInfo> prepareRepartitionTopics(
            Dictionary<int, TopicsInfo> topicGroups,
            Cluster metadata)
        {
            Dictionary<string, InternalTopicConfig> repartitionTopicMetadata = this.computeRepartitionTopicMetadata(topicGroups, metadata);

            this.setRepartitionTopicMetadataNumberOfPartitions(repartitionTopicMetadata, topicGroups, metadata);

            // ensure the co-partitioning topics within the group have the same number of partitions,
            // and enforce the number of partitions for those repartition topics to be the same if they
            // are co-partitioned as well.
            ensureCopartitioning(this.taskManager.Builder().CopartitionGroups(), repartitionTopicMetadata, metadata);

            // make sure the repartition source topics exist with the right number of partitions,
            // create these topics if necessary
            this.internalTopicManager.MakeReady(repartitionTopicMetadata);

            // augment the metadata with the newly computed number of partitions for all the
            // repartition source topics
            Dictionary<TopicPartition, PartitionInfo> allRepartitionTopicPartitions = new Dictionary<TopicPartition, PartitionInfo>();
            foreach (var entry in repartitionTopicMetadata)
            {
                string topic = entry.Key;
                int? numPartitions = entry.Value.numberOfPartitions;// ?? -1;

                for (int partition = 0; partition < numPartitions; partition++)
                {
                    allRepartitionTopicPartitions.Put(
                        new TopicPartition(topic, partition),
                        new PartitionInfo(topic, partition, null, Array.Empty<Node>(), Array.Empty<Node>())
                    );
                }
            }

            return allRepartitionTopicPartitions;
        }

        /**
         * Computes the number of partitions and sets it for each repartition topic in repartitionTopicMetadata
         */
        private void setRepartitionTopicMetadataNumberOfPartitions(
            Dictionary<string, InternalTopicConfig> repartitionTopicMetadata,
            Dictionary<int, TopicsInfo> topicGroups,
            Cluster metadata)
        {
            bool numPartitionsNeeded;
            do
            {
                numPartitionsNeeded = false;

                foreach (TopicsInfo topicsInfo in topicGroups.Values)
                {
                    foreach (string topicName in topicsInfo.repartitionSourceTopics.Keys)
                    {
                        int? maybeNumPartitions = repartitionTopicMetadata[topicName]
                            .numberOfPartitions;

                        int? numPartitions = null;

                        if (!maybeNumPartitions.HasValue)
                        {
                            // try set the number of partitions for this repartition topic if it is not set yet
                            foreach (TopicsInfo otherTopicsInfo in topicGroups.Values)
                            {
                                HashSet<string> otherSinkTopics = otherTopicsInfo.sinkTopics;

                                if (otherSinkTopics.Contains(topicName))
                                {
                                    // if this topic is one of the sink topics of this topology,
                                    // use the maximum of all its source topic partitions as the number of partitions
                                    foreach (string sourceTopicName in otherTopicsInfo.sourceTopics)
                                    {
                                        int? numPartitionsCandidate = null;
                                        // It is possible the sourceTopic is another internal topic, i.e,
                                        // map().join().join(map())
                                        if (repartitionTopicMetadata.ContainsKey(sourceTopicName))
                                        {
                                            if (repartitionTopicMetadata[sourceTopicName].numberOfPartitions.HasValue)
                                            {
                                                numPartitionsCandidate =
                                                    repartitionTopicMetadata[sourceTopicName].numberOfPartitions;
                                            }
                                        }
                                        else
                                        {
                                            int? count = metadata.partitionCountForTopic(sourceTopicName);
                                            if (count == null)
                                            {
                                                throw new InvalidOperationException(
                                                    "No partition count found for source topic "
                                                        + sourceTopicName
                                                        + ", but it should have been."
                                                );
                                            }
                                            numPartitionsCandidate = count;
                                        }

                                        if (numPartitionsCandidate != null)
                                        {
                                            if (numPartitions == null || numPartitionsCandidate > numPartitions)
                                            {
                                                numPartitions = numPartitionsCandidate;
                                            }
                                        }
                                    }
                                }
                            }

                            // if we still have not found the right number of partitions,
                            // another iteration is needed
                            if (numPartitions == null)
                            {
                                numPartitionsNeeded = true;
                            }
                            else
                            {
                                repartitionTopicMetadata[topicName].SetNumberOfPartitions(numPartitions);
                            }
                        }
                    }
                }
            } while (numPartitionsNeeded);
        }

        /**
         * Populates the taskForPartition and tasksForTopicGroup maps, and checks that partitions are assigned to exactly
         * one task.
         *
         * @param taskForPartition a map from partition to the corresponding task. Populated here.
         * @param tasksForTopicGroup a map from the topicGroupId to the set of corresponding tasks. Populated here.
         * @param allSourceTopics a set of all source topics in the topology
         * @param partitionsForTask a map from task to the set of input partitions
         * @param fullMetadata the cluster metadata
         */
        private void populateTasksForMaps(
            Dictionary<TopicPartition, TaskId> taskForPartition,
            Dictionary<int, HashSet<TaskId>> tasksForTopicGroup,
            HashSet<string> allSourceTopics,
            Dictionary<TaskId, HashSet<TopicPartition>> partitionsForTask,
            Cluster fullMetadata)
        {
            // check if all partitions are assigned, and there are no duplicates of partitions in multiple tasks
            HashSet<TopicPartition> allAssignedPartitions = new HashSet<TopicPartition>();
            foreach (var entry in partitionsForTask)
            {
                TaskId id = entry.Key;
                HashSet<TopicPartition> partitions = entry.Value;

                foreach (TopicPartition partition in partitions)
                {
                    taskForPartition.Put(partition, id);
                    if (allAssignedPartitions.Contains(partition))
                    {
                        this.log.LogWarning("Partition {} is assigned to more than one tasks: {}", partition, partitionsForTask);
                    }
                }
                allAssignedPartitions.AddRange(partitions);

                if (!tasksForTopicGroup.ContainsKey(id.TopicGroupId))
                {
                    tasksForTopicGroup.Add(id.TopicGroupId, new HashSet<TaskId>());
                }

                tasksForTopicGroup[id.TopicGroupId].Add(id);
            }

            this.checkAllPartitions(allSourceTopics, partitionsForTask, allAssignedPartitions, fullMetadata);
        }

        // Logs a warning if any partitions are not assigned to a task, or a task has no assigned partitions
        private void checkAllPartitions(HashSet<string> allSourceTopics,
                                        Dictionary<TaskId, HashSet<TopicPartition>> partitionsForTask,
                                        HashSet<TopicPartition> allAssignedPartitions,
                                        Cluster fullMetadata)
        {
            foreach (string topic in allSourceTopics)
            {
                List<PartitionInfo> partitionInfoList = fullMetadata.partitionsForTopic(topic);
                if (partitionInfoList.IsEmpty())
                {
                    this.log.LogWarning("No partitions found for topic {}", topic);
                }
                else
                {
                    foreach (PartitionInfo partitionInfo in partitionInfoList)
                    {
                        TopicPartition partition = new TopicPartition(partitionInfo.Topic,
                            partitionInfo.Partition);

                        if (!allAssignedPartitions.Contains(partition))
                        {
                            this.log.LogWarning("Partition {} is not assigned to any tasks: {}"
                                         + " Possible causes of a partition not getting assigned"
                                         + " is that another topic defined in the topology has not been"
                                         + " created when starting your streams application,"
                                         + " resulting in no tasks created for this topology at all.", partition,
                                partitionsForTask);
                        }
                    }
                }
            }
        }

        /**
         * Resolve changelog topic metadata and create them if necessary. Fills in the changelogsByStatefulTask map and
         * the optimizedSourceChangelogs set and returns the set of changelogs which were newly created.
         */
        private HashSet<string> prepareChangelogTopics(
            Dictionary<int, TopicsInfo> topicGroups,
            Dictionary<int, HashSet<TaskId>> tasksForTopicGroup,
            Dictionary<TaskId, HashSet<TopicPartition>> changelogsByStatefulTask,
            HashSet<string> optimizedSourceChangelogs)
        {
            // add tasks to state change log topic subscribers
            Dictionary<string, InternalTopicConfig> changelogTopicMetadata = new Dictionary<string, InternalTopicConfig>();
            foreach (var entry in topicGroups)
            {
                int topicGroupId = entry.Key;
                TopicsInfo topicsInfo = entry.Value;

                HashSet<TaskId> topicGroupTasks = tasksForTopicGroup[topicGroupId];
                if (topicGroupTasks == null)
                {
                    this.log.LogDebug("No tasks found for topic group {}", topicGroupId);
                    continue;
                }
                else if (topicsInfo.stateChangelogTopics.IsEmpty())
                {
                    continue;
                }

                foreach (TaskId task in topicGroupTasks)
                {
                    changelogsByStatefulTask.Put(
                        task,
                        topicsInfo.stateChangelogTopics
                            .Keys
                            .Select(topic => new TopicPartition(topic, task.Partition))
                            .ToHashSet());
                }

                foreach (InternalTopicConfig topicConfig in topicsInfo.nonSourceChangelogTopics())
                {
                    // the expected number of partitions is the max value of TaskId.Partition + 1
                    int numPartitions = StreamsAssignmentProtocolVersions.UNKNOWN;
                    foreach (TaskId task in topicGroupTasks)
                    {
                        if (numPartitions < task.Partition + 1)
                        {
                            numPartitions = task.Partition + 1;
                        }
                    }
                    topicConfig.SetNumberOfPartitions(numPartitions);
                    changelogTopicMetadata.Put(topicConfig.Name, topicConfig);
                }

                optimizedSourceChangelogs.AddRange(topicsInfo.sourceTopicChangelogs());
            }

            HashSet<string> newlyCreatedTopics = this.internalTopicManager.MakeReady(changelogTopicMetadata);
            this.log.LogDebug("Created state changelog topics {} from the parsed topology.", changelogTopicMetadata.Values);
            return newlyCreatedTopics;
        }

        /**
         * Assigns a set of tasks to each client (Streams instance) using the configured task assignor, and also
         * populate the stateful tasks that have been assigned to the clients
         * @return true if a probing rebalance should be triggered
         */
        private bool assignTasksToClients(
            Cluster fullMetadata,
            HashSet<string> allSourceTopics,
            Dictionary<int, TopicsInfo> topicGroups,
            Dictionary<Guid, ClientMetadata> clientMetadataMap,
            Dictionary<TaskId, HashSet<TopicPartition>> partitionsForTask,
            HashSet<TaskId> statefulTasks)
        {
            if (!statefulTasks.IsEmpty())
            {
                throw new InvalidProgramException("The stateful tasks should not be populated before assigning tasks to clients");
            }

            Dictionary<TopicPartition, TaskId> taskForPartition = new Dictionary<TopicPartition, TaskId>();
            Dictionary<int, HashSet<TaskId>> tasksForTopicGroup = new Dictionary<int, HashSet<TaskId>>();
            this.populateTasksForMaps(taskForPartition, tasksForTopicGroup, allSourceTopics, partitionsForTask, fullMetadata);

            Dictionary<TaskId, HashSet<TopicPartition>> changelogsByStatefulTask = new Dictionary<TaskId, HashSet<TopicPartition>>();
            HashSet<string> optimizedSourceChangelogs = new HashSet<string>();
            HashSet<string> newlyCreatedChangelogs =
                this.prepareChangelogTopics(topicGroups, tasksForTopicGroup, changelogsByStatefulTask, optimizedSourceChangelogs);

            Dictionary<Guid, ClientState> clientStates = new Dictionary<Guid, ClientState>();
            bool lagComputationSuccessful =
                this.populateClientStatesMap(clientStates,
                    clientMetadataMap,
                    taskForPartition,
                    changelogsByStatefulTask,
                    newlyCreatedChangelogs,
                    optimizedSourceChangelogs);

            HashSet<TaskId> allTasks = partitionsForTask.Keys.ToHashSet();
            statefulTasks.AddRange(changelogsByStatefulTask.Keys);

            this.log.LogDebug("Assigning tasks {} to clients {} with number of replicas {}",
                allTasks, clientStates, this.numStandbyReplicas());

            var taskAssignor = this.createTaskAssignor(lagComputationSuccessful);

            bool probingRebalanceNeeded = taskAssignor.Assign(
                clientStates,
                allTasks,
                statefulTasks,
                this.assignmentConfigs);

            this.log.LogInformation("Assigned tasks {} including stateful {} to clients as: \n{}.",
                    allTasks, statefulTasks, clientStates
                    .Select(entry => entry.Key + "=" + entry.Value.currentAssignment()));

            return probingRebalanceNeeded;
        }

        private ITaskAssignor createTaskAssignor(bool lagComputationSuccessful)
        {
            ITaskAssignor taskAssignor = this.taskAssignorSupplier.Get();
            if (taskAssignor is StickyTaskAssignor)
            {
                // special case: to preserve pre-existing behavior, we invoke the StickyTaskAssignor
                // whether or not lag computation failed.
                return taskAssignor;
            }
            else if (lagComputationSuccessful)
            {
                return taskAssignor;
            }
            else
            {
                this.log.LogInformation("Failed to fetch end offsets for changelogs, will return previous assignment to clients and "
                             + "trigger another rebalance to retry.");
                return new FallbackPriorTaskAssignor();
            }
        }

        /**
         * Builds a map from client to state, and readies each ClientState for assignment by adding any missing prev tasks
         * and computing the per-task overall lag based on the fetched end offsets for each changelog.
         *
         * @param clientStates a map from each client to its state, including offset lags. Populated by this method.
         * @param clientMetadataMap a map from each client to its full metadata
         * @param taskForPartition map from topic partition to its corresponding task
         * @param changelogsByStatefulTask map from each stateful task to its set of changelog topic partitions
         *
         * @return whether we were able to successfully fetch the changelog end offsets and compute each client's lag
         */
        private bool populateClientStatesMap(
            Dictionary<Guid, ClientState> clientStates,
            Dictionary<Guid, ClientMetadata> clientMetadataMap,
            Dictionary<TopicPartition, TaskId> taskForPartition,
            Dictionary<TaskId, HashSet<TopicPartition>> changelogsByStatefulTask,
            HashSet<string> newlyCreatedChangelogs,
            HashSet<string> optimizedSourceChangelogs)
        {
            bool fetchEndOffsetsSuccessful;
            Dictionary<TaskId, long> allTaskEndOffsetSums;
            try
            {
                List<TopicPartition> allChangelogPartitions =
                    changelogsByStatefulTask.Values
                    .SelectMany(t => t.ToList())
                    .ToList();

                HashSet<TopicPartition> preexistingChangelogPartitions = new HashSet<TopicPartition>();
                HashSet<TopicPartition> preexistingSourceChangelogPartitions = new HashSet<TopicPartition>();
                HashSet<TopicPartition> newlyCreatedChangelogPartitions = new HashSet<TopicPartition>();

                foreach (TopicPartition changelog in allChangelogPartitions)
                {
                    if (newlyCreatedChangelogs.Contains(changelog.Topic))
                    {
                        newlyCreatedChangelogPartitions.Add(changelog);
                    }
                    else if (optimizedSourceChangelogs.Contains(changelog.Topic))
                    {
                        preexistingSourceChangelogPartitions.Add(changelog);
                    }
                    else
                    {
                        preexistingChangelogPartitions.Add(changelog);
                    }
                }

                // Make the listOffsets request first so it can  fetch the offsets for non-source changelogs
                // asynchronously while we use the blocking Consumer#committed call to fetch source-changelog offsets
                Dictionary<TopicPartition, TopicPartitionOffset> endOffsetsFuture =
                     ClientUtils.fetchEndOffsetsFuture(preexistingChangelogPartitions, this.adminClient);

                Dictionary<TopicPartition, long> sourceChangelogEndOffsets =
                    fetchCommittedOffsets(preexistingSourceChangelogPartitions, this.taskManager.mainConsumer);

                Dictionary<TopicPartition, TopicPartitionOffset> endOffsets = ClientUtils.getEndOffsets(endOffsetsFuture);

                allTaskEndOffsetSums = this.computeEndOffsetSumsByTask(
                    changelogsByStatefulTask,
                    endOffsets,
                    sourceChangelogEndOffsets,
                    newlyCreatedChangelogPartitions);
                fetchEndOffsetsSuccessful = true;
            }
            catch (StreamsException e)
            {
                allTaskEndOffsetSums = changelogsByStatefulTask.Keys
                    .ToDictionary(t => t, t => SubscriptionInfo.UNKNOWN_OFFSET_SUM);

                fetchEndOffsetsSuccessful = false;
            }

            foreach (var entry in clientMetadataMap)
            {
                Guid uuid = entry.Key;
                ClientState state = entry.Value.state;
                state.initializePrevTasks(taskForPartition);

                state.computeTaskLags(uuid, allTaskEndOffsetSums);
                clientStates.Put(uuid, state);
            }
            return fetchEndOffsetsSuccessful;
        }

        /**
         * @param changelogsByStatefulTask map from stateful task to its set of changelog topic partitions
         * @param endOffsets the listOffsets result from the adminClient
         * @param sourceChangelogEndOffsets the end (committed) offsets of optimized source changelogs
         * @param newlyCreatedChangelogPartitions any changelogs that were just created duringthis assignment
         *
         * @return Map from stateful task to its total end offset summed across all changelog partitions
         */
        private Dictionary<TaskId, long> computeEndOffsetSumsByTask(
            Dictionary<TaskId, HashSet<TopicPartition>> changelogsByStatefulTask,
            Dictionary<TopicPartition, TopicPartitionOffset> endOffsets,
            Dictionary<TopicPartition, long> sourceChangelogEndOffsets,
            IEnumerable<TopicPartition> newlyCreatedChangelogPartitions)
        {
            Dictionary<TaskId, long> taskEndOffsetSums = new Dictionary<TaskId, long>();
            foreach (var taskEntry in changelogsByStatefulTask)
            {
                TaskId task = taskEntry.Key;
                HashSet<TopicPartition> changelogs = taskEntry.Value;

                taskEndOffsetSums.Put(task, 0L);
                foreach (TopicPartition changelog in changelogs)
                {
                    long changelogEndOffset;
                    if (newlyCreatedChangelogPartitions.Contains(changelog))
                    {
                        changelogEndOffset = 0L;
                    }
                    else if (sourceChangelogEndOffsets.ContainsKey(changelog))
                    {
                        changelogEndOffset = sourceChangelogEndOffsets[changelog];
                    }
                    else if (endOffsets.ContainsKey(changelog))
                    {
                        changelogEndOffset = endOffsets[changelog].Offset;
                    }
                    else
                    {
                        this.log.LogDebug("Fetched offsets did not contain the changelog {} of task {}", changelog, task);
                        throw new InvalidOperationException("Could not get end offset for " + changelog);
                    }
                    long newEndOffsetSum = taskEndOffsetSums[task] + changelogEndOffset;
                    if (newEndOffsetSum < 0)
                    {
                        taskEndOffsetSums.Put(task, long.MaxValue);
                        break;
                    }
                    else
                    {
                        taskEndOffsetSums.Put(task, newEndOffsetSum);
                    }
                }
            }
            return taskEndOffsetSums;
        }

        /**
         * Populates the global partitionsByHost and standbyPartitionsByHost maps that are sent to each member
         *
         * @param partitionsByHost a map from host to the set of partitions hosted there. Populated here.
         * @param standbyPartitionsByHost a map from host to the set of standby partitions hosted there. Populated here.
         * @param partitionsForTask a map from task to its set of assigned partitions
         * @param clientMetadataMap a map from client to its metadata and state
         */
        private void populatePartitionsByHostMaps(
            Dictionary<HostInfo, HashSet<TopicPartition>> partitionsByHost,
            Dictionary<HostInfo, HashSet<TopicPartition>> standbyPartitionsByHost,
            Dictionary<TaskId, HashSet<TopicPartition>> partitionsForTask,
            Dictionary<Guid, ClientMetadata> clientMetadataMap)
        {
            foreach (var entry in clientMetadataMap)
            {
                HostInfo hostInfo = entry.Value.hostInfo;

                // if application server is configured, also include host state map
                if (hostInfo != null)
                {
                    HashSet<TopicPartition> topicPartitions = new HashSet<TopicPartition>();
                    HashSet<TopicPartition> standbyPartitions = new HashSet<TopicPartition>();
                    ClientState state = entry.Value.state;

                    foreach (TaskId id in state.ActiveTasks())
                    {
                        topicPartitions.AddRange(partitionsForTask[id]);
                    }

                    foreach (TaskId id in state.standbyTasks())
                    {
                        standbyPartitions.AddRange(partitionsForTask[id]);
                    }

                    partitionsByHost.Put(hostInfo, topicPartitions);
                    standbyPartitionsByHost.Put(hostInfo, standbyPartitions);
                }
            }
        }

        /**
         * Computes the assignment of tasks to threads within each client and assembles the assignment to send out.
         *
         * @return the assignment for each StreamThread consumer
         */
        private Dictionary<string, Assignment> computeNewAssignment(
            HashSet<TaskId> statefulTasks,
            Dictionary<Guid, ClientMetadata> clientsMetadata,
            Dictionary<TaskId, HashSet<TopicPartition>> partitionsForTask,
            Dictionary<HostInfo, HashSet<TopicPartition>> partitionsByHostState,
            Dictionary<HostInfo, HashSet<TopicPartition>> standbyPartitionsByHost,
            HashSet<TopicPartition> allOwnedPartitions,
            int minUserMetadataVersion,
            int minSupportedMetadataVersion,
            bool versionProbing,
            bool shouldTriggerProbingRebalance)
        {
            bool rebalanceRequired = shouldTriggerProbingRebalance || versionProbing;
            Dictionary<string, Assignment> assignment = new Dictionary<string, Assignment>();

            // within the client, distribute tasks to its owned consumers
            foreach (var clientEntry in clientsMetadata)
            {
                Guid clientId = clientEntry.Key;
                ClientMetadata clientMetadata = clientEntry.Value;
                ClientState state = clientMetadata.state;
                SortedSet<string> consumers = clientMetadata.consumers;

                Dictionary<string, List<TaskId>> activeTaskAssignment = assignTasksToThreads(
                    state.statefulActiveTasks(),
                    state.statelessActiveTasks(),
                    consumers,
                    state
                );

                Dictionary<string, List<TaskId>> standbyTaskAssignment = assignTasksToThreads(
                    state.standbyTasks(),
                    new List<TaskId>(),
                    consumers,
                    state);

                // Arbitrarily choose the leader's client to be responsible for triggering the probing rebalance,
                // note once we pick the first consumer within the process to trigger probing rebalance, other consumer
                // would not set to trigger any more.
                bool encodeNextProbingRebalanceTime = shouldTriggerProbingRebalance && clientId.Equals(this.taskManager.processId());

                bool tasksRevoked = this.addClientAssignments(
                    statefulTasks,
                    assignment,
                    clientMetadata,
                    partitionsForTask,
                    partitionsByHostState,
                    standbyPartitionsByHost,
                    allOwnedPartitions,
                    activeTaskAssignment,
                    standbyTaskAssignment,
                    minUserMetadataVersion,
                    minSupportedMetadataVersion,
                    encodeNextProbingRebalanceTime);

                if (tasksRevoked || encodeNextProbingRebalanceTime)
                {
                    rebalanceRequired = true;
                    this.log.LogDebug("Requested client {} to schedule a followup rebalance", clientId);
                }

                this.log.LogInformation("Client {} per-consumer assignment:\n" +
                    "\tprev owned active {}\n" +
                    "\tprev owned standby {}\n" +
                    "\tassigned active {}\n" +
                    "\trevoking active {}" +
                    "\tassigned standby {}\n",
                    clientId,
                    clientMetadata.state.prevOwnedActiveTasksByConsumer(),
                    clientMetadata.state.prevOwnedStandbyByConsumer(),
                    clientMetadata.state.assignedActiveTasksByConsumer(),
                    clientMetadata.state.revokingActiveTasksByConsumer(),
                    clientMetadata.state.assignedStandbyTasksByConsumer());
            }

            if (rebalanceRequired)
            {
                this.assignmentListener.OnAssignmentComplete(false);
                this.log.LogInformation("Finished unstable assignment of tasks, a followup rebalance will be scheduled.");
            }
            else
            {
                this.assignmentListener.OnAssignmentComplete(true);
                this.log.LogInformation("Finished stable assignment of tasks, no followup rebalances required.");
            }

            return assignment;
        }

        /**
         * Adds the encoded assignment for each StreamThread consumer in the client to the overall assignment map
         * @return true if a followup rebalance will be required due to revoked tasks
         */
        private bool addClientAssignments(
            HashSet<TaskId> statefulTasks,
            Dictionary<string, Assignment> assignment,
            ClientMetadata clientMetadata,
            Dictionary<TaskId, HashSet<TopicPartition>> partitionsForTask,
            Dictionary<HostInfo, HashSet<TopicPartition>> partitionsByHostState,
            Dictionary<HostInfo, HashSet<TopicPartition>> standbyPartitionsByHost,
            HashSet<TopicPartition> allOwnedPartitions,
            Dictionary<string, List<TaskId>> activeTaskAssignments,
            Dictionary<string, List<TaskId>> standbyTaskAssignments,
            int minUserMetadataVersion,
            int minSupportedMetadataVersion,
            bool probingRebalanceNeeded)
        {
            bool followupRebalanceRequiredForRevokedTasks = false;

            // We only want to encode a scheduled probing rebalance for a single member in this client
            bool shouldEncodeProbingRebalance = probingRebalanceNeeded;

            // Loop through the consumers and build their assignment
            foreach (string consumer in clientMetadata.consumers)
            {
                List<TaskId> activeTasksForConsumer = activeTaskAssignments[consumer];

                // These will be filled in by populateActiveTaskAndPartitionsLists below
                List<TopicPartition> activePartitionsList = new List<TopicPartition>();
                List<TaskId> assignedActiveList = new List<TaskId>();

                HashSet<TaskId> activeTasksRemovedPendingRevokation = this.populateActiveTaskAndPartitionsLists(
                    activePartitionsList,
                    assignedActiveList,
                    consumer,
                    clientMetadata.state,
                    activeTasksForConsumer,
                    partitionsForTask,
                    allOwnedPartitions
                );

                Dictionary<TaskId, HashSet<TopicPartition>> standbyTaskMap = buildStandbyTaskMap(
                        consumer,
                        standbyTaskAssignments[consumer],
                        activeTasksRemovedPendingRevokation,
                        statefulTasks,
                        partitionsForTask,
                        clientMetadata.state
                    );

                AssignmentInfo info = new AssignmentInfo(
                    minUserMetadataVersion,
                    minSupportedMetadataVersion,
                    assignedActiveList,
                    standbyTaskMap,
                    partitionsByHostState,
                    standbyPartitionsByHost,
                    AssignorError.NONE.code()
                );

                if (!activeTasksRemovedPendingRevokation.IsEmpty())
                {
                    // TODO: once KAFKA-10078 is resolved we can leave it to the client to trigger this rebalance
                    this.log.LogInformation("Requesting {} followup rebalance be scheduled immediately due to tasks changing ownership.", consumer);
                    info.setNextRebalanceTime(0L);
                    followupRebalanceRequiredForRevokedTasks = true;
                    // Don't bother to schedule a probing rebalance if an immediate one is already scheduled
                    shouldEncodeProbingRebalance = false;
                }
                else if (shouldEncodeProbingRebalance)
                {
                    long nextRebalanceTimeMs = this.time.NowAsEpochMilliseconds + this.probingRebalanceIntervalMs();

                    this.log.LogInformation("Requesting {} followup rebalance be scheduled for {} ms to probe for caught-up replica tasks.",
                            consumer, nextRebalanceTimeMs);
                    info.setNextRebalanceTime(nextRebalanceTimeMs);
                    shouldEncodeProbingRebalance = false;
                }

                assignment.Put(
                    consumer,
                    new Assignment(
                        activePartitionsList,
                        info.Encode()
                    )
                );
            }
            return followupRebalanceRequiredForRevokedTasks;
        }

        /**
         * Populates the lists of active tasks and active task partitions for the consumer with a 1:1 mapping between them
         * such that the nth task corresponds to the nth partition in the list. This means tasks with multiple partitions
         * will be repeated in the list.
         */
        private HashSet<TaskId> populateActiveTaskAndPartitionsLists(
            List<TopicPartition> activePartitionsList,
            List<TaskId> assignedActiveList,
            string consumer,
            ClientState clientState,
            List<TaskId> activeTasksForConsumer,
            Dictionary<TaskId, HashSet<TopicPartition>> partitionsForTask,
            HashSet<TopicPartition> allOwnedPartitions)
        {
            List<AssignedPartition> assignedPartitions = new List<AssignedPartition>();
            HashSet<TaskId> removedActiveTasks = new HashSet<TaskId>();

            foreach (TaskId taskId in activeTasksForConsumer)
            {
                // Populate the consumer for assigned tasks without considering revocation,
                // this is for debugging purposes only
                clientState.assignActiveToConsumer(taskId, consumer);

                List<AssignedPartition> assignedPartitionsForTask = new List<AssignedPartition>();
                foreach (TopicPartition partition in partitionsForTask[taskId])
                {
                    string oldOwner = clientState.previousOwnerForPartition(partition);
                    bool newPartitionForConsumer = oldOwner == null || !oldOwner.Equals(consumer);

                    // If the partition is new to this consumer but is still owned by another, remove from the assignment
                    // until it has been revoked and can safely be reassigned according to the COOPERATIVE protocol
                    if (newPartitionForConsumer && allOwnedPartitions.Contains(partition))
                    {
                        this.log.LogInformation(
                            "Removing task {} from {} active assignment until it is safely revoked in followup rebalance",
                            taskId,
                            consumer
                        );
                        removedActiveTasks.Add(taskId);

                        clientState.revokeActiveFromConsumer(taskId, consumer);

                        // Clear the assigned partitions list for this task if any partition can not safely be assigned,
                        // so as not to encode a partial task
                        assignedPartitionsForTask.Clear();

                        // This has no effect on the assignment, as we'll never consult the ClientState again, but
                        // it does perform a useful assertion that the task was actually assigned.
                        clientState.unassignActive(taskId);
                        break;
                    }
                    else
                    {
                        assignedPartitionsForTask.Add(new AssignedPartition(taskId, partition));
                    }
                }
                // assignedPartitionsForTask will either contain all partitions for the task or be empty, so just add all
                assignedPartitions.AddRange(assignedPartitionsForTask);
            }

            // Add one copy of a task for each corresponding partition, so the receiver can determine the task <=> tp mapping
            Collections.sort(assignedPartitions);
            foreach (AssignedPartition partition in assignedPartitions)
            {
                assignedActiveList.Add(partition.taskId);
                activePartitionsList.Add(partition.Partition);
            }
            return removedActiveTasks;
        }

        /**
         * @return map from task id to its assigned partitions for all standby tasks
         */
        private Dictionary<TaskId, HashSet<TopicPartition>> buildStandbyTaskMap(
            string consumer,
            IEnumerable<TaskId> standbyTasks,
            IEnumerable<TaskId> revokedTasks,
            HashSet<TaskId> allStatefulTasks,
            Dictionary<TaskId, HashSet<TopicPartition>> partitionsForTask,
            ClientState clientState)
        {
            Dictionary<TaskId, HashSet<TopicPartition>> standbyTaskMap = new Dictionary<TaskId, HashSet<TopicPartition>>();

            foreach (TaskId task in standbyTasks)
            {
                clientState.assignStandbyToConsumer(task, consumer);
                standbyTaskMap.Put(task, partitionsForTask[task]);
            }

            foreach (TaskId task in revokedTasks)
            {
                if (allStatefulTasks.Contains(task))
                {
                    this.log.LogInformation("Adding removed stateful active task {} as a standby for {} before it is revoked in followup rebalance",
                            task, consumer);

                    // This has no effect on the assignment, as we'll never consult the ClientState again, but
                    // it does perform a useful assertion that the it's legal to assign this task as a standby to this instance
                    clientState.assignStandbyToConsumer(task, consumer);
                    clientState.assignStandby(task);

                    standbyTaskMap.Put(task, partitionsForTask[task]);
                }
            }

            return standbyTaskMap;
        }

        /**
         * Generate an assignment that tries to preserve thread-level stickiness of stateful tasks without violating
         * balance. The stateful and total task load are both balanced across threads. Tasks without previous owners
         * will be interleaved by group id to spread subtopologies across threads and further balance the workload.
         */
        static Dictionary<string, List<TaskId>> assignTasksToThreads(
            IEnumerable<TaskId> statefulTasksToAssign,
            IEnumerable<TaskId> statelessTasksToAssign,
            SortedSet<string> consumers,
            ClientState state)
        {
            Dictionary<string, List<TaskId>> assignment = new Dictionary<string, List<TaskId>>();
            foreach (string consumer in consumers)
            {
                assignment.Put(consumer, new List<TaskId>());
            }

            List<TaskId> unassignedStatelessTasks = new List<TaskId>(statelessTasksToAssign);
            Collections.sort(unassignedStatelessTasks);

            IEnumerator<TaskId> unassignedStatelessTasksIter = unassignedStatelessTasks.GetEnumerator();

            int minStatefulTasksPerThread = (int)Math.Floor(((double)statefulTasksToAssign.Count()) / consumers.Count);

            SimplePriorityQueue<TaskId> unassignedStatefulTasks = new SimplePriorityQueue<TaskId>();
            foreach (var task in unassignedStatefulTasks)
            {
                unassignedStatefulTasks.Enqueue(task, task.TopicGroupId);
            }

            Queue<string> consumersToFill = new Queue<string>();
            // keep track of tasks that we have to skip during the first pass in case we can reassign them later
            // using tree-map to make sure the iteration ordering over keys are preserved
            Dictionary<TaskId, string> unassignedTaskToPreviousOwner = new Dictionary<TaskId, string>();

            if (!unassignedStatefulTasks.IsEmpty())
            {
                // First assign stateful tasks to previous owner, up to the min expected tasks/thread
                foreach (string consumer in consumers)
                {
                    List<TaskId> threadAssignment = assignment[consumer];

                    foreach (TaskId task in getPreviousTasksByLag(state, consumer))
                    {
                        if (unassignedStatefulTasks.Contains(task))
                        {
                            if (threadAssignment.Count < minStatefulTasksPerThread)
                            {
                                threadAssignment.Add(task);
                                unassignedStatefulTasks.Remove(task);
                            }
                            else
                            {
                                unassignedTaskToPreviousOwner.Put(task, consumer);
                            }
                        }
                    }

                    if (threadAssignment.Count < minStatefulTasksPerThread)
                    {
                        consumersToFill.Enqueue(consumer);
                    }
                }

                // Next interleave remaining unassigned tasks amongst unfilled consumers
                while (!consumersToFill.IsEmpty())
                {
                    TaskId task = unassignedStatefulTasks.Dequeue();
                    if (task != null)
                    {
                        string consumer = consumersToFill.Peek();
                        List<TaskId> threadAssignment = assignment[consumer];
                        threadAssignment.Add(task);
                        if (threadAssignment.Count < minStatefulTasksPerThread)
                        {
                            consumersToFill.Enqueue(consumer);
                        }
                    }
                    else
                    {
                        throw new InvalidOperationException("Ran out of unassigned stateful tasks but some members were not at capacity");
                    }
                }

                // At this point all consumers are at the min capacity, so there may be up to N - 1 unassigned
                // stateful tasks still remaining that should now be distributed over the consumers
                if (!unassignedStatefulTasks.IsEmpty())
                {
                    foreach (var consumer in consumers)
                    {
                        consumersToFill.Enqueue(consumer);
                    }

                    // Go over the tasks we skipped earlier and assign them to their previous owner when possible
                    foreach (var taskEntry in unassignedTaskToPreviousOwner)
                    {
                        TaskId task = taskEntry.Key;
                        string consumer = taskEntry.Value;
                        if (consumersToFill.Contains(consumer) && unassignedStatefulTasks.Contains(task))
                        {
                            assignment[consumer].Add(task);
                            unassignedStatefulTasks.Remove(task);
                            // Remove this consumer since we know it is now at minCapacity + 1
                            // consumersToFill.Remove(consumer);
                        }
                    }

                    // Now just distribute the remaining unassigned stateful tasks over the consumers still at min capacity
                    foreach (TaskId task in unassignedStatefulTasks)
                    {
                        string consumer = consumersToFill.Dequeue();
                        List<TaskId> threadAssignment = assignment[consumer];
                        threadAssignment.Add(task);
                    }


                    // There must be at least one consumer still at min capacity while all the others are at min
                    // capacity + 1, so start distributing stateless tasks to get all consumers back to the same count
                    while (unassignedStatelessTasksIter.MoveNext())
                    {
                        string consumer = consumersToFill.Dequeue();
                        if (consumer != null)
                        {
                            TaskId task = unassignedStatelessTasksIter.Current;
                            // unassignedStatelessTasksIter.remove();
                            assignment.GetValueOrDefault(consumer).Add(task);
                        }
                        else
                        {
                            break;
                        }
                    }
                }
            }

            // Now just distribute tasks while circling through all the consumers
            foreach (var consumer in consumers)
            {
                consumersToFill.Enqueue(consumer);
            }

            while (unassignedStatelessTasksIter.MoveNext())
            {
                TaskId task = unassignedStatelessTasksIter.Current;
                string consumer = consumersToFill.Dequeue();
                assignment.GetValueOrDefault(consumer).Add(task);
                consumersToFill.Enqueue(consumer);
            }

            return assignment;
        }

        private static SortedSet<TaskId> getPreviousTasksByLag(ClientState state, string consumer)
        {
            SortedSet<TaskId> prevTasksByLag = new SortedSet<TaskId>(comparingLong(state.lagFor).thenComparing(TaskId.CompareTo));
            prevTasksByLag.AddRange(state.prevOwnedStatefulTasksByConsumer(consumer));
            return prevTasksByLag;
        }

        private void validateMetadataVersions(
            int receivedAssignmentMetadataVersion,
            int latestCommonlySupportedVersion)
        {

            if (receivedAssignmentMetadataVersion > this.usedSubscriptionMetadataVersion)
            {
                this.log.LogError("Leader sent back an assignment with version {} which was greater than our used version {}",
                    receivedAssignmentMetadataVersion, this.usedSubscriptionMetadataVersion);

                throw new TaskAssignmentException(
                    "Sent a version " + this.usedSubscriptionMetadataVersion
                        + " subscription but got an assignment with higher version "
                        + receivedAssignmentMetadataVersion + ".");
            }

            if (latestCommonlySupportedVersion > StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION)
            {
                this.log.LogError("Leader sent back assignment with commonly supported version {} that is greater than our "
                    + "actual latest supported version {}", latestCommonlySupportedVersion, StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION);
                throw new TaskAssignmentException("Can't upgrade to metadata version greater than we support");
            }
        }

        // Returns true if subscription version was changed, indicating version probing and need to rebalance again
        protected bool maybeUpdateSubscriptionVersion(
            int receivedAssignmentMetadataVersion,
            int latestCommonlySupportedVersion)
        {
            if (receivedAssignmentMetadataVersion >= StreamsAssignmentProtocolVersions.EARLIEST_PROBEABLE_VERSION)
            {
                // If the latest commonly supported version is now greater than our used version, this indicates we have just
                // completed the rolling upgrade and can now update our subscription version for the rebalance
                if (latestCommonlySupportedVersion > this.usedSubscriptionMetadataVersion)
                {
                    this.log.LogInformation(
                        "Sent a version {} subscription and group's latest commonly supported version is {} (successful "
                            +
                            "version probing and end of rolling upgrade). Upgrading subscription metadata version to " +
                            "{} for next rebalance.",
                        this.usedSubscriptionMetadataVersion,
                        latestCommonlySupportedVersion,
                        latestCommonlySupportedVersion);

                    this.usedSubscriptionMetadataVersion = latestCommonlySupportedVersion;

                    return true;
                }

                // If we received a lower version than we sent, someone else in the group still hasn't upgraded. We
                // should downgrade our subscription until everyone is on the latest version
                if (receivedAssignmentMetadataVersion < this.usedSubscriptionMetadataVersion)
                {
                    this.log.LogInformation(
                        "Sent a version {} subscription and got version {} assignment back (successful version probing). "
                            +
                            "Downgrade subscription metadata to commonly supported version {} and trigger new rebalance.",
                        this.usedSubscriptionMetadataVersion,
                        receivedAssignmentMetadataVersion,
                        latestCommonlySupportedVersion
                    );
                    this.usedSubscriptionMetadataVersion = latestCommonlySupportedVersion;
                    return true;
                }
            }
            else
            {
                this.log.LogDebug("Received an assignment version {} that is less than the earliest version that allows version " +
                    "probing {}. If this is not during a rolling upgrade from version 2.0 or below, this is an error.",
                    receivedAssignmentMetadataVersion, StreamsAssignmentProtocolVersions.EARLIEST_PROBEABLE_VERSION);
            }

            return false;
        }

        /**
         * @throws TaskAssignmentException if there is no task id for one of the partitions specified
         */

        public void onAssignment(Assignment assignment, IConsumerGroupMetadata metadata)
        {
            List<TopicPartition> partitions = new List<TopicPartition>(assignment.partitions);
            partitions.Sort(PARTITION_COMPARATOR);

            AssignmentInfo info = AssignmentInfo.decode(assignment.userData);
            if (info.errCode != AssignorError.NONE.code())
            {
                // set flag to shutdown streams app
                Interlocked.Exchange(ref this.assignmentErrorCode, info.errCode);
                return;
            }
            /*
             * latestCommonlySupportedVersion belongs to [usedSubscriptionMetadataVersion, LATEST_SUPPORTED_VERSION]
             * receivedAssignmentMetadataVersion belongs to [EARLIEST_PROBEABLE_VERSION, usedSubscriptionMetadataVersion]
             *
             * usedSubscriptionMetadataVersion will be downgraded to receivedAssignmentMetadataVersion during a rolling
             * bounce upgrade with version probing.
             *
             * usedSubscriptionMetadataVersion will be upgraded to latestCommonlySupportedVersion when all members have
             * been bounced and it is safe to use the latest version.
             */
            int receivedAssignmentMetadataVersion = info.version();
            int latestCommonlySupportedVersion = info.commonlySupportedVersion;

            this.validateMetadataVersions(receivedAssignmentMetadataVersion, latestCommonlySupportedVersion);

            // version 1 field
            Dictionary<TaskId, HashSet<TopicPartition>> activeTasks;
            // version 2 fields
            Dictionary<TopicPartition, PartitionInfo> topicToPartitionInfo;
            Dictionary<HostInfo, HashSet<TopicPartition>> partitionsByHost;
            Dictionary<HostInfo, HashSet<TopicPartition>> standbyPartitionsByHost;
            long encodedNextScheduledRebalanceMs;

            switch (receivedAssignmentMetadataVersion)
            {
                case 1:
                    validateActiveTaskEncoding(partitions, info, this.logPrefix);

                    activeTasks = getActiveTasks(partitions, info);
                    partitionsByHost = new Dictionary<HostInfo, HashSet<TopicPartition>>();
                    standbyPartitionsByHost = new Dictionary<HostInfo, HashSet<TopicPartition>>();
                    topicToPartitionInfo = new Dictionary<TopicPartition, PartitionInfo>();
                    encodedNextScheduledRebalanceMs = long.MaxValue;
                    break;
                case 2:
                case 3:
                case 4:
                case 5:
                    validateActiveTaskEncoding(partitions, info, this.logPrefix);

                    activeTasks = getActiveTasks(partitions, info);
                    partitionsByHost = info.partitionsByHost;
                    standbyPartitionsByHost = new Dictionary<HostInfo, HashSet<TopicPartition>>();
                    topicToPartitionInfo = getTopicPartitionInfo(partitionsByHost);
                    encodedNextScheduledRebalanceMs = long.MaxValue;
                    break;
                case 6:
                    validateActiveTaskEncoding(partitions, info, this.logPrefix);

                    activeTasks = getActiveTasks(partitions, info);
                    partitionsByHost = info.partitionsByHost;
                    standbyPartitionsByHost = info.standbyPartitionsByHost;
                    topicToPartitionInfo = getTopicPartitionInfo(partitionsByHost);
                    encodedNextScheduledRebalanceMs = long.MaxValue;
                    break;
                case 7:
                    validateActiveTaskEncoding(partitions, info, this.logPrefix);

                    activeTasks = getActiveTasks(partitions, info);
                    partitionsByHost = info.partitionsByHost;
                    standbyPartitionsByHost = info.standbyPartitionByHost;
                    topicToPartitionInfo = getTopicPartitionInfo(partitionsByHost);
                    encodedNextScheduledRebalanceMs = info.nextRebalanceMs;
                    break;
                default:
                    throw new InvalidOperationException(
                        "This code should never be reached."
                            + " Please file a bug report at https://issues.apache.org/jira/projects/KAFKA/"
                    );
            }

            maybeScheduleFollowupRebalance(
                encodedNextScheduledRebalanceMs,
                receivedAssignmentMetadataVersion,
                latestCommonlySupportedVersion,
                partitionsByHost.Keys.ToHashSet());

            Cluster fakeCluster = Cluster.empty().withPartitions(topicToPartitionInfo);
            this.streamsMetadataState.OnChange(partitionsByHost, standbyPartitionsByHost, fakeCluster);

            // we do not capture any exceptions but just let the exception thrown from consumer.poll directly
            // since when stream thread captures it, either we close all tasks as dirty or we close thread
            this.taskManager.handleAssignment(activeTasks, info.standbyTasks);
        }

        private void maybeScheduleFollowupRebalance(
            long encodedNextScheduledRebalanceMs,
            int receivedAssignmentMetadataVersion,
            int latestCommonlySupportedVersion,
            HashSet<HostInfo> groupHostInfo)
        {
            if (this.maybeUpdateSubscriptionVersion(receivedAssignmentMetadataVersion, latestCommonlySupportedVersion))
            {
                this.log.LogInformation("Requested to schedule immediate rebalance due to version probing.");
                Interlocked.Exchange(ref this.nextScheduledRebalanceMs, 0L);
            }
            else if (!this.verifyHostInfo(groupHostInfo))
            {
                this.log.LogInformation("Requested to schedule immediate rebalance to update group with new host endpoint = {}.", this.userEndPoint);
                Interlocked.Exchange(ref this.nextScheduledRebalanceMs, 0L);
            }
            else if (encodedNextScheduledRebalanceMs == 0L)
            {
                this.log.LogInformation("Requested to schedule immediate rebalance for new tasks to be safely revoked from current owner.");
                Interlocked.Exchange(ref this.nextScheduledRebalanceMs, 0L);
            }
            else if (encodedNextScheduledRebalanceMs < long.MaxValue)
            {
                this.log.LogInformation("Requested to schedule probing rebalance for {} ms.", encodedNextScheduledRebalanceMs);
                Interlocked.Exchange(ref this.nextScheduledRebalanceMs, encodedNextScheduledRebalanceMs);
            }
            else
            {
                this.log.LogInformation("No followup rebalance was requested, resetting the rebalance schedule.");
                Interlocked.Exchange(ref this.nextScheduledRebalanceMs, long.MaxValue);
            }
        }

        /**
         * Verify that this client's host info was included in the map returned in the assignment, and trigger a
         * rebalance if not. This may be necessary when using static membership, as a rejoining client will be handed
         * back its original assignment to avoid an unnecessary rebalance. If the client's endpoint has changed, we need
         * to force a rebalance for the other members in the group to get the updated host info for this client.
         *
         * @param groupHostInfo the HostInfo of all clients in the group
         * @return false if the current host info does not match that in the group assignment
         */
        private bool verifyHostInfo(HashSet<HostInfo> groupHostInfo)
        {
            if (this.userEndPoint != null && !groupHostInfo.IsEmpty())
            {
                HostInfo myHostInfo = HostInfo.BuildFromEndpoint(this.userEndPoint);

                return groupHostInfo.Contains(myHostInfo);
            }
            else
            {
                return true;
            }
        }

        // protected for upgrade test
        protected static Dictionary<TaskId, HashSet<TopicPartition>> getActiveTasks(List<TopicPartition> partitions, AssignmentInfo info)
        {
            Dictionary<TaskId, HashSet<TopicPartition>> activeTasks = new Dictionary<TaskId, HashSet<TopicPartition>>();
            for (int i = 0; i < partitions.Count; i++)
            {
                TopicPartition partition = partitions[i];
                TaskId id = info.activeTasks[i];

                if (!activeTasks.ContainsKey(id))
                {
                    activeTasks.Add(id, new HashSet<TopicPartition>());
                }

                activeTasks[id].Add(partition);
            }

            return activeTasks;
        }

        static Dictionary<TopicPartition, PartitionInfo> getTopicPartitionInfo(Dictionary<HostInfo, HashSet<TopicPartition>> partitionsByHost)
        {
            Dictionary<TopicPartition, PartitionInfo> topicToPartitionInfo = new Dictionary<TopicPartition, PartitionInfo>();
            foreach (HashSet<TopicPartition> value in partitionsByHost.Values)
            {
                foreach (TopicPartition topicPartition in value)
                {
                    topicToPartitionInfo.Put(
                        topicPartition,
                        new PartitionInfo(
                            topicPartition.Topic,
                            topicPartition.Partition,
                            null,
                            Array.Empty<Node>(),
                            Array.Empty<Node>()
                        )
                    );
                }
            }

            return topicToPartitionInfo;
        }

        private static void validateActiveTaskEncoding(List<TopicPartition> partitions, AssignmentInfo info, string logPrefix)
        {
            // the number of assigned partitions should be the same as number of active tasks, which
            // could be duplicated if one task has more than one assigned partitions
            if (partitions.Count != info.activeTasks.Count)
            {
                throw new TaskAssignmentException(
                    string.Format(
                        "%sNumber of assigned partitions %d is not equal to "
                            + "the number of active taskIds %d, assignmentInfo=%s",
                        logPrefix,
                        partitions.Count,
                        info.activeTasks.Count,
                        info.ToString()));
            }
        }

        private void ensureCopartitioning(
            List<HashSet<string>> copartitionGroups,
            Dictionary<string, InternalTopicConfig> allRepartitionTopicsNumPartitions,
            Cluster metadata)
        {
            foreach (HashSet<string> copartitionGroup in copartitionGroups)
            {
                this.copartitionedTopicsEnforcer.enforce(copartitionGroup, allRepartitionTopicsNumPartitions, metadata);
            }
        }

        private int updateMinReceivedVersion(int usedVersion, int minReceivedMetadataVersion)
        {
            return Math.Min(usedVersion, minReceivedMetadataVersion);
        }

        private int updateMinSupportedVersion(int supportedVersion, int minSupportedMetadataVersion)
        {
            if (supportedVersion < minSupportedMetadataVersion)
            {
                this.log.LogDebug("Downgrade the current minimum supported version {} to the smaller seen supported version {}",
                    minSupportedMetadataVersion, supportedVersion);

                return supportedVersion;
            }
            else
            {
                this.log.LogDebug("Current minimum supported version remains at {}, last seen supported version was {}",
                    minSupportedMetadataVersion, supportedVersion);
                return minSupportedMetadataVersion;
            }
        }

        // following functions are for test only
        void setInternalTopicManager(InternalTopicManager internalTopicManager)
        {
            this.internalTopicManager = internalTopicManager;
        }

        protected void handleRebalanceStart(HashSet<string> topics)
        {
            this.taskManager.handleRebalanceStart(topics);
        }

        long acceptableRecoveryLag()
        {
            return this.assignmentConfigs.AcceptableRecoveryLag;
        }

        int maxWarmupReplicas()
        {
            return this.assignmentConfigs.maxWarmupReplicas;
        }

        int numStandbyReplicas()
        {
            return this.assignmentConfigs.numStandbyReplicas;
        }

        long probingRebalanceIntervalMs()
        {
            return this.assignmentConfigs.probingRebalanceIntervalMs;
        }

        private class PartitionComparator : IComparer<TopicPartition>
        {
            public int Compare(TopicPartition x, TopicPartition y)
            {
                var topicCompare = x.Topic.CompareTo(y.Topic);

                return topicCompare != 0
                    ? topicCompare
                    : x.Partition.Value.CompareTo(y.Partition.Value);
            }
        }
    }
}
