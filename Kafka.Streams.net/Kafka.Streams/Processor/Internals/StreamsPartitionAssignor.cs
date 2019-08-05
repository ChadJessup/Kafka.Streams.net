/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for.Additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using Confluent.Kafka;
using Kafka.Streams.State;
using Kafka.Streams.Processor.Internals.Assignment;
using System.Collections.Generic;

namespace Kafka.Streams.Processor.Internals
{




    public class StreamsPartitionAssignor : ConsumerPartitionAssignor, Configurable
    {


        static int UNKNOWN = -1;
        private static int VERSION_ONE = 1;
        private static int VERSION_TWO = 2;
        private static int VERSION_THREE = 3;
        private static int VERSION_FOUR = 4;
        private static int EARLIEST_PROBEABLE_VERSION = VERSION_THREE;
        protected HashSet<int> supportedVersions = new HashSet<>();

        private ILogger log;
        private string logPrefix;
        public enum Error
        {

            NONE(0),
        INCOMPLETE_SOURCE_TOPIC_METADATA(1),
        VERSION_PROBING(2);

        private int code;

        Error(int code)
        {
            this.code = code;
        }

        public int code()
        {
            return code;
        }

        public static Error fromCode(int code)
        {
            switch (code)
            {
                case 0:
                    return NONE;
                case 1:
                    return INCOMPLETE_SOURCE_TOPIC_METADATA;
                case 2:
                    return VERSION_PROBING;
                default:
                    throw new System.ArgumentException("Unknown error code: " + code);
            }
        }
    }

    private static class AssignedPartition : Comparable<AssignedPartition>
    {
        public TaskId taskId;
        public TopicPartition partition;

        AssignedPartition(TaskId taskId,
                          TopicPartition partition)
        {
            this.taskId = taskId;
            this.partition = partition;
        }


        public int compareTo(AssignedPartition that)
        {
            return PARTITION_COMPARATOR.compare(this.partition, that.partition);
        }


        public bool Equals(object o)
        {
            if (!(o is AssignedPartition))
            {
                return false;
            }
            AssignedPartition other = (AssignedPartition)o;
            return compareTo(other) == 0;
        }


        public int GetHashCode()
        {
            // Only partition is important for compareTo, Equals and GetHashCode().
            return partition.GetHashCode();
        }
    }

    private static class ClientMetadata
    {

        HostInfo hostInfo;
        HashSet<string> consumers;
        ClientState state;

        ClientMetadata(string endPoint)
        {

            // get the host info if possible
            if (endPoint != null)
            {
                string host = getHost(endPoint);
                int port = getPort(endPoint);

                if (host == null || port == null)
                {
                    throw new ConfigException(string.Format("Error parsing host.Address %s. Expected format host:port.", endPoint));
                }

                hostInfo = new HostInfo(host, port);
            }
            else
            {

                hostInfo = null;
            }

            // initialize the consumer memberIds
            consumers = new HashSet<>();

            // initialize the client state
            state = new ClientState();
        }

        void addConsumer(string consumerMemberId,
                         SubscriptionInfo info)
        {
            consumers.Add(consumerMemberId);
            state.addPreviousActiveTasks(info.prevTasks());
            state.addPreviousStandbyTasks(info.standbyTasks());
            state.incrementCapacity();
        }


        public string ToString()
        {
            return "ClientMetadata{" +
                    "hostInfo=" + hostInfo +
                    ", consumers=" + consumers +
                    ", state=" + state +
                    '}';
        }

        protected static IComparator<TopicPartition> PARTITION_COMPARATOR = (p1, p2) =>
        {
            int result = p1.topic().compareTo(p2.topic());

            if (result != 0)
            {
                return result;
            }
            else
            {

                return int.compare(p1.partition(), p2.partition());
            }
        };

        private string userEndPoint;
        private int numStandbyReplicas;

        private TaskManager taskManager;
        private PartitionGrouper partitionGrouper;
        private AtomicInteger assignmentErrorCode;

        protected int usedSubscriptionMetadataVersion = SubscriptionInfo.LATEST_SUPPORTED_VERSION;

        private InternalTopicManager internalTopicManager;
        private CopartitionedTopicsValidator copartitionedTopicsValidator;

        protected string userEndPoint()
        {
            return userEndPoint;
        }

        protected TaskManager taskManger()
        {
            return taskManager;
        }

        /**
         * We need to have the PartitionAssignor and its StreamThread to be mutually accessible
         * since the former needs later's cached metadata while sending subscriptions,
         * and the latter needs former's returned assignment when.Adding tasks.
         * @throws KafkaException if the stream thread is not specified
         */

        public void configure(Dictionary<string, object> configs)
        {
            StreamsConfig streamsConfig = new InternalStreamsConfig(configs);

            // Setting the logger with the passed in client thread name
            logPrefix = string.Format("stream-thread [%s] ", streamsConfig.getString(CommonClientConfigs.CLIENT_ID_CONFIG));
            LogContext logContext = new LogContext(logPrefix);
            log = logContext.logger(GetType());

            string upgradeFrom = streamsConfig.getString(StreamsConfig.UPGRADE_FROM_CONFIG);
            if (upgradeFrom != null)
            {
                switch (upgradeFrom)
                {
                    case StreamsConfig.UPGRADE_FROM_0100:
                        log.LogInformation("Downgrading metadata version from {} to 1 for upgrade from 0.10.0.x.", SubscriptionInfo.LATEST_SUPPORTED_VERSION);
                        usedSubscriptionMetadataVersion = VERSION_ONE;
                        break;
                    case StreamsConfig.UPGRADE_FROM_0101:
                    case StreamsConfig.UPGRADE_FROM_0102:
                    case StreamsConfig.UPGRADE_FROM_0110:
                    case StreamsConfig.UPGRADE_FROM_10:
                    case StreamsConfig.UPGRADE_FROM_11:
                        log.LogInformation("Downgrading metadata version from {} to 2 for upgrade from {}.x.", SubscriptionInfo.LATEST_SUPPORTED_VERSION, upgradeFrom);
                        usedSubscriptionMetadataVersion = VERSION_TWO;
                        break;
                    default:
                        throw new System.ArgumentException("Unknown configuration value for parameter 'upgrade.from': " + upgradeFrom);
                }
            }

            object o = configs[StreamsConfig.InternalConfig.TASK_MANAGER_FOR_PARTITION_ASSIGNOR];
            if (o == null)
            {
                KafkaException fatalException = new KafkaException("TaskManager is not specified");
                log.LogError(fatalException.getMessage(), fatalException);
                throw fatalException;
            }

            if (!(o is TaskManager))
            {
                KafkaException fatalException = new KafkaException(string.Format("%s is not an instance of %s", o.GetType().getName(), TaskManager.getName()));
                log.LogError(fatalException.getMessage(), fatalException);
                throw fatalException;
            }

            taskManager = (TaskManager)o;

            object ai = configs[StreamsConfig.InternalConfig.ASSIGNMENT_ERROR_CODE];
            if (ai == null)
            {
                KafkaException fatalException = new KafkaException("assignmentErrorCode is not specified");
                log.LogError(fatalException.getMessage(), fatalException);
                throw fatalException;
            }

            if (!(ai is AtomicInteger))
            {
                KafkaException fatalException = new KafkaException(string.Format("%s is not an instance of %s",
                    ai.GetType().getName(), AtomicInteger.getName()));
                log.LogError(fatalException.getMessage(), fatalException);
                throw fatalException;
            }
            assignmentErrorCode = (AtomicInteger)ai;

            numStandbyReplicas = streamsConfig.getInt(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG);

            partitionGrouper = streamsConfig.getConfiguredInstance(StreamsConfig.PARTITION_GROUPER_CLASS_CONFIG, PartitionGrouper);

            string userEndPoint = streamsConfig.getString(StreamsConfig.APPLICATION_SERVER_CONFIG);
            if (userEndPoint != null && !userEndPoint.isEmpty())
            {
                try
                {

                    string host = getHost(userEndPoint);
                    int port = getPort(userEndPoint);

                    if (host == null || port == null)
                    {
                        throw new ConfigException(string.Format("%s Config %s isn't in the correct format. Expected a host:port pair" +
                                " but received %s",
                            logPrefix, StreamsConfig.APPLICATION_SERVER_CONFIG, userEndPoint));
                    }
                }
                catch (NumberFormatException nfe)
                {
                    throw new ConfigException(string.Format("%s Invalid port supplied in %s for config %s",
                            logPrefix, userEndPoint, StreamsConfig.APPLICATION_SERVER_CONFIG));
                }

                this.userEndPoint = userEndPoint;
            }

            internalTopicManager = new InternalTopicManager(taskManager.adminClient, streamsConfig);

            copartitionedTopicsValidator = new CopartitionedTopicsValidator(logPrefix);
        }


        public string name()
        {
            return "stream";
        }


        public ByteBuffer subscriptionUserData(Set<string> topics)
        {
            // Adds the following information to subscription
            // 1. Client UUID (a unique id assigned to an instance of KafkaStreams)
            // 2. Task ids of previously running tasks
            // 3. Task ids of valid local states on the client's state directory.

            HashSet<TaskId> previousActiveTasks = taskManager.prevActiveTaskIds();
            HashSet<TaskId> standbyTasks = taskManager.cachedTasksIds();
            standbyTasks.removeAll(previousActiveTasks);
            SubscriptionInfo data = new SubscriptionInfo(
                usedSubscriptionMetadataVersion,
                taskManager.processId(),
                previousActiveTasks,
                standbyTasks,
                this.userEndPoint);

            taskManager.updateSubscriptionsFromMetadata(topics);

            return data.encode();
        }

        private Dictionary<string, Assignment> errorAssignment(Dictionary<UUID, ClientMetadata> clientsMetadata,
                                                        string topic,
                                                        int errorCode)
        {
            log.LogError("{} is unknown yet during rebalance," +
                " please make sure they have been pre-created before starting the Streams application.", topic);
            Dictionary<string, Assignment> assignment = new HashMap<>();
            foreach (ClientMetadata clientMetadata in clientsMetadata.values())
            {
                foreach (string consumerId in clientMetadata.consumers)
                {
                    assignment.Add(consumerId, new Assignment(
                        Collections.emptyList(),
                        new AssignmentInfo(AssignmentInfo.LATEST_SUPPORTED_VERSION,
                            Collections.emptyList(),
                            Collections.emptyMap(),
                            Collections.emptyMap(),
                            errorCode).encode()
                    ));
                }
            }
            return assignment;
        }
        /*
         * This assigns tasks to consumer clients in the following steps.
         *
         * 0. check all repartition source topics and use internal topic manager to make sure
         *    they have been created with the right number of partitions.
         *
         * 1. using user customized partition grouper to generate tasks along with their
         *    assigned partitions; also make sure that the task's corresponding changelog topics
         *    have been created with the right number of partitions.
         *
         * 2. using TaskAssignor to assign tasks to consumer clients.
         *    - Assign a task to a client which was running it previously.
         *      If there is no such client, assign a task to a client which has its valid local state.
         *    - A client may have more than one stream threads.
         *      The assignor tries to assign tasks to a client proportionally to the number of threads.
         *    - We try not to assign the same set of tasks to two different clients
         *    We do the assignment in one-pass. The result may not satisfy above all.
         *
         * 3. within each client, tasks are assigned to consumer clients in round-robin manner.
         */

        public GroupAssignment assign(Cluster metadata, GroupSubscription groupSubscription)
        {
            Dictionary<string, Subscription> subscriptions = groupSubscription.groupSubscription();
            // construct the client metadata from the decoded subscription info
            Dictionary<UUID, ClientMetadata> clientMetadataMap = new HashMap<>();
            HashSet<string> futureConsumers = new HashSet<>();

            int minReceivedMetadataVersion = SubscriptionInfo.LATEST_SUPPORTED_VERSION;

            supportedVersions.clear();
            int futureMetadataVersion = UNKNOWN;
            foreach (Map.Entry<string, Subscription> entry in subscriptions.entrySet())
            {
                string consumerId = entry.Key;
                Subscription subscription = entry.Value;

                SubscriptionInfo info = SubscriptionInfo.decode(subscription.userData());
                int usedVersion = info.version();
                supportedVersions.Add(info.latestSupportedVersion());
                if (usedVersion > SubscriptionInfo.LATEST_SUPPORTED_VERSION)
                {
                    futureMetadataVersion = usedVersion;
                    futureConsumers.Add(consumerId);
                    continue;
                }
                if (usedVersion < minReceivedMetadataVersion)
                {
                    minReceivedMetadataVersion = usedVersion;
                }

                // create the new client metadata if necessary
                ClientMetadata clientMetadata = clientMetadataMap[info.processId()];

                if (clientMetadata == null)
                {
                    clientMetadata = new ClientMetadata(info.userEndPoint());
                    clientMetadataMap.Add(info.processId(), clientMetadata);
                }

                //.Add the consumer to the client
                clientMetadata.AddConsumer(consumerId, info);
            }

            bool versionProbing;
            if (futureMetadataVersion != UNKNOWN)
            {
                if (minReceivedMetadataVersion >= EARLIEST_PROBEABLE_VERSION)
                {
                    log.LogInformation("Received a future (version probing) subscription (version: {}). Sending empty assignment back (with supported version {}).",
                        futureMetadataVersion,
                        SubscriptionInfo.LATEST_SUPPORTED_VERSION);
                    versionProbing = true;
                }
                else
                {

                    throw new InvalidOperationException("Received a future (version probing) subscription (version: " + futureMetadataVersion
                        + ") and an incompatible pre Kafka 2.0 subscription (version: " + minReceivedMetadataVersion + ") at the same time.");
                }
            }
            else
            {

                versionProbing = false;
            }

            if (minReceivedMetadataVersion < SubscriptionInfo.LATEST_SUPPORTED_VERSION)
            {
                log.LogInformation("Downgrading metadata to version {}. Latest supported version is {}.",
                    minReceivedMetadataVersion,
                    SubscriptionInfo.LATEST_SUPPORTED_VERSION);
            }

            log.LogDebug("Constructed client metadata {} from the member subscriptions.", clientMetadataMap);

            // ---------------- Step Zero ---------------- //

            // parse the topology to determine the repartition source topics,
            // making sure they are created with the number of partitions as
            // the maximum of the depending sub-topologies source topics' number of partitions
            Dictionary<int, InternalTopologyBuilder.TopicsInfo> topicGroups = taskManager.builder().topicGroups();

            Dictionary<string, InternalTopicConfig> repartitionTopicMetadata = new HashMap<>();
            foreach (InternalTopologyBuilder.TopicsInfo topicsInfo in topicGroups.values())
            {
                foreach (string topic in topicsInfo.sourceTopics)
                {
                    if (!topicsInfo.repartitionSourceTopics.keySet().contains(topic) &&
                        !metadata.topics().contains(topic))
                    {
                        log.LogError("Missing source topic {} durign assignment. Returning error {}.",
                                  topic, Error.INCOMPLETE_SOURCE_TOPIC_METADATA.name());
                        return new GroupAssignment(errorAssignment(clientMetadataMap, topic, Error.INCOMPLETE_SOURCE_TOPIC_METADATA.code));
                    }
                }
                for (InternalTopicConfig topic: topicsInfo.repartitionSourceTopics.values())
                {
                    repartitionTopicMetadata.Add(topic.name(), topic);
                }
            }

            bool numPartitionsNeeded;
            do
            {

                numPartitionsNeeded = false;

                foreach (InternalTopologyBuilder.TopicsInfo topicsInfo in topicGroups.values())
                {
                    foreach (string topicName in topicsInfo.repartitionSourceTopics.keySet())
                    {
                        int numPartitions = repartitionTopicMetadata[topicName].numberOfPartitions();

                        if (numPartitions == UNKNOWN)
                        {
                            // try set the number of partitions for this repartition topic if it is not set yet
                            foreach (InternalTopologyBuilder.TopicsInfo otherTopicsInfo in topicGroups.values())
                            {
                                HashSet<string> otherSinkTopics = otherTopicsInfo.sinkTopics;

                                if (otherSinkTopics.contains(topicName))
                                {
                                    // if this topic is one of the sink topics of this topology,
                                    // use the maximum of all its source topic partitions as the number of partitions
                                    foreach (string sourceTopicName in otherTopicsInfo.sourceTopics)
                                    {
                                        int numPartitionsCandidate;
                                        // It is possible the sourceTopic is another internal topic, i.e,
                                        // map().join().join(map())
                                        if (repartitionTopicMetadata.ContainsKey(sourceTopicName))
                                        {
                                            numPartitionsCandidate = repartitionTopicMetadata[sourceTopicName].numberOfPartitions();
                                        }
                                        else
                                        {

                                            numPartitionsCandidate = metadata.partitionCountForTopic(sourceTopicName);
                                        }

                                        if (numPartitionsCandidate > numPartitions)
                                        {
                                            numPartitions = numPartitionsCandidate;
                                        }
                                    }
                                }
                            }
                            // if we still have not find the right number of partitions,
                            // another iteration is needed
                            if (numPartitions == UNKNOWN)
                            {
                                numPartitionsNeeded = true;
                            }
                            else
                            {

                                repartitionTopicMetadata[topicName].setNumberOfPartitions(numPartitions);
                            }
                        }
                    }
                }
            } while (numPartitionsNeeded);


            // ensure the co-partitioning topics within the group have the same number of partitions,
            // and enforce the number of partitions for those repartition topics to be the same if they
            // are co-partitioned as well.
            ensureCopartitioning(taskManager.builder().copartitionGroups(), repartitionTopicMetadata, metadata);

            // make sure the repartition source topics exist with the right number of partitions,
            // create these topics if necessary
            prepareTopic(repartitionTopicMetadata);

            // augment the metadata with the newly computed number of partitions for all the
            // repartition source topics
            Dictionary<TopicPartition, PartitionInfo> allRepartitionTopicPartitions = new HashMap<>();
            foreach (Map.Entry<string, InternalTopicConfig> entry in repartitionTopicMetadata.entrySet())
            {
                string topic = entry.Key;
                int numPartitions = entry.Value.numberOfPartitions();

                for (int partition = 0; partition < numPartitions; partition++)
                {
                    allRepartitionTopicPartitions.Add(new TopicPartition(topic, partition),
                            new PartitionInfo(topic, partition, null, new Node[0], new Node[0]));
                }
            }

            Cluster fullMetadata = metadata.withPartitions(allRepartitionTopicPartitions);
            taskManager.setClusterMetadata(fullMetadata);

            log.LogDebug("Created repartition topics {} from the parsed topology.", allRepartitionTopicPartitions.values());

            // ---------------- Step One ---------------- //

            // get the tasks as partition groups from the partition grouper
            HashSet<string> allSourceTopics = new HashSet<>();
            Dictionary<int, HashSet<string>> sourceTopicsByGroup = new HashMap<>();
            foreach (Map.Entry<int, InternalTopologyBuilder.TopicsInfo> entry in topicGroups.entrySet())
            {
                allSourceTopics.AddAll(entry.Value.sourceTopics);
                sourceTopicsByGroup.Add(entry.Key, entry.Value.sourceTopics);
            }

            Dictionary<TaskId, HashSet<TopicPartition>> partitionsForTask = partitionGrouper.partitionGroups(sourceTopicsByGroup, fullMetadata);

            // check if all partitions are assigned, and there are no duplicates of partitions in multiple tasks
            HashSet<TopicPartition> allAssignedPartitions = new HashSet<>();
            Dictionary<int, HashSet<TaskId>> tasksByTopicGroup = new HashMap<>();
            foreach (Map.Entry<TaskId, HashSet<TopicPartition>> entry in partitionsForTask.entrySet())
            {
                HashSet<TopicPartition> partitions = entry.Value;
                foreach (TopicPartition partition in partitions)
                {
                    if (allAssignedPartitions.contains(partition))
                    {
                        log.LogWarning("Partition {} is assigned to more than one tasks: {}", partition, partitionsForTask);
                    }
                }
                allAssignedPartitions.AddAll(partitions);

                TaskId id = entry.Key;
                tasksByTopicGroup.computeIfAbsent(id.topicGroupId, k-> new HashSet<>()).Add(id);
            }
            foreach (string topic in allSourceTopics)
            {
                List<PartitionInfo> partitionInfoList = fullMetadata.partitionsForTopic(topic);
                if (!partitionInfoList.isEmpty())
                {
                    foreach (PartitionInfo partitionInfo in partitionInfoList)
                    {
                        TopicPartition partition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
                        if (!allAssignedPartitions.contains(partition))
                        {
                            log.LogWarning("Partition {} is not assigned to any tasks: {}"
                                     + " Possible causes of a partition not getting assigned"
                                     + " is that another topic defined in the topology has not been"
                                     + " created when starting your streams application,"
                                     + " resulting in no tasks created for this topology at all.", partition, partitionsForTask);
                        }
                    }
                }
                else
                {

                    log.LogWarning("No partitions found for topic {}", topic);
                }
            }

            //.Add tasks to state change log topic subscribers
            Dictionary<string, InternalTopicConfig> changelogTopicMetadata = new HashMap<>();
            foreach (Map.Entry<int, InternalTopologyBuilder.TopicsInfo> entry in topicGroups.entrySet())
            {
                int topicGroupId = entry.Key;
                Dictionary<string, InternalTopicConfig> stateChangelogTopics = entry.Value.stateChangelogTopics;

                foreach (InternalTopicConfig topicConfig in stateChangelogTopics.values())
                {
                    // the expected number of partitions is the max value of TaskId.partition + 1
                    int numPartitions = UNKNOWN;
                    if (tasksByTopicGroup[topicGroupId] != null)
                    {
                        foreach (TaskId task in tasksByTopicGroup[topicGroupId])
                        {
                            if (numPartitions < task.partition + 1)
                            {
                                numPartitions = task.partition + 1;
                            }
                        }
                        topicConfig.setNumberOfPartitions(numPartitions);

                        changelogTopicMetadata.Add(topicConfig.name(), topicConfig);
                    }
                    else
                    {

                        log.LogDebug("No tasks found for topic group {}", topicGroupId);
                    }
                }
            }

            prepareTopic(changelogTopicMetadata);

            log.LogDebug("Created state changelog topics {} from the parsed topology.", changelogTopicMetadata.values());

            // ---------------- Step Two ---------------- //

            // assign tasks to clients
            Dictionary<UUID, ClientState> states = new HashMap<>();
            foreach (Map.Entry<UUID, ClientMetadata> entry in clientMetadataMap.entrySet())
            {
                states.Add(entry.Key, entry.Value.state);
            }

            log.LogDebug("Assigning tasks {} to clients {} with number of replicas {}",
                    partitionsForTask.keySet(), states, numStandbyReplicas);

            StickyTaskAssignor<UUID> taskAssignor = new StickyTaskAssignor<>(states, partitionsForTask.keySet());
            taskAssignor.assign(numStandbyReplicas);

            log.LogInformation("Assigned tasks to clients as {}.", states);

            // ---------------- Step Three ---------------- //

            // construct the global partition assignment per host map
            Dictionary<HostInfo, HashSet<TopicPartition>> partitionsByHostState = new HashMap<>();
            if (minReceivedMetadataVersion >= 2)
            {
                foreach (Map.Entry<UUID, ClientMetadata> entry in clientMetadataMap.entrySet())
                {
                    HostInfo hostInfo = entry.Value.hostInfo;

                    if (hostInfo != null)
                    {
                        HashSet<TopicPartition> topicPartitions = new HashSet<>();
                        ClientState state = entry.Value.state;

                        foreach (TaskId id in state.activeTasks())
                        {
                            topicPartitions.addAll(partitionsForTask[id]);
                        }

                        partitionsByHostState.Add(hostInfo, topicPartitions);
                    }
                }
            }
            taskManager.setPartitionsByHostState(partitionsByHostState);

            Dictionary<string, Assignment> assignment;
            if (versionProbing)
            {
                assignment = versionProbingAssignment(clientMetadataMap, partitionsForTask, partitionsByHostState, futureConsumers, minReceivedMetadataVersion);
            }
            else
            {

                assignment = computeNewAssignment(clientMetadataMap, partitionsForTask, partitionsByHostState, minReceivedMetadataVersion);
            }

            return new GroupAssignment(assignment);
        }

        private Dictionary<string, Assignment> computeNewAssignment(Dictionary<UUID, ClientMetadata> clientsMetadata,
                                                             Dictionary<TaskId, HashSet<TopicPartition>> partitionsForTask,
                                                             Dictionary<HostInfo, HashSet<TopicPartition>> partitionsByHostState,
                                                             int minUserMetadataVersion)
        {
            Dictionary<string, Assignment> assignment = new HashMap<>();

            // within the client, distribute tasks to its owned consumers
            foreach (Map.Entry<UUID, ClientMetadata> entry in clientsMetadata.entrySet())
            {
                HashSet<string> consumers = entry.Value.consumers;
                ClientState state = entry.Value.state;

                List<List<TaskId>> interleavedActive = interleaveTasksByGroupId(state.activeTasks(), consumers.size());
                List<List<TaskId>> interleavedStandby = interleaveTasksByGroupId(state.standbyTasks(), consumers.size());

                int consumerTaskIndex = 0;

                foreach (string consumer in consumers)
                {
                    Dictionary<TaskId, HashSet<TopicPartition>> standby = new HashMap<>();
                    List<AssignedPartition> assignedPartitions = new List<>();

                    List<TaskId> assignedActiveList = interleavedActive[consumerTaskIndex];

                    foreach (TaskId taskId in assignedActiveList)
                    {
                        foreach (TopicPartition partition in partitionsForTask[taskId])
                        {
                            assignedPartitions.Add(new AssignedPartition(taskId, partition));
                        }
                    }

                    if (!state.standbyTasks().isEmpty())
                    {
                        List<TaskId> assignedStandbyList = interleavedStandby[consumerTaskIndex];
                        foreach (TaskId taskId in assignedStandbyList)
                        {
                            standby.computeIfAbsent(taskId, k-> new HashSet<>()).addAll(partitionsForTask[taskId]);
                        }
                    }

                    consumerTaskIndex++;

                    Collections.sort(assignedPartitions);
                    List<TaskId> active = new List<>();
                    List<TopicPartition> activePartitions = new List<>();
                    foreach (AssignedPartition partition in assignedPartitions)
                    {
                        active.Add(partition.taskId);
                        activePartitions.Add(partition.partition);
                    }

                    // finally, encode the assignment before sending back to coordinator
                    assignment.Add(consumer, new Assignment(
                        activePartitions,
                        new AssignmentInfo(minUserMetadataVersion, active, standby, partitionsByHostState, 0).encode()));
                }
            }

            return assignment;
        }

        private Dictionary<string, Assignment> versionProbingAssignment(Dictionary<UUID, ClientMetadata> clientsMetadata,
                                                                 Dictionary<TaskId, HashSet<TopicPartition>> partitionsForTask,
                                                                 Dictionary<HostInfo, HashSet<TopicPartition>> partitionsByHostState,
                                                                 HashSet<string> futureConsumers,
                                                                 int minUserMetadataVersion)
        {
            Dictionary<string, Assignment> assignment = new HashMap<>();

            // assign previously assigned tasks to "old consumers"
            foreach (ClientMetadata clientMetadata in clientsMetadata.values())
            {
                foreach (string consumerId in clientMetadata.consumers)
                {

                    if (futureConsumers.contains(consumerId))
                    {
                        continue;
                    }

                    List<TaskId> activeTasks = new List<>(clientMetadata.state.prevActiveTasks());

                    List<TopicPartition> assignedPartitions = new List<>();
                    foreach (TaskId taskId in activeTasks)
                    {
                        assignedPartitions.AddAll(partitionsForTask[taskId]);
                    }

                    Dictionary<TaskId, HashSet<TopicPartition>> standbyTasks = new HashMap<>();
                    foreach (TaskId taskId in clientMetadata.state.prevStandbyTasks())
                    {
                        standbyTasks.Add(taskId, partitionsForTask[taskId]);
                    }

                    assignment.Add(consumerId, new Assignment(
                        assignedPartitions,
                        new AssignmentInfo(
                            minUserMetadataVersion,
                            activeTasks,
                            standbyTasks,
                            partitionsByHostState,
                            0)
                            .encode()
                    ));
                }
            }

            //.Add empty assignment for "future version" clients (ie, empty version probing response)
            foreach (string consumerId in futureConsumers)
            {
                assignment.Add(consumerId, new Assignment(
                    Collections.emptyList(),
                    new AssignmentInfo().encode()
                ));
            }

            return assignment;
        }

        // visible for testing
        List<List<TaskId>> interleaveTasksByGroupId(Collection<TaskId> taskIds, int numberThreads)
        {
            LinkedList<TaskId> sortedTasks = new LinkedList<>(taskIds);
            Collections.sort(sortedTasks);
            List<List<TaskId>> taskIdsForConsumerAssignment = new List<>(numberThreads);
            for (int i = 0; i < numberThreads; i++)
            {
                taskIdsForConsumerAssignment.Add(new List<>());
            }
            while (!sortedTasks.isEmpty())
            {
                foreach (List<TaskId> taskIdList in taskIdsForConsumerAssignment)
                {
                    TaskId taskId = sortedTasks.poll();
                    if (taskId == null)
                    {
                        break;
                    }
                    taskIdList.Add(taskId);
                }
            }
            return taskIdsForConsumerAssignment;
        }

        /**
         * @throws TaskAssignmentException if there is no task id for one of the partitions specified
         */

        public void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata)
        {
            List<TopicPartition> partitions = new List<>(assignment.partitions());
            partitions.sort(PARTITION_COMPARATOR);

            AssignmentInfo info = AssignmentInfo.decode(assignment.userData());
            if (info.errCode() != Error.NONE.code)
            {
                // set flag to shutdown streams app
                assignmentErrorCode.set(info.errCode());
                return;
            }
            int receivedAssignmentMetadataVersion = info.version();
            int leaderSupportedVersion = info.latestSupportedVersion();

            if (receivedAssignmentMetadataVersion > usedSubscriptionMetadataVersion)
            {
                throw new InvalidOperationException("Sent a version " + usedSubscriptionMetadataVersion
                    + " subscription but got an assignment with higher version " + receivedAssignmentMetadataVersion + ".");
            }

            if (receivedAssignmentMetadataVersion < usedSubscriptionMetadataVersion
                && receivedAssignmentMetadataVersion >= EARLIEST_PROBEABLE_VERSION)
            {

                if (receivedAssignmentMetadataVersion == leaderSupportedVersion)
                {
                    log.LogInformation("Sent a version {} subscription and got version {} assignment back (successful version probing). " +
                            "Downgrading subscription metadata to received version and trigger new rebalance.",
                        usedSubscriptionMetadataVersion,
                        receivedAssignmentMetadataVersion);
                    usedSubscriptionMetadataVersion = receivedAssignmentMetadataVersion;
                }
                else
                {

                    log.LogInformation("Sent a version {} subscription and got version {} assignment back (successful version probing). " +
                        "Setting subscription metadata to leaders supported version {} and trigger new rebalance.",
                        usedSubscriptionMetadataVersion,
                        receivedAssignmentMetadataVersion,
                        leaderSupportedVersion);
                    usedSubscriptionMetadataVersion = leaderSupportedVersion;
                }

                assignmentErrorCode.set(Error.VERSION_PROBING.code);
                return;
            }

            // version 1 field
            Dictionary<TaskId, HashSet<TopicPartition>> activeTasks = new HashMap<>();
            // version 2 fields
            Dictionary<TopicPartition, PartitionInfo> topicToPartitionInfo = new HashMap<>();
            Dictionary<HostInfo, HashSet<TopicPartition>> partitionsByHost;

            switch (receivedAssignmentMetadataVersion)
            {
                case VERSION_ONE:
                    processVersionOneAssignment(info, partitions, activeTasks);
                    partitionsByHost = Collections.emptyMap();
                    break;
                case VERSION_TWO:
                    processVersionTwoAssignment(info, partitions, activeTasks, topicToPartitionInfo);
                    partitionsByHost = info.partitionsByHost();
                    break;
                case VERSION_THREE:
                    if (leaderSupportedVersion > usedSubscriptionMetadataVersion)
                    {
                        log.LogInformation("Sent a version {} subscription and group leader's latest supported version is {}. " +
                            "Upgrading subscription metadata version to {} for next rebalance.",
                            usedSubscriptionMetadataVersion,
                            leaderSupportedVersion,
                            leaderSupportedVersion);
                        usedSubscriptionMetadataVersion = leaderSupportedVersion;
                    }
                    processVersionThreeAssignment(info, partitions, activeTasks, topicToPartitionInfo);
                    partitionsByHost = info.partitionsByHost();
                    break;
                case VERSION_FOUR:
                    if (leaderSupportedVersion > usedSubscriptionMetadataVersion)
                    {
                        log.LogInformation("Sent a version {} subscription and group leader's latest supported version is {}. " +
                            "Upgrading subscription metadata version to {} for next rebalance.",
                            usedSubscriptionMetadataVersion,
                            leaderSupportedVersion,
                            leaderSupportedVersion);
                        usedSubscriptionMetadataVersion = leaderSupportedVersion;
                    }
                    processVersionFourAssignment(info, partitions, activeTasks, topicToPartitionInfo);
                    partitionsByHost = info.partitionsByHost();
                    break;
                default:
                    throw new InvalidOperationException("This code should never be reached. Please file a bug report at https://issues.apache.org/jira/projects/KAFKA/");
            }

            taskManager.setClusterMetadata(Cluster.empty().withPartitions(topicToPartitionInfo));
            taskManager.setPartitionsByHostState(partitionsByHost);
            taskManager.setAssignmentMetadata(activeTasks, info.standbyTasks());
            taskManager.updateSubscriptionsFromAssignment(partitions);
        }

        private void processVersionOneAssignment(AssignmentInfo info,
                                                 List<TopicPartition> partitions,
                                                 Dictionary<TaskId, HashSet<TopicPartition>> activeTasks)
        {
            // the number of assigned partitions should be the same as number of active tasks, which
            // could be duplicated if one task has more than one assigned partitions
            if (partitions.size() != info.activeTasks().size())
            {
                throw new TaskAssignmentException(
                    string.Format("%sNumber of assigned partitions %d is not equal to the number of active taskIds %d" +
                        ", assignmentInfo=%s", logPrefix, partitions.size(), info.activeTasks().size(), info.ToString())
                );
            }

            for (int i = 0; i < partitions.size(); i++)
            {
                TopicPartition partition = partitions[i];
                TaskId id = info.activeTasks()[i];
                activeTasks.computeIfAbsent(id, k-> new HashSet<>()).Add(partition);
            }
        }

        private void processVersionTwoAssignment(AssignmentInfo info,
                                                 List<TopicPartition> partitions,
                                                 Dictionary<TaskId, HashSet<TopicPartition>> activeTasks,
                                                 Dictionary<TopicPartition, PartitionInfo> topicToPartitionInfo)
        {
            processVersionOneAssignment(info, partitions, activeTasks);

            // process partitions by host
            Dictionary<HostInfo, HashSet<TopicPartition>> partitionsByHost = info.partitionsByHost();
            foreach (Set<TopicPartition> value in partitionsByHost.values())
            {
                foreach (TopicPartition topicPartition in value)
                {
                    topicToPartitionInfo.Add(
                        topicPartition,
                        new PartitionInfo(topicPartition.topic(), topicPartition.partition(), null, new Node[0], new Node[0]));
                }
            }
        }

        private void processVersionThreeAssignment(AssignmentInfo info,
                                                   List<TopicPartition> partitions,
                                                   Dictionary<TaskId, HashSet<TopicPartition>> activeTasks,
                                                   Dictionary<TopicPartition, PartitionInfo> topicToPartitionInfo)
        {
            processVersionTwoAssignment(info, partitions, activeTasks, topicToPartitionInfo);
        }

        private void processVersionFourAssignment(AssignmentInfo info,
                                                  List<TopicPartition> partitions,
                                                  Dictionary<TaskId, HashSet<TopicPartition>> activeTasks,
                                                  Dictionary<TopicPartition, PartitionInfo> topicToPartitionInfo)
        {
            processVersionThreeAssignment(info, partitions, activeTasks, topicToPartitionInfo);
        }

        // for testing
        protected void processLatestVersionAssignment(AssignmentInfo info,
                                                      List<TopicPartition> partitions,
                                                      Dictionary<TaskId, HashSet<TopicPartition>> activeTasks,
                                                      Dictionary<TopicPartition, PartitionInfo> topicToPartitionInfo)
        {
            processVersionThreeAssignment(info, partitions, activeTasks, topicToPartitionInfo);
        }

        /**
         * Internal helper function that creates a Kafka topic
         *
         * @param topicPartitions Map that contains the topic names to be created with the number of partitions
         */
        private void prepareTopic(Dictionary<string, InternalTopicConfig> topicPartitions)
        {
            log.LogDebug("Starting to validate internal topics {} in partition assignor.", topicPartitions);

            // first construct the topics to make ready
            Dictionary<string, InternalTopicConfig> topicsToMakeReady = new HashMap<>();

            foreach (InternalTopicConfig topic in topicPartitions.values())
            {
                int numPartitions = topic.numberOfPartitions();
                if (numPartitions == UNKNOWN)
                {
                    throw new StreamsException(string.Format("%sTopic [%s] number of partitions not defined", logPrefix, topic.name()));
                }

                topic.setNumberOfPartitions(numPartitions);
                topicsToMakeReady.Add(topic.name(), topic);
            }

            if (!topicsToMakeReady.isEmpty())
            {
                internalTopicManager.makeReady(topicsToMakeReady);
            }

            log.LogDebug("Completed validating internal topics {} in partition assignor.", topicPartitions);
        }

        private void ensureCopartitioning(Collection<Set<string>> copartitionGroups,
                                          Dictionary<string, InternalTopicConfig> allRepartitionTopicsNumPartitions,
                                          Cluster metadata)
        {
            foreach (Set<string> copartitionGroup in copartitionGroups)
            {
                copartitionedTopicsValidator.validate(copartitionGroup, allRepartitionTopicsNumPartitions, metadata);
            }
        }

        static class CopartitionedTopicsValidator
        {

            private string logPrefix;
            private ILogger log;

            CopartitionedTopicsValidator(string logPrefix)
            {
                this.logPrefix = logPrefix;
                LogContext logContext = new LogContext(logPrefix);
                log = logContext.logger(GetType());
            }

            void validate(Set<string> copartitionGroup,
                          Dictionary<string, InternalTopicConfig> allRepartitionTopicsNumPartitions,
                          Cluster metadata)
            {
                int numPartitions = UNKNOWN;

                foreach (string topic in copartitionGroup)
                {
                    if (!allRepartitionTopicsNumPartitions.ContainsKey(topic))
                    {
                        int partitions = metadata.partitionCountForTopic(topic);
                        if (partitions == null)
                        {
                            string str = string.Format("%sTopic not found: %s", logPrefix, topic);
                            log.LogError(str);
                            throw new InvalidOperationException(str);
                        }

                        if (numPartitions == UNKNOWN)
                        {
                            numPartitions = partitions;
                        }
                        else if (numPartitions != partitions)
                        {
                            string[] topics = copartitionGroup.toArray(new string[0]);
                            Arrays.sort(topics);
                            throw new org.apache.kafka.streams.errors.TopologyException(string.Format("%sTopics not co-partitioned: [%s]", logPrefix, Utils.join(Arrays.asList(topics), ",")));
                        }
                    }
                }

                // if all topics for this co-partition group is repartition topics,
                // then set the number of partitions to be the maximum of the number of partitions.
                if (numPartitions == UNKNOWN)
                {
                    for (Map.Entry<string, InternalTopicConfig> entry: allRepartitionTopicsNumPartitions.entrySet())
                    {
                        if (copartitionGroup.contains(entry.Key))
                        {
                            int partitions = entry.Value.numberOfPartitions();
                            if (partitions > numPartitions)
                            {
                                numPartitions = partitions;
                            }
                        }
                    }
                }
                // enforce co-partitioning restrictions to repartition topics by updating their number of partitions
                foreach (Map.Entry<string, InternalTopicConfig> entry in allRepartitionTopicsNumPartitions.entrySet())
                {
                    if (copartitionGroup.contains(entry.Key))
                    {
                        entry.Value.setNumberOfPartitions(numPartitions);
                    }
                }

            }
        }

        // following functions are for test only
        void setInternalTopicManager(InternalTopicManager internalTopicManager)
        {
            this.internalTopicManager = internalTopicManager;
        }
    }
}