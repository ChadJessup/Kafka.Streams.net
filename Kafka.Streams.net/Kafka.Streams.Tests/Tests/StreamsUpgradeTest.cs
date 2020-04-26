namespace Kafka.Streams.Tests.Tests
{
}
//using Confluent.Kafka;
//using Xunit;

//namespace Kafka.Streams.Tests.Tools
//{
//    public class StreamsUpgradeTest
//    {


//        public static void main(string[] args)
//        {
//            if (args.Length < 1)
//            {
//                System.Console.Error.WriteLine("StreamsUpgradeTest requires one argument (properties-file) but no provided: ");
//            }
//            string propFileName = args.Length > 0 ? args[0] : null;

//            StreamsConfig streamsProperties = Utils.loadProps(propFileName);

//            System.Console.Out.WriteLine("StreamsTest instance started (StreamsUpgradeTest trunk)");
//            System.Console.Out.WriteLine("props=" + streamsProperties);

//            StreamsBuilder builder = new StreamsBuilder();
//            KStream dataStream = builder.Stream("data");
//            dataStream.Process(SmokeTestUtil.printProcessorSupplier("data"));
//            dataStream.To("echo");

//            StreamsConfig config = new StreamsConfig();
//            config.Set(StreamsConfig.ApplicationIdConfig, "StreamsUpgradeTest");
//            config.Put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

//            KafkaClientSupplier kafkaClientSupplier;
//            if (streamsProperties.ContainsKey("test.future.metadata"))
//            {
//                streamsProperties.remove("test.future.metadata");
//                kafkaClientSupplier = new FutureKafkaClientSupplier();
//            }
//            else
//            {
//                kafkaClientSupplier = new DefaultKafkaClientSupplier();
//            }
//            config.PutAll(streamsProperties);

//            KafkaStreamsThread streams = new KafkaStreamsThread(builder.Build(), config, kafkaClientSupplier);
//            streams.Start();

//            Runtime.getRuntime().addShutdownHook(new Thread(() =>
//            {
//                System.Console.Out.WriteLine("closing Kafka Streams instance");
//                System.Console.Out.Flush();
//                streams.Close();
//                System.Console.Out.WriteLine("UPGRADE-TEST-CLIENT-CLOSED");
//                System.Console.Out.Flush();

//            }));
//        }

//        private static class FutureKafkaClientSupplier : DefaultKafkaClientSupplier
//        {


//            public IConsumer<byte[], byte[]> getConsumer(Dictionary<string, object> config)
//            {
//                config.Put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, FutureStreamsPartitionAssignor.getName());
//            return new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
//        }
//    }

//    public static class FutureStreamsPartitionAssignor : StreamsPartitionAssignor
//    {

//        public FutureStreamsPartitionAssignor()
//        {
//            usedSubscriptionMetadataVersion = SubscriptionInfo.LATEST_SUPPORTED_VERSION + 1;
//        }


//        public ByteBuffer subscriptionUserData(HashSet<string> topics)
//        {
//            // Adds the following information to subscription
//            // 1. Client UUID (a unique id assigned to an instance of KafkaStreamsThread)
//            // 2. Task ids of previously running tasks
//            // 3. Task ids of valid local states on the client's state directory.

//            TaskManager taskManager = taskManger();
//            HashSet<TaskId> PreviousActiveTasks = taskManager.prevActiveTaskIds();
//            HashSet<TaskId> StandbyTasks = taskManager.cachedTasksIds();
//            StandbyTasks.removeAll(PreviousActiveTasks);
//            FutureSubscriptionInfo data = new FutureSubscriptionInfo(
//                usedSubscriptionMetadataVersion,
//                SubscriptionInfo.LATEST_SUPPORTED_VERSION + 1,
//                taskManager.processId(),
//                PreviousActiveTasks,
//                StandbyTasks,
//                userEndPoint());

//            taskManager.UpdateSubscriptionsFromMetadata(topics);

//            return data.encode();
//        }


//        public void onAssignment(ConsumerPartitionAssignor.Assignment assignment, ConsumerGroupMetadata metadata)
//        {
//            try
//            {
//                base.onAssignment(assignment, metadata);
//                return;
//            }
//            catch (TaskAssignmentException cannotProcessFutureVersion)
//            {
//                // continue
//            }

//            ByteBuffer data = assignment.userData();
//            data.rewind();

//            int usedVersion;
//            try
//            {
//                (DataInputStream in = new DataInputStream(new ByteBufferInputStream(data)));
//                usedVersion = in.readInt();
//            }
//            catch (IOException ex)
//            {
//                throw new TaskAssignmentException("Failed to decode AssignmentInfo", ex);
//            }

//            if (usedVersion > AssignmentInfo.LATEST_SUPPORTED_VERSION + 1)
//            {
//                throw new IllegalStateException("Unknown metadata version: " + usedVersion
//                    + "; latest supported version: " + AssignmentInfo.LATEST_SUPPORTED_VERSION + 1);
//            }

//            AssignmentInfo info = AssignmentInfo.decode(
//                assignment.userData().putInt(0, AssignmentInfo.LATEST_SUPPORTED_VERSION));

//            List<TopicPartition> partitions = new List<>(assignment.partitions());
//            partitions.sort(PARTITION_COMPARATOR);

//            // version 1 field
//            Dictionary<TaskId, HashSet<TopicPartition>> ActiveTasks = new HashMap<>();
//            // version 2 fields
//            Dictionary<TopicPartition, PartitionInfo> topicToPartitionInfo = new HashMap<>();
//            Dictionary<HostInfo, HashSet<TopicPartition>> partitionsByHost;

//            processLatestVersionAssignment(info, partitions, ActiveTasks, topicToPartitionInfo);
//            partitionsByHost = info.partitionsByHost();

//            TaskManager taskManager = taskManger();
//            taskManager.setClusterMetadata(Cluster.empty().withPartitions(topicToPartitionInfo));
//            taskManager.setPartitionsByHostState(partitionsByHost);
//            taskManager.SetAssignmentMetadata(ActiveTasks, info.StandbyTasks());
//            taskManager.UpdateSubscriptionsFromAssignment(partitions);
//        }


//        public GroupAssignment assign(Cluster metadata, GroupSubscription groupSubscription)
//        {
//            Dictionary<string, Subscription> subscriptions = groupSubscription.groupSubscription();
//            Dictionary<string, Assignment> assignment = null;

//            Dictionary<string, Subscription> downgradedSubscriptions = new HashMap<>();
//            foreach (Subscription subscription in subscriptions.values())
//            {
//                SubscriptionInfo info = SubscriptionInfo.decode(subscription.userData());
//                if (info.version() < SubscriptionInfo.LATEST_SUPPORTED_VERSION + 1)
//                {
//                    assignment = base.Assign(metadata, new GroupSubscription(subscriptions)).groupAssignment();
//                    break;
//                }
//            }

//            bool bumpUsedVersion = false;
//            bool bumpSupportedVersion;
//            if (assignment != null)
//            {
//                bumpSupportedVersion = supportedVersions.Count == 1 && supportedVersions.iterator().MoveNext() == SubscriptionInfo.LATEST_SUPPORTED_VERSION + 1;
//            }
//            else
//            {
//                foreach (Map.Entry<string, Subscription> entry in subscriptions)
//                {
//                    Subscription subscription = entry.Value;

//                    SubscriptionInfo info = SubscriptionInfo.decode(subscription.userData()
//                        .putInt(0, SubscriptionInfo.LATEST_SUPPORTED_VERSION)
//                        .putInt(4, SubscriptionInfo.LATEST_SUPPORTED_VERSION));

//                    downgradedSubscriptions.Put(
//                        entry.Key,
//                        new Subscription(
//                            subscription.topics(),
//                            new SubscriptionInfo(
//                                info.processId(),
//                                info.prevTasks(),
//                                info.StandbyTasks(),
//                                info.userEndPoint())
//                                .encode()));
//                }
//                assignment = base.Assign(metadata, new GroupSubscription(downgradedSubscriptions)).groupAssignment();
//                bumpUsedVersion = true;
//                bumpSupportedVersion = true;
//            }

//            Dictionary<string, Assignment> newAssignment = new HashMap<>();
//            foreach (Map.Entry<string, Assignment> entry in assignment)
//            {
//                Assignment singleAssignment = entry.Value;
//                newAssignment.Put(
//                    entry.Key,
//                    new Assignment(
//                        singleAssignment.partitions(),
//                        new FutureAssignmentInfo(
//                            bumpUsedVersion,
//                            bumpSupportedVersion,
//                            singleAssignment.userData())
//                            .encode()));
//            }

//            return new GroupAssignment(newAssignment);
//        }
//    }

//    private static class FutureSubscriptionInfo : SubscriptionInfo
//    {
//        // for testing only; don't apply version checks
//        FutureSubscriptionInfo(int version,
//                                   int latestSupportedVersion,
//                                   UUID processId,
//                                   HashSet<TaskId> prevTasks,
//                                   HashSet<TaskId> StandbyTasks,
//                                   string userEndPoint)
//            : base(version, latestSupportedVersion, processId, prevTasks, StandbyTasks, userEndPoint)
//        {
//        }

//        public ByteBuffer encode()
//        {
//            if (version() <= SubscriptionInfo.LATEST_SUPPORTED_VERSION)
//            {
//                ByteBuffer buf = base.encode();
//                // base.encode() always encodes `LATEST_SUPPORTED_VERSION` as "latest supported version"
//                // need to update to future version
//                buf.putInt(4, latestSupportedVersion());
//                return buf;
//            }

//            ByteBuffer buf = encodeFutureVersion();
//            buf.rewind();
//            return buf;
//        }

//        private ByteBuffer encodeFutureVersion()
//        {
//            byte[] endPointBytes = prepareUserEndPoint();

//            ByteBuffer buf = new ByteBuffer().Allocate(getVersionThreeAndFourByteLength(endPointBytes));

//            buf.putInt(LATEST_SUPPORTED_VERSION + 1); // used version
//            buf.putInt(LATEST_SUPPORTED_VERSION + 1); // supported version
//            encodeClientUUID(buf);
//            encodeTasks(buf, prevTasks());
//            encodeTasks(buf, StandbyTasks());
//            encodeUserEndPoint(buf, endPointBytes);

//            return buf;
//        }

//    }

//    private static class FutureAssignmentInfo : AssignmentInfo
//    {
//        private bool bumpUsedVersion;
//        private bool bumpSupportedVersion;
//        ByteBuffer originalUserMetadata;

//        private FutureAssignmentInfo(bool bumpUsedVersion,
//                                     bool bumpSupportedVersion,
//                                     ByteBuffer bytes)
//        {
//            this.bumpUsedVersion = bumpUsedVersion;
//            this.bumpSupportedVersion = bumpSupportedVersion;
//            originalUserMetadata = bytes;
//        }


//        public ByteBuffer encode()
//        {
//            ByteArrayOutputStream baos = new ByteArrayOutputStream();

//            originalUserMetadata.rewind();

//            try
//            {
//                (DataOutputStream out = new DataOutputStream(baos));
//                if (bumpUsedVersion)
//                {
//                    originalUserMetadata.GetInt(); // discard original used version
//                    System.Console.Out.writeInt(AssignmentInfo.LATEST_SUPPORTED_VERSION + 1);
//                }
//                else
//                {
//                    System.Console.Out.writeInt(originalUserMetadata.GetInt());
//                }
//                if (bumpSupportedVersion)
//                {
//                    originalUserMetadata.GetInt(); // discard original supported version
//                    System.Console.Out.writeInt(AssignmentInfo.LATEST_SUPPORTED_VERSION + 1);
//                }

//                try
//                {
//                    while (true)
//                    {
//                        System.Console.Out.write(originalUserMetadata.Get());
//                    }
//                }
//                catch (BufferUnderflowException expectedWhenAllDataCopied) { }

//                System.Console.Out.Flush();
//                System.Console.Out.Close();

//                return new ByteBuffer().Wrap(baos.toByteArray());
//            }
//            catch (IOException ex)
//            {
//                throw new TaskAssignmentException("Failed to encode AssignmentInfo", ex);
//            }
//        }
//    }
//}
