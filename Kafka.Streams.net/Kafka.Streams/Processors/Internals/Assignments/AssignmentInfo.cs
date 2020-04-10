//using Confluent.Kafka;
//using Kafka.Streams.Errors;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.State;
//using Microsoft.Extensions.Logging;
//using System;
//using System.Collections.Generic;
//using System.IO;

//namespace Kafka.Streams.Processors.Internals.Assignments
//{
//    public class AssignmentInfo
//    {
//        private static ILogger log = new LoggerFactory().CreateLogger<AssignmentInfo>();

//        public static int LATEST_SUPPORTED_VERSION = 4;
//        static int UNKNOWN = -1;

//        private int usedVersion;
//        public int latestSupportedVersion { get; }
//        public int errCode { get; }
//        public List<TaskId> activeTasks { get; }
//        public Dictionary<TaskId, HashSet<TopicPartition>> standbyTasks { get; set; }
//        public Dictionary<HostInfo, HashSet<TopicPartition>> partitionsByHost { get; set; }

//        // used for decoding; don't apply version checks
//        private AssignmentInfo(int version,
//                               int latestSupportedVersion)
//        {
//            this.usedVersion = version;
//            this.latestSupportedVersion = latestSupportedVersion;
//            this.errCode = 0;
//        }

//        public AssignmentInfo(List<TaskId> activeTasks,
//                              Dictionary<TaskId, HashSet<TopicPartition>> standbyTasks,
//                              Dictionary<HostInfo, HashSet<TopicPartition>> hostState)
//            : this(LATEST_SUPPORTED_VERSION, activeTasks, standbyTasks, hostState, 0)
//        {
//        }

//        public AssignmentInfo()
//            : this(LATEST_SUPPORTED_VERSION,
//                Collections.emptyList(),
//                Collections.emptyMap(),
//                Collections.emptyMap(),
//                0)
//        {
//        }

//        public AssignmentInfo(int version,
//                              List<TaskId> activeTasks,
//                              Dictionary<TaskId, HashSet<TopicPartition>> standbyTasks,
//                              Dictionary<HostInfo, HashSet<TopicPartition>> hostState,
//                              int errCode)
//            : this(version, LATEST_SUPPORTED_VERSION, activeTasks, standbyTasks, hostState, errCode)
//        {
//            if (version < 1 || version > LATEST_SUPPORTED_VERSION)
//            {
//                throw new System.ArgumentException("version must be between 1 and " + LATEST_SUPPORTED_VERSION
//                    + "; was: " + version);
//            }
//        }

//        // for testing only; don't apply version checks
//        public AssignmentInfo(int version,
//                       int latestSupportedVersion,
//                       List<TaskId> activeTasks,
//                       Dictionary<TaskId, HashSet<TopicPartition>> standbyTasks,
//                       Dictionary<HostInfo, HashSet<TopicPartition>> hostState,
//                       int errCode)
//        {
//            this.usedVersion = version;
//            this.latestSupportedVersion = latestSupportedVersion;
//            this.activeTasks = activeTasks;
//            this.standbyTasks = standbyTasks;
//            this.partitionsByHost = hostState;
//            this.errCode = errCode;
//        }

//        public int version()
//        {
//            return usedVersion;
//        }

//        /**
//         * @throws TaskAssignmentException if method fails to encode the data, e.g., if there is an
//         * IO exception during encoding
//         */
//        public ByteBuffer encode()
//        {
//            ByteArrayOutputStream baos = new ByteArrayOutputStream();
//            try
//            {
//                using (DataOutputStream @out = new DataOutputStream(baos))
//                {
//                    switch (usedVersion)
//                    {
//                        case 1:
//                            encodeVersionOne(@out);
//                            break;
//                        case 2:
//                            encodeVersionTwo(@out);
//                            break;
//                        case 3:
//                            encodeVersionThree(@out);
//                            break;
//                        case 4:
//                            encodeVersionFour(@out);
//                            break;
//                        default:
//                            throw new InvalidOperationException("Unknown metadata version: " + usedVersion
//                                + "; latest supported version: " + LATEST_SUPPORTED_VERSION);
//                    }

//                    @out.Flush();
//                    @out.Close();

//                    return new ByteBuffer().Wrap(baos.toByteArray());
//                }
//            }
//            catch (IOException ex)
//            {
//                throw new TaskAssignmentException("Failed to encode AssignmentInfo", ex);
//            }
//        }

//        private void encodeVersionOne(DataOutputStream @out)
//        {
//            @out.writeInt(1); // version
//            encodeActiveAndStandbyTaskAssignment(@out);
//        }

//        private void encodeActiveAndStandbyTaskAssignment(DataOutputStream @out)
//        {
//            // encode active tasks
//            @out.writeInt(activeTasks.size());
//            foreach (TaskId id in activeTasks)
//            {
//                id.writeTo(@out);
//            }

//            // encode standby tasks
//            @out.writeInt(standbyTasks.size());
//            foreach (KeyValuePair<TaskId, HashSet<TopicPartition>> entry in standbyTasks)
//            {
//                TaskId id = entry.Key;
//                id.writeTo(@out);

//                HashSet<TopicPartition> partitions = entry.Value;
//                writeTopicPartitions(@out, partitions);
//            }
//        }

//        private void encodeVersionTwo(DataOutputStream @out)
//        {
//            @out.writeInt(2); // version
//            encodeActiveAndStandbyTaskAssignment(@out);
//            encodePartitionsByHost(@out);
//        }

//        private void encodePartitionsByHost(DataOutputStream @out)
//        {
//            // encode partitions by host
//            @out.writeInt(partitionsByHost.Count);
//            foreach (KeyValuePair<HostInfo, HashSet<TopicPartition>> entry in partitionsByHost)
//            {
//                HostInfo hostInfo = entry.Key;
//                @out.writeUTF(hostInfo.host);
//                @out.writeInt(hostInfo.port);
//                writeTopicPartitions(@out, entry.Value);
//            }
//        }

//        private void writeTopicPartitions(DataOutputStream @out,
//                                          HashSet<TopicPartition> partitions)
//        {
//            @out.writeInt(partitions.size());
//            foreach (TopicPartition partition in partitions)
//            {
//                @out.writeUTF(partition.Topic);
//                @out.writeInt(partition.Partition);
//            }
//        }

//        private void encodeVersionThree(DataOutputStream output)
//        {
//            output.writeInt(3);
//            output.writeInt(LATEST_SUPPORTED_VERSION);
//            encodeActiveAndStandbyTaskAssignment(output);
//            encodePartitionsByHost(output);
//        }

//        private void encodeVersionFour(DataOutputStream output)
//        {
//            output.writeInt(4);
//            output.writeInt(LATEST_SUPPORTED_VERSION);
//            encodeActiveAndStandbyTaskAssignment(output);
//            encodePartitionsByHost(output);
//            output.writeInt(errCode);
//        }

//        /**
//         * @throws TaskAssignmentException if method fails to decode the data or if the data version is unknown
//         */
//        public static AssignmentInfo decode(ByteBuffer data)
//        {
//            // ensure we are at the beginning of the ByteBuffer
//            data.rewind();

//            try
//            {
//                using (DataInputStream input = new DataInputStream(new ByteBufferInputStream(data)))
//                {
//                    AssignmentInfo assignmentInfo;

//                    int usedVersion = input.readInt();
//                    int latestSupportedVersion;
//                    switch (usedVersion)
//                    {
//                        case 1:
//                            assignmentInfo = new AssignmentInfo(usedVersion, UNKNOWN);
//                            decodeVersionOneData(assignmentInfo, input);
//                            break;
//                        case 2:
//                            assignmentInfo = new AssignmentInfo(usedVersion, UNKNOWN);
//                            decodeVersionTwoData(assignmentInfo, input);
//                            break;
//                        case 3:
//                            latestSupportedVersion = input.readInt();
//                            assignmentInfo = new AssignmentInfo(usedVersion, latestSupportedVersion);
//                            decodeVersionThreeData(assignmentInfo, input);
//                            break;
//                        case 4:
//                            latestSupportedVersion = input.readInt();
//                            assignmentInfo = new AssignmentInfo(usedVersion, latestSupportedVersion);
//                            decodeVersionFourData(assignmentInfo, input);
//                            break;
//                        default:
//                            TaskAssignmentException fatalException = new TaskAssignmentException("Unable to decode assignment data: " +
//                                "used version: " + usedVersion + "; latest supported version: " + LATEST_SUPPORTED_VERSION);
//                            log.LogError(fatalException.getMessage(), fatalException);
//                            throw fatalException;
//                    }

//                    return assignmentInfo;
//                }
//            }
//            catch (IOException ex)
//            {
//                throw new TaskAssignmentException("Failed to decode AssignmentInfo", ex);
//            }
//        }

//        private static void decodeVersionOneData(AssignmentInfo assignmentInfo,
//                                                 DataInputStream input)
//        {
//            decodeActiveTasks(assignmentInfo, input);
//            decodeStandbyTasks(assignmentInfo, input);
//            assignmentInfo.partitionsByHost = new Dictionary<>();
//        }

//        private static void decodeActiveTasks(
//            AssignmentInfo assignmentInfo,
//            DataInputStream input)
//        {
//            int count = input.readInt();
//            assignmentInfo.activeTasks = new List<TaskId>(count);
//            for (int i = 0; i < count; i++)
//            {
//                //assignmentInfo.activeTasks.Add(TaskId.readFrom());
//            }
//        }

//        private static void decodeStandbyTasks(AssignmentInfo assignmentInfo,
//                                               DataInputStream input)
//        {
//            int count = input.readInt();
//            assignmentInfo.standbyTasks = new Dictionary<>(count);
//            for (int i = 0; i < count; i++)
//            {
//                // TaskId id = TaskId.readFrom(in);
//                assignmentInfo.standbyTasks.Add(id, readTopicPartitions(input));
//            }
//        }

//        private static void decodeVersionTwoData(
//            AssignmentInfo assignmentInfo,
//            DataInputStream input)
//        {
//            decodeActiveTasks(assignmentInfo, input);
//            decodeStandbyTasks(assignmentInfo, input);
//            decodeGlobalAssignmentData(assignmentInfo, input);
//        }

//        private static void decodeGlobalAssignmentData(
//            AssignmentInfo assignmentInfo,
//            DataInputStream input)
//        {
//            assignmentInfo.partitionsByHost = new Dictionary<HostInfo, HashSet<TopicPartition>>();
//            int numEntries = input.readInt();
//            for (int i = 0; i < numEntries; i++)
//            {
//                HostInfo hostInfo = new HostInfo(input.readUTF(), input.readInt());
//                assignmentInfo.partitionsByHost.Add(hostInfo, readTopicPartitions(input));
//            }
//        }

//        private static HashSet<TopicPartition> readTopicPartitions(DataInputStream input)
//        {
//            int numPartitions = input.readInt();
//            HashSet<TopicPartition> partitions = new HashSet<TopicPartition>(numPartitions);
//            for (int j = 0; j < numPartitions; j++)
//            {
//                partitions.Add(new TopicPartition(input.readUTF(), input.readInt()));
//            }
//            return partitions;
//        }

//        private static void decodeVersionThreeData(AssignmentInfo assignmentInfo,
//                                                   DataInputStream input)
//        {
//            decodeActiveTasks(assignmentInfo, input);
//            decodeStandbyTasks(assignmentInfo, input);
//            decodeGlobalAssignmentData(assignmentInfo, input);
//        }

//        private static void decodeVersionFourData(AssignmentInfo assignmentInfo,
//                                                  DataInputStream input)
//        {
//            decodeVersionThreeData(assignmentInfo, input);
//            assignmentInfo.errCode = input.readInt();
//        }


//        public int GetHashCode()
//        {
//            return usedVersion ^ latestSupportedVersion ^ activeTasks.GetHashCode() ^ standbyTasks.GetHashCode()
//                ^ partitionsByHost.GetHashCode() ^ errCode;
//        }


//        public bool Equals(object o)
//        {
//            if (o is AssignmentInfo)
//            {
//                AssignmentInfo other = (AssignmentInfo)o;
//                return usedVersion == other.usedVersion &&
//                        latestSupportedVersion == other.latestSupportedVersion &&
//                        errCode == other.errCode &&
//                        activeTasks.Equals(other.activeTasks) &&
//                        standbyTasks.Equals(other.standbyTasks) &&
//                        partitionsByHost.Equals(other.partitionsByHost);
//            }
//            else
//            {

//                return false;
//            }
//        }


//        public string ToString()
//        {
//            return "[version=" + usedVersion
//                + ", supported version=" + latestSupportedVersion
//                + ", active tasks=" + activeTasks
//                + ", standby tasks=" + standbyTasks
//                + ", global assignment=" + partitionsByHost + "]";
//        }

//    }
//}