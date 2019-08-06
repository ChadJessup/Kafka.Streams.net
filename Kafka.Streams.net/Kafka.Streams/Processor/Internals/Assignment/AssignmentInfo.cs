using Microsoft.Extensions.Logging;
using System.Collections.Generic;

namespace Kafka.Streams.Processor.Internals.Assignment
{
    public class AssignmentInfo
    {
        private static ILogger log = new LoggerFactory().CreateLogger<AssignmentInfo>();

        public static int LATEST_SUPPORTED_VERSION = 4;
        static int UNKNOWN = -1;

        private int usedVersion;
        private int latestSupportedVersion;
        private int errCode;
        private List<TaskId> activeTasks;
        private Dictionary<TaskId, HashSet<TopicPartition>> standbyTasks;
        private Dictionary<HostInfo, HashSet<TopicPartition>> partitionsByHost;

        // used for decoding; don't apply version checks
        private AssignmentInfo(int version,
                               int latestSupportedVersion)
        {
            this.usedVersion = version;
            this.latestSupportedVersion = latestSupportedVersion;
            this.errCode = 0;
        }

        public AssignmentInfo(List<TaskId> activeTasks,
                              Dictionary<TaskId, HashSet<TopicPartition>> standbyTasks,
                              Dictionary<HostInfo, HashSet<TopicPartition>> hostState)
        {
            this(LATEST_SUPPORTED_VERSION, activeTasks, standbyTasks, hostState, 0);
        }

        public AssignmentInfo()
        {
            this(LATEST_SUPPORTED_VERSION,
                Collections.emptyList(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                0);
        }

        public AssignmentInfo(int version,
                              List<TaskId> activeTasks,
                              Dictionary<TaskId, HashSet<TopicPartition>> standbyTasks,
                              Dictionary<HostInfo, HashSet<TopicPartition>> hostState,
                              int errCode)
        {
            this(version, LATEST_SUPPORTED_VERSION, activeTasks, standbyTasks, hostState, errCode);
            if (version < 1 || version > LATEST_SUPPORTED_VERSION)
            {
                throw new System.ArgumentException("version must be between 1 and " + LATEST_SUPPORTED_VERSION
                    + "; was: " + version);
            }
        }

        // for testing only; don't apply version checks
        AssignmentInfo(int version,
                       int latestSupportedVersion,
                       List<TaskId> activeTasks,
                       Dictionary<TaskId, HashSet<TopicPartition>> standbyTasks,
                       Dictionary<HostInfo, HashSet<TopicPartition>> hostState,
                       int errCode)
        {
            this.usedVersion = version;
            this.latestSupportedVersion = latestSupportedVersion;
            this.activeTasks = activeTasks;
            this.standbyTasks = standbyTasks;
            this.partitionsByHost = hostState;
            this.errCode = errCode;
        }

        public int version()
        {
            return usedVersion;
        }

        public int errCode()
        {
            return errCode;
        }

        public int latestSupportedVersion()
        {
            return latestSupportedVersion;
        }

        public List<TaskId> activeTasks()
        {
            return activeTasks;
        }

        public Dictionary<TaskId, HashSet<TopicPartition>> standbyTasks()
        {
            return standbyTasks;
        }

        public Dictionary<HostInfo, HashSet<TopicPartition>> partitionsByHost()
        {
            return partitionsByHost;
        }

        /**
         * @throws TaskAssignmentException if method fails to encode the data, e.g., if there is an
         * IO exception during encoding
         */
        public ByteBuffer encode()
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            try (DataOutputStream out = new DataOutputStream(baos))
{
                switch (usedVersion)
                {
                    case 1:
                        encodeVersionOne(out);
                        break;
                    case 2:
                        encodeVersionTwo(out);
                        break;
                    case 3:
                        encodeVersionThree(out);
                        break;
                    case 4:
                        encodeVersionFour(out);
                        break;
                    default:
                        throw new InvalidOperationException("Unknown metadata version: " + usedVersion
                            + "; latest supported version: " + LATEST_SUPPORTED_VERSION);
                }

            out.flush();
            out.close();

                return ByteBuffer.wrap(baos.toByteArray());
            } catch (IOException ex)
            {
                throw new TaskAssignmentException("Failed to encode AssignmentInfo", ex);
            }
        }

        private void encodeVersionOne(DataOutputStream out)
        {
        out.writeInt(1); // version
            encodeActiveAndStandbyTaskAssignment(out);
        }

        private void encodeActiveAndStandbyTaskAssignment(DataOutputStream out)
        {
        // encode active tasks
        out.writeInt(activeTasks.size());
            foreach (TaskId id in activeTasks)
            {
                id.writeTo(out);
            }

        // encode standby tasks
        out.writeInt(standbyTasks.size());
            foreach (KeyValuePair<TaskId, HashSet<TopicPartition>> entry in standbyTasks.entrySet())
            {
                TaskId id = entry.Key;
                id.writeTo(out);

                HashSet<TopicPartition> partitions = entry.Value;
                writeTopicPartitions(out, partitions);
            }
        }

        private void encodeVersionTwo(DataOutputStream out)
        {
        out.writeInt(2); // version
            encodeActiveAndStandbyTaskAssignment(out);
            encodePartitionsByHost(out);
        }

        private void encodePartitionsByHost(DataOutputStream out)
        {
        // encode partitions by host
        out.writeInt(partitionsByHost.size());
            foreach (KeyValuePair<HostInfo, HashSet<TopicPartition>> entry in partitionsByHost.entrySet())
            {
                HostInfo hostInfo = entry.Key;
            out.writeUTF(hostInfo.host());
            out.writeInt(hostInfo.port());
                writeTopicPartitions(out, entry.Value);
            }
        }

        private void writeTopicPartitions(DataOutputStream out,
                                          HashSet<TopicPartition> partitions)
        {
        out.writeInt(partitions.size());
            foreach (TopicPartition partition in partitions)
            {
            out.writeUTF(partition.Topic);
            out.writeInt(partition.partition());
            }
        }

        private void encodeVersionThree(DataOutputStream output)
        {
            output.writeInt(3);
            output.writeInt(LATEST_SUPPORTED_VERSION);
            encodeActiveAndStandbyTaskAssignment(output);
            encodePartitionsByHost(output);
        }

        private void encodeVersionFour(DataOutputStream output)
        {
            output.writeInt(4);
            output.writeInt(LATEST_SUPPORTED_VERSION);
            encodeActiveAndStandbyTaskAssignment(output);
            encodePartitionsByHost(output);
            output.writeInt(errCode);
        }

        /**
         * @throws TaskAssignmentException if method fails to decode the data or if the data version is unknown
         */
        public static AssignmentInfo decode(ByteBuffer data)
        {
            // ensure we are at the beginning of the ByteBuffer
            data.rewind();

            try (DataInputStream in = new DataInputStream(new ByteBufferInputStream(data)))
{
                AssignmentInfo assignmentInfo;

                int usedVersion = in.readInt();
                int latestSupportedVersion;
                switch (usedVersion)
                {
                    case 1:
                        assignmentInfo = new AssignmentInfo(usedVersion, UNKNOWN);
                        decodeVersionOneData(assignmentInfo, in);
                        break;
                    case 2:
                        assignmentInfo = new AssignmentInfo(usedVersion, UNKNOWN);
                        decodeVersionTwoData(assignmentInfo, in);
                        break;
                    case 3:
                        latestSupportedVersion = in.readInt();
                        assignmentInfo = new AssignmentInfo(usedVersion, latestSupportedVersion);
                        decodeVersionThreeData(assignmentInfo, in);
                        break;
                    case 4:
                        latestSupportedVersion = in.readInt();
                        assignmentInfo = new AssignmentInfo(usedVersion, latestSupportedVersion);
                        decodeVersionFourData(assignmentInfo, in);
                        break;
                    default:
                        TaskAssignmentException fatalException = new TaskAssignmentException("Unable to decode assignment data: " +
                            "used version: " + usedVersion + "; latest supported version: " + LATEST_SUPPORTED_VERSION);
                        log.LogError(fatalException.getMessage(), fatalException);
                        throw fatalException;
                }

                return assignmentInfo;
            } catch (IOException ex)
            {
                throw new TaskAssignmentException("Failed to decode AssignmentInfo", ex);
            }
        }

        private static void decodeVersionOneData(AssignmentInfo assignmentInfo,
                                                 DataInputStream in)
        {
            decodeActiveTasks(assignmentInfo, in);
            decodeStandbyTasks(assignmentInfo, in);
            assignmentInfo.partitionsByHost = new HashMap<>();
        }

        private static void decodeActiveTasks(AssignmentInfo assignmentInfo,
                                              DataInputStream in)
        {
            int count = in.readInt();
            assignmentInfo.activeTasks = new List<>(count);
            for (int i = 0; i < count; i++)
            {
                assignmentInfo.activeTasks.Add(TaskId.readFrom(in));
            }
        }

        private static void decodeStandbyTasks(AssignmentInfo assignmentInfo,
                                               DataInputStream in)
        {
            int count = in.readInt();
            assignmentInfo.standbyTasks = new HashMap<>(count);
            for (int i = 0; i < count; i++)
            {
                TaskId id = TaskId.readFrom(in);
                assignmentInfo.standbyTasks.Add(id, readTopicPartitions(in));
            }
        }

        private static void decodeVersionTwoData(AssignmentInfo assignmentInfo,
                                                 DataInputStream in)
        {
            decodeActiveTasks(assignmentInfo, in);
            decodeStandbyTasks(assignmentInfo, in);
            decodeGlobalAssignmentData(assignmentInfo, in);
        }

        private static void decodeGlobalAssignmentData(AssignmentInfo assignmentInfo,
                                                       DataInputStream in)
        {
            assignmentInfo.partitionsByHost = new HashMap<>();
            int numEntries = in.readInt();
            for (int i = 0; i < numEntries; i++)
            {
                HostInfo hostInfo = new HostInfo(in.readUTF(), in.readInt());
                assignmentInfo.partitionsByHost.Add(hostInfo, readTopicPartitions(in));
            }
        }

        private static HashSet<TopicPartition> readTopicPartitions(DataInputStream in)
        {
            int numPartitions = in.readInt();
            HashSet<TopicPartition> partitions = new HashSet<>(numPartitions);
            for (int j = 0; j < numPartitions; j++)
            {
                partitions.Add(new TopicPartition(in.readUTF(), in.readInt()));
            }
            return partitions;
        }

        private static void decodeVersionThreeData(AssignmentInfo assignmentInfo,
                                                   DataInputStream in)
        {
            decodeActiveTasks(assignmentInfo, in);
            decodeStandbyTasks(assignmentInfo, in);
            decodeGlobalAssignmentData(assignmentInfo, in);
        }

        private static void decodeVersionFourData(AssignmentInfo assignmentInfo,
                                                  DataInputStream in)
        {
            decodeVersionThreeData(assignmentInfo, in);
            assignmentInfo.errCode = in.readInt();
        }


        public int GetHashCode()
        {
            return usedVersion ^ latestSupportedVersion ^ activeTasks.GetHashCode() ^ standbyTasks.GetHashCode()
                ^ partitionsByHost.GetHashCode() ^ errCode;
        }


        public bool Equals(object o)
        {
            if (o is AssignmentInfo)
            {
                AssignmentInfo other = (AssignmentInfo)o;
                return usedVersion == other.usedVersion &&
                        latestSupportedVersion == other.latestSupportedVersion &&
                        errCode == other.errCode &&
                        activeTasks.Equals(other.activeTasks) &&
                        standbyTasks.Equals(other.standbyTasks) &&
                        partitionsByHost.Equals(other.partitionsByHost);
            }
            else
            {

                return false;
            }
        }


        public string ToString()
        {
            return "[version=" + usedVersion
                + ", supported version=" + latestSupportedVersion
                + ", active tasks=" + activeTasks
                + ", standby tasks=" + standbyTasks
                + ", global assignment=" + partitionsByHost + "]";
        }

    }
}