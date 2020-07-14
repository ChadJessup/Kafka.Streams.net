using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Confluent.Kafka;
using Kafka.Streams.Errors;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.State;
using Kafka.Streams.Tasks;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.Processors.Internals.Assignments
{
    public class StreamsAssignmentProtocolVersions
    {
        public static int UNKNOWN = -1;
        public static int EARLIEST_PROBEABLE_VERSION = 3;
        public static int LATEST_SUPPORTED_VERSION = 7;
    }

    public class AssignmentInfo
    {
        private static ILogger<AssignmentInfo> log = null;

        private int usedVersion;
        private int commonlySupportedVersion;
        private List<TaskId> activeTasks;
        private Dictionary<TaskId, HashSet<TopicPartition>> standbyTasks;
        private Dictionary<HostInfo, HashSet<TopicPartition>> partitionsByHost;
        private Dictionary<HostInfo, HashSet<TopicPartition>> standbyPartitionsByHost;
        private int errCode;
        private long nextRebalanceMs = long.MaxValue;

        // used for decoding and "future consumer" assignments during version probing
        public AssignmentInfo(int version, int commonlySupportedVersion)
            : this(
                  version,
                  commonlySupportedVersion,
                  new List<TaskId>(),
                  new Dictionary<TaskId, HashSet<TopicPartition>>(),
                  new Dictionary<HostInfo, HashSet<TopicPartition>>(),
                  new Dictionary<HostInfo, HashSet<TopicPartition>>(),
                  0)
        {
        }

        public AssignmentInfo(
            int version,
            List<TaskId> activeTasks,
            Dictionary<TaskId, HashSet<TopicPartition>> standbyTasks,
            Dictionary<HostInfo, HashSet<TopicPartition>> partitionsByHost,
            Dictionary<HostInfo, HashSet<TopicPartition>> standbyPartitionsByHost,
            int errCode)
            : this(
                  version,
                  StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION,
                  activeTasks,
                  standbyTasks,
                  partitionsByHost,
                  standbyPartitionsByHost,
                  errCode)
        {
        }

        public AssignmentInfo(
            int version,
            int commonlySupportedVersion,
            List<TaskId> activeTasks,
            Dictionary<TaskId, HashSet<TopicPartition>> standbyTasks,
            Dictionary<HostInfo, HashSet<TopicPartition>> partitionsByHost,
            Dictionary<HostInfo, HashSet<TopicPartition>> standbyPartitionsByHost,
            int errCode)
        {
            this.usedVersion = version;
            this.commonlySupportedVersion = commonlySupportedVersion;
            this.activeTasks = activeTasks;
            this.standbyTasks = standbyTasks;
            this.partitionsByHost = partitionsByHost;
            this.standbyPartitionsByHost = standbyPartitionsByHost;
            this.errCode = errCode;

            if (version < 1 || version > StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION)
            {
                throw new ArgumentException("version must be between 1 and " + StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION
                    + "; was: " + version);
            }
        }

        public void setNextRebalanceTime(long nextRebalanceTimeMs)
        {
            this.nextRebalanceMs = nextRebalanceTimeMs;
        }

        public int version()
        {
            return this.usedVersion;
        }

        /**
         * @throws TaskAssignmentException if method fails to encode the data, e.g., if there is an
         * IO exception during encoding
         */
        public ByteBuffer Encode()
        {
            using var baos = new MemoryStream();

            try
            {
                using var bw = new BinaryWriter(baos);

                switch (this.usedVersion)
                {
                    case 1:
                        bw.Write(this.usedVersion); // version
                        this.encodeActiveAndStandbyTaskAssignment(baos);
                        break;
                    case 2:
                        bw.Write(this.usedVersion); // version
                        this.encodeActiveAndStandbyTaskAssignment(baos);
                        this.EncodePartitionsByHost(baos);
                        break;
                    case 3:
                        bw.Write(this.usedVersion);
                        bw.Write(this.commonlySupportedVersion);
                        this.encodeActiveAndStandbyTaskAssignment(baos);
                        this.EncodePartitionsByHost(baos);
                        break;
                    case 4:
                        bw.Write(this.usedVersion);
                        bw.Write(this.commonlySupportedVersion);
                        this.encodeActiveAndStandbyTaskAssignment(baos);
                        this.EncodePartitionsByHost(baos);
                        bw.Write(this.errCode);
                        break;
                    case 5:
                        bw.Write(this.usedVersion);
                        bw.Write(this.commonlySupportedVersion);
                        this.encodeActiveAndStandbyTaskAssignment(baos);
                        this.encodePartitionsByHostAsDictionary(baos);
                        bw.Write(this.errCode);
                        break;
                    case 6:
                        bw.Write(this.usedVersion);
                        bw.Write(this.commonlySupportedVersion);
                        this.encodeActiveAndStandbyTaskAssignment(baos);
                        this.encodeActiveAndStandbyHostPartitions(baos);
                        bw.Write(this.errCode);
                        break;
                    case 7:
                        bw.Write(this.usedVersion);
                        bw.Write(this.commonlySupportedVersion);
                        this.encodeActiveAndStandbyTaskAssignment(baos);
                        this.encodeActiveAndStandbyHostPartitions(baos);
                        bw.Write(this.errCode);
                        bw.Write(this.nextRebalanceMs);
                        break;
                    default:
                        throw new InvalidProgramException("Unknown metadata version: " + this.usedVersion
                                + "; latest commonly supported version: " + this.commonlySupportedVersion);
                }

                baos.Flush();
                baos.Close();

                return new ByteBuffer().Wrap(baos.ToArray());
            }
            catch (IOException ex)
            {
                throw new TaskAssignmentException("Failed to encode AssignmentInfo", ex);
            }
        }

        private void encodeActiveAndStandbyTaskAssignment(Stream output)
        {
            using var bw = new BinaryWriter(output);

            // encode active tasks
            bw.Write(this.activeTasks.Count);
            foreach (TaskId id in this.activeTasks)
            {
                id.WriteTo(output);
            }

            // encode standby tasks
            bw.Write(this.standbyTasks.Count);
            foreach (var entry in this.standbyTasks)
            {
                TaskId id = entry.Key;
                id.WriteTo(output);

                HashSet<TopicPartition> partitions = entry.Value;
                this.writeTopicPartitions(output, partitions);
            }
        }

        private void EncodePartitionsByHost(Stream output)
        {
            using var bw = new BinaryWriter(output);

            // encode partitions by host
            bw.Write(this.partitionsByHost.Count);
            foreach (var entry in this.partitionsByHost)
            {
                this.writeHostInfo(output, entry.Key);
                this.writeTopicPartitions(output, entry.Value);
            }
        }

        private void encodeHostPartitionMapUsingDictionary(
            Stream output,
            Dictionary<string, int> topicNameDict,
            Dictionary<HostInfo, HashSet<TopicPartition>> hostPartitionMap)
        {
            using var bw = new BinaryWriter(output);

            // encode partitions by host
            bw.Write(hostPartitionMap.Count);

            // Write the topic index, partition
            foreach (var entry in hostPartitionMap)
            {
                this.writeHostInfo(output, entry.Key);
                bw.Write(entry.Value.Count);
                foreach (TopicPartition partition in entry.Value)
                {
                    bw.Write(topicNameDict[partition.Topic]);
                    bw.Write(partition.Partition);
                }
            }
        }

        private Dictionary<string, int> encodeTopicDictionaryAndGet(
            Stream output,
            HashSet<TopicPartition> topicPartitions)
        {
            using var bw = new BinaryWriter(output);

            // Build a dictionary to encode topicNames
            int topicIndex = 0;
            Dictionary<string, int> topicNameDict = new Dictionary<string, int>();
            foreach (TopicPartition topicPartition in topicPartitions)
            {
                if (!topicNameDict.ContainsKey(topicPartition.Topic))
                {
                    topicNameDict.Add(topicPartition.Topic, topicIndex++);
                }
            }

            // write the topic name dictionary output
            bw.Write(topicNameDict.Count);
            foreach (var entry in topicNameDict)
            {
                bw.Write(entry.Value);
                bw.Write(entry.Key);
            }

            return topicNameDict;
        }

        private void encodePartitionsByHostAsDictionary(Stream output)
        {
            HashSet<TopicPartition> allTopicPartitions = this.partitionsByHost.Values
                .SelectMany(t => t)
                .ToHashSet();

            Dictionary<string, int> topicNameDict = this.encodeTopicDictionaryAndGet(output, allTopicPartitions);
            this.encodeHostPartitionMapUsingDictionary(output, topicNameDict, this.partitionsByHost);
        }

        private void encodeActiveAndStandbyHostPartitions(Stream output)
        {
            HashSet<TopicPartition> allTopicPartitions =
                this.partitionsByHost.Values.Concat(this.standbyPartitionsByHost.Values)
                .SelectMany(t => t)
                .ToHashSet();

            Dictionary<string, int> topicNameDict = this.encodeTopicDictionaryAndGet(output, allTopicPartitions);
            this.encodeHostPartitionMapUsingDictionary(output, topicNameDict, this.partitionsByHost);
            this.encodeHostPartitionMapUsingDictionary(output, topicNameDict, this.standbyPartitionsByHost);
        }

        private void writeHostInfo(Stream output, HostInfo hostInfo)
        {
            using var bw = new BinaryWriter(output);

            bw.Write(hostInfo.host);
            bw.Write(hostInfo.port);
        }

        private void writeTopicPartitions(Stream output, HashSet<TopicPartition> partitions)
        {
            using var bw = new BinaryWriter(output);

            bw.Write(partitions.Count);
            foreach (TopicPartition partition in partitions)
            {
                bw.Write(partition.Topic);
                bw.Write(partition.Partition);
            }
        }

        /**
         * @throws TaskAssignmentException if method fails to decode the data or if the data version is unknown
         */
        public static AssignmentInfo decode(ByteBuffer data)
        {
            // ensure we are at the beginning of the ByteBuffer
            data.Rewind();

            try
            {
                Stream stream = new MemoryStream(data.Array());
                using var br = new BinaryReader(stream);
                AssignmentInfo assignmentInfo;

                int usedVersion = br.ReadInt32();
                int commonlySupportedVersion;
                switch (usedVersion)
                {
                    case 1:
                        assignmentInfo = new AssignmentInfo(usedVersion, StreamsAssignmentProtocolVersions.UNKNOWN);
                        decodeActiveTasks(assignmentInfo, stream);
                        decodeStandbyTasks(assignmentInfo, stream);
                        assignmentInfo.partitionsByHost = new Dictionary<HostInfo, HashSet<TopicPartition>>();
                        break;
                    case 2:
                        assignmentInfo = new AssignmentInfo(usedVersion, StreamsAssignmentProtocolVersions.UNKNOWN);
                        decodeActiveTasks(assignmentInfo, stream);
                        decodeStandbyTasks(assignmentInfo, stream);
                        decodePartitionsByHost(assignmentInfo, stream);
                        break;
                    case 3:
                        commonlySupportedVersion = br.ReadInt32();
                        assignmentInfo = new AssignmentInfo(usedVersion, commonlySupportedVersion);
                        decodeActiveTasks(assignmentInfo, stream);
                        decodeStandbyTasks(assignmentInfo, stream);
                        decodePartitionsByHost(assignmentInfo, stream);
                        break;
                    case 4:
                        commonlySupportedVersion = br.ReadInt32();
                        assignmentInfo = new AssignmentInfo(usedVersion, commonlySupportedVersion);
                        decodeActiveTasks(assignmentInfo, stream);
                        decodeStandbyTasks(assignmentInfo, stream);
                        decodePartitionsByHost(assignmentInfo, stream);
                        assignmentInfo.errCode = br.ReadInt32();
                        break;
                    case 5:
                        commonlySupportedVersion = br.ReadInt32();
                        assignmentInfo = new AssignmentInfo(usedVersion, commonlySupportedVersion);
                        decodeActiveTasks(assignmentInfo, stream);
                        decodeStandbyTasks(assignmentInfo, stream);
                        decodePartitionsByHostUsingDictionary(assignmentInfo, stream);
                        assignmentInfo.errCode = br.ReadInt32();
                        break;
                    case 6:
                        commonlySupportedVersion = br.ReadInt32();
                        assignmentInfo = new AssignmentInfo(usedVersion, commonlySupportedVersion);
                        decodeActiveTasks(assignmentInfo, stream);
                        decodeStandbyTasks(assignmentInfo, stream);
                        decodeActiveAndStandbyHostPartitions(assignmentInfo, stream);
                        assignmentInfo.errCode = br.ReadInt32();
                        break;
                    case 7:
                        commonlySupportedVersion = br.ReadInt32();
                        assignmentInfo = new AssignmentInfo(usedVersion, commonlySupportedVersion);
                        decodeActiveTasks(assignmentInfo, stream);
                        decodeStandbyTasks(assignmentInfo, stream);
                        decodeActiveAndStandbyHostPartitions(assignmentInfo, stream);
                        assignmentInfo.errCode = br.ReadInt32();
                        assignmentInfo.nextRebalanceMs = br.ReadInt64();
                        break;
                    default:
                        TaskAssignmentException fatalException = new TaskAssignmentException("Unable to decode assignment data: " +
                            "used version: " + usedVersion + "; latest supported version: " + StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION);
                        log.LogError(fatalException.Message, fatalException);
                        throw fatalException;
                }

                return assignmentInfo;
            }
            catch (IOException ex)
            {
                throw new TaskAssignmentException("Failed to decode AssignmentInfo", ex);
            }
        }

        private static void decodeActiveTasks(
            AssignmentInfo assignmentInfo,
            Stream stream)
        {
            using var br = new BinaryReader(stream);
            int count = br.ReadInt32();
            assignmentInfo.activeTasks = new List<TaskId>(count);
            for (int i = 0; i < count; i++)
            {
                assignmentInfo.activeTasks.Add(TaskId.ReadFrom(stream));
            }
        }

        private static void decodeStandbyTasks(AssignmentInfo assignmentInfo, Stream stream)
        {
            using var br = new BinaryReader(stream);
            int count = br.ReadInt32();

            assignmentInfo.standbyTasks = new Dictionary<TaskId, HashSet<TopicPartition>>(count);
            for (int i = 0; i < count; i++)
            {
                TaskId id = TaskId.ReadFrom(stream);
                assignmentInfo.standbyTasks.Add(id, readTopicPartitions(stream));
            }
        }

        private static void decodePartitionsByHost(AssignmentInfo assignmentInfo, Stream stream)
        {
            using var br = new BinaryReader(stream);

            assignmentInfo.partitionsByHost = new Dictionary<HostInfo, HashSet<TopicPartition>>();
            int numEntries = br.ReadInt32();
            for (int i = 0; i < numEntries; i++)
            {
                HostInfo hostInfo = new HostInfo(br.ReadString(), br.ReadInt32());
                assignmentInfo.partitionsByHost.Add(hostInfo, readTopicPartitions(stream));
            }
        }

        private static HashSet<TopicPartition> readTopicPartitions(Stream stream)
        {
            using var br = new BinaryReader(stream);
            int numPartitions = br.ReadInt32();
            HashSet<TopicPartition> partitions = new HashSet<TopicPartition>(numPartitions);
            for (int j = 0; j < numPartitions; j++)
            {
                partitions.Add(new TopicPartition(br.ReadString(), br.ReadInt32()));
            }
            return partitions;
        }

        private static Dictionary<int, string> decodeTopicIndexAndGet(Stream stream)
        {
            using var br = new BinaryReader(stream);

            int dictSize = br.ReadInt32();
            Dictionary<int, string> topicIndexDict = new Dictionary<int, string>(dictSize);
            for (int i = 0; i < dictSize; i++)
            {
                topicIndexDict.Add(br.ReadInt32(), br.ReadString());
            }
            return topicIndexDict;
        }

        private static Dictionary<HostInfo, HashSet<TopicPartition>> decodeHostPartitionMapUsingDictionary(
            Stream stream,
            Dictionary<int, string> topicIndexDict)
        {
            using var br = new BinaryReader(stream);

            Dictionary<HostInfo, HashSet<TopicPartition>> hostPartitionMap = new Dictionary<HostInfo, HashSet<TopicPartition>>();
            int numEntries = br.ReadInt32();
            for (int i = 0; i < numEntries; i++)
            {
                HostInfo hostInfo = new HostInfo(br.ReadString(), br.ReadInt32());
                hostPartitionMap.Add(hostInfo, ReadTopicPartitions(stream, topicIndexDict));
            }

            return hostPartitionMap;
        }

        private static void decodePartitionsByHostUsingDictionary(
            AssignmentInfo assignmentInfo,
            Stream stream)
        {
            Dictionary<int, string> topicIndexDict = decodeTopicIndexAndGet(stream);
            assignmentInfo.partitionsByHost = decodeHostPartitionMapUsingDictionary(stream, topicIndexDict);
        }

        private static void decodeActiveAndStandbyHostPartitions(
            AssignmentInfo assignmentInfo,
            Stream stream)
        {
            Dictionary<int, string> topicIndexDict = decodeTopicIndexAndGet(stream);
            assignmentInfo.partitionsByHost = decodeHostPartitionMapUsingDictionary(stream, topicIndexDict);
            assignmentInfo.standbyPartitionsByHost = decodeHostPartitionMapUsingDictionary(stream, topicIndexDict);
        }

        private static HashSet<TopicPartition> ReadTopicPartitions(
            Stream stream,
            Dictionary<int, string> topicIndexDict)
        {
            using var br = new BinaryReader(stream);

            int numPartitions = br.ReadInt32();
            HashSet<TopicPartition> partitions = new HashSet<TopicPartition>(numPartitions);
            for (int j = 0; j < numPartitions; j++)
            {
                partitions.Add(new TopicPartition(topicIndexDict[br.ReadInt32()], br.ReadInt32()));
            }

            return partitions;
        }

        public override int GetHashCode()
        {
            int hostMapHashCode = this.partitionsByHost.GetHashCode() ^ this.standbyPartitionsByHost.GetHashCode();
            return this.usedVersion
                ^ this.commonlySupportedVersion
                ^ this.activeTasks.GetHashCode()
                ^ this.standbyTasks.GetHashCode()
                ^ hostMapHashCode
                ^ this.errCode;
        }

        public override bool Equals(object o)
        {
            if (o is AssignmentInfo other)
            {
                return this.usedVersion == other.usedVersion
                    && this.commonlySupportedVersion == other.commonlySupportedVersion
                    && this.errCode == other.errCode
                    && this.activeTasks.Equals(other.activeTasks)
                    && this.standbyTasks.Equals(other.standbyTasks)
                    && this.partitionsByHost.Equals(other.partitionsByHost)
                    && this.standbyPartitionsByHost.Equals(other.standbyPartitionsByHost);
            }
            else
            {
                return false;
            }
        }

        public override string ToString()
        {
            return "[version=" + this.usedVersion
                + ", supported version=" + this.commonlySupportedVersion
                + ", active tasks=" + this.activeTasks
                + ", standby tasks=" + this.standbyTasks
                + ", partitions by host=" + this.partitionsByHost
                + ", standbyPartitions by host=" + this.standbyPartitionsByHost
                + "]";
        }
    }
}
