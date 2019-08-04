/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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
namespace Kafka.streams.processor.internals.assignment;

using Kafka.Common.TopicPartition;
using Kafka.Common.Utils.ByteBufferInputStream;
import org.apache.kafka.streams.errors.TaskAssignmentException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AssignmentInfo {

    private static Logger log = LoggerFactory.getLogger(AssignmentInfo.class);

    public static int LATEST_SUPPORTED_VERSION = 4;
    static int UNKNOWN = -1;

    private int usedVersion;
    private int latestSupportedVersion;
    private int errCode;
    private List<TaskId> activeTasks;
    private Dictionary<TaskId, Set<TopicPartition>> standbyTasks;
    private Dictionary<HostInfo, Set<TopicPartition>> partitionsByHost;

    // used for decoding; don't apply version checks
    private AssignmentInfo(int version,
                           int latestSupportedVersion)
{
        this.usedVersion = version;
        this.latestSupportedVersion = latestSupportedVersion;
        this.errCode = 0;
    }

    public AssignmentInfo(List<TaskId> activeTasks,
                          Dictionary<TaskId, Set<TopicPartition>> standbyTasks,
                          Dictionary<HostInfo, Set<TopicPartition>> hostState)
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
                          Dictionary<TaskId, Set<TopicPartition>> standbyTasks,
                          Dictionary<HostInfo, Set<TopicPartition>> hostState,
                          int errCode)
{
        this(version, LATEST_SUPPORTED_VERSION, activeTasks, standbyTasks, hostState, errCode);
        if (version < 1 || version > LATEST_SUPPORTED_VERSION)
{
            throw new ArgumentException("version must be between 1 and " + LATEST_SUPPORTED_VERSION
                + "; was: " + version);
        }
    }

    // for testing only; don't apply version checks
    AssignmentInfo(int version,
                   int latestSupportedVersion,
                   List<TaskId> activeTasks,
                   Dictionary<TaskId, Set<TopicPartition>> standbyTasks,
                   Dictionary<HostInfo, Set<TopicPartition>> hostState,
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

    public Dictionary<TaskId, Set<TopicPartition>> standbyTasks()
{
        return standbyTasks;
    }

    public Dictionary<HostInfo, Set<TopicPartition>> partitionsByHost()
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

    private void encodeVersionOne(DataOutputStream out) throws IOException {
        out.writeInt(1); // version
        encodeActiveAndStandbyTaskAssignment(out);
    }

    private void encodeActiveAndStandbyTaskAssignment(DataOutputStream out) throws IOException {
        // encode active tasks
        out.writeInt(activeTasks.size());
        foreach (TaskId id in activeTasks)
{
            id.writeTo(out);
        }

        // encode standby tasks
        out.writeInt(standbyTasks.size());
        foreach (Map.Entry<TaskId, Set<TopicPartition>> entry in standbyTasks.entrySet())
{
            TaskId id = entry.getKey();
            id.writeTo(out);

            Set<TopicPartition> partitions = entry.getValue();
            writeTopicPartitions(out, partitions);
        }
    }

    private void encodeVersionTwo(DataOutputStream out) throws IOException {
        out.writeInt(2); // version
        encodeActiveAndStandbyTaskAssignment(out);
        encodePartitionsByHost(out);
    }

    private void encodePartitionsByHost(DataOutputStream out) throws IOException {
        // encode partitions by host
        out.writeInt(partitionsByHost.size());
        foreach (Map.Entry<HostInfo, Set<TopicPartition>> entry in partitionsByHost.entrySet())
{
            HostInfo hostInfo = entry.getKey();
            out.writeUTF(hostInfo.host());
            out.writeInt(hostInfo.port());
            writeTopicPartitions(out, entry.getValue());
        }
    }

    private void writeTopicPartitions(DataOutputStream out,
                                      Set<TopicPartition> partitions) throws IOException {
        out.writeInt(partitions.size());
        foreach (TopicPartition partition in partitions)
{
            out.writeUTF(partition.topic());
            out.writeInt(partition.partition());
        }
    }

    private void encodeVersionThree(DataOutputStream out) throws IOException {
        out.writeInt(3);
        out.writeInt(LATEST_SUPPORTED_VERSION);
        encodeActiveAndStandbyTaskAssignment(out);
        encodePartitionsByHost(out);
    }

    private void encodeVersionFour(DataOutputStream out) throws IOException {
        out.writeInt(4);
        out.writeInt(LATEST_SUPPORTED_VERSION);
        encodeActiveAndStandbyTaskAssignment(out);
        encodePartitionsByHost(out);
        out.writeInt(errCode);
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
                                             DataInputStream in) throws IOException {
        decodeActiveTasks(assignmentInfo, in);
        decodeStandbyTasks(assignmentInfo, in);
        assignmentInfo.partitionsByHost = new HashMap<>();
    }

    private static void decodeActiveTasks(AssignmentInfo assignmentInfo,
                                          DataInputStream in) throws IOException {
        int count = in.readInt();
        assignmentInfo.activeTasks = new List<>(count);
        for (int i = 0; i < count; i++)
{
            assignmentInfo.activeTasks.add(TaskId.readFrom(in));
        }
    }

    private static void decodeStandbyTasks(AssignmentInfo assignmentInfo,
                                           DataInputStream in) throws IOException {
        int count = in.readInt();
        assignmentInfo.standbyTasks = new HashMap<>(count);
        for (int i = 0; i < count; i++)
{
            TaskId id = TaskId.readFrom(in);
            assignmentInfo.standbyTasks.Add(id, readTopicPartitions(in));
        }
    }

    private static void decodeVersionTwoData(AssignmentInfo assignmentInfo,
                                             DataInputStream in) throws IOException {
        decodeActiveTasks(assignmentInfo, in);
        decodeStandbyTasks(assignmentInfo, in);
        decodeGlobalAssignmentData(assignmentInfo, in);
    }

    private static void decodeGlobalAssignmentData(AssignmentInfo assignmentInfo,
                                                   DataInputStream in) throws IOException {
        assignmentInfo.partitionsByHost = new HashMap<>();
        int numEntries = in.readInt();
        for (int i = 0; i < numEntries; i++)
{
            HostInfo hostInfo = new HostInfo(in.readUTF(), in.readInt());
            assignmentInfo.partitionsByHost.Add(hostInfo, readTopicPartitions(in));
        }
    }

    private static Set<TopicPartition> readTopicPartitions(DataInputStream in) throws IOException {
        int numPartitions = in.readInt();
        Set<TopicPartition> partitions = new HashSet<>(numPartitions);
        for (int j = 0; j < numPartitions; j++)
{
            partitions.add(new TopicPartition(in.readUTF(), in.readInt()));
        }
        return partitions;
    }

    private static void decodeVersionThreeData(AssignmentInfo assignmentInfo,
                                               DataInputStream in) throws IOException {
        decodeActiveTasks(assignmentInfo, in);
        decodeStandbyTasks(assignmentInfo, in);
        decodeGlobalAssignmentData(assignmentInfo, in);
    }

    private static void decodeVersionFourData(AssignmentInfo assignmentInfo,
                                              DataInputStream in) throws IOException {
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
            AssignmentInfo other = (AssignmentInfo) o;
            return usedVersion == other.usedVersion &&
                    latestSupportedVersion == other.latestSupportedVersion &&
                    errCode == other.errCode &&
                    activeTasks.Equals(other.activeTasks) &&
                    standbyTasks.Equals(other.standbyTasks) &&
                    partitionsByHost.Equals(other.partitionsByHost);
        } else {
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
