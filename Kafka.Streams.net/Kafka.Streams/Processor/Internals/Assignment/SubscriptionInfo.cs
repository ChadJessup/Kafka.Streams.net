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
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.Processor.Internals.assignment;













public SubscriptionInfo
{


    private static ILogger log = new LoggerFactory().CreateLogger<SubscriptionInfo>();

    public static int LATEST_SUPPORTED_VERSION = 4;
    static int UNKNOWN = -1;

    private int usedVersion;
    private int latestSupportedVersion;
    private UUID processId;
    private HashSet<TaskId> prevTasks;
    private HashSet<TaskId> standbyTasks;
    private string userEndPoint;

    // used for decoding; don't apply version checks
    private SubscriptionInfo(int version,
                             int latestSupportedVersion)
{
        this.usedVersion = version;
        this.latestSupportedVersion = latestSupportedVersion;
    }

    public SubscriptionInfo(UUID processId,
                            HashSet<TaskId> prevTasks,
                            HashSet<TaskId> standbyTasks,
                            string userEndPoint)
{
        this(LATEST_SUPPORTED_VERSION, processId, prevTasks, standbyTasks, userEndPoint);
    }

    public SubscriptionInfo(int version,
                            UUID processId,
                            HashSet<TaskId> prevTasks,
                            HashSet<TaskId> standbyTasks,
                            string userEndPoint)
{
        this(version, LATEST_SUPPORTED_VERSION, processId, prevTasks, standbyTasks, userEndPoint);

        if (version < 1 || version > LATEST_SUPPORTED_VERSION)
{
            throw new System.ArgumentException("version must be between 1 and " + LATEST_SUPPORTED_VERSION
                + "; was: " + version);
        }
    }

    // for testing only; don't apply version checks
    protected SubscriptionInfo(int version,
                               int latestSupportedVersion,
                               UUID processId,
                               HashSet<TaskId> prevTasks,
                               HashSet<TaskId> standbyTasks,
                               string userEndPoint)
{
        this.usedVersion = version;
        this.latestSupportedVersion = latestSupportedVersion;
        this.processId = processId;
        this.prevTasks = prevTasks;
        this.standbyTasks = standbyTasks;
        this.userEndPoint = userEndPoint;
    }

    public int version()
{
        return usedVersion;
    }

    public int latestSupportedVersion()
{
        return latestSupportedVersion;
    }

    public UUID processId()
{
        return processId;
    }

    public HashSet<TaskId> prevTasks()
{
        return prevTasks;
    }

    public HashSet<TaskId> standbyTasks()
{
        return standbyTasks;
    }

    public string userEndPoint()
{
        return userEndPoint;
    }

    /**
     * @throws TaskAssignmentException if method fails to encode the data
     */
    public ByteBuffer encode()
{
        ByteBuffer buf;

        switch (usedVersion)
{
            case 1:
                buf = encodeVersionOne();
                break;
            case 2:
                buf = encodeVersionTwo();
                break;
            case 3:
                buf = encodeVersionThree();
                break;
            case 4:
                buf = encodeVersionFour();
                break;
            default:
                throw new InvalidOperationException("Unknown metadata version: " + usedVersion
                    + "; latest supported version: " + LATEST_SUPPORTED_VERSION);
        }

        buf.rewind();
        return buf;
    }

    private ByteBuffer encodeVersionOne()
{
        ByteBuffer buf = ByteBuffer.allocate(getVersionOneByteLength());

        buf.putInt(1); // version
        encodeClientUUID(buf);
        encodeTasks(buf, prevTasks);
        encodeTasks(buf, standbyTasks);

        return buf;
    }

    private int getVersionOneByteLength()
{
        return 4 + // version
               16 + // client ID
               4 + prevTasks.size() * 8 + // length + prev tasks
               4 + standbyTasks.size() * 8; // length + standby tasks
    }

    protected void encodeClientUUID(ByteBuffer buf)
{
        buf.putLong(processId.getMostSignificantBits());
        buf.putLong(processId.getLeastSignificantBits());
    }

    protected void encodeTasks(ByteBuffer buf,
                               Collection<TaskId> taskIds)
{
        buf.putInt(taskIds.size());
        foreach (TaskId id in taskIds)
{
            id.writeTo(buf);
        }
    }

    protected byte[] prepareUserEndPoint()
{
        if (userEndPoint == null)
{
            return new byte[0];
        } else
{

            return userEndPoint.getBytes(System.Text.Encoding.UTF8);
        }
    }

    private ByteBuffer encodeVersionTwo()
{
        byte[] endPointBytes = prepareUserEndPoint();

        ByteBuffer buf = ByteBuffer.allocate(getVersionTwoByteLength(endPointBytes));

        buf.putInt(2); // version
        encodeClientUUID(buf);
        encodeTasks(buf, prevTasks);
        encodeTasks(buf, standbyTasks);
        encodeUserEndPoint(buf, endPointBytes);

        return buf;
    }

    private int getVersionTwoByteLength(byte[] endPointBytes)
{
        return 4 + // version
               16 + // client ID
               4 + prevTasks.size() * 8 + // length + prev tasks
               4 + standbyTasks.size() * 8 + // length + standby tasks
               4 + endPointBytes.Length; // length + userEndPoint
    }

    protected void encodeUserEndPoint(ByteBuffer buf,
                                      byte[] endPointBytes)
{
        if (endPointBytes != null)
{
            buf.putInt(endPointBytes.Length);
            buf.Add(endPointBytes);
        }
    }

    private ByteBuffer encodeVersionThree()
{
        byte[] endPointBytes = prepareUserEndPoint();

        ByteBuffer buf = ByteBuffer.allocate(getVersionThreeAndFourByteLength(endPointBytes));

        buf.putInt(3); // used version
        buf.putInt(LATEST_SUPPORTED_VERSION); // supported version
        encodeClientUUID(buf);
        encodeTasks(buf, prevTasks);
        encodeTasks(buf, standbyTasks);
        encodeUserEndPoint(buf, endPointBytes);

        return buf;
    }

    private ByteBuffer encodeVersionFour()
{
        byte[] endPointBytes = prepareUserEndPoint();

        ByteBuffer buf = ByteBuffer.allocate(getVersionThreeAndFourByteLength(endPointBytes));

        buf.putInt(4); // used version
        buf.putInt(LATEST_SUPPORTED_VERSION); // supported version
        encodeClientUUID(buf);
        encodeTasks(buf, prevTasks);
        encodeTasks(buf, standbyTasks);
        encodeUserEndPoint(buf, endPointBytes);

        return buf;
    }

    protected int getVersionThreeAndFourByteLength(byte[] endPointBytes)
{
        return 4 + // used version
               4 + // latest supported version version
               16 + // client ID
               4 + prevTasks.size() * 8 + // length + prev tasks
               4 + standbyTasks.size() * 8 + // length + standby tasks
               4 + endPointBytes.Length; // length + userEndPoint
    }

    /**
     * @throws TaskAssignmentException if method fails to decode the data
     */
    public static SubscriptionInfo decode(ByteBuffer data)
{
        SubscriptionInfo subscriptionInfo;

        // ensure we are at the beginning of the ByteBuffer
        data.rewind();

        int usedVersion = data.getInt();
        int latestSupportedVersion;
        switch (usedVersion)
{
            case 1:
                subscriptionInfo = new SubscriptionInfo(usedVersion, UNKNOWN);
                decodeVersionOneData(subscriptionInfo, data);
                break;
            case 2:
                subscriptionInfo = new SubscriptionInfo(usedVersion, UNKNOWN);
                decodeVersionTwoData(subscriptionInfo, data);
                break;
            case 3:
            case 4:
                latestSupportedVersion = data.getInt();
                subscriptionInfo = new SubscriptionInfo(usedVersion, latestSupportedVersion);
                decodeVersionThreeData(subscriptionInfo, data);
                break;
            default:
                latestSupportedVersion = data.getInt();
                subscriptionInfo = new SubscriptionInfo(usedVersion, latestSupportedVersion);
                log.LogInformation("Unable to decode subscription data: used version: {}; latest supported version: {}", usedVersion, LATEST_SUPPORTED_VERSION);
        }

        return subscriptionInfo;
    }

    private static void decodeVersionOneData(SubscriptionInfo subscriptionInfo,
                                             ByteBuffer data)
{
        decodeClientUUID(subscriptionInfo, data);
        decodeTasks(subscriptionInfo, data);
    }

    private static void decodeClientUUID(SubscriptionInfo subscriptionInfo,
                                         ByteBuffer data)
{
        subscriptionInfo.processId = new UUID(data.getLong(), data.getLong());
    }

    private static void decodeTasks(SubscriptionInfo subscriptionInfo,
                                    ByteBuffer data)
{
        subscriptionInfo.prevTasks = new HashSet<>();
        int numPrevTasks = data.getInt();
        for (int i = 0; i < numPrevTasks; i++)
{
            subscriptionInfo.prevTasks.Add(TaskId.readFrom(data));
        }

        subscriptionInfo.standbyTasks = new HashSet<>();
        int numStandbyTasks = data.getInt();
        for (int i = 0; i < numStandbyTasks; i++)
{
            subscriptionInfo.standbyTasks.Add(TaskId.readFrom(data));
        }
    }

    private static void decodeVersionTwoData(SubscriptionInfo subscriptionInfo,
                                             ByteBuffer data)
{
        decodeClientUUID(subscriptionInfo, data);
        decodeTasks(subscriptionInfo, data);
        decodeUserEndPoint(subscriptionInfo, data);
    }

    private static void decodeUserEndPoint(SubscriptionInfo subscriptionInfo,
                                           ByteBuffer data)
{
        int bytesLength = data.getInt();
        if (bytesLength != 0)
{
            byte[] bytes = new byte[bytesLength];
            data[bytes];
            subscriptionInfo.userEndPoint = new string(bytes, System.Text.Encoding.UTF8);
        }
    }

    private static void decodeVersionThreeData(SubscriptionInfo subscriptionInfo,
                                               ByteBuffer data)
{
        decodeClientUUID(subscriptionInfo, data);
        decodeTasks(subscriptionInfo, data);
        decodeUserEndPoint(subscriptionInfo, data);
    }


    public int GetHashCode()
{
        int GetHashCode() = usedVersion ^ latestSupportedVersion ^ processId.GetHashCode() ^ prevTasks.GetHashCode() ^ standbyTasks.GetHashCode();
        if (userEndPoint == null)
{
            return GetHashCode();
        }
        return GetHashCode() ^ userEndPoint.GetHashCode();
    }


    public bool Equals(object o)
{
        if (o is SubscriptionInfo)
{
            SubscriptionInfo other = (SubscriptionInfo) o;
            return this.usedVersion == other.usedVersion &&
                    this.latestSupportedVersion == other.latestSupportedVersion &&
                    this.processId.Equals(other.processId) &&
                    this.prevTasks.Equals(other.prevTasks) &&
                    this.standbyTasks.Equals(other.standbyTasks) &&
                    this.userEndPoint != null ? this.userEndPoint.Equals(other.userEndPoint) : other.userEndPoint == null;
        } else
{

            return false;
        }
    }


    public string ToString()
{
        return "[version=" + usedVersion
            + ", supported version=" + latestSupportedVersion
            + ", process ID=" + processId
            + ", prev tasks=" + prevTasks
            + ", standby tasks=" + standbyTasks
            + ", user endpoint=" + userEndPoint + "]";
    }
}
