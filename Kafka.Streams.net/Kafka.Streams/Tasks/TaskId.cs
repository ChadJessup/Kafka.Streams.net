using Kafka.Streams.Errors;
using Kafka.Streams.KStream.Internals;
using System;
using System.IO;
using System.Text;

namespace Kafka.Streams.Tasks
{
    /**
     * The task ID representation composed as topic group ID plus the assigned partition ID.
     */
    public class TaskId : IComparable<TaskId>
    {
        /** The ID of the topic group. */
        public int topicGroupId;
        /** The ID of the partition. */
        public int partition;

        public TaskId(int topicGroupId, int partition)
        {
            this.topicGroupId = topicGroupId;
            this.partition = partition;
        }

        public override string ToString()
        {
            return topicGroupId + "_" + partition;
        }

        /**
         * @throws TaskIdFormatException if the taskIdStr is not a valid {@link TaskId}
         */
        public static TaskId parse(string taskIdStr)
        {
            int index = taskIdStr.IndexOf('_');

            if (index <= 0 || index + 1 >= taskIdStr.Length)
            {
                throw new TaskIdFormatException(taskIdStr);
            }

            try
            {
                int topicGroupId = int.Parse(taskIdStr.Substring(0, index));
                int partition = int.Parse(taskIdStr.Substring(index + 1));

                return new TaskId(topicGroupId, partition);
            }
            catch (Exception)
            {
                throw new TaskIdFormatException(taskIdStr);
            }
        }

        /**
         * @throws IOException if cannot write to output stream
         */
        public void writeTo(Stream outputStream)
        {
            var bw = new BinaryWriter(outputStream, Encoding.UTF8, leaveOpen: true);

            bw.Write(topicGroupId);
            bw.Write(partition);
        }

        /**
         * @throws IOException if cannot read from input stream
         */
        public static TaskId readFrom(Stream input)
        {
            var bw = new BinaryReader(input, Encoding.UTF8, leaveOpen: true);

            return new TaskId(bw.ReadInt32(), bw.ReadInt32());
        }

        public void writeTo(ByteBuffer buf)
        {
            buf.putInt(topicGroupId);
            buf.putInt(partition);
        }

        public static TaskId readFrom(ByteBuffer buf)
        {
            return new TaskId(buf.getInt(), buf.getInt());
        }


        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }

            if (o is TaskId)
            {
                TaskId other = (TaskId)o;
                return other.topicGroupId == this.topicGroupId && other.partition == this.partition;
            }
            else
            {

                return false;
            }
        }

        public override int GetHashCode()
        {
            long n = ((long)topicGroupId << 32) | (long)partition;
            return (int)(n % 0xFFFFFFFFL);
        }

        public int CompareTo(TaskId other)
        {
            int compare = this.topicGroupId.CompareTo(other.topicGroupId);

            return compare != 0
                ? compare
                : this.partition.CompareTo(other.partition);
        }
    }
}