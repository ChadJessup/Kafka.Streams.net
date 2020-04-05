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
        public static TaskId Parse(string taskIdStr)
        {
            var index = taskIdStr.IndexOf('_');

            if (index <= 0 || index + 1 >= taskIdStr.Length)
            {
                throw new TaskIdFormatException(taskIdStr);
            }

            try
            {
                var topicGroupId = int.Parse(taskIdStr.Substring(0, index));
                var partition = int.Parse(taskIdStr.Substring(index + 1));

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
        public void WriteTo(Stream outputStream)
        {
            var bw = new BinaryWriter(outputStream, Encoding.UTF8, leaveOpen: true);

            bw.Write(topicGroupId);
            bw.Write(partition);
        }

        /**
         * @throws IOException if cannot read from input stream
         */
        public static TaskId ReadFrom(Stream input)
        {
            var bw = new BinaryReader(input, Encoding.UTF8, leaveOpen: true);

            return new TaskId(bw.ReadInt32(), bw.ReadInt32());
        }

        public void WriteTo(ByteBuffer buf)
        {
            buf.PutInt(topicGroupId);
            buf.PutInt(partition);
        }

        public static TaskId ReadFrom(ByteBuffer buf)
        {
            return new TaskId(buf.GetInt(), buf.GetInt());
        }


        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }

            if (o is TaskId)
            {
                return ((TaskId)o).topicGroupId == this.topicGroupId && ((TaskId)o).partition == this.partition;
            }
            else
            {
                return false;
            }
        }

        public override int GetHashCode()
        {
            var n = ((long)topicGroupId << 32) | (long)partition;
            return (int)(n % 0xFFFFFFFFL);
        }

        public int CompareTo(TaskId other)
        {
            var compare = this.topicGroupId.CompareTo(other.topicGroupId);

            return compare != 0
                ? compare
                : this.partition.CompareTo(other.partition);
        }
    }
}