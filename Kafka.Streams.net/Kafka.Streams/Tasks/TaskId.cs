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
        public int TopicGroupId { get; }
        /** The ID of the partition. */
        public int Partition { get; }

        public TaskId(int topicGroupId, int partition)
        {
            this.TopicGroupId = topicGroupId;
            this.Partition = partition;
        }

        public override string ToString()
        {
            return this.TopicGroupId + "_" + this.Partition;
        }

        /**
         * @throws TaskIdFormatException if the taskIdStr is not a valid {@link TaskId}
         */
        public static TaskId Parse(string taskIdStr)
        {
            if (string.IsNullOrWhiteSpace(taskIdStr))
            {
                throw new ArgumentException("message", nameof(taskIdStr));
            }

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
            using var bw = new BinaryWriter(outputStream, Encoding.UTF8, leaveOpen: true);

            bw.Write(this.TopicGroupId);
            bw.Write(this.Partition);
        }

        /**
         * @throws IOException if cannot read from input stream
         */
        public static TaskId ReadFrom(Stream input)
        {
            using var bw = new BinaryReader(input, Encoding.UTF8, leaveOpen: true);

            return new TaskId(bw.ReadInt32(), bw.ReadInt32());
        }

        public void WriteTo(ByteBuffer buf)
        {
            buf.PutInt(this.TopicGroupId);
            buf.PutInt(this.Partition);
        }

        public static TaskId ReadFrom(ByteBuffer buffer)
        {
            if (buffer is null)
            {
                throw new ArgumentNullException(nameof(buffer));
            }

            return new TaskId(buffer.GetInt(), buffer.GetInt());
        }

        public override bool Equals(object other)
        {
            if (this == other)
            {
                return true;
            }

            if (other is TaskId)
            {
                return ((TaskId)other).TopicGroupId == this.TopicGroupId
                    && ((TaskId)other).Partition == this.Partition;
            }
            else
            {
                return false;
            }
        }

        public override int GetHashCode()
        {
            var n = ((long)this.TopicGroupId << 32) | (long)this.Partition;
            return (int)(n % 0xFFFFFFFFL);
        }

        public int CompareTo(TaskId other)
        {
            var compare = this.TopicGroupId.CompareTo(other.TopicGroupId);

            return compare != 0
                ? compare
                : this.Partition.CompareTo(other.Partition);
        }
    }
}
