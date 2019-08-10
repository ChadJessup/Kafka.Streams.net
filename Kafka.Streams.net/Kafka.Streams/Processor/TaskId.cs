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
using Kafka.Streams.Errors;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.IProcessor.Internals.Assignment;
using System;

namespace Kafka.Streams.IProcessor
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

        public string ToString()
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
            catch (Exception e)
            {
                throw new TaskIdFormatException(taskIdStr);
            }
        }

        /**
         * @throws IOException if cannot write to output stream
         */
        public void writeTo(DataOutputStream @out)
        {
            @out.writeInt(topicGroupId);
            @out.writeInt(partition);
        }

        /**
         * @throws IOException if cannot read from input stream
         */
        public static TaskId readFrom(DataInputStream input)
        {
            return new TaskId(input.readInt(), input.readInt());
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
            return compare != 0 ? compare : this.partition.CompareTo(other.partition);
        }
    }
}