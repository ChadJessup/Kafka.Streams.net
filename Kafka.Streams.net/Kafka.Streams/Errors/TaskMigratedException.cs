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
using Confluent.Kafka;
using Kafka.Streams.Processor.Internals;

namespace Kafka.Streams.Errors
{






    /**
     * Indicates that a task got migrated to another thread.
     * Thus, the task raising this exception can be cleaned up and closed as "zombie".
     */
    public class TaskMigratedException : StreamsException
    {


        private static long serialVersionUID = 1L;

        private ITask task;
        private ProducerFencedException fatal;

        // this is for unit test only
        public TaskMigratedException()
            : base("A task has been migrated unexpectedly", null)
        {

            this.task = null;
        }

        public TaskMigratedException(ITask task,
                                      TopicPartition topicPartition,
                                      long endOffset,
                                      long pos)
            : base(string.Format("Log end offset of %s should not change while restoring: old end offset %d, current offset %d",
                                topicPartition,
                                endOffset,
                                pos),
                null)
        {

            this.task = task;
        }

        public TaskMigratedException(ITask task)
            : base(string.Format("Task %s is unexpectedly closed during processing", task.id()), null)
        {

            this.task = task;
        }

        public TaskMigratedException(ITask task,
                                      Throwable throwable)
            : base(string.Format("Client request for task %s has been fenced due to a rebalance", task.id()), throwable)
        {

            this.task = task;
        }

        public TaskMigratedException(ITask task, ProducerFencedException fatal) : this(task)
        {
            this.fatal = fatal;
        }

        public ITask migratedTask()
        {
            return task;
        }

    }
}