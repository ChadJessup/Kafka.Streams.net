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
namespace Kafka.Streams.Processor
{
    /**
     * Represents the state of a single thread running within a {@link KafkaStreams} application.
     */
    public class ThreadMetadata
    {


        private string threadName;

        private string threadState;

        private HashSet<TaskMetadata> activeTasks;

        private HashSet<TaskMetadata> standbyTasks;

        private string mainConsumerClientId;

        private string restoreConsumerClientId;

        private HashSet<string> producerClientIds;

        // the admin client should be shared among all threads, so the client id should be the same;
        // we keep it at the thread-level for user's convenience and possible extensions in the future
        private string adminClientId;

        public ThreadMetadata(string threadName,
                              string threadState,
                              string mainConsumerClientId,
                              string restoreConsumerClientId,
                              HashSet<string> producerClientIds,
                              string adminClientId,
                              HashSet<TaskMetadata> activeTasks,
                              HashSet<TaskMetadata> standbyTasks)
        {
            this.mainConsumerClientId = mainConsumerClientId;
            this.restoreConsumerClientId = restoreConsumerClientId;
            this.producerClientIds = producerClientIds;
            this.adminClientId = adminClientId;
            this.threadName = threadName;
            this.threadState = threadState;
            this.activeTasks = Collections.unmodifiableSet(activeTasks);
            this.standbyTasks = Collections.unmodifiableSet(standbyTasks);
        }

        public string threadState()
        {
            return threadState;
        }

        public string threadName()
        {
            return threadName;
        }

        public HashSet<TaskMetadata> activeTasks()
        {
            return activeTasks;
        }

        public HashSet<TaskMetadata> standbyTasks()
        {
            return standbyTasks;
        }

        public string consumerClientId()
        {
            return mainConsumerClientId;
        }

        public string restoreConsumerClientId()
        {
            return restoreConsumerClientId;
        }

        public HashSet<string> producerClientIds()
        {
            return producerClientIds;
        }

        public string adminClientId()
        {
            return adminClientId;
        }


        public bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || GetType() != o.GetType())
            {
                return false;
            }
            ThreadMetadata that = (ThreadMetadata)o;
            return Objects.Equals(threadName, that.threadName) &&
                   Objects.Equals(threadState, that.threadState) &&
                   Objects.Equals(activeTasks, that.activeTasks) &&
                   Objects.Equals(standbyTasks, that.standbyTasks) &&
                   mainConsumerClientId.Equals(that.mainConsumerClientId) &&
                   restoreConsumerClientId.Equals(that.restoreConsumerClientId) &&
                   Objects.Equals(producerClientIds, that.producerClientIds) &&
                   adminClientId.Equals(that.adminClientId);
        }


        public int GetHashCode()
        {
            return Objects.hash(
                threadName,
                threadState,
                activeTasks,
                standbyTasks,
                mainConsumerClientId,
                restoreConsumerClientId,
                producerClientIds,
                adminClientId);
        }


        public string ToString()
        {
            return "ThreadMetadata{" +
                    "threadName=" + threadName +
                    ", threadState=" + threadState +
                    ", activeTasks=" + activeTasks +
                    ", standbyTasks=" + standbyTasks +
                    ", consumerClientId=" + mainConsumerClientId +
                    ", restoreConsumerClientId=" + restoreConsumerClientId +
                    ", producerClientIds=" + producerClientIds +
                    ", adminClientId=" + adminClientId +
                    '}';
        }
    }
}