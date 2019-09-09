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
using System.Collections.Generic;

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

        public ThreadMetadata(
            string threadName,
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
            this.activeTasks = activeTasks;
            this.standbyTasks = standbyTasks;
        }

        public string consumerClientId => mainConsumerClientId;

        public override bool Equals(object o)
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
            return threadName.Equals(that.threadName)
                && threadState.Equals(that.threadState)
                && activeTasks.Equals(that.activeTasks)
                && standbyTasks.Equals(that.standbyTasks)
                && mainConsumerClientId.Equals(that.mainConsumerClientId)
                && restoreConsumerClientId.Equals(that.restoreConsumerClientId)
                && producerClientIds.Equals(that.producerClientIds)
                && adminClientId.Equals(that.adminClientId);
        }


        public override int GetHashCode()
        {
            // can only hash 7 things at once...
            return
            (
                (threadName, threadState).GetHashCode(),
                activeTasks,
                standbyTasks,
                mainConsumerClientId,
                restoreConsumerClientId,
                producerClientIds,
                adminClientId
            ).GetHashCode();
        }


        public override string ToString()
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