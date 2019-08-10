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

namespace Kafka.Streams.IProcessor.Internals
{
    static class StandbyTaskCreator : AbstractTaskCreator<StandbyTask>
    {
        private Sensor createTaskSensor;

        StandbyTaskCreator(InternalTopologyBuilder builder,
                           StreamsConfig config,
                           StreamsMetricsImpl streamsMetrics,
                           StateDirectory stateDirectory,
                           IChangelogReader storeChangelogReader,
                           ITime time,
                           ILogger log)
            : base(
                builder,
                config,
                streamsMetrics,
                stateDirectory,
                storeChangelogReader,
                time,
                log)
        {
            createTaskSensor = ThreadMetrics.createTaskSensor(streamsMetrics);
        }


        StandbyTask createTask(IConsumer<byte[], byte[]> consumer,
                               TaskId taskId,
                               HashSet<TopicPartition> partitions)
        {
            createTaskSensor.record();

            ProcessorTopology topology = builder.build(taskId.topicGroupId);

            if (!topology.stateStores().isEmpty() && !topology.storeToChangelogTopic().isEmpty())
            {
                return new StandbyTask(
                    taskId,
                    partitions,
                    topology,
                    consumer,
                    storeChangelogReader,
                    config,
                    streamsMetrics,
                    stateDirectory);
            }
            else
            {

                log.LogTrace(
                    "Skipped standby task {} with assigned partitions {} " +
                        "since it does not have any state stores to materialize",
                    taskId, partitions
                );
                return null;
            }
        }
    }
}
