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
namespace Kafka.Streams.Processor.Internals;

using Kafka.Common.TopicPartition;








interface StateManager : Checkpointable
{

    File baseDir();

    /**
     * @throws ArgumentException if the store name has already been registered or if it is not a valid name
     * (e.g., when it conflicts with the names of internal topics, like the checkpoint file name)
     * @throws StreamsException if the store's change log does not contain the partition
     */
    void register(IStateStore store, StateRestoreCallback stateRestoreCallback);

    void flush();

    void reinitializeStateStoresForPartitions(Collection<TopicPartition> partitions,
                                              IInternalProcessorContext processorContext);

    void close(bool clean);

    IStateStore getGlobalStore(string name);

    IStateStore getStore(string name);
}
