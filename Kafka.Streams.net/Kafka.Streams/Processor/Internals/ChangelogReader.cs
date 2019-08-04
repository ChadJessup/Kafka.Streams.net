/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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
namespace Kafka.streams.processor.internals;

using Kafka.Common.TopicPartition;

import java.util.Collection;
import java.util.Map;

/**
 * Performs bulk read operations from a set of partitions. Used to
 * restore  {@link org.apache.kafka.streams.processor.IStateStore}s from their
 * change logs
 */
public interface ChangelogReader {
    /**
     * Register a state store and it's partition for later restoration.
     * @param restorer the state restorer to register
     */
    void register(StateRestorer restorer);

    /**
     * Restore all registered state stores by reading from their changelogs.
     * @return all topic partitions that have been restored
     */
    Collection<TopicPartition> restore(RestoringTasks active);

    /**
     * @return the restored offsets for all persistent stores.
     */
    Dictionary<TopicPartition, Long> restoredOffsets();

    void reset();
}
