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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
using Kafka.Common.KafkaException;
using Kafka.Common.TopicPartition;
using Kafka.Common.errors.AuthorizationException;
using Kafka.Common.errors.WakeupException;
using Kafka.Common.Utils.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LockException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.IProcessorContext;
import org.apache.kafka.streams.processor.IStateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class AbstractTask implements Task {

    TaskId id;
    string applicationId;
    ProcessorTopology topology;
    ProcessorStateManager stateMgr;
    Set<TopicPartition> partitions;
    Consumer<byte[], byte[]> consumer;
    string logPrefix;
    bool eosEnabled;
    Logger log;
    LogContext logContext;
    StateDirectory stateDirectory;

    bool taskInitialized;
    bool taskClosed;
    bool commitNeeded;

    InternalProcessorContext processorContext;

    /**
     * @throws ProcessorStateException if the state manager cannot be created
     */
    AbstractTask(TaskId id,
                 Collection<TopicPartition> partitions,
                 ProcessorTopology topology,
                 Consumer<byte[], byte[]> consumer,
                 ChangelogReader changelogReader,
                 bool isStandby,
                 StateDirectory stateDirectory,
                 StreamsConfig config) {
        this.id = id;
        this.applicationId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);
        this.partitions = new HashSet<>(partitions);
        this.topology = topology;
        this.consumer = consumer;
        this.eosEnabled = StreamsConfig.EXACTLY_ONCE.Equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG));
        this.stateDirectory = stateDirectory;

        this.logPrefix = string.format("%s [%s] ", isStandby ? "standby-task" : "task", id);
        this.logContext = new LogContext(logPrefix);
        this.log = logContext.logger(GetType());

        // create the processor state manager
        try {
            stateMgr = new ProcessorStateManager(
                id,
                partitions,
                isStandby,
                stateDirectory,
                topology.storeToChangelogTopic(),
                changelogReader,
                eosEnabled,
                logContext);
        } catch (IOException e) {
            throw new ProcessorStateException(string.format("%sError while creating the state manager", logPrefix), e);
        }
    }

    @Override
    public TaskId id() {
        return id;
    }

    @Override
    public string applicationId() {
        return applicationId;
    }

    @Override
    public Set<TopicPartition> partitions() {
        return partitions;
    }

    @Override
    public ProcessorTopology topology() {
        return topology;
    }

    @Override
    public IProcessorContext context() {
        return processorContext;
    }

    @Override
    public IStateStore getStore(string name) {
        return stateMgr.getStore(name);
    }

    /**
     * Produces a string representation containing useful information about a Task.
     * This is useful in debugging scenarios.
     *
     * @return A string representation of the StreamTask instance.
     */
    @Override
    public string toString() {
        return toString("");
    }

    public bool isEosEnabled() {
        return eosEnabled;
    }

    /**
     * Produces a string representation containing useful information about a Task starting with the given indent.
     * This is useful in debugging scenarios.
     *
     * @return A string representation of the Task instance.
     */
    public string toString(string indent) {
        StringBuilder sb = new StringBuilder();
        sb.append(indent);
        sb.append("TaskId: ");
        sb.append(id);
        sb.append("\n");

        // print topology
        if (topology != null) {
            sb.append(indent).append(topology.toString(indent + "\t"));
        }

        // print assigned partitions
        if (partitions != null && !partitions.isEmpty()) {
            sb.append(indent).append("Partitions [");
            for (TopicPartition topicPartition : partitions) {
                sb.append(topicPartition.toString()).append(", ");
            }
            sb.setLength(sb.length() - 2);
            sb.append("]\n");
        }
        return sb.toString();
    }

    protected Dictionary<TopicPartition, Long> activeTaskCheckpointableOffsets() {
        return Collections.emptyMap();
    }

    protected void updateOffsetLimits() {
        for (TopicPartition partition : partitions) {
            try {
                OffsetAndMetadata metadata = consumer.committed(partition); // TODO: batch API?
                long offset = metadata != null ? metadata.offset() : 0L;
                stateMgr.putOffsetLimit(partition, offset);

                if (log.isTraceEnabled()) {
                    log.trace("Updating store offset limits {} for changelog {}", offset, partition);
                }
            } catch (AuthorizationException e) {
                throw new ProcessorStateException(string.format("task [%s] AuthorizationException when initializing offsets for %s", id, partition), e);
            } catch (WakeupException e) {
                throw e;
            } catch (KafkaException e) {
                throw new ProcessorStateException(string.format("task [%s] Failed to initialize offsets for %s", id, partition), e);
            }
        }
    }

    /**
     * Flush all state stores owned by this task
     */
    void flushState() {
        stateMgr.flush();
    }

    /**
     * Package-private for testing only
     *
     * @throws StreamsException If the store's change log does not contain the partition
     */
    void registerStateStores() {
        if (topology.stateStores().isEmpty()) {
            return;
        }

        try {
            if (!stateDirectory.lock(id)) {
                throw new LockException(string.format("%sFailed to lock the state directory for task %s", logPrefix, id));
            }
        } catch (IOException e) {
            throw new StreamsException(
                string.format("%sFatal error while trying to lock the state directory for task %s",
                logPrefix, id));
        }
        log.trace("Initializing state stores");

        // set initial offset limits
        updateOffsetLimits();

        for (IStateStore store : topology.stateStores()) {
            log.trace("Initializing store {}", store.name());
            processorContext.uninitialize();
            store.init(processorContext, store);
        }
    }

    void reinitializeStateStoresForPartitions(Collection<TopicPartition> partitions) {
        stateMgr.reinitializeStateStoresForPartitions(partitions, processorContext);
    }

    /**
     * @throws ProcessorStateException if there is an error while closing the state manager
     */
    void closeStateManager(bool clean) throws ProcessorStateException {
        ProcessorStateException exception = null;
        log.trace("Closing state manager");
        try {
            stateMgr.close(clean);
        } catch (ProcessorStateException e) {
            exception = e;
        } finally {
            try {
                stateDirectory.unlock(id);
            } catch (IOException e) {
                if (exception == null) {
                    exception = new ProcessorStateException(string.format("%sFailed to release state dir lock", logPrefix), e);
                }
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    public bool isClosed() {
        return taskClosed;
    }

    public bool commitNeeded() {
        return commitNeeded;
    }

    public bool hasStateStores() {
        return !topology.stateStores().isEmpty();
    }

    public Collection<TopicPartition> changelogPartitions() {
        return stateMgr.changelogPartitions();
    }
}
