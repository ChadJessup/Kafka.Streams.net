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

import org.apache.kafka.clients.producer.Producer;
using Kafka.Common.TopicPartition;
using Kafka.Common.header.Headers;
using Kafka.Common.serialization.Serializer;
using Kafka.Common.Utils.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.IStateStore;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.ThreadCache;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

class StandbyContextImpl : AbstractProcessorContext implements RecordCollector.Supplier {

    private static RecordCollector NO_OP_COLLECTOR = new RecordCollector() {
        @Override
        public <K, V> void send(string topic,
                                K key,
                                V value,
                                Headers headers,
                                Integer partition,
                                Long timestamp,
                                Serializer<K> keySerializer,
                                Serializer<V> valueSerializer) {
        }

        @Override
        public <K, V> void send(string topic,
                                K key,
                                V value,
                                Headers headers,
                                Long timestamp,
                                Serializer<K> keySerializer,
                                Serializer<V> valueSerializer,
                                StreamPartitioner<? super K, ? super V> partitioner) {}

        @Override
        public void init(Producer<byte[], byte[]> producer) {}

        @Override
        public void flush() {}

        @Override
        public void close() {}

        @Override
        public Dictionary<TopicPartition, Long> offsets() {
            return Collections.emptyMap();
        }
    };

    StandbyContextImpl(TaskId id,
                       StreamsConfig config,
                       ProcessorStateManager stateMgr,
                       StreamsMetricsImpl metrics) {
        super(
            id,
            config,
            metrics,
            stateMgr,
            new ThreadCache(
                new LogContext(string.format("stream-thread [%s] ", Thread.currentThread().getName())),
                0,
                metrics
            )
        );
    }


    StateManager getStateMgr() {
        return stateManager;
    }

    @Override
    public RecordCollector recordCollector() {
        return NO_OP_COLLECTOR;
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public IStateStore getStateStore(string name) {
        throw new UnsupportedOperationException("this should not happen: getStateStore() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public string topic() {
        throw new UnsupportedOperationException("this should not happen: topic() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public int partition() {
        throw new UnsupportedOperationException("this should not happen: partition() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public long offset() {
        throw new UnsupportedOperationException("this should not happen: offset() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public long timestamp() {
        throw new UnsupportedOperationException("this should not happen: timestamp() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public <K, V> void forward(K key, V value) {
        throw new UnsupportedOperationException("this should not happen: forward() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public <K, V> void forward(K key, V value, To to) {
        throw new UnsupportedOperationException("this should not happen: forward() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    @Deprecated
    public <K, V> void forward(K key, V value, int childIndex) {
        throw new UnsupportedOperationException("this should not happen: forward() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    @Deprecated
    public <K, V> void forward(K key, V value, string childName) {
        throw new UnsupportedOperationException("this should not happen: forward() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public void commit() {
        throw new UnsupportedOperationException("this should not happen: commit() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    @Deprecated
    public ICancellable schedule(long interval, PunctuationType type, Punctuator callback) {
        throw new UnsupportedOperationException("this should not happen: schedule() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public ICancellable schedule(Duration interval, PunctuationType type, Punctuator callback) throws IllegalArgumentException {
        throw new UnsupportedOperationException("this should not happen: schedule() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public ProcessorRecordContext recordContext() {
        throw new UnsupportedOperationException("this should not happen: recordContext not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public void setRecordContext(ProcessorRecordContext recordContext) {
        throw new UnsupportedOperationException("this should not happen: setRecordContext not supported in standby tasks.");
    }

    @Override
    public void setCurrentNode(ProcessorNode currentNode) {
        // no-op. can't throw as this is called on commit when the StateStores get flushed.
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public ProcessorNode currentNode() {
        throw new UnsupportedOperationException("this should not happen: currentNode not supported in standby tasks.");
    }
}
