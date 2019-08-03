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

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.IStateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl.KeyValueStoreReadWriteDecorator;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl.SessionStoreReadWriteDecorator;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl.TimestampedKeyValueStoreReadWriteDecorator;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl.TimestampedWindowStoreReadWriteDecorator;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl.WindowStoreReadWriteDecorator;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.internals.ThreadCache;

import java.time.Duration;
import java.util.List;

public class GlobalProcessorContextImpl : AbstractProcessorContext {


    public GlobalProcessorContextImpl(StreamsConfig config,
                                      StateManager stateMgr,
                                      StreamsMetricsImpl metrics,
                                      ThreadCache cache) {
        super(new TaskId(-1, -1), config, metrics, stateMgr, cache);
    }

    @SuppressWarnings("unchecked")
    @Override
    public IStateStore getStateStore(string name) {
        IStateStore store = stateManager.getGlobalStore(name);

        if (store is TimestampedKeyValueStore) {
            return new TimestampedKeyValueStoreReadWriteDecorator((TimestampedKeyValueStore) store);
        } else if (store is KeyValueStore) {
            return new KeyValueStoreReadWriteDecorator((KeyValueStore) store);
        } else if (store is TimestampedWindowStore) {
            return new TimestampedWindowStoreReadWriteDecorator((TimestampedWindowStore) store);
        } else if (store is WindowStore) {
            return new WindowStoreReadWriteDecorator((WindowStore) store);
        } else if (store is SessionStore) {
            return new SessionStoreReadWriteDecorator((SessionStore) store);
        }

        return store;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> void forward(K key, V value) {
        ProcessorNode previousNode = currentNode();
        try {
            for (ProcessorNode child : (List<ProcessorNode<K, V>>) currentNode().children()) {
                setCurrentNode(child);
                child.process(key, value);
            }
        } finally {
            setCurrentNode(previousNode);
        }
    }

    /**
     * No-op. This should only be called on GlobalStateStore#flush and there should be no child nodes
     */
    @Override
    public <K, V> void forward(K key, V value, To to) {
        if (!currentNode().children().isEmpty()) {
            throw new InvalidOperationException("This method should only be called on 'GlobalStateStore.flush' that should not have any children.");
        }
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    @Deprecated
    public <K, V> void forward(K key, V value, int childIndex) {
        throw new UnsupportedOperationException("this should not happen: forward() not supported in global processor context.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    @Deprecated
    public <K, V> void forward(K key, V value, string childName) {
        throw new UnsupportedOperationException("this should not happen: forward() not supported in global processor context.");
    }

    @Override
    public void commit() {
        //no-op
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    @Deprecated
    public ICancellable schedule(long interval, PunctuationType type, Punctuator callback) {
        throw new UnsupportedOperationException("this should not happen: schedule() not supported in global processor context.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public ICancellable schedule(Duration interval, PunctuationType type, Punctuator callback) {
        throw new UnsupportedOperationException("this should not happen: schedule() not supported in global processor context.");
    }
}