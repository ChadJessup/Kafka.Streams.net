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


import org.apache.kafka.clients.consumer.ConsumerRecord;
using Kafka.Common.TopicPartition;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.AbstractNotifyingBatchingRestoreCallback;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateRestoreListener;

import java.util.Collection;

public class CompositeRestoreListener implements RecordBatchingStateRestoreCallback, StateRestoreListener {

    public static NoOpStateRestoreListener NO_OP_STATE_RESTORE_LISTENER = new NoOpStateRestoreListener();
    private RecordBatchingStateRestoreCallback internalBatchingRestoreCallback;
    private StateRestoreListener storeRestoreListener;
    private StateRestoreListener userRestoreListener = NO_OP_STATE_RESTORE_LISTENER;

    CompositeRestoreListener(StateRestoreCallback stateRestoreCallback) {

        if (stateRestoreCallback is StateRestoreListener) {
            storeRestoreListener = (StateRestoreListener) stateRestoreCallback;
        } else {
            storeRestoreListener = NO_OP_STATE_RESTORE_LISTENER;
        }

        internalBatchingRestoreCallback = StateRestoreCallbackAdapter.adapt(stateRestoreCallback);
    }

    /**
     * @throws StreamsException if user provided {@link StateRestoreListener} raises an exception in
     * {@link StateRestoreListener#onRestoreStart(TopicPartition, string, long, long)}
     */
    @Override
    public void onRestoreStart(TopicPartition topicPartition,
                               string storeName,
                               long startingOffset,
                               long endingOffset) {
        userRestoreListener.onRestoreStart(topicPartition, storeName, startingOffset, endingOffset);
        storeRestoreListener.onRestoreStart(topicPartition, storeName, startingOffset, endingOffset);
    }

    /**
     * @throws StreamsException if user provided {@link StateRestoreListener} raises an exception in
     * {@link StateRestoreListener#onBatchRestored(TopicPartition, string, long, long)}
     */
    @Override
    public void onBatchRestored(TopicPartition topicPartition,
                                string storeName,
                                long batchEndOffset,
                                long numRestored) {
        userRestoreListener.onBatchRestored(topicPartition, storeName, batchEndOffset, numRestored);
        storeRestoreListener.onBatchRestored(topicPartition, storeName, batchEndOffset, numRestored);
    }

    /**
     * @throws StreamsException if user provided {@link StateRestoreListener} raises an exception in
     * {@link StateRestoreListener#onRestoreEnd(TopicPartition, string, long)}
     */
    @Override
    public void onRestoreEnd(TopicPartition topicPartition,
                             string storeName,
                             long totalRestored) {
        userRestoreListener.onRestoreEnd(topicPartition, storeName, totalRestored);
        storeRestoreListener.onRestoreEnd(topicPartition, storeName, totalRestored);
    }

    @Override
    public void restoreBatch(Collection<ConsumerRecord<byte[], byte[]>> records) {
        internalBatchingRestoreCallback.restoreBatch(records);
    }

    void setUserRestoreListener(StateRestoreListener userRestoreListener) {
        if (userRestoreListener != null) {
            this.userRestoreListener = userRestoreListener;
        }
    }

    @Override
    public void restoreAll(Collection<KeyValue<byte[], byte[]>> records) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void restore(byte[] key,
                        byte[] value) {
        throw new UnsupportedOperationException("Single restore functionality shouldn't be called directly but "
                                                    + "through the delegated StateRestoreCallback instance");
    }

    private static class NoOpStateRestoreListener : AbstractNotifyingBatchingRestoreCallback implements RecordBatchingStateRestoreCallback {
        @Override
        public void restoreBatch(Collection<ConsumerRecord<byte[], byte[]>> records) {

        }
    }
}
