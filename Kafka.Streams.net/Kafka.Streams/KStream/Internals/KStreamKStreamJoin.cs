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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.IProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KStreamKStreamJoin<K, R, V1, V2> implements ProcessorSupplier<K, V1> {
    private static  Logger LOG = LoggerFactory.getLogger(KStreamKStreamJoin.class);

    private  string otherWindowName;
    private  long joinBeforeMs;
    private  long joinAfterMs;

    private  ValueJoiner<? super V1, ? super V2, ? : R> joiner;
    private  bool outer;

    KStreamKStreamJoin( string otherWindowName,  long joinBeforeMs,  long joinAfterMs,  ValueJoiner<? super V1, ? super V2, ? : R> joiner,  bool outer) {
        this.otherWindowName = otherWindowName;
        this.joinBeforeMs = joinBeforeMs;
        this.joinAfterMs = joinAfterMs;
        this.joiner = joiner;
        this.outer = outer;
    }

    @Override
    public Processor<K, V1> get() {
        return new KStreamKStreamJoinProcessor();
    }

    private class KStreamKStreamJoinProcessor : AbstractProcessor<K, V1> {

        private WindowStore<K, V2> otherWindow;
        private StreamsMetricsImpl metrics;
        private Sensor skippedRecordsSensor;

        @SuppressWarnings("unchecked")
        @Override
        public void init( IProcessorContext context) {
            super.init(context);
            metrics = (StreamsMetricsImpl) context.metrics();
            skippedRecordsSensor = ThreadMetrics.skipRecordSensor(metrics);

            otherWindow = (WindowStore<K, V2>) context.getStateStore(otherWindowName);
        }


        @Override
        public void process( K key,  V1 value) {
            // we do join iff keys are equal, thus, if key is null we cannot join and just ignore the record
            //
            // we also ignore the record if value is null, because in a key-value data model a null-value indicates
            // an empty message (ie, there is nothing to be joined) -- this contrast SQL NULL semantics
            // furthermore, on left/outer joins 'null' in ValueJoiner#apply() indicates a missing record --
            // thus, to be consistent and to avoid ambiguous null semantics, null values are ignored
            if (key == null || value == null) {
                LOG.warn(
                    "Skipping record due to null key or value. key=[{}] value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                    key, value, context().topic(), context().partition(), context().offset()
                );
                skippedRecordsSensor.record();
                return;
            }

            bool needOuterJoin = outer;

             long inputRecordTimestamp = context().timestamp();
             long timeFrom = Math.max(0L, inputRecordTimestamp - joinBeforeMs);
             long timeTo = Math.max(0L, inputRecordTimestamp + joinAfterMs);

            try ( WindowStoreIterator<V2> iter = otherWindow.fetch(key, timeFrom, timeTo)) {
                while (iter.hasNext()) {
                    needOuterJoin = false;
                     KeyValue<Long, V2> otherRecord = iter.next();
                    context().forward(
                        key,
                        joiner.apply(value, otherRecord.value),
                        To.all().withTimestamp(Math.max(inputRecordTimestamp, otherRecord.key)));
                }

                if (needOuterJoin) {
                    context().forward(key, joiner.apply(value, null));
                }
            }
        }
    }
}
