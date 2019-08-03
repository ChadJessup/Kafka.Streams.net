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
package org.apache.kafka.streams.kstream.internals.suppress;

import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.IProcessorContext;

 class TimeDefinitions {
    private TimeDefinitions() {}

    enum TimeDefinitionType {
        RECORD_TIME, WINDOW_END_TIME
    }

    /**
     * This interface should never be instantiated outside of this class.
     */
    interface TimeDefinition<K> {
        long time( IProcessorContext context,  K key);

        TimeDefinitionType type();
    }

    public static class RecordTimeDefintion<K> implements TimeDefinition<K> {
        private static  RecordTimeDefintion INSTANCE = new RecordTimeDefintion();

        private RecordTimeDefintion() {}

        @SuppressWarnings("unchecked")
        public static <K> RecordTimeDefintion<K> instance() {
            return RecordTimeDefintion.INSTANCE;
        }

        @Override
        public long time( IProcessorContext context,  K key) {
            return context.timestamp();
        }

        @Override
        public TimeDefinitionType type() {
            return TimeDefinitionType.RECORD_TIME;
        }
    }

    public static class WindowEndTimeDefinition<K : Windowed> implements TimeDefinition<K> {
        private static  WindowEndTimeDefinition INSTANCE = new WindowEndTimeDefinition();

        private WindowEndTimeDefinition() {}

        @SuppressWarnings("unchecked")
        public static <K : Windowed> WindowEndTimeDefinition<K> instance() {
            return WindowEndTimeDefinition.INSTANCE;
        }

        @Override
        public long time( IProcessorContext context,  K key) {
            return key.window().end();
        }

        @Override
        public TimeDefinitionType type() {
            return TimeDefinitionType.WINDOW_END_TIME;
        }
    }
}
