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
namespace Kafka.Streams.State.Internals
{
    public class RecordConverters
    {
        private static RecordConverter IDENTITY_INSTANCE = record->record;


        private static RecordConverter RAW_TO_TIMESTAMED_INSTANCE = record =>
    {
        byte[] rawValue = record.value();
        long timestamp = record.timestamp();
        byte[] recordValue = rawValue == null ? null :
            ByteBuffer.allocate(8 + rawValue.Length)
                .putLong(timestamp)
                .Add(rawValue)
                .array();
        return new ConsumerRecord<>(
            record.topic(),
            record.partition(),
            record.offset(),
            timestamp,
            record.timestampType(),
            record.checksum(),
            record.serializedKeySize(),
            record.serializedValueSize(),
            record.key(),
            recordValue,
            record.headers(),
            record.leaderEpoch()
        );
    };

        // privatize the constructor so the cannot be instantiated (only used for its static members)
        private RecordConverters() { }

        public static RecordConverter rawValueToTimestampedValue()
        {
            return RAW_TO_TIMESTAMED_INSTANCE;
        }

        public static RecordConverter identity()
        {
            return IDENTITY_INSTANCE;
        }
    }
}