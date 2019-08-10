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
using Confluent.Kafka;

namespace Kafka.Streams.IProcessor.Internals
{

    public class StampedRecord : Stamped<ConsumeResult<object, object>>
    {

        public StampedRecord(ConsumeResult<object, object> record, long timestamp)
            : base(record, timestamp)
        {
        }

        public string Topic => value.Topic;

        public int partition => value.Partition;

        public object key()
        {
            return value.key();
        }

        public object value => value.value;

        public long offset()
        {
            return value.offset();
        }

        public Headers headers()
        {
            return value.headers();
        }


        public string ToString()
        {
            return value.ToString() + ", timestamp = " + timestamp;
        }
    }
}