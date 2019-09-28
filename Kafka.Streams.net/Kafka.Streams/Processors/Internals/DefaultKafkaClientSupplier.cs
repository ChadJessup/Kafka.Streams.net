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
using Kafka.Streams.Interfaces;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.Processor.Internals
{
    public class DefaultKafkaClientSupplier : IKafkaClientSupplier
    {
        public IAdminClient GetAdminClient(Dictionary<string, string> config)
        {
            // create a new client upon each call; but expect this call to be only triggered once so this should be fine
            var convertedConfig = config.ToDictionary(k => k.Key, v => v.Value.ToString());

            return new AdminClientBuilder(convertedConfig)
                .Build();
        }

        public IProducer<byte[], byte[]> getProducer(Dictionary<string, string> config)
        {
            var convertedConfig = config.ToDictionary(k => k.Key, v => v.Value.ToString());

            return new ProducerBuilder<byte[], byte[]>(convertedConfig)
                .Build();
        }

        public IConsumer<byte[], byte[]> getConsumer(Dictionary<string, string> config)
        {
            var convertedConfig = config.ToDictionary(k => k.Key, v => v.Value.ToString());

            return new ConsumerBuilder<byte[], byte[]>(convertedConfig)
                .Build();
        }

        public IConsumer<byte[], byte[]> getRestoreConsumer(Dictionary<string, string> config)
        {
            var convertedConfig = config.ToDictionary(k => k.Key, v => v.Value.ToString());

            return new ConsumerBuilder<byte[], byte[]>(convertedConfig)
                .Build();
        }

        public IConsumer<byte[], byte[]> getGlobalConsumer(Dictionary<string, string> config)
        {
            var convertedConfig = config.ToDictionary(k => k.Key, v => v.Value.ToString());

            return new ConsumerBuilder<byte[], byte[]>(convertedConfig)
                .Build();
        }
    }
}