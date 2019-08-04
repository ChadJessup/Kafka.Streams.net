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
namespace Kafka.Streams.Processor.Internals;








using Kafka.Common.serialization.ByteArrayDeserializer;
using Kafka.Common.serialization.ByteArraySerializer;


public class DefaultKafkaClientSupplier : KafkaClientSupplier {
    
    public Admin getAdminClient(Dictionary<string, object> config)
{
        // create a new client upon each call; but expect this call to be only triggered once so this should be fine
        return Admin.create(config);
    }

    
    public Producer<byte[], byte[]> getProducer(Dictionary<string, object> config)
{
        return new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer());
    }

    
    public Consumer<byte[], byte[]> getConsumer(Dictionary<string, object> config)
{
        return new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    
    public Consumer<byte[], byte[]> getRestoreConsumer(Dictionary<string, object> config)
{
        return new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    
    public Consumer<byte[], byte[]> getGlobalConsumer(Dictionary<string, object> config)
{
        return new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }
}
