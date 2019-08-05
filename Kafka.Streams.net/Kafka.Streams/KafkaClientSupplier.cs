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
namespace Kafka.streams;









/**
 * {@code KafkaClientSupplier} can be used to provide custom Kafka clients to a {@link KafkaStreams} instance.
 *
 * @see KafkaStreams#KafkaStreams(Topology, java.util.Properties, KafkaClientSupplier)
 */
public interface KafkaClientSupplier
{

    /**
     * Create an {@link Admin} which is used for internal topic management.
     *
     * @param config Supplied by the {@link java.util.Properties} given to the {@link KafkaStreams}
     * @return an instance of {@link Admin}
     */
    Admin getAdminClient( Map<string, object> config);

    /**
     * Create a {@link Producer} which is used to write records to sink topics.
     *
     * @param config {@link StreamsConfig#getProducerConfigs(string) producer config} which is supplied by the
     *               {@link java.util.Properties} given to the {@link KafkaStreams} instance
     * @return an instance of Kafka producer
     */
    Producer<byte[], byte[]> getProducer( Map<string, object> config);

    /**
     * Create a {@link Consumer} which is used to read records of source topics.
     *
     * @param config {@link StreamsConfig#getMainConsumerConfigs(string, string, int) consumer config} which is
     *               supplied by the {@link java.util.Properties} given to the {@link KafkaStreams} instance
     * @return an instance of Kafka consumer
     */
    Consumer<byte[], byte[]> getConsumer( Map<string, object> config);

    /**
     * Create a {@link Consumer} which is used to read records to restore {@link IStateStore}s.
     *
     * @param config {@link StreamsConfig#getRestoreConsumerConfigs(string) restore consumer config} which is supplied
     *               by the {@link java.util.Properties} given to the {@link KafkaStreams}
     * @return an instance of Kafka consumer
     */
    Consumer<byte[], byte[]> getRestoreConsumer( Map<string, object> config);

    /**
     * Create a {@link Consumer} which is used to consume records for {@link GlobalKTable}.
     *
     * @param config {@link StreamsConfig#getGlobalConsumerConfigs(string) global consumer config} which is supplied
     *               by the {@link java.util.Properties} given to the {@link KafkaStreams}
     * @return an instance of Kafka consumer
     */
    Consumer<byte[], byte[]> getGlobalConsumer( Map<string, object> config);
}
