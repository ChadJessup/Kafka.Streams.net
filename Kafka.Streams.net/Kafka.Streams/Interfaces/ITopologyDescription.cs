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
using System.Collections.Generic;

namespace Kafka.Streams.Interfaces
{
    /**
     * A meta representation of a {@link Topology topology}.
     * <p>
     * The nodes of a topology are grouped into {@link Subtopology sub-topologies} if they are connected.
     * In contrast, two sub-topologies are not connected but can be linked to each other via topics, i.e., if one
     * sub-topology {@link Topology.AddSink(string, string, string...) writes} into a topic and another sub-topology
     * {@link Topology.AddSource(string, string...) reads} from the same topic.
     * <p>
     * When {@link KafkaStreams#start()} is called, different sub-topologies will be constructed and executed as independent
     * {@link StreamTask<K, V> tasks}.
     */
    public interface ITopologyDescription
    {
        /**
         * All sub-topologies of the represented topology.
         * @return set of all sub-topologies
         */
        HashSet<ISubtopology> subtopologies();

        /**
         * All global stores of the represented topology.
         * @return set of all global stores
         */
        HashSet<IGlobalStore> globalStores();
    }
}