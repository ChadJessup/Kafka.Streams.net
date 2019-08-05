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
namespace Kafka.Streams;







/**
 * A meta representation of a {@link Topology topology}.
 * <p>
 * The nodes of a topology are grouped into {@link Subtopology sub-topologies} if they are connected.
 * In contrast, two sub-topologies are not connected but can be linked to each other via topics, i.e., if one
 * sub-topology {@link Topology.AddSink(string, string, string...) writes} into a topic and another sub-topology
 * {@link Topology.AddSource(string, string...) reads} from the same topic.
 * <p>
 * When {@link KafkaStreams#start()} is called, different sub-topologies will be constructed and executed as independent
 * {@link StreamTask tasks}.
 */
public interface TopologyDescription
{

    /**
     * A connected sub-graph of a {@link Topology}.
     * <p>
     * Nodes of a {@code Subtopology} are connected {@link Topology.AddProcessor(string,
     * org.apache.kafka.streams.processor.ProcessorSupplier, string...) directly} or indirectly via
     * {@link Topology#connectProcessorAndStateStores(string, string...) state stores}
     * (i.e., if multiple processors share the same state).
     */
    interface Subtopology
{

        /**
         * Internally assigned unique ID.
         * @return the ID of the sub-topology
         */
        int id();

        /**
         * All nodes of this sub-topology.
         * @return set of all nodes within the sub-topology
         */
        HashSet<Node> nodes();
    }

    /**
     * Represents a {@link Topology.AddGlobalStore(org.apache.kafka.streams.state.StoreBuilder, string,
     * org.apache.kafka.common.serialization.Deserializer, org.apache.kafka.common.serialization.Deserializer, string,
     * string, org.apache.kafka.streams.processor.ProcessorSupplier) global store}.
     * Adding a global store results in.Adding a source node and one stateful processor node.
     * Note, that all.Added global stores form a single unit (similar to a {@link Subtopology}) even if different
     * global stores are not connected to each other.
     * Furthermore, global stores are available to all processors without connecting them explicitly, and thus global
     * stores will never be part of any {@link Subtopology}.
     */
    interface GlobalStore
{

        /**
         * The source node reading from a "global" topic.
         * @return the "global" source node
         */
        Source source();

        /**
         * The processor node maintaining the global store.
         * @return the "global" processor node
         */
        Processor processor();

        int id();
    }

    /**
     * A node of a topology. Can be a source, sink, or processor node.
     */
    interface Node
{

        /**
         * The name of the node. Will never be {@code null}.
         * @return the name of the node
         */
        string name();
        /**
         * The predecessors of this node within a sub-topology.
         * Note, sources do not have any predecessors.
         * Will never be {@code null}.
         * @return set of all predecessors
         */
        HashSet<Node> predecessors();
        /**
         * The successor of this node within a sub-topology.
         * Note, sinks do not have any successors.
         * Will never be {@code null}.
         * @return set of all successor
         */
        HashSet<Node> successors();
    }


    /**
     * A source node of a topology.
     */
    interface Source : Node
{

        /**
         * The topic names this source node is reading from.
         * @return comma separated list of topic names or pattern (as string)
         * @deprecated use {@link #topicSet()} or {@link #topicPattern()} instead
         */
        [System.Obsolete]
        string topics();

        /**
         * The topic names this source node is reading from.
         * @return a set of topic names
         */
        HashSet<string> topicSet();

        /**
         * The pattern used to match topic names that is reading from.
         * @return the pattern used to match topic names
         */
        Pattern topicPattern();
    }

    /**
     * A processor node of a topology.
     */
    interface Processor : Node
{

        /**
         * The names of all connected stores.
         * @return set of store names
         */
        HashSet<string> stores();
    }

    /**
     * A sink node of a topology.
     */
    interface Sink : Node
{

        /**
         * The topic name this sink node is writing to.
         * Could be {@code null} if the topic name can only be dynamically determined based on {@link TopicNameExtractor}
         * @return a topic name
         */
        string topic();

        /**
         * The {@link TopicNameExtractor} that this sink node uses to dynamically extract the topic name to write to.
         * Could be {@code null} if the topic name is not dynamically determined.
         * @return the {@link TopicNameExtractor} used get the topic name
         */
        TopicNameExtractor topicNameExtractor();
    }

    /**
     * All sub-topologies of the represented topology.
     * @return set of all sub-topologies
     */
    HashSet<Subtopology> subtopologies();

    /**
     * All global stores of the represented topology.
     * @return set of all global stores
     */
    HashSet<GlobalStore> globalStores();

}

