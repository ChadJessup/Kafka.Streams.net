﻿/*
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

namespace Kafka.Streams.IProcessor.Internals
{
    public abstract class NodeFactory
    {
        public string name { get; protected set; }
        public string[] predecessors { get; protected set; }
    }

    public abstract class NodeFactory<K, V> : NodeFactory
    {
        public NodeFactory(
            string name,
            string[] predecessors)
        {
            this.name = name;
            this.predecessors = predecessors;
        }

        public abstract ProcessorNode<K, V> build();

        public abstract AbstractNode describe();
    }
}
