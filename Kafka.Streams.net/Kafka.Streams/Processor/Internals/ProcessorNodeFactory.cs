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
using Kafka.Streams.Interfaces;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.IProcessor.Internals
{
    public class ProcessorNodeFactory<K, V> : NodeFactory<K, V>
    {
        private IProcessorSupplier<K, V> supplier;
        protected HashSet<string> stateStoreNames { get; } = new HashSet<string>();

        public ProcessorNodeFactory(
            string name,
            string[] predecessors,
            IProcessorSupplier<K, V> supplier)
            : base(name, predecessors.Select(p => p).ToArray())
        {
            this.supplier = supplier;
        }

        public void addStateStore(string stateStoreName)
        {
            stateStoreNames.Add(stateStoreName);
        }

        public override ProcessorNode<K, V> build()
        {
            return new ProcessorNode<K, V>(name, supplier, stateStoreNames);
        }

        public override IProcessor describe()
        {
            return new IProcessor(name, new HashSet<string>(stateStoreNames));
        }
    }
}
