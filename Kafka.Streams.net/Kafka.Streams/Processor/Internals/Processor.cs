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
using Kafka.Common;
using System.Collections.Generic;

namespace Kafka.Streams.Processor.Internals
{
    public class InternalTopologyBuilder
    {
        public static class Processor : AbstractNode, TopologyDescription.Processor
        {

            private HashSet<string> stores;

            public Processor(string name,
                             HashSet<string> stores)
            {
                base(name);
                this.stores = stores;
            }


            public HashSet<string> stores()
            {
                return Collections.unmodifiableSet(stores);
            }


            public string ToString()
            {
                return "Processor: " + name + " (stores: " + stores + ")\n      --> "
                    + nodeNames(successors) + "\n      <-- " + nodeNames(predecessors);
            }


            public bool Equals(object o)
            {
                if (this == o)
                {
                    return true;
                }
                if (o == null || GetType() != o.GetType())
                {
                    return false;
                }

                Processor processor = (Processor)o;
                // omit successor to avoid infinite loops
                return name.Equals(processor.name)
                    && stores.Equals(processor.stores)
                    && predecessors.Equals(processor.predecessors);
            }


            public int GetHashCode()
            {
                // omit successor as it might change and alter the hash code
                return Objects.hash(name, stores);
            }
        }
            }
}
