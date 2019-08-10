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
using Kafka.Streams.Interfaces;
using System;

namespace Kafka.Streams.IProcessor.Internals
{
    public partial class InternalTopologyBuilder
    {
        public class GlobalStore : IGlobalStore
        {

            private Source source;
            private IProcessor processor;
            private int id;

            public GlobalStore(string sourceName,
                               string processorName,
                               string storeName,
                               string topicName,
                               int id)
            {
                source = new Source(sourceName, Collections.singleton(topicName), null);
                processor = new IProcessor(processorName, Collections.singleton(storeName));
                source.successors.Add(processor);
                processor.predecessors.Add(source);
                this.id = id;
            }

            public string ToString()
            {
                return "Sub-topology: " + id + " for global store (will not generate tasks)\n"
                        + "    " + source.ToString() + "\n"
                        + "    " + processor.ToString() + "\n";
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

                GlobalStore that = (GlobalStore)o;
                return source.Equals(that.source)
                    && processor.Equals(that.processor);
            }


            public int GetHashCode()
            {
                return Objects.hash(source, processor);
            }
        }
    }
}
