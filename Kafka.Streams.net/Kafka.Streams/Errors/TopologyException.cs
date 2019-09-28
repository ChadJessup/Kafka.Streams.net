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
using System;

namespace Kafka.Streams.Errors
{
    /**
     * Indicates a pre run time error occurred while parsing the {@link org.apache.kafka.streams.Topology logical topology}
     * to construct the {@link org.apache.kafka.streams.processor.Internals.ProcessorTopology physical processor topology}.
     */
    public class TopologyException : StreamsException
    {
        public TopologyException(string message)
            : base("Invalid topology" + (message == null ? "" : ": " + message))
        {
        }

        public TopologyException(string message, Exception throwable)
            : base("Invalid topology" + (message == null ? "" : ": " + message), throwable)
        {
        }

        public TopologyException(Exception throwable)
            : base(throwable)
        {
        }
    }
}