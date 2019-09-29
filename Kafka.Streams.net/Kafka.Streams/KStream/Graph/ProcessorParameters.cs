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
using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals.Graph
{
    /**
     * Class used to represent a {@link IProcessorSupplier} and the name
     * used to register it with the {@link org.apache.kafka.streams.processor.Internals.InternalTopologyBuilder}
     *
     * Used by the Join nodes as there are several parameters, this abstraction helps
     * keep the number of arguments more reasonable.
     */
    public class ProcessorParameters<K, V>
    {
        public IProcessorSupplier<K, V> ProcessorSupplier { get; }
        public string processorName { get; }

        public ProcessorParameters(
            IProcessorSupplier<K, V> processorSupplier,
            string processorName)
        {
            this.ProcessorSupplier = processorSupplier;
            this.processorName = processorName;
        }

        public override string ToString()
            => $"ProcessorParameters{{processor={ProcessorSupplier.GetType()}, " +
            $"processor name='{processorName}'}}";

        //public static ProcessorParameters<K, VR> ConvertFrom<VR>(
        //    ProcessorParameters<K, Change<V>> processorParameters)
        //{
        //    var pp = new ProcessorParameters<K, VR>(
        //        processorParameters.ProcessorSupplier,
        //        processorParameters.processorName);
        //}
    }
}
