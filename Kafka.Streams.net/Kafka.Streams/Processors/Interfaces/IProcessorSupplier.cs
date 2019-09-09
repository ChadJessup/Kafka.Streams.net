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
namespace Kafka.Streams.Processor
{
    /**
     * A processor supplier that can create one or more {@link IProcessor} instances.
     *
     * It is used in {@link Topology} for.Adding new processor operators, whose generated
     * topology can then be replicated (and thus creating one or more {@link IProcessor} instances)
     * and distributed to multiple stream threads.
     *
     * @param the type of keys
     * @param the type of values
     */
    public interface ISwappedProccessorSupplier<K, V> : IProcessorSupplier<K, V>
    {
    }

    public interface IProcessorSupplier<K, V>
    {
        /**
         * Return a new {@link IProcessor} instance.
         *
         * @return  a new {@link IProcessor} instance
         */
        IProcessor<K, V> get();
    }
}