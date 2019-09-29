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
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Interfaces
{
    public interface IKStreamAggProcessorSupplier<K, KR, V, VR> : IProcessorSupplier<K, V>
    {
        IKTableValueGetterSupplier<KR, VR> view();

        IProcessorSupplier<KR, VR> GetSwappedProcessorSupplier();
        void enableSendingOldValues();
    }
}