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
using Kafka.Streams.IProcessor;
using Kafka.Streams.IProcessor.Interfaces;
using Kafka.Streams.State.Interfaces;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamJoinWindowProcessor<K, V> : AbstractProcessor<K, V>
    {

        private IWindowStore<K, V> window;

        public KStreamJoinWindowProcessor(string windowName)
        {
            this.windowName = windowName;
        }

        public string windowName { get; }


        public override void init(IProcessorContext<K, V> context)
        {
            base.init(context);

            window = (IWindowStore<K, V>)context.getStateStore(windowName);
        }


        public override void process(K key, V value)
        {
            // if the key is null, we do not need to put the record into window store
            // since it will never be considered for join operations
            if (key != null)
            {
                context.forward(key, value);
                // Every record basically starts a new window. We're using a window store mostly for the retention.
                window.put(key, value, context.timestamp());
            }
        }
    }
}