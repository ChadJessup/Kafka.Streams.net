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
using Confluent.Kafka;
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Processor.Interfaces;
using System;
using System.Collections.Generic;
using System.IO;

namespace Kafka.Streams.Processor.Internals
{
    /**
     * {@code IProcessorContext} implementation that will throw on any forward call.
     */
    public class ForwardingDisabledProcessorContext<K, V> : IProcessorContext<K, V>
    {
        private IProcessorContext<K, V> @delegate;

        public ForwardingDisabledProcessorContext(IProcessorContext<K, V> @delegate)
        {
            this.@delegate = @delegate = @delegate ?? throw new System.ArgumentNullException("@delegate", nameof(@delegate));
        }


        public string applicationId()
        {
            return @delegate.applicationId();
        }


        public TaskId taskId()
        {
            return @delegate.taskId();
        }


        public ISerde<K> keySerde => @delegate.keySerde;
        public ISerde<V> valueSerde => @delegate.valueSerde;

        public DirectoryInfo stateDir()
        {
            return @delegate.stateDir();
        }

        public IStreamsMetrics metrics
            => @delegate.metrics;

        public void register(
            IStateStore store,
            IStateRestoreCallback stateRestoreCallback)
        {
            @delegate.register(store, stateRestoreCallback);
        }


        public IStateStore getStateStore(string name)
        {
            return @delegate.getStateStore(name);
        }


        [System.Obsolete]
        public ICancellable schedule(
            long intervalMs,
            PunctuationType type,
            Punctuator callback)
        {
            return @delegate.schedule(TimeSpan.FromMilliseconds(intervalMs), type, callback);
        }


        public ICancellable schedule(
            TimeSpan interval,
            PunctuationType type,
            Punctuator callback)
        {
            return @delegate.schedule(interval, type, callback);
        }


        public void forward(K key, V value)
        {
            throw new StreamsException("IProcessorContext#forward() not supported.");
        }


        public void forward(K key, V value, To to)
        {
            throw new StreamsException("IProcessorContext#forward() not supported.");
        }


        [System.Obsolete]
        public void forward(K key, V value, int childIndex)
        {
            throw new StreamsException("IProcessorContext#forward() not supported.");
        }


        [System.Obsolete]
        public void forward(K key, V value, string childName)
        {
            throw new StreamsException("IProcessorContext#forward() not supported.");
        }

        public void commit()
        {
            @delegate.commit();
        }

        public string Topic => @delegate.Topic;

        public int partition()
        {
            return @delegate.partition();
        }

        public long offset()
        {
            return @delegate.offset();
        }

        public Headers headers()
        {
            return @delegate.headers();
        }


        public long timestamp()
        {
            return @delegate.timestamp();
        }


        public Dictionary<string, object> appConfigs()
        {
            return @delegate.appConfigs();
        }


        public Dictionary<string, object> appConfigsWithPrefix(string prefix)
        {
            return @delegate.appConfigsWithPrefix(prefix);
        }
    }
}