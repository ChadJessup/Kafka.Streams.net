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
using Kafka.Streams.IProcessor;
using Kafka.Streams.IProcessor.Interfaces;
using Kafka.Streams.IProcessor.Internals;
using System;

namespace Kafka.Streams.IProcessor.Internals
{
    using Confluent.Kafka;
    using System.Collections.Generic;
    using System;
    using Kafka.Streams.IProcessor.Interfaces;

    public class StandbyContextImpl : AbstractProcessorContext, IRecordCollector.Supplier
    {

        private static IRecordCollector NO_OP_COLLECTOR = new IRecordCollector()
        {

        public void send(
            string topic,
            K key,
            V value,
            Headers headers,
            int partition,
            long timestamp,
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer)
        {
        }


        public void send(
            string topic,
            K key,
            V value,
            Headers headers,
            long timestamp,
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer,
            IStreamPartitioner<K, V> partitioner)
        { }


        public void init(IProducer<byte[], byte[]> producer) { }


        public void flush() { }


        public void close() { }


        public Dictionary<TopicPartition, long> offsets()
        {
            return new Dictionary<TopicPartition, long>();
        }
    };

    StandbyContextImpl(TaskId id,
                       StreamsConfig config,
                       ProcessorStateManager stateMgr,
                       StreamsMetricsImpl metrics)
         : base(
            id,
            config,
            metrics,
            stateMgr,
            new ThreadCache(
                new LogContext(string.Format("stream-thread [%s] ", Thread.CurrentThread.getName())],
                0,
                metrics))
    {
    }


    IStateManager getStateMgr()
    {
        return stateManager;
    }


    public IRecordCollector recordCollector()
    {
        return NO_OP_COLLECTOR;
    }

    /**
     * @throws InvalidOperationException on every invocation
     */

    public IStateStore getStateStore(string name)
    {
        throw new InvalidOperationException("this should not happen: getStateStore() not supported in standby tasks.");
    }

    /**
     * @throws InvalidOperationException on every invocation
     */

    public string Topic
    {
        throw new InvalidOperationException("this should not happen: Topic not supported in standby tasks.");
}

/**
 * @throws InvalidOperationException on every invocation
 */

public int partition()
{
    throw new InvalidOperationException("this should not happen: partition() not supported in standby tasks.");
}

/**
 * @throws InvalidOperationException on every invocation
 */

public long offset()
{
    throw new InvalidOperationException("this should not happen: offset() not supported in standby tasks.");
}

/**
 * @throws InvalidOperationException on every invocation
 */

public long timestamp()
{
    throw new InvalidOperationException("this should not happen: timestamp() not supported in standby tasks.");
}

/**
 * @throws InvalidOperationException on every invocation
 */

public void forward(K key, V value)
{
    throw new InvalidOperationException("this should not happen: forward() not supported in standby tasks.");
}

/**
 * @throws InvalidOperationException on every invocation
 */

public void forward(K key, V value, To to)
{
    throw new InvalidOperationException("this should not happen: forward() not supported in standby tasks.");
}

/**
 * @throws InvalidOperationException on every invocation
 */

[System.Obsolete]
public void forward(K key, V value, int childIndex)
{
    throw new InvalidOperationException("this should not happen: forward() not supported in standby tasks.");
}

/**
 * @throws InvalidOperationException on every invocation
 */

[Obsolete]
public void forward(K key, V value, string childName)
{
    throw new InvalidOperationException("this should not happen: forward() not supported in standby tasks.");
}

/**
 * @throws InvalidOperationException on every invocation
 */

public void commit()
{
    throw new InvalidOperationException("this should not happen: commit() not supported in standby tasks.");
}

/**
 * @throws InvalidOperationException on every invocation
 */

[System.Obsolete]
public ICancellable schedule(long interval, PunctuationType type, Punctuator callback)
{
    throw new InvalidOperationException("this should not happen: schedule() not supported in standby tasks.");
}

/**
 * @throws InvalidOperationException on every invocation
 */

public ICancellable schedule(TimeSpan interval, PunctuationType type, Punctuator callback)
{
    throw new InvalidOperationException("this should not happen: schedule() not supported in standby tasks.");
}

/**
 * @throws InvalidOperationException on every invocation
 */

public ProcessorRecordContext recordContext()
{
    throw new InvalidOperationException("this should not happen: recordContext not supported in standby tasks.");
}

/**
 * @throws InvalidOperationException on every invocation
 */

public void setRecordContext(ProcessorRecordContext recordContext)
{
    throw new InvalidOperationException("this should not happen: setRecordContext not supported in standby tasks.");
}


public void setCurrentNode(ProcessorNode currentNode)
{
    // no-op. can't throw as this is called on commit when the StateStores get flushed.
}

/**
 * @throws InvalidOperationException on every invocation
 */

public ProcessorNode currentNode()
{
    throw new InvalidOperationException("this should not happen: currentNode not supported in standby tasks.");
}
