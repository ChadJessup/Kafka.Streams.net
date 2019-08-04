/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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
namespace Kafka.streams.processor.internals;

using Kafka.Common.header.Headers;
using Kafka.Common.serialization.Serde;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.IProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.IStateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;

import java.io.File;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/**
 * {@code IProcessorContext} implementation that will throw on any forward call.
 */
public class ForwardingDisabledProcessorContext : IProcessorContext {
    private IProcessorContext delegate;

    public ForwardingDisabledProcessorContext(IProcessorContext delegate)
{
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    
    public string applicationId()
{
        return delegate.applicationId();
    }

    
    public TaskId taskId()
{
        return delegate.taskId();
    }

    
    public ISerde<?> keySerde()
{
        return delegate.keySerde();
    }

    
    public ISerde<?> valueSerde()
{
        return delegate.valueSerde();
    }

    
    public File stateDir()
{
        return delegate.stateDir();
    }

    
    public StreamsMetrics metrics()
{
        return delegate.metrics();
    }

    
    public void register(IStateStore store,
                         StateRestoreCallback stateRestoreCallback)
{
        delegate.register(store, stateRestoreCallback);
    }

    
    public IStateStore getStateStore(string name)
{
        return delegate.getStateStore(name);
    }

    
    @Deprecated
    public ICancellable schedule(long intervalMs,
                                PunctuationType type,
                                Punctuator callback)
{
        return delegate.schedule(intervalMs, type, callback);
    }

    
    public ICancellable schedule(Duration interval,
                                PunctuationType type,
                                Punctuator callback) throws ArgumentException {
        return delegate.schedule(interval, type, callback);
    }

    
    public <K, V> void forward(K key, V value)
{
        throw new StreamsException("IProcessorContext#forward() not supported.");
    }

    
    public <K, V> void forward(K key, V value, To to)
{
        throw new StreamsException("IProcessorContext#forward() not supported.");
    }

    
    @Deprecated
    public <K, V> void forward(K key, V value, int childIndex)
{
        throw new StreamsException("IProcessorContext#forward() not supported.");
    }

    
    @Deprecated
    public <K, V> void forward(K key, V value, string childName)
{
        throw new StreamsException("IProcessorContext#forward() not supported.");
    }

    
    public void commit()
{
        delegate.commit();
    }

    
    public string topic()
{
        return delegate.topic();
    }

    
    public int partition()
{
        return delegate.partition();
    }

    
    public long offset()
{
        return delegate.offset();
    }

    
    public Headers headers()
{
        return delegate.headers();
    }

    
    public long timestamp()
{
        return delegate.timestamp();
    }

    
    public Dictionary<string, object> appConfigs()
{
        return delegate.appConfigs();
    }

    
    public Dictionary<string, object> appConfigsWithPrefix(string prefix)
{
        return delegate.appConfigsWithPrefix(prefix);
    }
}
