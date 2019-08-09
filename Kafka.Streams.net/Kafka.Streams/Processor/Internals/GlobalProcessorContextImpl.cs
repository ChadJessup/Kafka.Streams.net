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
namespace Kafka.Streams.Processor.Internals;
























public class GlobalProcessorContextImpl : AbstractProcessorContext
{



    public GlobalProcessorContextImpl(StreamsConfig config,
                                      IStateManager stateMgr,
                                      StreamsMetricsImpl metrics,
                                      ThreadCache cache)
{
        base(new TaskId(-1, -1), config, metrics, stateMgr, cache);
    }



    public IStateStore getStateStore(string name)
{
        IStateStore store = stateManager.getGlobalStore(name);

        if (store is TimestampedKeyValueStore)
{
            return new TimestampedKeyValueStoreReadWriteDecorator((TimestampedKeyValueStore) store);
        } else if (store is IKeyValueStore)
{
            return new KeyValueStoreReadWriteDecorator((IKeyValueStore) store);
        } else if (store is TimestampedWindowStore)
{
            return new TimestampedWindowStoreReadWriteDecorator((TimestampedWindowStore) store);
        } else if (store is WindowStore)
{
            return new WindowStoreReadWriteDecorator((WindowStore) store);
        } else if (store is ISessionStore)
{
            return new SessionStoreReadWriteDecorator((ISessionStore) store);
        }

        return store;
    }



    public void forward(K key, V value)
{
        ProcessorNode previousNode = currentNode();
        try
{

            foreach (ProcessorNode child in (List<ProcessorNode<K, V>>) currentNode().children())
{
                setCurrentNode(child);
                child.process(key, value);
            }
        } finally
{

            setCurrentNode(previousNode);
        }
    }

    /**
     * No-op. This should only be called on GlobalStateStore#flush and there should be no child nodes
     */

    public void forward(K key, V value, To to)
{
        if (!currentNode().children().isEmpty())
{
            throw new InvalidOperationException("This method should only be called on 'GlobalStateStore.flush' that should not have any children.");
        }
    }

    /**
     * @throws InvalidOperationException on every invocation
     */

    [System.Obsolete]
    public void forward(K key, V value, int childIndex)
{
        throw new InvalidOperationException("this should not happen: forward() not supported in global processor context.");
    }

    /**
     * @throws InvalidOperationException on every invocation
     */

    [System.Obsolete]
    public void forward(K key, V value, string childName)
{
        throw new InvalidOperationException("this should not happen: forward() not supported in global processor context.");
    }


    public void commit()
{
        //no-op
    }

    /**
     * @throws InvalidOperationException on every invocation
     */

    [System.Obsolete]
    public ICancellable schedule(long interval, PunctuationType type, Punctuator callback)
{
        throw new InvalidOperationException("this should not happen: schedule() not supported in global processor context.");
    }

    /**
     * @throws InvalidOperationException on every invocation
     */

    public ICancellable schedule(TimeSpan interval, PunctuationType type, Punctuator callback)
{
        throw new InvalidOperationException("this should not happen: schedule() not supported in global processor context.");
    }
}