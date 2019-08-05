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
namespace Kafka.Streams.KStream.Internals
{









class SessionCacheFlushListener<K, V> : CacheFlushListener<Windowed<K>, V> {
    private  IInternalProcessorContext context;
    private  ProcessorNode myNode;

    SessionCacheFlushListener( IProcessorContext context)
{
        this.context = (IInternalProcessorContext) context;
        myNode = this.context.currentNode();
    }

    
    public void apply( Windowed<K> key,
                       V newValue,
                       V oldValue,
                       long timestamp)
{
         ProcessorNode prev = context.currentNode();
        context.setCurrentNode(myNode);
        try
{

            context.forward(key, new Change<>(newValue, oldValue), To.all().withTimestamp(key.window().end()));
        } finally
{

            context.setCurrentNode(prev);
        }
    }
}
