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
namespace Kafka.Streams.State.Internals;

using Kafka.Common.Utils.Bytes;
using Kafka.Streams.State.KeyValueStore;




public class ChangeLoggingTimestampedKeyValueBytesStore : ChangeLoggingKeyValueBytesStore
{

    ChangeLoggingTimestampedKeyValueBytesStore(IKeyValueStore<Bytes, byte[]> inner)
{
        super(inner);
    }

    
    void log(Bytes key,
             byte[] valueAndTimestamp)
{
        if (valueAndTimestamp != null)
{
            changeLogger.logChange(key, rawValue(valueAndTimestamp), timestamp(valueAndTimestamp));
        } else
{
            changeLogger.logChange(key, null);
        }
    }
}