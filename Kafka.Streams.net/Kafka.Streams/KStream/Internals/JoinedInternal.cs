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
namespace Kafka.Streams.KStream.Internals {




public JoinedInternal<K, V, VO> : Joined<K, V, VO>  {

    JoinedInternal( Joined<K, V, VO> joined)
{
        base(joined);
    }

    public ISerde<K> keySerde()
{
        return keySerde;
    }

    public ISerde<V> valueSerde()
{
        return valueSerde;
    }

    public ISerde<VO> otherValueSerde()
{
        return otherValueSerde;
    }

     // TODO Remove annotation when base.name() is removed
    @SuppressWarnings("deprecation") // this method should not be removed if base.name() is removed
    public string name()
{
        return name;
    }
}
