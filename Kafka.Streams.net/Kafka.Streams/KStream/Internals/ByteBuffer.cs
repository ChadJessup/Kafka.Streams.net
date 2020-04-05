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
using System;

namespace Kafka.Streams.KStream.Internals
{
    public class ByteBuffer
    {
        public ByteBuffer Add(byte[] serializedKey)
        {
            return this;
        }

        internal byte[] Array()
        {
            throw new NotImplementedException();
        }

        public ByteBuffer Allocate(int v)
        {
            return this;
        }

        internal void Add(byte v)
        {
            throw new NotImplementedException();
        }

        internal void PutInt(int oldSize)
        {
            throw new NotImplementedException();
        }

        internal ByteBuffer Wrap(byte[] data)
        {
            return this;
        }

        internal int GetInt()
        {
            throw new NotImplementedException();
        }

        internal void Get(byte[] oldBytes)
        {
            throw new NotImplementedException();
        }

        internal ByteBuffer PutLong(long timestamp)
        {
            return this;
        }

        internal long GetLong()
        {
            throw new NotImplementedException();
        }
    }
}