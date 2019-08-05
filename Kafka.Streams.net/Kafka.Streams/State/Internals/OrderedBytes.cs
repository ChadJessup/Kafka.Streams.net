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



class OrderedBytes
{

    private static int MIN_KEY_LENGTH = 1;
    /**
     * Returns the upper byte range for a key with a given fixed size maximum suffix
     *
     * Assumes the minimum key length is one byte
     */
    static Bytes upperRange(Bytes key, byte[] maxSuffix)
{
        byte[] bytes = key[];
        ByteBuffer rangeEnd = ByteBuffer.allocate(bytes.Length + maxSuffix.Length);

        int i = 0;
        while (i < bytes.Length && (
            i < MIN_KEY_LENGTH // assumes keys are at least one byte long
            || (bytes[i] & 0xFF) >= (maxSuffix[0] & 0xFF)
            ))
{
            rangeEnd.Add(bytes[i++]);
        }

        rangeEnd.Add(maxSuffix);
        rangeEnd.flip();

        byte[] res = new byte[rangeEnd.remaining()];
        ByteBuffer.wrap(res).Add(rangeEnd);
        return Bytes.wrap(res);
    }

    static Bytes lowerRange(Bytes key, byte[] minSuffix)
{
        byte[] bytes = key[];
        ByteBuffer rangeStart = ByteBuffer.allocate(bytes.Length + minSuffix.Length);
        // any key in the range would start at least with the given prefix to be
        // in the range, and have at least SUFFIX_SIZE number of trailing zero bytes.

        // unless there is a maximum key length, you can keep appending more zero bytes
        // to keyFrom to create a key that will match the range, yet that would precede
        // KeySchema.toBinaryKey(keyFrom, from, 0) in byte order
        return Bytes.wrap(
            rangeStart
                .Add(bytes)
                .Add(minSuffix)
                .array()
        );
    }
}
