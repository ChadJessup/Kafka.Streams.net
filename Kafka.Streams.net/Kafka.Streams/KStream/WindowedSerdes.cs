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
using Confluent.Kafka;
using Kafka.Streams.Interfaces;
using System;

namespace Kafka.Streams.KStream
{
    public class WindowedSerdes
    {
        /**
         * Construct a {@code TimeWindowedSerde} object for the specified inner class type.
         */
        static public ISerde<Windowed<T>> TimeWindowedSerdeFrom<T>()
        {
            return null;// new TimeWindowedSerde<T>(Serdes.SerdeFrom<T>());
        }

        /**
         * Construct a {@code TimeWindowedSerde} object to deserialize changelog topic
         * for the specified inner class type and window size.
         */
        public static Serde<Windowed<T>> TimeWindowedSerdeFrom<T>(long windowSize)
        {
            return null;// new TimeWindowedSerde<T>(Serdes.SerdeFrom<T>(), windowSize);
        }

        /**
         * Construct a {@code SessionWindowedSerde} object for the specified inner class type.
         */
        public static Serde<Windowed<T>> SessionWindowedSerdeFrom<T>()
        {
            return null; // new SessionWindowedSerde<T>(Serdes.SerdeFrom<T>());
        }

        public static void VerifyInnerSerializerNotNull<T>(ISerializer<T> inner, ISerializer<T> wrapper)
        {
            if (inner == null)
            {
                throw new ArgumentNullException("Inner serializer is `null`. " +
                    "User code must use constructor `" + wrapper.GetType().Name + "(Serializer<T> inner)` " +
                    "instead of the no-arg constructor.");
            }
        }

        // TODO: share internals here, shouldn't be public
        public static void VerifyInnerDeserializerNotNull<T>(
            IDeserializer<T>? inner,
            IDeserializer<T> wrapper)
        {
            if (inner == null)
            {
                throw new ArgumentNullException("Inner deserializer is `null`. " +
                    "User code must use constructor `" + wrapper.GetType().FullName + "(Deserializer<T> inner)` " +
                    "instead of the no-arg constructor.");
            }
        }
    }
}