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
using Kafka.Streams.KStream.Internals;
using System;

namespace Kafka.Streams.KStream
{
    public class WindowedSerdes
    {
        /**
         * Construct a {@code TimeWindowedSerde} object for the specified inner class type.
         */
        static public ISerde<Windowed<T>> timeWindowedSerdeFrom<T>()
        {
            return new TimeWindowedSerde<T>(Serdes.serdeFrom<T>());
        }

        /**
         * Construct a {@code TimeWindowedSerde} object to deserialize changelog topic
         * for the specified inner class type and window size.
         */
        public static Serde<Windowed<T>> timeWindowedSerdeFrom<T>(long windowSize)
        {
            return new TimeWindowedSerde<T>(Serdes.serdeFrom<T>(), windowSize);
        }

        /**
         * Construct a {@code SessionWindowedSerde} object for the specified inner class type.
         */
        public static Serde<Windowed<T>> sessionWindowedSerdeFrom<T>()
        {
            return new SessionWindowedSerde<T>(Serdes.serdeFrom<T>());
        }

        public static void verifyInnerSerializerNotNull<T>(
            ISerializer<T> inner,
             ISerializer<T> wrapper)
        {
            if (inner == null)
            {
                throw new ArgumentNullException("Inner serializer is `null`. " +
                    "User code must use constructor `" + wrapper.GetType().Name + "(Serializer<T> inner)` " +
                    "instead of the no-arg constructor.");
            }
        }

        static void verifyInnerDeserializerNotNull<T>(
            IDeserializer<T> inner,
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