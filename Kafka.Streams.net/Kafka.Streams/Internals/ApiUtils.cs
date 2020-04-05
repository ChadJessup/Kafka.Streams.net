﻿using Kafka.Common.Extensions;
using Kafka.Streams.KStream.Internals;
using System;

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
namespace Kafka.Streams.Internals
{
    public class ApiUtils
    {
        private const string MILLISECOND_VALIDATION_FAIL_MSG_FRMT = "Invalid value for parameter \"%s\" (value was: %s). ";
        private const string VALIDATE_MILLISECOND_NULL_SUFFIX = "It shouldn't be null.";
        private const string VALIDATE_MILLISECOND_OVERFLOW_SUFFIX = "It can't be converted to milliseconds.";

        /**
            * Validates that milliseconds from {@code duration} can be retrieved.
            * @param duration Duration to check.
            * @param messagePrefix Prefix text for an error message.
            * @return Milliseconds from {@code duration}.
            */
        public static TimeSpan ValidateMillisecondDuration(TimeSpan duration, string messagePrefix)
        {
            try
            {
                if (duration == null)
                {
                    throw new ArgumentException(messagePrefix + VALIDATE_MILLISECOND_NULL_SUFFIX);
                }

                return duration;
            }
            catch (ArithmeticException e)
            {
                throw new ArgumentException(messagePrefix + VALIDATE_MILLISECOND_OVERFLOW_SUFFIX, e);
            }
        }

        /**
            * Validates that milliseconds from {@code instant} can be retrieved.
            * @param instant Instant to check.
            * @param messagePrefix Prefix text for an error message.
            * @return Milliseconds from {@code instant}.
            */
        public static long ValidateMillisecondInstant(DateTime instant, string messagePrefix)
        {
            try
            {
                if (instant == null)
                {
                    throw new ArgumentException(messagePrefix + VALIDATE_MILLISECOND_NULL_SUFFIX);
                }

                return instant.ToEpochMilliseconds();
            }
            catch (ArithmeticException e)
            {
                throw new ArgumentException(messagePrefix + VALIDATE_MILLISECOND_OVERFLOW_SUFFIX, e);
            }
        }

        /**
            * Generates the prefix message for validateMillisecondXXXXXX() utility
            * @param value Object to be converted to milliseconds
            * @param name Object name
            * @return Error message prefix to use in exception
            */
        public static string PrepareMillisCheckFailMsgPrefix(object value, string name)
        {
            return string.Format(MILLISECOND_VALIDATION_FAIL_MSG_FRMT, name, value);
        }

        public static byte[] ConvertToTimestampedFormat(byte[] plainValue)
        {
            if (plainValue == null)
            {
                return null;
            }

            return new ByteBuffer()
                .Allocate(8 + plainValue.Length)
                .PutLong(-1)
                .Add(plainValue)
                .Array();
        }
    }
}
