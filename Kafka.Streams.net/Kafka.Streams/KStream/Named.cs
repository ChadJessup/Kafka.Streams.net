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
using Kafka.Common;
using Kafka.Streams.Errors;
using Kafka.Streams.KStream.Interfaces;

namespace Kafka.Streams.KStream
{
    public class Named : INamedOperation<Named>
    {

        private static int MAX_NAME_LENGTH = 249;

        protected string name;

        protected Named(Named named)
            : this(Objects.requireNonNull(named, "named can't be null").name)
        {
        }

        protected Named(string name)
        {
            this.name = name;
            if (name != null)
            {
                validate(name);
            }
        }

        /**
         * Create a Named instance with provided name.
         *
         * @param name  the processor name to be used. If {@code null} a default processor name will be generated.
         * @return      A new {@link Named} instance configured with name
         *
         * @throws TopologyException if an invalid name is specified; valid characters are ASCII alphanumerics, '.', '_' and '-'.
         */
        public static Named As(string name)
        {
            Objects.requireNonNull(name, "name can't be null");
            return new Named(name);
        }

        public Named withName(string name)
        {
            return new Named(name);
        }

        public static void validate(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new TopologyException("Name is illegal, it can't be empty");
            }

            if (name.Equals(".") || name.Equals(".."))
            {
                throw new TopologyException("Name cannot be \".\" or \"..\"");
            }

            if (name.Length > MAX_NAME_LENGTH)
            {
                throw new TopologyException("Name is illegal, it can't be longer than " + MAX_NAME_LENGTH +
                        " characters, name: " + name);
            }

            if (!containsValidPattern(name))
                throw new TopologyException("Name \"" + name + "\" is illegal, it contains a character other than " +
                        "ASCII alphanumerics, '.', '_' and '-'");
        }

        /**
         * Valid characters for Kafka topics are the ASCII alphanumerics, '.', '_', and '-'
         */
        private static bool containsValidPattern(string topic)
        {
            for (int i = 0; i < topic.Length; ++i)
            {
                char c = topic[i];

                // We don't use Character.isLetterOrDigit(c) because it's slower
                bool validLetterOrDigit = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z');
                bool validChar = validLetterOrDigit || c == '.' || c == '_' || c == '-';
                if (!validChar)
                {
                    return false;
                }
            }
            return true;
        }
    }
}