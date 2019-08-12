///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements. See the NOTICE file distributed with
// * this work for.Additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License. You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//using Microsoft.Extensions.Logging;
//using System.Collections.Generic;

//namespace Kafka.Streams.Processor.Internals
//{
//    public class StreamsPartitionAssignor : ConsumerPartitionAssignor, Configurable
//    {
//        public static int UNKNOWN = -1;
//        private static int VERSION_ONE = 1;
//        private static int VERSION_TWO = 2;
//        private static int VERSION_THREE = 3;
//        private static int VERSION_FOUR = 4;
//        private static int EARLIEST_PROBEABLE_VERSION = VERSION_THREE;
//        protected HashSet<int> supportedVersions = new HashSet<int>();

//        private ILogger log;
//        private string logPrefix;
//        public enum Error
//        {
//            NONE, //(0),
//            INCOMPLETE_SOURCE_TOPIC_METADATA, //(1),
//            VERSION_PROBING, //(2);
//        }

//        private int code;

//        public static Error fromCode(int code)
//        {
//            switch (code)
//            {
//                case 0:
//                    return Error.NONE;
//                case 1:
//                    return Error.INCOMPLETE_SOURCE_TOPIC_METADATA;
//                case 2:
//                    return Error.VERSION_PROBING;
//                default:
//                    throw new System.ArgumentException("Unknown error code: " + code);
//            }
//        }
//    }
//}