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

//namespace Kafka.Streams.Processors
//{
//    /**
//     * Abstract implementation of the  {@link StateRestoreCallback} used for batch restoration operations.
//     *
//     * Includes default no-op methods of the {@link StateRestoreListener} {@link StateRestoreListener#onRestoreStart(TopicPartition, string, long, long)},
//     * {@link StateRestoreListener#onBatchRestored(TopicPartition, string, long, long)}, and {@link StateRestoreListener#onRestoreEnd(TopicPartition, string, long)}.
//     */
//    public abstract class AbstractNotifyingRestoreCallback : IStateRestoreCallback, IStateRestoreListener
//    {



//        /**
//         * @see StateRestoreListener#onRestoreStart(TopicPartition, string, long, long)
//         *
//         * This method does nothing by default; if desired, sues should override it with custom functionality.
//         *
//         */

//        public void onRestoreStart(TopicPartition topicPartition,
//                                   string storeName,
//                                   long startingOffset,
//                                   long endingOffset)
//        {

//        }


//        /**
//         * @see StateRestoreListener#onBatchRestored(TopicPartition, string, long, long)
//         *
//         * This method does nothing by default; if desired, sues should override it with custom functionality.
//         *
//         */

//        public void onBatchRestored(TopicPartition topicPartition,
//                                    string storeName,
//                                    long batchEndOffset,
//                                    long numRestored)
//        {

//        }

//        /**
//         * @see StateRestoreListener#onRestoreEnd(TopicPartition, string, long)
//         *
//         * This method does nothing by default; if desired, sues should override it with custom functionality.
//         *
//         */

//        public void onRestoreEnd(TopicPartition topicPartition,
//                                 string storeName,
//                                 long totalRestored)
//        {

//        }
//    }
//}