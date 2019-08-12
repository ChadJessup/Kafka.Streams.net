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
//using Confluent.Kafka;
//using Kafka.Streams.Errors.Interfaces;
//using Kafka.Streams.Processor.Interfaces;
//using Kafka.Streams.Processor.Internals.Metrics;
//using System.Collections.Generic;

//namespace Kafka.Streams.Processor.Internals
//{

//    /**
//     * Updates the state for all Global State Stores.
//     */
//    public class GlobalStateUpdateTask : IGlobalStateMaintainer
//    {
//        private ProcessorTopology topology;
//        private IInternalProcessorContext<K, V> processorContext;
//        private Dictionary<TopicPartition, long> offsets = new Dictionary<>();
//        private Dictionary<string, RecordDeserializer> deserializers = new Dictionary<>();
//        private IGlobalStateManager stateMgr;
//        private IDeserializationExceptionHandler deserializationExceptionHandler;
//        private LogContext logContext;

//        public GlobalStateUpdateTask(ProcessorTopology topology,
//                                     IInternalProcessorContext<K, V> processorContext,
//                                     IGlobalStateManager stateMgr,
//                                     IDeserializationExceptionHandler deserializationExceptionHandler,
//                                     LogContext logContext)
//        {
//            this.topology = topology;
//            this.stateMgr = stateMgr;
//            this.processorContext = processorContext;
//            this.deserializationExceptionHandler = deserializationExceptionHandler;
//            this.logContext = logContext;
//        }

//        /**
//         * @throws InvalidOperationException If store gets registered after initialized is already finished
//         * @throws StreamsException      If the store's change log does not contain the partition
//         */

//        public Dictionary<TopicPartition, long> initialize()
//        {
//            HashSet<string> storeNames = stateMgr.initialize();
//            Dictionary<string, string> storeNameToTopic = topology.storeToChangelogTopic();
//            foreach (string storeName in storeNames)
//            {
//                string sourceTopic = storeNameToTopic[storeName];
//                SourceNode source = topology.source(sourceTopic);
//                deserializers.Add(
//                    sourceTopic,
//                    new RecordDeserializer(
//                        source,
//                        deserializationExceptionHandler,
//                        logContext,
//                        ThreadMetrics.skipRecordSensor(processorContext.metrics)
//                    )
//                );
//            }
//            initTopology();
//            processorContext.initialize();
//            return stateMgr.checkpointed();
//        }



//        public void update(ConsumeResult<byte[], byte[]> record)
//        {
//            RecordDeserializer sourceNodeAndDeserializer = deserializers[record.Topic];
//            ConsumeResult<object, object> deserialized = sourceNodeAndDeserializer.Deserialize(processorContext, record);

//            if (deserialized != null)
//            {
//                ProcessorRecordContext recordContext =
//                    new ProcessorRecordContext(deserialized.timestamp(),
//                        deserialized.offset(),
//                        deserialized.partition(),
//                        deserialized.Topic,
//                        deserialized.headers());
//                processorContext.setRecordContext(recordContext);
//                processorContext.setCurrentNode(sourceNodeAndDeserializer.sourceNode());
//                sourceNodeAndDeserializer.sourceNode().process(deserialized.key(), deserialized.value());
//            }

//            offsets.Add(new TopicPartition(record.Topic, record.partition()), record.offset() + 1);
//        }

//        public void flushState()
//        {
//            stateMgr.flush();
//            stateMgr.checkpoint(offsets);
//        }

//        public void close()
//        {
//            stateMgr.close(true);
//        }

//        private void initTopology()
//        {
//            foreach (ProcessorNode node in this.topology.processors())
//            {
//                processorContext.setCurrentNode(node);
//                try
//                {

//                    node.init(this.processorContext);
//                }
//                finally
//                {

//                    processorContext.setCurrentNode(null);
//                }
//            }
//        }
//    }
//}