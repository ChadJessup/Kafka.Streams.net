using Confluent.Kafka;
using Kafka.Streams.Errors.Interfaces;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors.Interfaces;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public class GlobalStateUpdateTask : IGlobalStateMaintainer
    {
        private ProcessorTopology topology;
        private IInternalProcessorContext processorContext;
        private Dictionary<TopicPartition, long> offsets = new Dictionary<TopicPartition, long>();
        private Dictionary<string, RecordDeserializer> deserializers = new Dictionary<string, RecordDeserializer>();
        private GlobalStateManager stateMgr;
        private IDeserializationExceptionHandler deserializationExceptionHandler;

        public GlobalStateUpdateTask(
            ProcessorTopology topology,
            IInternalProcessorContext processorContext,
            GlobalStateManager stateMgr,
            IDeserializationExceptionHandler deserializationExceptionHandler)
        {
            this.topology = topology;
            this.stateMgr = stateMgr;
            this.processorContext = processorContext;
            this.deserializationExceptionHandler = deserializationExceptionHandler;
        }

        public Dictionary<TopicPartition, long?> Initialize()
        {
            HashSet<string> storeNames = this.stateMgr.Initialize();
            Dictionary<string, string> storeNameToTopic = this.topology.StoreToChangelogTopic;

            foreach (string storeName in storeNames)
            {
                string sourceTopic = storeNameToTopic[storeName];
                ISourceNode source = this.topology.Source(sourceTopic);
                this.deserializers.Add(
                    sourceTopic,
                    new RecordDeserializer(
                        null,
                        source,
                        this.deserializationExceptionHandler)
                );
            }

            this.InitTopology();
            this.processorContext.Initialize();
            return this.stateMgr.Checkpointed();
        }

        public void Update(ConsumeResult<byte[], byte[]> record)
        {
            var sourceNodeAndDeserializer = this.deserializers[record.Topic];
            ConsumeResult<object, object> deserialized = sourceNodeAndDeserializer.Deserialize<object, object>(this.processorContext, record);

            if (deserialized != null)
            {
                var recordContext =
                    new ProcessorRecordContext(
                        deserialized.Timestamp.UtcDateTime,
                        deserialized.Offset,
                        deserialized.Partition,
                        deserialized.Topic,
                        deserialized.Headers);
                this.processorContext.SetRecordContext(recordContext);
                this.processorContext.SetCurrentNode(sourceNodeAndDeserializer.SourceNode);

                sourceNodeAndDeserializer.SourceNode.Process(deserialized.Key, deserialized.Value);
            }

            this.offsets.Add(new TopicPartition(record.Topic, record.Partition), record.Offset + 1);
        }

        public void FlushState()
        {
            this.stateMgr.Flush();
            this.stateMgr.Checkpoint(this.offsets);
        }

        public void Close()
        {
            this.stateMgr.Close(true);
        }

        private void InitTopology()
        {
            foreach (ProcessorNode node in this.topology.Processors())
            {
                this.processorContext.SetCurrentNode(node);

                try
                {
                    node.Init(this.processorContext);
                }
                finally
                {
                    this.processorContext.SetCurrentNode(null);
                }
            }
        }
    }
}
