namespace Kafka.Streams.Tests.Processor.Internals
{
    /*






    *

    *





    */












    public class StateManagerStub : StateManager
    {


        public File BaseDir()
        {
            return null;
        }


        public void Register(StateStore store,
                             StateRestoreCallback stateRestoreCallback)
        { }


        public void ReinitializeStateStoresForPartitions(Collection<TopicPartition> partitions,
                                                         InternalProcessorContext processorContext)
        { }


        public void Flush() { }


        public void Close(bool clean)
        { //throws IOException}


            public StateStore getGlobalStore(string name)
            {
                return null;
            }


            public StateStore getStore(string name)
            {
                return null;
            }


            public Dictionary<TopicPartition, long> checkpointed()
            {
                return null;
            }


            public void checkpoint(Dictionary<TopicPartition, long> offsets) { }

        }
}
/*






*

*





*/












