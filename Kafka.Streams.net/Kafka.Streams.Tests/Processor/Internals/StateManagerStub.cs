/*






 *

 *





 */












public class StateManagerStub : StateManager {

    
    public File baseDir() {
        return null;
    }

    
    public void register(StateStore store,
                         StateRestoreCallback stateRestoreCallback) {}

    
    public void reinitializeStateStoresForPartitions(Collection<TopicPartition> partitions,
                                                     InternalProcessorContext processorContext) {}

    
    public void flush() {}

    
    public void close(bool clean){ //throws IOException}

    
    public StateStore getGlobalStore(string name) {
        return null;
    }

    
    public StateStore getStore(string name) {
        return null;
    }

    
    public Dictionary<TopicPartition, long> checkpointed() {
        return null;
    }

    
    public void checkpoint(Dictionary<TopicPartition, long> offsets) {}

}
