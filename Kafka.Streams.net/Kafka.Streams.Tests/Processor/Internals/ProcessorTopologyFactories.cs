/*






 *

 *





 */








public class ProcessorTopologyFactories {
    private ProcessorTopologyFactories() {}


    public static ProcessorTopology With(List<ProcessorNode> processorNodes,
                                         Dictionary<string, SourceNode> sourcesByTopic,
                                         List<StateStore> stateStoresByName,
                                         Dictionary<string, string> storeToChangelogTopic) {
        return new ProcessorTopology(processorNodes,
                                     sourcesByTopic,
                                     Collections.emptyMap(),
                                     stateStoresByName,
                                     Collections.emptyList(),
                                     storeToChangelogTopic,
                                     Collections.emptySet());
    }

    static ProcessorTopology WithLocalStores(List<StateStore> stateStores,
                                             Dictionary<string, string> storeToChangelogTopic) {
        return new ProcessorTopology(Collections.emptyList(),
                                     Collections.emptyMap(),
                                     Collections.emptyMap(),
                                     stateStores,
                                     Collections.emptyList(),
                                     storeToChangelogTopic,
                                     Collections.emptySet());
    }

}
