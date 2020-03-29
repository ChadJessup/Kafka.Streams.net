/*






 *

 *





 */












public class PunctuationQueueTest {

    private MockProcessorNode<string, string> node = new MockProcessorNode<>();
    private PunctuationQueue queue = new PunctuationQueue();
    private Punctuator punctuator = new Punctuator() {
        
        public void punctuate(long timestamp) {
            node.mockProcessor.punctuatedStreamTime.add(timestamp);
        }
    };

    [Xunit.Fact]
    public void testPunctuationInterval() {
        PunctuationSchedule sched = new PunctuationSchedule(node, 0L, 100L, punctuator);
        long now = sched.timestamp - 100L;

        queue.schedule(sched);

        ProcessorNodePunctuator processorNodePunctuator = new ProcessorNodePunctuator() {
            
            public void punctuate(ProcessorNode node, long time, PunctuationType type, Punctuator punctuator) {
                punctuator.punctuate(time);
            }
        };

        queue.mayPunctuate(now, PunctuationType.STREAM_TIME, processorNodePunctuator);
        Assert.Equal(0, node.mockProcessor.punctuatedStreamTime.Count);

        queue.mayPunctuate(now + 99L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        Assert.Equal(0, node.mockProcessor.punctuatedStreamTime.Count);

        queue.mayPunctuate(now + 100L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        Assert.Equal(1, node.mockProcessor.punctuatedStreamTime.Count);

        queue.mayPunctuate(now + 199L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        Assert.Equal(1, node.mockProcessor.punctuatedStreamTime.Count);

        queue.mayPunctuate(now + 200L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        Assert.Equal(2, node.mockProcessor.punctuatedStreamTime.Count);

        queue.mayPunctuate(now + 1001L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        Assert.Equal(3, node.mockProcessor.punctuatedStreamTime.Count);

        queue.mayPunctuate(now + 1002L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        Assert.Equal(3, node.mockProcessor.punctuatedStreamTime.Count);

        queue.mayPunctuate(now + 1100L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        Assert.Equal(4, node.mockProcessor.punctuatedStreamTime.Count);
    }

    [Xunit.Fact]
    public void testPunctuationIntervalCustomAlignment() {
        PunctuationSchedule sched = new PunctuationSchedule(node, 50L, 100L, punctuator);
        long now = sched.timestamp - 50L;

        queue.schedule(sched);

        ProcessorNodePunctuator processorNodePunctuator = new ProcessorNodePunctuator() {
            
            public void punctuate(ProcessorNode node, long time, PunctuationType type, Punctuator punctuator) {
                punctuator.punctuate(time);
            }
        };

        queue.mayPunctuate(now, PunctuationType.STREAM_TIME, processorNodePunctuator);
        Assert.Equal(0, node.mockProcessor.punctuatedStreamTime.Count);

        queue.mayPunctuate(now + 49L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        Assert.Equal(0, node.mockProcessor.punctuatedStreamTime.Count);

        queue.mayPunctuate(now + 50L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        Assert.Equal(1, node.mockProcessor.punctuatedStreamTime.Count);

        queue.mayPunctuate(now + 149L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        Assert.Equal(1, node.mockProcessor.punctuatedStreamTime.Count);

        queue.mayPunctuate(now + 150L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        Assert.Equal(2, node.mockProcessor.punctuatedStreamTime.Count);

        queue.mayPunctuate(now + 1051L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        Assert.Equal(3, node.mockProcessor.punctuatedStreamTime.Count);

        queue.mayPunctuate(now + 1052L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        Assert.Equal(3, node.mockProcessor.punctuatedStreamTime.Count);

        queue.mayPunctuate(now + 1150L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        Assert.Equal(4, node.mockProcessor.punctuatedStreamTime.Count);
    }

    [Xunit.Fact]
    public void testPunctuationIntervalCancelFromPunctuator() {
        PunctuationSchedule sched = new PunctuationSchedule(node, 0L, 100L, punctuator);
        long now = sched.timestamp - 100L;

        Cancellable cancellable = queue.schedule(sched);

        ProcessorNodePunctuator processorNodePunctuator = new ProcessorNodePunctuator() {
            
            public void punctuate(ProcessorNode node, long time, PunctuationType type, Punctuator punctuator) {
                punctuator.punctuate(time);
                // simulate scheduler cancelled from within punctuator
                cancellable.cancel();
            }
        };

        queue.mayPunctuate(now, PunctuationType.STREAM_TIME, processorNodePunctuator);
        Assert.Equal(0, node.mockProcessor.punctuatedStreamTime.Count);

        queue.mayPunctuate(now + 100L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        Assert.Equal(1, node.mockProcessor.punctuatedStreamTime.Count);

        queue.mayPunctuate(now + 200L, PunctuationType.STREAM_TIME, processorNodePunctuator);
        Assert.Equal(1, node.mockProcessor.punctuatedStreamTime.Count);
    }

    private static class TestProcessor : AbstractProcessor<string, string> {

        
        public void init(ProcessorContext context) {}

        
        public void process(string key, string value) {}

        
        public void close() {}
    }

}
