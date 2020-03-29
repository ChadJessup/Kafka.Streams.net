/*






 *

 *





 */













public class ForwardingDisabledProcessorContextTest {
    @Mock(MockType.NICE)
    private ProcessorContext delegate;
    private ForwardingDisabledProcessorContext context;

    
    public void SetUp() {
        context = new ForwardingDisabledProcessorContext(delegate);
    }

    [Xunit.Fact]// (expected = StreamsException)
    public void ShouldThrowOnForward() {
        context.forward("key", "value");
    }

    [Xunit.Fact]// (expected = StreamsException)
    public void ShouldThrowOnForwardWithTo() {
        context.forward("key", "value", To.all());
    }

     // need to test deprecated code until removed
    [Xunit.Fact]// (expected = StreamsException)
    public void ShouldThrowOnForwardWithChildIndex() {
        context.forward("key", "value", 1);
    }

     // need to test deprecated code until removed
    [Xunit.Fact]// (expected = StreamsException)
    public void ShouldThrowOnForwardWithChildName() {
        context.forward("key", "value", "child1");
    }
}