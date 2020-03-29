/*






 *

 *





 */













public class ForwardingDisabledProcessorContextTest {
    @Mock(MockType.NICE)
    private ProcessorContext delegate;
    private ForwardingDisabledProcessorContext context;

    
    public void setUp() {
        context = new ForwardingDisabledProcessorContext(delegate);
    }

    [Xunit.Fact]// (expected = StreamsException)
    public void shouldThrowOnForward() {
        context.forward("key", "value");
    }

    [Xunit.Fact]// (expected = StreamsException)
    public void shouldThrowOnForwardWithTo() {
        context.forward("key", "value", To.all());
    }

     // need to test deprecated code until removed
    [Xunit.Fact]// (expected = StreamsException)
    public void shouldThrowOnForwardWithChildIndex() {
        context.forward("key", "value", 1);
    }

     // need to test deprecated code until removed
    [Xunit.Fact]// (expected = StreamsException)
    public void shouldThrowOnForwardWithChildName() {
        context.forward("key", "value", "child1");
    }
}