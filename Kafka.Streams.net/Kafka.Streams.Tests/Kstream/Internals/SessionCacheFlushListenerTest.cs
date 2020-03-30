namespace Kafka.Streams.Tests.Kstream.Internals
{
}
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.Processors;
//using Kafka.Streams.Processors.Interfaces;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class SessionCacheFlushListenerTest
//    {
//        [Fact]
//        public void shouldForwardKeyNewValueOldValueAndTimestamp()
//        {
//            IInternalProcessorContext context = mock(IInternalProcessorContext);
//            expect(context.currentNode()).andReturn(null).anyTimes();
//            context.setCurrentNode(null);
//            context.setCurrentNode(null);
//            context.forward(
//                new Windowed<>("key", new SessionWindow(21L, 73L)),
//                new Change<>("newValue", "oldValue"),
//                To.All().WithTimestamp(73L));
//            expect.AstCall();
//            replay(context);

//            new SessionCacheFlushListener<>(context).apply(
//                new Windowed<>("key", new SessionWindow(21L, 73L)),
//                "newValue",
//                "oldValue",
//                42L);

//            verify(context);
//        }
//    }
//}
