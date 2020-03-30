namespace Kafka.Streams.Tests.Kstream.Internals
{
}
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.Processors;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.State;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class TimestampedCacheFlushListenerTest
//    {

//        [Fact]
//        public void shouldForwardValueTimestampIfNewValueExists()
//        {
//            IInternalProcessorContext context = mock(typeof(IInternalProcessorContext));
//            expect(context.currentNode()).andReturn(null).anyTimes();
//            context.setCurrentNode(null);
//            context.setCurrentNode(null);
//            context.forward(
//                "key",
//                new Change<>("newValue", "oldValue"),
//                To.All().WithTimestamp(42L));
//            expect.AstCall();
//            replay(context);

//            new TimestampedCacheFlushListener<>(context).apply(
//                "key",
//                ValueAndTimestamp.make("newValue", 42L),
//                ValueAndTimestamp.make("oldValue", 21L),
//                73L);

//            verify(context);
//        }

//        [Fact]
//        public void shouldForwardParameterTimestampIfNewValueIsNull()
//        {
//            IInternalProcessorContext context = mock(typeof(IInternalProcessorContext));
//            expect(context.currentNode()).andReturn(null).anyTimes();
//            context.setCurrentNode(null);
//            context.setCurrentNode(null);
//            context.forward(
//                "key",
//                new Change<>(null, "oldValue"),
//                To.All().WithTimestamp(73L));
//            expect.AstCall();
//            replay(context);

//            new TimestampedCacheFlushListener<>(context).apply(
//                "key",
//                null,
//                ValueAndTimestamp.make("oldValue", 21L),
//                73L);

//            verify(context);
//        }
//    }
//}
