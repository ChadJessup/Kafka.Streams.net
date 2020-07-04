//using Kafka.Streams.State;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.Processors;
//using Kafka.Streams.Processors.Interfaces;
//using Moq;
//using Xunit;

//namespace Kafka.Streams.Tests.Kstream.Internals
//{
//    public class TimestampedCacheFlushListenerTest
//    {

//        [Fact]
//        public void shouldForwardValueTimestampIfNewValueExists()
//        {
//            IInternalProcessorContext context = Mock.Of<IInternalProcessorContext>();
//            expect(context.currentNode()).andReturn(null).anyTimes();
//            context.setCurrentNode(null);
//            context.setCurrentNode(null);
//            context.Forward(
//                "key",
//                new Change<string>("newValue", "oldValue"),
//                To.All().WithTimestamp(42L));
//            expect.AstCall();
//            replay(context);

//            new TimestampedCacheFlushListener<>(context).apply(
//                "key",
//                ValueAndTimestamp.Make("newValue", 42L),
//                ValueAndTimestamp.Make("oldValue", 21L),
//                73L);

//            verify(context);
//        }

//        [Fact]
//        public void shouldForwardParameterTimestampIfNewValueIsNull()
//        {
//            IInternalProcessorContext context = Mock.Of<IInternalProcessorContext>();
//            expect(context.currentNode()).andReturn(null).anyTimes();
//            context.setCurrentNode(null);
//            context.setCurrentNode(null);
//            context.Forward(
//                "key",
//                new Change<string>(null, "oldValue"),
//                To.All().WithTimestamp(73L));
//            expect.AstCall();
//            replay(context);

//            new TimestampedCacheFlushListener<>(context).apply(
//                "key",
//                null,
//                ValueAndTimestamp.Make("oldValue", 21L),
//                73L);

//            verify(context);
//        }
//    }
//}
