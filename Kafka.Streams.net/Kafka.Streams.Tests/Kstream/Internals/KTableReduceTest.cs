//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.State.TimeStamped;
//using System.Collections.Generic;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KTableReduceTest
//    {

//        [Fact]
//        public void shouldAddAndSubtract()
//        {
//            var context = new InternalMockProcessorContext();

//            Processor<string, Change<HashSet<string>>> reduceProcessor =
//                new KTableReduce<string, HashSet<string>>(
//                    "myStore",
//                    unionNotNullArgs,
//                    differenceNotNullArgs
//                ).get();

//            ITimestampedKeyValueStore<string, HashSet<string>> myStore =
//                new GenericInMemoryTimestampedKeyValueStore<>("myStore");

//            context.register(myStore, null);
//            reduceProcessor.init(context);
//            context.setCurrentNode(new ProcessorNode<>("reduce", reduceProcessor, singleton("myStore")));

//            context.setTime(10L);
//            reduceProcessor.process("A", new Change<>(singleton("a"), null));
//            Assert.Equal(ValueAndTimestamp.make(singleton("a"), 10L), myStore.get("A"));
//            context.setTime(15L);
//            reduceProcessor.process("A", new Change<>(singleton("b"), singleton("a")));
//            Assert.Equal(ValueAndTimestamp.make(singleton("b"), 15L), myStore.get("A"));
//            context.setTime(12L);
//            reduceProcessor.process("A", new Change<>(null, singleton("b")));
//            Assert.Equal(ValueAndTimestamp.make(emptySet(), 15L), myStore.get("A"));
//        }

//        private HashSet<string> differenceNotNullArgs(HashSet<string> left, HashSet<string> right)
//        {
//            Assert.NotNull(left);
//            Assert.NotNull(right);

//            HashSet<string> strings = new HashSet<string>(left);
//            strings.RemoveWhere(s => right.Contains(s));
//            return strings;
//        }

//        private HashSet<string> unionNotNullArgs(HashSet<string> left, HashSet<string> right)
//        {
//            Assert.NotNull(left);
//            Assert.NotNull(right);

//            HashSet<string> strings = new HashSet<string>();
//            strings.AddRange(left);
//            strings.AddRange(right);
//            return strings;
//        }
//    }
//}
