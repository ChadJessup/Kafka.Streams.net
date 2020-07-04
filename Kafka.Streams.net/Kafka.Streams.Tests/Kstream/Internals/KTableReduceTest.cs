using Kafka.Streams.KStream.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.TimeStamped;
using Kafka.Streams.Temporary;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{
    public class KTableReduceTest
    {

        // [Fact]
        // public void shouldAddAndSubtract()
        // {
        //     var context = new InternalMockProcessorContext();
        // 
        //     var reduceProcessor =
        //         new KTableReduce<string, HashSet<string>>(
        //             "myStore",
        //             unionNotNullArgs,
        //             differenceNotNullArgs)
        //         .Get();
        // 
        //     ITimestampedKeyValueStore<string, HashSet<string>> myStore =
        //         new GenericInMemoryTimestampedKeyValueStore<>("myStore");
        // 
        //     context.register(myStore, null);
        //     reduceProcessor.Init(context);
        //     context.SetCurrentNode(new ProcessorNode<>("reduce", reduceProcessor, new HashSet<string> { "myStore" }));
        // 
        //     context.setTime(10L);
        //     reduceProcessor.Process("A", new Change<HashSet<string>>(Collections.singleton("a"), null));
        //     Assert.Equal(ValueAndTimestamp.Make(Collections.singleton("a"), 10L), myStore.Get("A"));
        //     context.setTime(15L);
        //     reduceProcessor.Process("A", new Change<HashSet<string>>(new HashSet<string> { "b" }, new HashSet<string> { "a" }));
        //     Assert.Equal(ValueAndTimestamp.Make(Collections.singleton("b"), 15L), myStore.Get("A"));
        //     context.setTime(12L);
        //     reduceProcessor.Process("A", new Change<HashSet<string>>(null, Collections.singleton("b")));
        //     Assert.Equal(ValueAndTimestamp.Make(Collections.emptySet<string>(), 15L), myStore.Get("A"));
        // }

        private HashSet<string> differenceNotNullArgs(HashSet<string> left, HashSet<string> right)
        {
            Assert.NotNull(left);
            Assert.NotNull(right);

            HashSet<string> strings = new HashSet<string>(left);
            strings.RemoveWhere(s => right.Contains(s));
            return strings;
        }

        private HashSet<string> unionNotNullArgs(HashSet<string> left, HashSet<string> right)
        {
            Assert.NotNull(left);
            Assert.NotNull(right);

            HashSet<string> strings = new HashSet<string>();
            strings.AddRange(left);
            strings.AddRange(right);
            return strings;
        }
    }
}
