using Kafka.Streams.Processors.Internals;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class QuickUnionTest
    {
        [Fact]
        public void TestUnite()
        {
            QuickUnion<long> qu = new QuickUnion<long>();

            long[] ids =
            {
                1L, 2L, 3L, 4L, 5L
            };

            foreach (long id in ids)
            {
                qu.Add(id);
            }

            Assert.Equal(5, this.Roots(qu, ids).Count);

            qu.Unite(1L, 2L);
            Assert.Equal(4, this.Roots(qu, ids).Count);
            Assert.Equal(qu.Root(1L), qu.Root(2L));

            qu.Unite(3L, 4L);
            Assert.Equal(3, this.Roots(qu, ids).Count);
            Assert.Equal(qu.Root(1L), qu.Root(2L));
            Assert.Equal(qu.Root(3L), qu.Root(4L));

            qu.Unite(1L, 5L);
            Assert.Equal(2, this.Roots(qu, ids).Count);
            Assert.Equal(qu.Root(1L), qu.Root(2L));
            Assert.Equal(qu.Root(2L), qu.Root(5L));
            Assert.Equal(qu.Root(3L), qu.Root(4L));

            qu.Unite(3L, 5L);
            Assert.Single(this.Roots(qu, ids));
            Assert.Equal(qu.Root(1L), qu.Root(2L));
            Assert.Equal(qu.Root(2L), qu.Root(3L));
            Assert.Equal(qu.Root(3L), qu.Root(4L));
            Assert.Equal(qu.Root(4L), qu.Root(5L));
        }

        [Fact]
        public void TestUniteMany()
        {
            QuickUnion<long> qu = new QuickUnion<long>();

            long[] ids = {
            1L, 2L, 3L, 4L, 5L
        };

            foreach (long id in ids)
            {
                qu.Add(id);
            }

            Assert.Equal(5, this.Roots(qu, ids).Count);

            qu.Unite(1L, 2L, 3L, 4L);
            Assert.Equal(2, this.Roots(qu, ids).Count);
            Assert.Equal(qu.Root(1L), qu.Root(2L));
            Assert.Equal(qu.Root(2L), qu.Root(3L));
            Assert.Equal(qu.Root(3L), qu.Root(4L));
            Assert.NotEqual(qu.Root(1L), qu.Root(5L));
        }

        private HashSet<long> Roots(QuickUnion<long> qu, params long[] ids)
        {
            HashSet<long> roots = new HashSet<long>();
            foreach (long id in ids)
            {
                roots.Add(qu.Root(id));
            }

            return roots;
        }
    }
}
