/*






 *

 *





 */










using Kafka.Streams.Processors.Internals;
using System.Collections.Generic;
using Xunit;

public class QuickUnionTest
{

    [Xunit.Fact]
    public void TestUnite()
    {
        QuickUnion<long> qu = new QuickUnion<>();

        long[] ids = {
            1L, 2L, 3L, 4L, 5L
        };

        foreach (long id in ids)
        {
            qu.add(id);
        }

        Assert.Equal(5, Roots(qu, ids).Count);

        qu.unite(1L, 2L);
        Assert.Equal(4, Roots(qu, ids).Count);
        Assert.Equal(qu.root(1L), qu.root(2L));

        qu.unite(3L, 4L);
        Assert.Equal(3, Roots(qu, ids).Count);
        Assert.Equal(qu.root(1L), qu.root(2L));
        Assert.Equal(qu.root(3L), qu.root(4L));

        qu.unite(1L, 5L);
        Assert.Equal(2, Roots(qu, ids).Count);
        Assert.Equal(qu.root(1L), qu.root(2L));
        Assert.Equal(qu.root(2L), qu.root(5L));
        Assert.Equal(qu.root(3L), qu.root(4L));

        qu.unite(3L, 5L);
        Assert.Equal(1, Roots(qu, ids).Count);
        Assert.Equal(qu.root(1L), qu.root(2L));
        Assert.Equal(qu.root(2L), qu.root(3L));
        Assert.Equal(qu.root(3L), qu.root(4L));
        Assert.Equal(qu.root(4L), qu.root(5L));
    }

    [Xunit.Fact]
    public void TestUniteMany()
    {
        QuickUnion<long> qu = new QuickUnion<long>();

        long[] ids = {
            1L, 2L, 3L, 4L, 5L
        };

        foreach (long id in ids)
        {
            qu.add(id);
        }

        Assert.Equal(5, Roots(qu, ids).Count);

        qu.unite(1L, 2L, 3L, 4L);
        Assert.Equal(2, Roots(qu, ids).Count);
        Assert.Equal(qu.root(1L), qu.root(2L));
        Assert.Equal(qu.root(2L), qu.root(3L));
        Assert.Equal(qu.root(3L), qu.root(4L));
        Assert.NotEqual(qu.root(1L), qu.root(5L));
    }

    private HashSet<long> Roots(QuickUnion<long> qu, params long[] ids)
    {
        HashSet<long> roots = new HashSet<long>();
        foreach (long id in ids)
        {
            roots.add(qu.root(id));
        }
        return roots;
    }
}
