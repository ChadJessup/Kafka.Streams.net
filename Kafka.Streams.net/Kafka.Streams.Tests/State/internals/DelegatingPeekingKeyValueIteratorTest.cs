/*






 *

 *





 */













public class DelegatingPeekingKeyValueIteratorTest {

    private string name = "name";
    private KeyValueStore<string, string> store;

    
    public void SetUp() {
        store = new GenericInMemoryKeyValueStore<>(name);
    }

    [Xunit.Fact]
    public void ShouldPeekNextKey() {
        store.put("A", "A");
        DelegatingPeekingKeyValueIterator<string, string> peekingIterator = new DelegatingPeekingKeyValueIterator<>(name, store.all());
        Assert.Equal("A", peekingIterator.peekNextKey());
        Assert.Equal("A", peekingIterator.peekNextKey());
        Assert.True(peekingIterator.hasNext());
        peekingIterator.close();
    }

    [Xunit.Fact]
    public void ShouldPeekNext() {
        store.put("A", "A");
        DelegatingPeekingKeyValueIterator<string, string> peekingIterator = new DelegatingPeekingKeyValueIterator<>(name, store.all());
        Assert.Equal(KeyValuePair.Create("A", "A"), peekingIterator.peekNext());
        Assert.Equal(KeyValuePair.Create("A", "A"), peekingIterator.peekNext());
        Assert.True(peekingIterator.hasNext());
        peekingIterator.close();
    }

    [Xunit.Fact]
    public void ShouldPeekAndIterate() {
        string[] kvs = {"a", "b", "c", "d", "e", "f"};
        foreach (string kv in kvs) {
            store.put(kv, kv);
        }

        DelegatingPeekingKeyValueIterator<string, string> peekingIterator = new DelegatingPeekingKeyValueIterator<>(name, store.all());
        int index = 0;
        while (peekingIterator.hasNext()) {
            string peekNext = peekingIterator.peekNextKey();
            string key = peekingIterator.next().key;
            Assert.Equal(kvs[index], peekNext);
            Assert.Equal(kvs[index], key);
            index++;
        }
        Assert.Equal(kvs.Length, index);
        peekingIterator.close();
    }

    [Xunit.Fact]// (expected = NoSuchElementException)
    public void ShouldThrowNoSuchElementWhenNoMoreItemsLeftAndNextCalled() {
        DelegatingPeekingKeyValueIterator<string, string> peekingIterator = new DelegatingPeekingKeyValueIterator<>(name, store.all());
        peekingIterator.next();
        peekingIterator.close();
    }

    [Xunit.Fact]// (expected = NoSuchElementException)
    public void ShouldThrowNoSuchElementWhenNoMoreItemsLeftAndPeekNextCalled() {
        DelegatingPeekingKeyValueIterator<string, string> peekingIterator = new DelegatingPeekingKeyValueIterator<>(name, store.all());
        peekingIterator.peekNextKey();
        peekingIterator.close();
    }


}