/*






 *

 *





 */





















public class KeyValueIteratorFacadeTest {
    @Mock
    private KeyValueIterator<string, ValueAndTimestamp<string>> mockedKeyValueIterator;

    private KeyValueIteratorFacade<string, string> keyValueIteratorFacade;

    
    public void setup() {
        keyValueIteratorFacade = new KeyValueIteratorFacade<>(mockedKeyValueIterator);
    }

    [Xunit.Fact]
    public void shouldForwardHasNext() {
        expect(mockedKeyValueIterator.hasNext()).andReturn(true).andReturn(false);
        replay(mockedKeyValueIterator);

        Assert.True(keyValueIteratorFacade.hasNext());
        Assert.False(keyValueIteratorFacade.hasNext());
        verify(mockedKeyValueIterator);
    }

    [Xunit.Fact]
    public void shouldForwardPeekNextKey() {
        expect(mockedKeyValueIterator.peekNextKey()).andReturn("key");
        replay(mockedKeyValueIterator);

        Assert.Equal(keyValueIteratorFacade.peekNextKey(), is("key"));
        verify(mockedKeyValueIterator);
    }

    [Xunit.Fact]
    public void shouldReturnPlainKeyValuePairOnGet() {
        expect(mockedKeyValueIterator.next()).andReturn(
            new KeyValuePair<>("key", ValueAndTimestamp.make("value", 42L)));
        replay(mockedKeyValueIterator);

        Assert.Equal(keyValueIteratorFacade.next(), is(KeyValuePair.Create("key", "value")));
        verify(mockedKeyValueIterator);
    }

    [Xunit.Fact]
    public void shouldCloseInnerIterator() {
        mockedKeyValueIterator.close();
        expectLastCall();
        replay(mockedKeyValueIterator);

        keyValueIteratorFacade.close();
        verify(mockedKeyValueIterator);
    }
}
