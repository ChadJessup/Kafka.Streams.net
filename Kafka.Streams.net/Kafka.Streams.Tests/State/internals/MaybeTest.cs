/*






 *

 *





 */











public class MaybeTest {
    [Xunit.Fact]
    public void shouldReturnDefinedValue() {
        Assert.Equal(Maybe.defined(null).getNullableValue(), nullValue());
        Assert.Equal(Maybe.defined("ASDF").getNullableValue(), is("ASDF"));
    }

    [Xunit.Fact]
    public void shouldAnswerIsDefined() {
        Assert.Equal(Maybe.defined(null).isDefined(), is(true));
        Assert.Equal(Maybe.defined("ASDF").isDefined(), is(true));
        Assert.Equal(Maybe.undefined().isDefined(), is(false));
    }

    [Xunit.Fact]
    public void shouldThrowOnGetUndefinedValue() {
        Maybe<object> undefined = Maybe.undefined();
        try {
            undefined.getNullableValue();
            Assert.True(false, );
        } catch (NoSuchElementException e) {
            // no assertion necessary
        }
    }

    [Xunit.Fact]
    public void shouldUpholdEqualityCorrectness() {
        Assert.Equal(Maybe.undefined().equals(Maybe.undefined()), is(true));
        Assert.Equal(Maybe.defined(null).equals(Maybe.defined(null)), is(true));
        Assert.Equal(Maybe.defined("q").equals(Maybe.defined("q")), is(true));

        Assert.Equal(Maybe.undefined().equals(Maybe.defined(null)), is(false));
        Assert.Equal(Maybe.undefined().equals(Maybe.defined("x")), is(false));

        Assert.Equal(Maybe.defined(null).equals(Maybe.undefined()), is(false));
        Assert.Equal(Maybe.defined(null).equals(Maybe.defined("x")), is(false));

        Assert.Equal(Maybe.defined("a").equals(Maybe.undefined()), is(false));
        Assert.Equal(Maybe.defined("a").equals(Maybe.defined(null)), is(false));
        Assert.Equal(Maybe.defined("a").equals(Maybe.defined("b")), is(false));
    }

    [Xunit.Fact]
    public void shouldUpholdHashCodeCorrectness() {
        // This specifies the current implementation, which is simpler to write than an exhaustive test.
        // As long as this implementation doesn't change, then the equals/hashcode contract is upheld.

        Assert.Equal(Maybe.undefined().hashCode(), is(-1));
        Assert.Equal(Maybe.defined(null).hashCode(), is(0));
        Assert.Equal(Maybe.defined("a").hashCode(), is("a".hashCode()));
    }
}
