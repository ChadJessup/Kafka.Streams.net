namespace Kafka.Streams.Tests.State.Internals
{
    /*






    *

    *





    */











    public class MaybeTest
    {
        [Xunit.Fact]
        public void ShouldReturnDefinedValue()
        {
            Assert.Equal(Maybe.defined(null).getNullableValue(), nullValue());
            Assert.Equal(Maybe.defined("ASDF").getNullableValue(), ("ASDF"));
        }

        [Xunit.Fact]
        public void ShouldAnswerIsDefined()
        {
            Assert.Equal(Maybe.defined(null).isDefined(), (true));
            Assert.Equal(Maybe.defined("ASDF").isDefined(), (true));
            Assert.Equal(Maybe.undefined().isDefined(), (false));
        }

        [Xunit.Fact]
        public void ShouldThrowOnGetUndefinedValue()
        {
            Maybe<object> undefined = Maybe.undefined();
            try
            {
                undefined.getNullableValue();
                Assert.True(false, );
            }
            catch (NoSuchElementException e)
            {
                // no assertion necessary
            }
        }

        [Xunit.Fact]
        public void ShouldUpholdEqualityCorrectness()
        {
            Assert.Equal(Maybe.undefined().equals(Maybe.undefined()), (true));
            Assert.Equal(Maybe.defined(null).equals(Maybe.defined(null)), (true));
            Assert.Equal(Maybe.defined("q").equals(Maybe.defined("q")), (true));

            Assert.Equal(Maybe.undefined().equals(Maybe.defined(null)), (false));
            Assert.Equal(Maybe.undefined().equals(Maybe.defined("x")), (false));

            Assert.Equal(Maybe.defined(null).equals(Maybe.undefined()), (false));
            Assert.Equal(Maybe.defined(null).equals(Maybe.defined("x")), (false));

            Assert.Equal(Maybe.defined("a").equals(Maybe.undefined()), (false));
            Assert.Equal(Maybe.defined("a").equals(Maybe.defined(null)), (false));
            Assert.Equal(Maybe.defined("a").equals(Maybe.defined("b")), (false));
        }

        [Xunit.Fact]
        public void ShouldUpholdHashCodeCorrectness()
        {
            // This specifies the current implementation, which is simpler to write than an exhaustive test.
            // As long as this implementation doesn't change, then the equals/hashcode contract is upheld.

            Assert.Equal(Maybe.undefined().hashCode(), (-1));
            Assert.Equal(Maybe.defined(null).hashCode(), (0));
            Assert.Equal(Maybe.defined("a").hashCode(), ("a".hashCode()));
        }
    }
}
/*






*

*





*/











