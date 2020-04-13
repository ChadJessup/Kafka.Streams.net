//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */











//    public class MaybeTest
//    {
//        [Fact]
//        public void ShouldReturnDefinedValue()
//        {
//            Assert.Equal(Maybe.defined(null).getNullableValue(), nullValue());
//            Assert.Equal(Maybe.defined("ASDF").getNullableValue(), ("ASDF"));
//        }

//        [Fact]
//        public void ShouldAnswerIsDefined()
//        {
//            Assert.Equal(Maybe.defined(null).isDefined(), (true));
//            Assert.Equal(Maybe.defined("ASDF").isDefined(), (true));
//            Assert.Equal(Maybe.undefined().isDefined(), (false));
//        }

//        [Fact]
//        public void ShouldThrowOnGetUndefinedValue()
//        {
//            Maybe<object> undefined = Maybe.undefined();
//            try
//            {
//                undefined.getNullableValue();
//                Assert.True(false, "");
//            }
//            catch (NoSuchElementException e)
//            {
//                // no assertion necessary
//            }
//        }

//        [Fact]
//        public void ShouldUpholdEqualityCorrectness()
//        {
//            Assert.Equal(Maybe.undefined().Equals(Maybe.undefined()), (true));
//            Assert.Equal(Maybe.defined(null).Equals(Maybe.defined(null)), (true));
//            Assert.Equal(Maybe.defined("q").Equals(Maybe.defined("q")), (true));

//            Assert.Equal(Maybe.undefined().Equals(Maybe.defined(null)), (false));
//            Assert.Equal(Maybe.undefined().Equals(Maybe.defined("x")), (false));

//            Assert.Equal(Maybe.defined(null).Equals(Maybe.undefined()), (false));
//            Assert.Equal(Maybe.defined(null).Equals(Maybe.defined("x")), (false));

//            Assert.Equal(Maybe.defined("a").Equals(Maybe.undefined()), (false));
//            Assert.Equal(Maybe.defined("a").Equals(Maybe.defined(null)), (false));
//            Assert.Equal(Maybe.defined("a").Equals(Maybe.defined("b")), (false));
//        }

//        [Fact]
//        public void ShouldUpholdHashCodeCorrectness()
//        {
//            // This specifies the current implementation, which is simpler to write than an exhaustive test.
//            // As long as this implementation doesn't change, then the.Equals/hashcode contract is upheld.

//            Assert.Equal(Maybe.undefined().hashCode(), (-1));
//            Assert.Equal(Maybe.defined(null).hashCode(), (0));
//            Assert.Equal(Maybe.defined("a").hashCode(), ("a".hashCode()));
//        }
//    }
//}
///*






//*

//*





//*/











