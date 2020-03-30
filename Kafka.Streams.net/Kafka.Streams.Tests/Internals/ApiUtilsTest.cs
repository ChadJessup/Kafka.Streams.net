namespace Kafka.Streams.Tests.Internals
{
    /*






    *

    *





    */
















    public class ApiUtilsTest
    {

        // This is the maximum limit that Duration accepts but fails when it converts to milliseconds.
        private static readonly long MAX_ACCEPTABLE_DAYS_FOR_DURATION = 106751991167300L;
        // This is the maximum limit that Duration accepts and converts to milliseconds with out fail.
        private static readonly long MAX_ACCEPTABLE_DAYS_FOR_DURATION_TO_MILLIS = 106751991167L;

        [Xunit.Fact]
        public void ShouldThrowNullPointerExceptionForNullDuration()
        {
            string nullDurationPrefix = prepareMillisCheckFailMsgPrefix(null, "nullDuration");

            try
            {
                validateMillisecondDuration(null, nullDurationPrefix);
                Assert.True(false, "Expected exception when null passed to duration.");
            }
            catch (IllegalArgumentException e)
            {
                Assert.Equal(e.getMessage(), containsString(nullDurationPrefix));
            }
        }

        [Xunit.Fact]
        public void ShouldThrowArithmeticExceptionForMaxDuration()
        {
            Duration maxDurationInDays = Duration.ofDays(MAX_ACCEPTABLE_DAYS_FOR_DURATION);
            string maxDurationPrefix = prepareMillisCheckFailMsgPrefix(maxDurationInDays, "maxDuration");

            try
            {
                validateMillisecondDuration(maxDurationInDays, maxDurationPrefix);
                Assert.True(false, "Expected exception when maximum days passed for duration, because of long overflow");
            }
            catch (IllegalArgumentException e)
            {
                Assert.Equal(e.getMessage(), containsString(maxDurationPrefix));
            }
        }

        [Xunit.Fact]
        public void ShouldThrowNullPointerExceptionForNullInstant()
        {
            string nullInstantPrefix = prepareMillisCheckFailMsgPrefix(null, "nullInstant");

            try
            {
                validateMillisecondInstant(null, nullInstantPrefix);
                Assert.True(false, "Expected exception when null value passed for instant.");
            }
            catch (IllegalArgumentException e)
            {
                Assert.Equal(e.getMessage(), containsString(nullInstantPrefix));
            }
        }

        [Xunit.Fact]
        public void ShouldThrowArithmeticExceptionForMaxInstant()
        {
            string maxInstantPrefix = prepareMillisCheckFailMsgPrefix(Instant.MAX, "maxInstant");

            try
            {
                validateMillisecondInstant(Instant.MAX, maxInstantPrefix);
                Assert.True(false, "Expected exception when maximum value passed for instant, because of long overflow.");
            }
            catch (IllegalArgumentException e)
            {
                Assert.Equal(e.getMessage(), containsString(maxInstantPrefix));
            }
        }

        [Xunit.Fact]
        public void ShouldReturnMillisecondsOnValidDuration()
        {
            Duration sampleDuration = Duration.ofDays(MAX_ACCEPTABLE_DAYS_FOR_DURATION_TO_MILLIS);

            Assert.Equal(sampleDuration.toMillis(), validateMillisecondDuration(sampleDuration, "sampleDuration"));
        }

        [Xunit.Fact]
        public void ShouldReturnMillisecondsOnValidInstant()
        {
            Instant sampleInstant = Instant.now();

            Assert.Equal(sampleInstant.toEpochMilli(), validateMillisecondInstant(sampleInstant, "sampleInstant"));
        }

        [Xunit.Fact]
        public void ShouldContainsNameAndValueInFailMsgPrefix()
        {
            string failMsgPrefix = prepareMillisCheckFailMsgPrefix("someValue", "variableName");

            Assert.Equal(failMsgPrefix, containsString("variableName"));
            Assert.Equal(failMsgPrefix, containsString("someValue"));
        }
    }
}
/*






*

*





*/
















