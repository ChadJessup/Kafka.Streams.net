using NodaTime;
using System;
using Xunit;

namespace Kafka.Streams.Tests.Internals
{
    public class ApiUtilsTest
    {
        // This is the maximum limit that Duration accepts but fails when it converts to milliseconds.
        private const long MAX_ACCEPTABLE_DAYS_FOR_DURATION = 106751991167300L;
        // This is the maximum limit that Duration accepts and converts to milliseconds with out fail.
        private const long MAX_ACCEPTABLE_DAYS_FOR_DURATION_TO_MILLIS = 106751991167L;

        [Xunit.Fact]
        public void ShouldThrowNullPointerExceptionForNullDuration()
        {
            string nullDurationPrefix = PrepareMillisCheckFailMsgPrefix(null, "nullDuration");

            try
            {
                ValidateMillisecondDuration(null, nullDurationPrefix);
                Assert.True(false, "Expected exception when null passed to duration.");
            }
            catch (ArgumentException e)
            {
                Assert.Contains(nullDurationPrefix, e.ToString());
            }
        }

        [Xunit.Fact]
        public void ShouldThrowArithmeticExceptionForMaxDuration()
        {
            Duration maxDurationInDays = Duration.FromDays(MAX_ACCEPTABLE_DAYS_FOR_DURATION);
            string maxDurationPrefix = PrepareMillisCheckFailMsgPrefix(maxDurationInDays, "maxDuration");

            try
            {
                ValidateMillisecondDuration(maxDurationInDays, maxDurationPrefix);
                Assert.True(false, "Expected exception when maximum days passed for duration, because of long overflow");
            }
            catch (ArgumentException e)
            {
                Assert.Contains(maxDurationPrefix, e.ToString());
            }
        }

        [Xunit.Fact]
        public void ShouldThrowNullPointerExceptionForNullInstant()
        {
            string nullInstantPrefix = PrepareMillisCheckFailMsgPrefix(null, "nullInstant");

            try
            {
                ValidateMillisecondInstant(null, nullInstantPrefix);
                Assert.True(false, "Expected exception when null value passed for instant.");
            }
            catch (ArgumentException e)
            {
                Assert.Contains(nullInstantPrefix, e.ToString());
            }
        }

        [Xunit.Fact]
        public void ShouldThrowArithmeticExceptionForMaxInstant()
        {
            string maxInstantPrefix = PrepareMillisCheckFailMsgPrefix(Instant.MaxValue, "maxInstant");

            try
            {
                ValidateMillisecondInstant(Instant.MaxValue, maxInstantPrefix);
                Assert.True(false, "Expected exception when maximum value passed for instant, because of long overflow.");
            }
            catch (ArgumentException e)
            {
                Assert.Contains(maxInstantPrefix, e.ToString());
            }
        }

        [Xunit.Fact]
        public void ShouldReturnMillisecondsOnValidDuration()
        {
            Duration sampleDuration = Duration.FromDays(MAX_ACCEPTABLE_DAYS_FOR_DURATION_TO_MILLIS);

            Assert.Equal(sampleDuration.TotalMilliseconds, ValidateMillisecondDuration(sampleDuration, "sampleDuration"));
        }

        [Xunit.Fact]
        public void ShouldReturnMillisecondsOnValidInstant()
        {
            Instant sampleInstant = SystemClock.Instance.GetCurrentInstant();

            Assert.Equal(sampleInstant.ToUnixTimeMilliseconds(), ValidateMillisecondInstant(sampleInstant, "sampleInstant"));
        }

        [Xunit.Fact]
        public void ShouldContainsNameAndValueInFailMsgPrefix()
        {
            string failMsgPrefix = PrepareMillisCheckFailMsgPrefix("someValue", "variableName");

            Assert.Contains("variableName", failMsgPrefix);
            Assert.Contains("someValue", failMsgPrefix);
        }


        private string PrepareMillisCheckFailMsgPrefix(object p, string v)
        {
            throw new NotImplementedException();
        }

        private long ValidateMillisecondInstant(object p, string nullInstantPrefix)
        {
            throw new NotImplementedException();
        }

        private double ValidateMillisecondDuration(Duration? maxDurationInDays, string maxDurationPrefix)
        {
            throw new NotImplementedException();
        }
    }
}
