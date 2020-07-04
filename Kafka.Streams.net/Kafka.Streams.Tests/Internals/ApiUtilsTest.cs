using Kafka.Common.Extensions;
using Kafka.Streams.Internals;
using Kafka.Streams.Kafka.Streams;
using Moq;

using System;
using Xunit;

namespace Kafka.Streams.Tests.Internals
{
    public class ApiUtilsTest
    {
        // This is the maximum limit that TimeSpan accepts but fails when it converts to milliseconds.
        private const double MAX_ACCEPTABLE_DAYS_FOR_DURATION = 10675199.116730064;
        // This is the maximum limit that TimeSpan accepts and converts to milliseconds with out fail.
        private const double MAX_ACCEPTABLE_DAYS_FOR_DURATION_TO_MILLIS = 10675199.116730064;

        [Fact]
        public void ShouldThrowNullPointerExceptionForNullDuration()
        {
            string nullDurationPrefix = ApiUtils.PrepareMillisCheckFailMsgPrefix(null, "nullDuration");

            try
            {
                ApiUtils.ValidateMillisecondDuration(null, nullDurationPrefix);
                Assert.True(false, "Expected exception when null passed to duration.");
            }
            catch (ArgumentException e)
            {
                Assert.Contains(nullDurationPrefix, e.ToString());
            }
        }

        [Fact]
        public void ShouldThrowNullPointerExceptionForNullInstant()
        {
            string nullInstantPrefix = ApiUtils.PrepareMillisCheckFailMsgPrefix(null, "nullInstant");

            try
            {
                ApiUtils.ValidateMillisecondInstant(null, nullInstantPrefix);
                Assert.True(false, "Expected exception when null value passed for instant.");
            }
            catch (ArgumentException e)
            {
                Assert.Contains(nullInstantPrefix, e.ToString());
            }
        }

        [Fact]
        public void ShouldReturnMillisecondsOnValidDuration()
        {
            var sampleDuration = TimeSpan.FromDays(MAX_ACCEPTABLE_DAYS_FOR_DURATION_TO_MILLIS);

            Assert.Equal(sampleDuration, ApiUtils.ValidateMillisecondDuration(sampleDuration, "sampleDuration"));
        }

        [Fact]
        public void ShouldReturnMillisecondsOnValidInstant()
        {
            DateTime sampleInstant = SystemClock.AsUtcNow;

            Assert.Equal(sampleInstant.ToEpochMilliseconds(), ApiUtils.ValidateMillisecondInstant(sampleInstant, "sampleInstant"));
        }

        [Fact]
        public void ShouldContainsNameAndValueInFailMsgPrefix()
        {
            string failMsgPrefix = ApiUtils.PrepareMillisCheckFailMsgPrefix("someValue", "variableName");

            Assert.Contains("variableName", failMsgPrefix);
            Assert.Contains("someValue", failMsgPrefix);
        }
    }
}
