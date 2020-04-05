using Kafka.Common.Extensions;
using Kafka.Streams.KStream.Internals;
using System;

namespace Kafka.Streams.Internals
{
    public static class ApiUtils
    {
        private const string MILLISECOND_VALIDATION_FAIL_MSG_FRMT = "Invalid value for parameter \"{0}\" (value was: {1}). ";
        private const string VALIDATE_MILLISECOND_NULL_SUFFIX = "It shouldn't be null.";
        private const string VALIDATE_MILLISECOND_OVERFLOW_SUFFIX = "It can't be converted to milliseconds.";

        /**
            * Validates that milliseconds from {@code duration} can be retrieved.
            * @param duration Duration to check.
            * @param messagePrefix Prefix text for an error message.
            * @return Milliseconds from {@code duration}.
            */
        public static TimeSpan ValidateMillisecondDuration(TimeSpan? duration, string messagePrefix)
        {
            try
            {
                if (duration == null)
                {
                    throw new ArgumentException(messagePrefix + VALIDATE_MILLISECOND_NULL_SUFFIX);
                }

                long maxTimeSpan = (long)TimeSpan.MaxValue.TotalMilliseconds;
                var tsl = TimeSpan.FromMilliseconds(maxTimeSpan);
                return duration.Value;
            }
            catch (ArithmeticException e)
            {
                throw new ArgumentException(messagePrefix + VALIDATE_MILLISECOND_OVERFLOW_SUFFIX, e);
            }
        }

        /**
            * Validates that milliseconds from {@code instant} can be retrieved.
            * @param instant Instant to check.
            * @param messagePrefix Prefix text for an error message.
            * @return Milliseconds from {@code instant}.
            */
        public static long ValidateMillisecondInstant(DateTime? instant, string messagePrefix)
        {
            try
            {
                if (instant == null)
                {
                    throw new ArgumentException(messagePrefix + VALIDATE_MILLISECOND_NULL_SUFFIX);
                }

                return instant.Value.ToEpochMilliseconds();
            }
            catch (ArithmeticException e)
            {
                throw new ArgumentException(messagePrefix + VALIDATE_MILLISECOND_OVERFLOW_SUFFIX, e);
            }
        }

        /**
            * Generates the prefix message for validateMillisecondXXXXXX() utility
            * @param value Object to be converted to milliseconds
            * @param name Object name
            * @return Error message prefix to use in exception
            */
        public static string PrepareMillisCheckFailMsgPrefix(object value, string name)
        {
            return string.Format(MILLISECOND_VALIDATION_FAIL_MSG_FRMT, name, value);
        }

        public static byte[] ConvertToTimestampedFormat(byte[] plainValue)
        {
            if (plainValue == null)
            {
                return null;
            }

            return new ByteBuffer()
                .Allocate(8 + plainValue.Length)
                .PutLong(-1)
                .Add(plainValue)
                .Array();
        }
    }
}
