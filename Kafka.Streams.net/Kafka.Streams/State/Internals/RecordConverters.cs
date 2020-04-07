
using Kafka.Streams.State.Interfaces;

namespace Kafka.Streams.State.Internals
{
    public static class RecordConverters
    {
        private static readonly IRecordConverter IDENTITY_INSTANCE = new IdentityRecordConverter();
        private static readonly IRecordConverter RAW_TO_TIMESTAMED_INSTANCE = new RawToTimeStampInstance();

        public static IRecordConverter RawValueToTimestampedValue()
        {
            return RAW_TO_TIMESTAMED_INSTANCE;
        }

        public static IRecordConverter Identity()
        {
            return IDENTITY_INSTANCE;
        }
    }
}
