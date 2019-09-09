using System.Runtime.Serialization;
using Kafka.Common.Utils.Interfaces;

namespace Kafka.Common.Utils
{
    public class LexicographicByteArrayComparator : IByteArrayComparator
    {
        public int Compare(byte[] buffer1, byte[] buffer2)
        {
            return Compare(buffer1, 0, buffer1.Length, buffer2, 0, buffer2.Length);
        }

        public int Compare(byte[] buffer1, int offset1, int Length1,
                           byte[] buffer2, int offset2, int Length2)
        {
            // short circuit equal case
            if (buffer1 == buffer2
                && offset1 == offset2
                && Length1 == Length2)
            {
                return 0;
            }

            // similar to Arrays.compare() but considers offset and Length
            int end1 = offset1 + Length1;
            int end2 = offset2 + Length2;
            for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++)
            {
                int a = buffer1[i] & 0xff;
                int b = buffer2[j] & 0xff;

                if (a != b)
                {
                    return a - b;
                }
            }

            return Length1 - Length2;
        }

        public void GetObjectData(SerializationInfo LogInformation, StreamingContext context)
        {
            throw new System.NotImplementedException();
        }
    }
}