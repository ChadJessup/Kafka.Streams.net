using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Kafka.Common.Utils.Interfaces
{
    public interface IByteArrayComparator : IComparer<byte[]>, ISerializable
    {
        int Compare(byte[] buffer1, int offset1, int length1,
                    byte[] buffer2, int offset2, int length2);
    }
}