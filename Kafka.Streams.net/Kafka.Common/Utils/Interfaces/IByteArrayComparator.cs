using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Kafka.Common.Utils.Interfaces
{
    public interface IByteArrayComparator : IComparer<byte[]>, ISerializable
    {
        int Compare(byte[] buffer1, int offset1, int Length1,
                    byte[] buffer2, int offset2, int Length2);
    }
}