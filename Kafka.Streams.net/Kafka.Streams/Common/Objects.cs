
using System;

namespace Kafka.Common
{
    public static class Objects
    {
        public static int hash<T>(T newValue, T oldValue)
        {
            return (newValue, oldValue).GetHashCode();
        }

        public static T requireNonNull<T>(T obj, string message)
        {
            if (obj == null)
            {
                throw new ArgumentNullException(message, nameof(obj));
            }

            return obj;
        }
    }
}