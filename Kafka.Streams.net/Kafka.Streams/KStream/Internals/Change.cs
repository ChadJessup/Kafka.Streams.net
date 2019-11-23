using Kafka.Common;

namespace Kafka.Streams.KStream.Internals
{
    public class Change<T>
    {
        public T newValue { get; }
        public T oldValue { get; }

        public Change(T newValue, T oldValue)
        {
            this.newValue = newValue;
            this.oldValue = oldValue;
        }

        public override string ToString()
            => $"({newValue}<-{oldValue})";

        public static explicit operator T(Change<T> change)
        {
            return change.newValue;
        }

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }

            if (o == null || GetType() != o.GetType())
            {
                return false;
            }

            Change<object> change = (Change<object>)o;

            return newValue?.Equals(change.newValue) ?? false
                && oldValue.Equals(change.oldValue);
        }

        public override int GetHashCode()
        {
            return (newValue, oldValue).GetHashCode();
        }
    }
}
