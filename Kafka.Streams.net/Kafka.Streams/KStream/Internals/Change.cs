using System;

namespace Kafka.Streams.KStream.Internals
{
    public interface IChange<out T>
    {
        T NewValue { get; }
        T OldValue { get; }
    }

    public class Change<V> : IChange<V>
    {
        public V NewValue { get; }
        public V OldValue { get; }

        public Change(V newValue = default, V oldValue = default)
        {
            this.NewValue = newValue;
            this.OldValue = oldValue;
        }

        public override string ToString()
            => $"({NewValue}<-{OldValue})";

        public static implicit operator V(Change<V> change)
        {
            return change.NewValue;
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

            var change = (Change<object>)o;

            return NewValue?.Equals(change.NewValue) ?? false
                && OldValue.Equals(change.OldValue);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(this.NewValue, this.OldValue);
        }
    }
}
