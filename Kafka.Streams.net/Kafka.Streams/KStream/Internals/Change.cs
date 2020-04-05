namespace Kafka.Streams.KStream.Internals
{
    public interface IChange<out T>
    {
        T newValue { get; }
        T oldValue { get; }
    }

    public class Change<V> : IChange<V>
    {
        public V newValue { get; }
        public V oldValue { get; }

        public Change(V newValue = default, V oldValue = default)
        {
            this.newValue = newValue;
            this.oldValue = oldValue;
        }

        public override string ToString()
            => $"({newValue}<-{oldValue})";

        public static implicit operator V(Change<V> change)
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

            var change = (Change<object>)o;

            return newValue?.Equals(change.newValue) ?? false
                && oldValue.Equals(change.oldValue);
        }

        public override int GetHashCode()
        {
            return (newValue, oldValue).GetHashCode();
        }
    }
}
