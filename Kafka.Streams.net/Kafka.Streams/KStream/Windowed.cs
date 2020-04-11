using System;

namespace Kafka.Streams.KStream
{
    public interface IWindowed
    {
        Window window { get; }
    }

    public interface IWindowed<out K> : IWindowed
    {
        K Key { get; }
    }

    public class Windowed2<K> : IWindowed<K>
    {
        public K Key { get; private set; }
        public Window window { get; private set; }

        public Windowed2(K key, Window window)
        {
            this.Key = key ?? throw new ArgumentNullException(nameof(key));
            this.window = window ?? throw new ArgumentNullException(nameof(key));
        }

        public override string ToString()
            => $"[{this.Key}@{this.window.Start()}/{this.window.End()}]";

        public override bool Equals(object obj)
        {
            if (obj == this)
            {
                return true;
            }

            if (!(obj is Windowed2<K>))
            {
                return false;
            }

            var that = (Windowed2<K>)obj;
            return this.window.Equals(that.window) && this.Key.Equals(that.Key);
        }

        public override int GetHashCode()
        {
            var n = ((long)this.window.GetHashCode() << 32) | this.Key.GetHashCode();
            return (int)(n % 0xFFFFFFFFL);
        }
    }
}
