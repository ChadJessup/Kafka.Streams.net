using System;

namespace Kafka.Streams.KStream
{
    public interface IWindowed
    {
        Window Window { get; }
    }

    public interface IWindowed<out K> : IWindowed
    {
        K Key { get; }
    }

    public class Windowed<K> : IWindowed<K>
    {
        public K Key { get; private set; }
        public Window Window { get; private set; }

        public Windowed(K key, Window window)
        {
            this.Key = key ?? throw new ArgumentNullException(nameof(key));
            this.Window = window ?? throw new ArgumentNullException(nameof(key));
        }

        public override string ToString()
            => $"[{this.Key}@{this.Window.Start()}/{this.Window.End()}]";

        public override bool Equals(object obj)
        {
            if (obj == this)
            {
                return true;
            }

            if (!(obj is Windowed<K>))
            {
                return false;
            }

            var that = (Windowed<K>)obj;
            return this.Window.Equals(that.Window) && this.Key.Equals(that.Key);
        }

        public override int GetHashCode()
        {
            var n = ((long)this.Window.GetHashCode() << 32) | this.Key.GetHashCode();
            return (int)(n % 0xFFFFFFFFL);
        }
    }
}
