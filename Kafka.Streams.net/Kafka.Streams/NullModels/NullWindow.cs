using System;
using Kafka.Streams.KStream;

namespace Kafka.Streams.NullModels
{
    internal class NullWindow : Window
    {
        public NullWindow()
            : base(TimeSpan.FromMilliseconds(1.0))
        {
        }

        public override bool Overlap(Window other) => false;
    }
}
