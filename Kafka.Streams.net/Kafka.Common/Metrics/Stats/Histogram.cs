using Kafka.Common.Metrics.Stats.Interfaces;
using System;
using System.Text;

namespace Kafka.Common.Metrics.Stats
{

    public class Histogram
    {
        private IBinScheme binScheme;
        private float[] hist;
        private double count;

        public Histogram(IBinScheme binScheme)
        {
            this.hist = new float[binScheme.bins];
            this.count = 0.0f;
            this.binScheme = binScheme;
        }

        public void record(double value)
        {
            this.hist[binScheme.toBin(value)] += 1.0f;
            this.count += 1.0d;
        }

        public double value(double quantile)
        {
            if (count == 0.0d)
                return Double.NaN;
            if (quantile > 1.00d)
                return float.PositiveInfinity;

            if (quantile < 0.00d)
                return float.NegativeInfinity;

            float sum = 0.0f;
            float quant = (float)quantile;
            for (int i = 0; i < this.hist.Length - 1; i++)
            {
                sum += this.hist[i];
                if (sum / count > quant)
                    return binScheme.fromBin(i);
            }
            return binScheme.fromBin(this.hist.Length - 1);
        }

        public float[] counts()
        {
            return this.hist;
        }

        public void clear()
        {
            for (int i = 0; i < this.hist.Length; i++)
                this.hist[i] = 0.0f;
            this.count = 0;
        }

        public override string ToString()
        {
            StringBuilder b = new StringBuilder("{");
            for (int i = 0; i < this.hist.Length - 1; i++)
            {
                b.Append(string.Format("%.10f", binScheme.fromBin(i)));
                b.Append(':');
                b.Append(string.Format("%.0f", this.hist[i]));
                b.Append(',');
            }

            b.Append(float.PositiveInfinity);
            b.Append(':');
            b.Append(string.Format("%.0f", this.hist[this.hist.Length - 1]));
            b.Append('}');

            return b.ToString();
        }
    }
}
