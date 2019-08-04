namespace Kafka.Common.Metrics
{
    /**
     * An upper or lower bound for metrics
     */
    public class Quota
    {
        public bool isUpperBound { get; set; }
        public double bound { get; set; }

        public Quota(double bound, bool isUpperBound)
        {
            this.bound = bound;
            this.isUpperBound = isUpperBound;
        }

        public static Quota upperBound(double upperBound)
        {
            return new Quota(upperBound, true);
        }

        public static Quota lowerBound(double lowerBound)
        {
            return new Quota(lowerBound, false);
        }

        public bool acceptable(double value)
        {
            return (isUpperBound && value <= bound) || (!isUpperBound && value >= bound);
        }

        public override int GetHashCode()
        {
            int prime = 31;
            int result = 1;
            result = prime * result + (int)this.bound;
            result = prime * result + (this.isUpperBound ? 1 : 0);
            return result;
        }


        public override bool Equals(object obj)
        {
            if (this == obj)
                return true;
            if (!(obj is Quota))
                return false;
            Quota that = (Quota)obj;
            return (that.bound == this.bound) && (that.isUpperBound == this.isUpperBound);
        }


        public override string ToString()
        {
            return (isUpperBound ? "upper=" : "lower=") + bound;
        }
    }
}