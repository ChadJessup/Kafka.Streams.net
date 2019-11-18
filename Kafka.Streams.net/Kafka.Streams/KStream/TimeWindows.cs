

//        [System.Obsolete]
//        public long maintainMs()
//        {
//            return Math.Max(maintainDurationMs, sizeMs);
//        }



//        public bool Equals(object o)
//        {
//            if (this == o)
//            {
//                return true;
//            }
//            if (o == null || GetType() != o.GetType())
//            {
//                return false;
//            }
//            TimeWindows that = (TimeWindows)o;
//            return maintainDurationMs == that.maintainDurationMs &&
//                segments == that.segments &&
//                sizeMs == that.sizeMs &&
//                advanceMs == that.advanceMs &&
//                graceMs == that.graceMs;
//        }

//        public int hashCode()
//        {
//            return (maintainDurationMs, segments, sizeMs, advanceMs, graceMs)
//                .GetHashCode();
//        }



//        public string ToString()
//        {
//            return "TimeWindows{" +
//                "maintainDurationMs=" + maintainDurationMs +
//                ", sizeMs=" + sizeMs +
//                ", advanceMs=" + advanceMs +
//                ", graceMs=" + graceMs +
//                ", segments=" + segments +
//                '}';
//        }
//    }
//}