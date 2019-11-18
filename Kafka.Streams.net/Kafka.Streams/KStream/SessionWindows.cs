
//        [System.Obsolete]
//        public long maintainMs()
//        {
//            return Math.Max(maintainDurationMs, gapMs);
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
//            SessionWindows that = (SessionWindows)o;
//            return gapMs == that.gapMs &&
//                maintainDurationMs == that.maintainDurationMs &&
//                graceMs == that.graceMs;
//        }


//        public int hashCode()
//        {
//            return Objects.hash(gapMs, maintainDurationMs, graceMs);
//        }


//        public string ToString()
//        {
//            return "SessionWindows{" +
//                "gapMs=" + gapMs +
//                ", maintainDurationMs=" + maintainDurationMs +
//                ", graceMs=" + graceMs +
//                '}';
//        }
//    }
//}
