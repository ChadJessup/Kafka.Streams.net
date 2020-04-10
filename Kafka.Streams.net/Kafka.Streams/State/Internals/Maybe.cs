
//    public class Maybe<T>
//    {
//        private T nullableValue;
//        private bool defined;

//        public static Maybe<T> defined(T nullableValue)
//        {
//            return new Maybe<>(nullableValue);
//        }

//        public static Maybe<T> undefined()
//        {
//            return new Maybe<>();
//        }

//        private Maybe(T nullableValue)
//        {
//            this.nullableValue = nullableValue;
//            defined = true;
//        }

//        private Maybe()
//        {
//            nullableValue = default;
//            defined = false;
//        }

//        public T getNullableValue()
//        {
//            if (defined)
//            {
//                return nullableValue;
//            }
//            else
//            {
//                throw new NoSuchElementException();
//            }
//        }

//        public bool isDefined()
//        {
//            return defined;
//        }

//        public override bool Equals(object o)
//        {
//            if (this == o) return true;
//            if (o == null || GetType() != o.GetType()) return false;
//            Maybe <object> maybe = (Maybe <object>) o;

//            // All undefined maybes are equal
//            // All defined null maybes are equal
//            return defined == maybe.defined &&
//                (!defined || Objects.Equals(nullableValue, maybe.nullableValue));
//        }

//        public override int GetHashCode()
//        {
//            // Since All undefined maybes are equal, we can hard-code their GetHashCode() to -1.
//            // Since All defined null maybes are equal, we can hard-code their GetHashCode() to 0.
//            return defined ? nullableValue == null ? 0 : nullableValue.GetHashCode() : -1;
//        }

//        public override string ToString()
//        {
//            if (defined)
//            {
//                return "DefinedMaybe{" + nullableValue + "}";
//            }
//            else
//            {
//                return "UndefinedMaybe{}";
//            }
//        }
//    }
//}