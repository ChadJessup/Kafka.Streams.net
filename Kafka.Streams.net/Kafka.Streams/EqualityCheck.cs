using System;

public static class EqualityCheck
{
    public static void VerifyEquality<T>(T o1, T o2)
    {
        // making sure we don't get an NPE in the test
        if (o1 == null && o2 == null)
        {
            return;
        }
        else if (o1 == null)
        {
            throw new Exception(string.Format("o1 was null, but o2[%s] was not.", o2));
        }
        else if (o2 == null)
        {
            throw new Exception(string.Format("o1[%s] was not null, but o2 was.", o1));
        }

        VerifyGeneralEqualityProperties(o1, o2);

        // the two objects should equal each other
        if (!o1.Equals(o2))
        {
            throw new Exception(string.Format("o1[%s] was not equal to o2[%s].", o1, o2));
        }

        if (!o2.Equals(o1))
        {
            throw new Exception(string.Format("o2[%s] was not equal to o1[%s].", o2, o1));
        }

        verifyHashCodeConsistency(o1, o2);

        // since these objects are equal, their hashcode MUST be the same
        if (o1.GetHashCode() != o2.GetHashCode())
        {
            throw new Exception(string.Format("o1[%s].hash[%d] was not equal to o2[%s].hash[%d].", o1, o1.GetHashCode(), o2, o2.GetHashCode()));
        }
    }

    public static void VerifyInEquality<T>(T o1, T o2)
    {
        // making sure we don't get an NPE in the test
        if (o1 == null && o2 == null)
        {
            throw new Exception("Both o1 and o2 were null.");
        }
        else if (o1 == null)
        {
            return;
        }
        else if (o2 == null)
        {
            return;
        }

        VerifyGeneralEqualityProperties(o1, o2);

        // these two objects should NOT equal each other
        if (o1.Equals(o2))
        {
            throw new Exception(string.Format("o1[%s] was equal to o2[%s].", o1, o2));
        }

        if (o2.Equals(o1))
        {
            throw new Exception(string.Format("o2[%s] was equal to o1[%s].", o2, o1));
        }

        verifyHashCodeConsistency(o1, o2);

        // since these objects are NOT equal, their hashcode SHOULD PROBABLY not be the same
        if (o1.GetHashCode() == o2.GetHashCode())
        {
            throw new Exception(
                string.Format(
                    "o1[%s].hash[%d] was equal to o2[%s].hash[%d], even though !o1.Equals(o2). " +
                        "This is NOT A BUG, but it is undesirable for hash collection performance.",
                    o1,
                    o1.GetHashCode(),
                    o2,
                    o2.GetHashCode()
                )
            );
        }
    }

    private static void VerifyGeneralEqualityProperties<T>(T o1, T o2)
    {
        // objects should equal themselves
        if (!o1.Equals(o1))
        {
            throw new Exception(string.Format("o1[%s] was not equal to itself.", o1));
        }

        if (!o2.Equals(o2))
        {
            throw new Exception(string.Format("o2[%s] was not equal to itself.", o2));
        }

        // non-null objects should not equal null
        if (o1.Equals(null))
        {
            throw new Exception(string.Format("o1[%s] was equal to null.", o1));
        }

        if (o2.Equals(null))
        {
            throw new Exception(string.Format("o2[%s] was equal to null.", o2));
        }

        // objects should not equal some random object
        if (o1.Equals(new object()))
        {
            throw new Exception($"o1[{o1}] was equal to an anonymous object.");
        }

        if (o2.Equals(new object()))
        {
            throw new Exception($"o2[{o2}] was equal to an anonymous object.");
        }
    }


    private static void verifyHashCodeConsistency<T>(T o1, T o2)
    {
        {
            int first = o1.GetHashCode();
            int second = o1.GetHashCode();
            if (first != second)
            {
                throw new Exception(
                    string.Format(
                        "o1[%s]'s hashcode was not consistent: [%d]!=[%d].",
                        o1,
                        first,
                        second
                    )
                );
            }
        }

        {
            int first = o2.GetHashCode();
            int second = o2.GetHashCode();
            if (first != second)
            {
                throw new Exception(
                    string.Format(
                        "o2[%s]'s hashcode was not consistent: [%d]!=[%d].",
                        o2,
                        first,
                        second
                    )
                );
            }
        }
    }
}
