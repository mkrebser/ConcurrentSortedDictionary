#if DEBUG

using System.Diagnostics;

namespace System.Collections.Concurrent.Extended;

public class AssertionException : Exception {
    public AssertionException(string location) : base(location) {}
}

public static class Test {
    public static void Assert(bool val) {
        if (!val) {
            var stackTrace = new StackTrace();
            var frame = stackTrace.GetFrame(stackTrace.FrameCount - 2);

            string location = "Assertion Failed, Line: " +
                frame?.GetFileLineNumber() + ", " +
                frame?.GetFileName() + ", " +
                frame?.GetMethod();

            throw new AssertionException(location);
        }
    }

    public static bool eq<T>(T l, T r) {
        return EqualityComparer<T>.Default.Equals(l, r);
    }

    public static void AssertEqual<T>(T l, T r) {
        Assert(eq(l, r));
    }
    public static void AssertNotEqual<T>(T l, T r) {
        Assert(!eq(l, r));
    }
    public static void AssertLessThan<T>(T leftIsLessThan, T right) {
        int val = Comparer<T>.Default.Compare(leftIsLessThan, right);
        Assert(val < 0);
    }
    public static List<int> K_Range = new List<int>() { 3, 4, 5, 6, 8, 10, 13, 16, 20, 32, 64, 101, 128, 1024 };
    public static List<int> Item_Count_Small = new List<int>() { 10, 100, 1000, 10000 };
}

public class CustomClass : IComparable<CustomClass>, IEquatable<CustomClass>, IComparable {
    public int value;

    public CustomClass(int val) { this.value = val; }

    public int CompareTo(CustomClass? other) {
        if (ReferenceEquals(null, other))
            return 1;
        return this.value.CompareTo(other.value);
    }
    public int CompareTo(object? obj) {
        if (ReferenceEquals(null, obj))
            return 1;
        return CompareTo(obj as CustomClass);
    }
    public override bool Equals(object? obj) {
            return Equals(obj as CustomClass);
        }
    public bool Equals(CustomClass? other) {
        return ReferenceEquals(null, other) ? false : other.value == this.value;
    }
    public override int GetHashCode() {
        return this.value;
    }
}

public struct CustomStruct : IComparable<CustomStruct>, IEquatable<CustomStruct>, IComparable {
    public int value;

    public CustomStruct(int val) { this.value = val; }

    public int CompareTo(CustomStruct other) {
        return this.value.CompareTo(other.value);
    }
    public int CompareTo(object? obj) {
        if (!(obj is CustomStruct))
            throw new ArgumentException("Comparison requires CustomStruct type");
        return CompareTo((CustomStruct)obj);
    }
    public override bool Equals(object? obj) {
        if (!(obj is CustomStruct)) 
            return false;
        return Equals((CustomStruct)obj);
    }
    public bool Equals(CustomStruct other) {
        return other.value == this.value;
    }
    public override int GetHashCode() {
        return this.value;
    }
}

#endif
