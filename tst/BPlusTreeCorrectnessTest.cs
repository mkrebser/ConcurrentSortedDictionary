#if DEBUG

namespace System.Collections.Concurrent.Extended;

public class BPlusTreeCorrectnessTest {

    private class TypedTest<K, V> where K: IComparable<K> {

        public void single_value_test(int k, K key, V val, V unusedV) {
            var tree = new ConcurrentSortedDictionary<K, V>(k);

            // Add something
            tree.AddOrUpdate(key, val);

            // Make a lot of assertions
            Test.Assert(tree.Count == 1);
            Test.Assert(!tree.IsEmpty);
            Test.AssertEqual(val, tree[key]);
            Test.Assert(tree.Count() == 1);
            Test.Assert(tree.ContainsKey(key));
            Test.Assert(tree.Depth == 1);
            
            V outVal;
            Test.Assert(tree.TryGetValue(key, out outVal));
            Test.AssertEqual(val, outVal);

            Test.AssertEqual(tree.GetOrAdd(key, unusedV), val);
            Test.Assert(tree.Count == 1);
            Test.Assert(tree.Where(x => Test.eq(x.Key, key)).ToList().Count == 1);
            tree.AssertTreeState(1);

            // Remove it
            Test.Assert(tree.TryRemove(key));

            Test.Assert(tree.Count == 0);
            Test.Assert(tree.IsEmpty);
            Test.Assert(!tree.TryGetValue(key, out outVal));
            Test.Assert(tree.Count() == 0);
            Test.Assert(!tree.ContainsKey(key));
            tree.AssertTreeState(0);

            // Add using TryAdd
            Test.Assert(tree.TryAdd(key, val));
            Test.AssertEqual(tree[key], val);
            Test.Assert(tree.Count == 1);
            Test.Assert(tree.Where(x => Test.eq(x.Key, key)).ToList().Count == 1);
            tree.AssertTreeState(1);

            // Remove using Clear
            tree.Clear();
            Test.Assert(tree.Count == 0);
            Test.Assert(tree.IsEmpty);
            Test.Assert(!tree.TryGetValue(key, out outVal));
            Test.Assert(tree.Count() == 0);
            Test.Assert(!tree.ContainsKey(key));
            tree.AssertTreeState(0);

            // Add using GetOrAdd
            Test.AssertEqual(tree.GetOrAdd(key, val), val);
        }

        public void multi_value_test(int k, List<ValueTuple<K, V>> pairs, V unusedV) {
            var tree = new ConcurrentSortedDictionary<K, V>(k);

            // sort the list...
            pairs.Sort((x, y) => x.Item1.CompareTo(y.Item1));

            int count = 0;
            foreach (var pair in pairs) {
                Test.Assert(tree.TryAdd(pair.Item1, pair.Item2));
                count++;

                // check in 3 times to verify state while adding
                if (pairs.Count % count == pairs.Count / 3) {
                    tree.AssertTreeState(count);
                    Test.AssertEqual(tree[pair.Item1], pair.Item2);
                }
            }
            tree.AssertTreeState(pairs.Count);

            var treeList = tree.ToList();

            // Assert that the tree maintained order...
            foreach (var tuple in pairs.Zip(treeList, (listPair, treePair) => (listPair, treePair))) {
                Test.AssertEqual(tuple.listPair.Item1, tuple.treePair.Key);
                Test.AssertEqual(tuple.listPair.Item2, tuple.treePair.Value);
            }

            // clear tree ...

            // add in reverse order ...

            // clear tree

            // add in random order ...
        }
    }

    public void run() {

        // TODO.. Reached max depth test!

        // singles tests
        foreach (var k in Test.K_Range) {
            // int int tests
            var intint = new TypedTest<int, int>();
            intint.single_value_test(k, 1, 2, 0);

            var stringstring = new TypedTest<string, string>();
            stringstring.single_value_test(k, "012345", "string", "");

            var structclass = new TypedTest<CustomStruct, CustomClass>();
            structclass.single_value_test(k, new CustomStruct(1), new CustomClass(2), new CustomClass(0));

            var classstruct = new TypedTest<CustomClass, CustomStruct>();
            classstruct.single_value_test(k, new CustomClass(1), new CustomStruct(2), new CustomStruct(0));
        }

        Console.WriteLine("Finished [1] item correctness test.");

        foreach (var k in Test.K_Range) {
            foreach (var count in Test.Item_Count_Small) {
                var intrange = Enumerable.Range(1, count);
                // int int tests
                var intint = new TypedTest<int, int>();
                var intint_pairs = intrange
                    .Select(x => new ValueTuple<int, int>(x, -x))
                    .ToList();
                intint.multi_value_test(k, intint_pairs, 0);

                var stringstring = new TypedTest<string, string>();
                var stringstring_pairs = intrange
                    .Select(x => new ValueTuple<string, string>(x.ToString(), (-x).ToString()))
                    .ToList();
                stringstring.multi_value_test(k, stringstring_pairs, "");

                var structclass = new TypedTest<CustomStruct, CustomClass>();
                var structclass_pairs = intrange
                    .Select(x => new ValueTuple<CustomStruct,CustomClass>(new CustomStruct(x), new CustomClass(-x)))
                    .ToList();
                structclass.multi_value_test(k, structclass_pairs, new CustomClass(0));

                var classstruct = new TypedTest<CustomClass, CustomStruct>();
                var classstruct_pairs = intrange
                    .Select(x => new ValueTuple<CustomClass, CustomStruct>(new CustomClass(x), new CustomStruct(-x)))
                    .ToList();
                classstruct.multi_value_test(k, classstruct_pairs, new CustomStruct(0));
            }
            Console.WriteLine("Finished [k:" + k.ToString() + "] tree correctness test.");
        }
    }
}

#endif
