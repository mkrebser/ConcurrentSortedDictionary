#if DEBUG

namespace System.Collections.Concurrent.Extended;

public class BPlusTreeCorrectnessTest {

    private class TypedTest<K, V> where K: IComparable<K> {

        public void single_value_test(int k, K key, V val, V unusedV) {
            var tree = new ConcurrentSortedDictionary<K, V>(k);

            Test.Assert(tree.StartingWith(key).ToList().Count == 0);
            Test.Assert(tree.EndingWith(key).ToList().Count == 0);
            Test.Assert(tree.StartingWith(key, true).ToList().Count == 0);
            Test.Assert(tree.EndingWith(key, true).ToList().Count == 0);

            // Add something
            tree.AddOrUpdate(key, val);

            // Make a lot of assertions
            Test.Assert(tree.Count == 1);
            Test.Assert(!tree.IsEmpty);
            Test.AssertEqual(val, tree[key]);
            Test.Assert(tree.Count() == 1);
            Test.Assert(tree.ContainsKey(key));
            Test.Assert(tree.Depth == 1);
            Test.Assert(tree.StartingWith(key).ToList()[0].Key.Equals(key));
            Test.Assert(tree.EndingWith(key).ToList().Count == 0);
            Test.Assert(tree.StartingWith(key, true).ToList()[0].Key.Equals(key));
            Test.Assert(tree.EndingWith(key, true).ToList().Count == 0);
            
            V outVal;
            Test.Assert(tree.TryGetValue(key, out outVal));
            Test.AssertEqual(val, outVal);

            Test.AssertEqual(tree.GetOrAdd(key, unusedV), val);
            Test.Assert(tree.Count == 1);
            Test.Assert(tree.Where(x => Test.eq(x.Key, key)).ToList().Count == 1);
            tree.AssertTreeState(1);

            // Remove it
            Test.Assert(tree.TryRemove(key));
            tree.AssertTreeState(0);

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

        void checkIterators(List<ValueTuple<K, V>> pairs, ConcurrentSortedDictionary<K, V> tree) {
            
            var orderedPairs = pairs.ToList(); orderedPairs.Sort((x, y) => x.Item1.CompareTo(y.Item1));
            var reversedPairs = pairs.ToList(); pairs.Reverse();

            K middle = orderedPairs[orderedPairs.Count / 2].Item1;
            var firstHalf = tree.EndingWith(middle).ToList();
            var secondHalf = tree.StartingWith(middle).ToList();

            firstHalf.AddRange(secondHalf);
            for (int i = 0; i < orderedPairs.Count; i++) {
                Test.AssertEqual(firstHalf[i].Key, orderedPairs[i].Item1);
                Test.AssertEqual(firstHalf[i].Value, orderedPairs[i].Item2);
            }

            firstHalf = tree.Range(orderedPairs[0].Item1, middle).ToList();
            var secondHalfMinusOne = tree.Range(middle, orderedPairs[orderedPairs.Count - 1].Item1);

            firstHalf.AddRange(secondHalfMinusOne);
            for (int i = 0; i < orderedPairs.Count - 1; i++) {
                Test.AssertEqual(firstHalf[i].Key, orderedPairs[i].Item1);
                Test.AssertEqual(firstHalf[i].Value, orderedPairs[i].Item2);
            }
        }

        void addAllToTree(List<ValueTuple<K, V>> pairs, ConcurrentSortedDictionary<K, V> tree, bool alwaysAssertTreeState) {
            int count = 0;
            foreach (var pair in pairs) {
                Test.Assert(tree.TryAdd(pair.Item1, pair.Item2));
                count++;

                // check in 3 times to verify state while adding
                if (alwaysAssertTreeState || pairs.Count % count == pairs.Count / 3) {
                    tree.AssertTreeState(count);
                    Test.AssertEqual(tree[pair.Item1], pair.Item2);
                }
            }
            tree.AssertTreeState(pairs.Count);

            var treeList = tree.ToList();
            var reversedTree = tree.Reversed().ToList();
            var orderedPairs = pairs.ToList(); orderedPairs.Sort((x, y) => x.Item1.CompareTo(y.Item1));
            // Assert that the tree maintained order...
            foreach (var tuple in orderedPairs.Zip(treeList, (listPair, treePair) => (listPair, treePair))) {
                Test.AssertEqual(tuple.listPair.Item1, tuple.treePair.Key);
                Test.AssertEqual(tuple.listPair.Item2, tuple.treePair.Value);
            }
            orderedPairs.Reverse();
            foreach (var tuple in orderedPairs.Zip(reversedTree, (listPair, treePair) => (listPair, treePair))) {
                Test.AssertEqual(tuple.listPair.Item1, tuple.treePair.Key);
                Test.AssertEqual(tuple.listPair.Item2, tuple.treePair.Value);
            }

            checkIterators(pairs, tree);
        }
        void removeAllFromTree(List<ValueTuple<K, V>> pairs, ConcurrentSortedDictionary<K, V> tree, bool alwaysAssertTreeState) {
            // clear tree one-by-one ...
            int count = 0;
            foreach (var pair in pairs) {
                Test.Assert(tree.TryRemove(pair.Item1));
                tree.AssertTreeRoot(pairs.Count - (count+1));
                if (alwaysAssertTreeState || count <= 0 || count >= pairs.Count - 1 || count == pairs.Count / 2) {
                    tree.AssertTreeState(pairs.Count - (count+1));
                }
                count++;
            }
            tree.AssertTreeState(0);
        }

        public void addall_removeall_test(int k, List<ValueTuple<K, V>> pairs, bool alwaysAssertTreeState = false) {

            var tree = new ConcurrentSortedDictionary<K, V>(k);

            // sort the list...
            pairs.Sort((x, y) => x.Item1.CompareTo(y.Item1));

            // add in sorted order
            addAllToTree(pairs, tree, alwaysAssertTreeState);
            removeAllFromTree(pairs, tree, alwaysAssertTreeState);

            // add in reverse order ...
            var reversedPairs = pairs.ToList(); reversedPairs.Reverse();
            addAllToTree(reversedPairs, tree, alwaysAssertTreeState);
            removeAllFromTree(reversedPairs, tree, alwaysAssertTreeState);

            // add in random order ...
            var rand = new Random(k * pairs.Count);
            var randPairs = pairs.OrderBy(pair => rand.Next()).ToList();
            addAllToTree(randPairs, tree, alwaysAssertTreeState);
            removeAllFromTree(randPairs, tree, alwaysAssertTreeState);

            // rand test again, but using clear()
            rand = new Random(k * pairs.Count + 27);
            randPairs = pairs.OrderBy(pair => rand.Next()).ToList();
            addAllToTree(pairs, tree, alwaysAssertTreeState);
            tree.Clear();
            tree.AssertTreeState(0);
        }

        public void rand_add_removeall_test(int k, List<ValueTuple<K, V>> pairs, int ops = 1000000, bool alwaysAssertTreeState = false) {
            int opCount = 0;
            var rand = new Random(k * pairs.Count);
            var tree = new ConcurrentSortedDictionary<K, V>(k);
            while (opCount < ops) {
                var randPairs = pairs.OrderBy(pair => rand.Next()).ToList();
                addAllToTree(randPairs, tree, alwaysAssertTreeState);
                removeAllFromTree(randPairs, tree, alwaysAssertTreeState);
                opCount += randPairs.Count * 2;
                if (opCount % 1000000 == 0) {
                    Console.WriteLine(".");
                }
            }
        }

        public void rand_add_remove_test(int k, List<ValueTuple<K, V>> pairs, int ops = 1000000, bool alwaysAssertTreeState = false) {
            var tree = new ConcurrentSortedDictionary<K, V>(k);
            var rand = new Random(k * pairs.Count/2);
            var randPairs = pairs.OrderBy(pair => rand.Next()).ToList();
            var start = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            int opCount = 0;
            while (true) {

                // Try add rand.. try remove rand
                var nextPair = randPairs[rand.Next() % randPairs.Count];

                bool added = true;
                if (rand.Next() % 5 == 0) {
                    tree.AddOrUpdate(nextPair.Item1, nextPair.Item2);
                } else {
                    added = tree.TryAdd(nextPair.Item1, nextPair.Item2);
                }

                if (added) {
                    Test.Assert(tree.ContainsKey(nextPair.Item1));
                    opCount++;
                }

                nextPair = randPairs[rand.Next() % randPairs.Count];
                bool removed = tree.TryRemove(nextPair.Item1);

                if (removed) {
                    Test.Assert(!tree.ContainsKey(nextPair.Item1));
                    opCount++;
                }

                if (alwaysAssertTreeState || rand.Next() % pairs.Count == 0) {
                    tree.AssertTreeState((int)tree.Count);
                }

                opCount += 2;

                if (opCount > ops) {
                    break;
                }
            }
            var elaspsed = DateTimeOffset.Now.ToUnixTimeMilliseconds() - start;

            Console.WriteLine("Finished rand-read-write: [k:" + k + "] [" + (elaspsed / 1000.0) + " seconds] [" + pairs.Count + " items ] [" + opCount + " ops].");
        }
    }

    public void run() {
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

        // basic add delete test (easier to debug) before doing more exhasutive tests below
        var tentest = new TypedTest<int, int>();
        var tentest_pairs = Enumerable.Range(1, 10)
            .Select(x => new ValueTuple<int, int>(x, -x))
            .ToList();
        tentest.addall_removeall_test(3, tentest_pairs, alwaysAssertTreeState: true);
        tentest.addall_removeall_test(4, tentest_pairs.ToList(), alwaysAssertTreeState: true);
        tentest.rand_add_remove_test(3, tentest_pairs, alwaysAssertTreeState: true);
        tentest.rand_add_remove_test(3, Enumerable.Range(1, 18)
            .Select(x => new ValueTuple<int, int>(x, -x))
            .ToList(), alwaysAssertTreeState: true);

        tentest.rand_add_removeall_test(32, Enumerable.Range(1, 640)
            .Select(x => new ValueTuple<int, int>(x, -x))
            .ToList(), 10000000, false);


        List<Thread> threads = new List<Thread>();
        // run tests on different types for many 'k' values and tree sizes
        foreach (var k in Test.K_Range) {
            foreach (var count in Test.Item_Count_Small) {
                var t = new Thread(() => {
                    var intrange = Enumerable.Range(1, count);
                    // int int tests
                    var intint = new TypedTest<int, int>();
                    var intint_pairs = intrange
                        .Select(x => new ValueTuple<int, int>(x, -x))
                        .ToList();
                    intint.addall_removeall_test(k, intint_pairs);

                    var stringstring = new TypedTest<string, string>();
                    var stringstring_pairs = intrange
                        .Select(x => new ValueTuple<string, string>(x.ToString(), (-x).ToString()))
                        .ToList();
                    stringstring.addall_removeall_test(k, stringstring_pairs);

                    var structclass = new TypedTest<CustomStruct, CustomClass>();
                    var structclass_pairs = intrange
                        .Select(x => new ValueTuple<CustomStruct,CustomClass>(new CustomStruct(x), new CustomClass(-x)))
                        .ToList();
                    structclass.addall_removeall_test(k, structclass_pairs);

                    var classstruct = new TypedTest<CustomClass, CustomStruct>();
                    var classstruct_pairs = intrange
                        .Select(x => new ValueTuple<CustomClass, CustomStruct>(new CustomClass(x), new CustomStruct(-x)))
                        .ToList();
                    classstruct.addall_removeall_test(k, classstruct_pairs);
                });
                t.Start();
                threads.Add(t);
            }
            Console.WriteLine("Finished [k:" + k.ToString() + "] tree correctness test.");
        }

        foreach (var t in threads)
            t.Join();

        // Perform 1 million random ops for every combo of 'k' and various item counts
        // additionally every 'N' ops, there is a full recursion through the tree to validate ordering, parent refs, balancing, and other tree properties
        threads = new List<Thread>();
        foreach (var k in Test.K_Range) {
            foreach (var count in Test.Item_Count_Small) {

                var t = new Thread(() => {
                    var intrange = Enumerable.Range(1, count);
                    // int int tests
                    var intint = new TypedTest<int, int>();
                    var intint_pairs = intrange
                        .Select(x => new ValueTuple<int, int>(x, -x))
                        .ToList();
                    intint.rand_add_remove_test(k, intint_pairs);

                    var stringstring = new TypedTest<string, string>();
                    var stringstring_pairs = intrange
                        .Select(x => new ValueTuple<string, string>(x.ToString(), (-x).ToString()))
                        .ToList();
                    stringstring.rand_add_remove_test(k, stringstring_pairs);

                    var structclass = new TypedTest<CustomStruct, CustomClass>();
                    var structclass_pairs = intrange
                        .Select(x => new ValueTuple<CustomStruct,CustomClass>(new CustomStruct(x), new CustomClass(-x)))
                        .ToList();
                    structclass.rand_add_remove_test(k, structclass_pairs);

                    var classstruct = new TypedTest<CustomClass, CustomStruct>();
                    var classstruct_pairs = intrange
                        .Select(x => new ValueTuple<CustomClass, CustomStruct>(new CustomClass(x), new CustomStruct(-x)))
                        .ToList();
                    classstruct.rand_add_remove_test(k, classstruct_pairs);
                });

                t.Start();
                threads.Add(t);
            }
        }

        foreach (var t in threads)
            t.Join();
    }
}

#endif
