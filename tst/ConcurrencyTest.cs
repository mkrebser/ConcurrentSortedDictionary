#if DEBUG

#nullable disable

namespace System.Collections.Concurrent.Extended;

public class ConcurrencyTest {

    private class TypedTest<K, V> where K: IComparable<K> {

        public void rand_add_remove_cycle_test(int k, List<ValueTuple<K, V>> pairs, int ms = 450000, int nThreads = 32, bool alwaysAssertTreeState = false) {
            // Similar to the other tests but this one will force a lot more splits ans merges by adding and deleting in cycles
            var tree = new ConcurrentSortedDictionary<K, V>(k);
            var rand = new Random(k * pairs.Count/2);
            var start = DateTimeOffset.Now.ToUnixTimeMilliseconds();

            // no deadlock testing on the tests that use barriers because they require all threads to be running...
            // otherwise it will never finish (:

            int index = 0;

            var sanity = new ConcurrentDictionary<K, V>();

            var barrier = new Barrier(nThreads);

            var threads = new List<Thread>();
            for (int i = 0; i < nThreads; i++) {

                var newList = new List<ValueTuple<K, V>>();
                int stop = (i + 1) * (pairs.Count / nThreads);
                while (index < stop) {
                    newList.Add(pairs[index]);
                    index++;
                }

                var t = new Thread(() => {

                    long opCount = 0;
                    long timeOfLastPing = 0;

                    foreach (var p in newList) {
                        if (sanity.ContainsKey(p.Item1))
                            throw new Exception("Must have unique lists");
                        Test.Assert(sanity.TryAdd(p.Item1, p.Item2));
                    }

                    var items = new ConcurrentQueue<ValueTuple<K, V>>(newList);
                    var treeItems = new ConcurrentQueue<ValueTuple<K, V>>();

                    float chanceCount = newList.Count * 0.7f;

                    while (true) {

                        ValueTuple<K, V> nextPair;
                        for (int i = 0; i < newList.Count; i++) {
                            if (rand.NextDouble() * newList.Count < chanceCount) {
                                if (items.TryDequeue(out nextPair)) {
                                    Test.Assert(tree.TryAdd(nextPair.Item1, nextPair.Item2));
                                    treeItems.Enqueue(nextPair);
                                    opCount++;
                                }
                            }
                        }

                        barrier.SignalAndWait();

                        while (treeItems.TryDequeue(out nextPair)) {
                            Test.Assert(tree.TryRemove(nextPair.Item1));
                            items.Enqueue(nextPair);
                            opCount++;
                        }

                        barrier.SignalAndWait();


                        var elaspsed = DateTimeOffset.Now.ToUnixTimeMilliseconds() - start;
                        if (elaspsed > ms) {
                            break;
                        }

                        if (DateTimeOffset.Now.ToUnixTimeMilliseconds() - timeOfLastPing > 1000) {
                            Console.WriteLine(".");
                            timeOfLastPing = DateTimeOffset.Now.ToUnixTimeMilliseconds();
                        }
                    }

                    Console.WriteLine("Finished rand-read-write: [k:" + k + "] [" + pairs.Count + " items ] [" + opCount + " ops].");

                });
                t.Start();
                threads.Add(t);
            }

            foreach (var t in threads)
                t.Join();
        }

        public void rand_add_remove_test(int k, List<ValueTuple<K, V>> pairs, int ms = 450000, int nThreads = 32, bool alwaysAssertTreeState = false) {
            var tree = new ConcurrentSortedDictionary<K, V>(k);
            var rand = new Random(k * pairs.Count/2);
            var start = DateTimeOffset.Now.ToUnixTimeMilliseconds();

            // deadlock testing
            List<long>[] pingTimes = new List<long>[nThreads];
            for (int i = 0; i < nThreads; i++) {
                pingTimes[i] = new List<long>();
            }

            int index = 0;

            var sanity = new ConcurrentDictionary<K, V>();

            var threads = new List<Thread>();
            for (int i = 0; i < nThreads; i++) {

                var newList = new List<ValueTuple<K, V>>();
                int stop = (i + 1) * (pairs.Count / nThreads);
                int id = i;
                while (index < stop) {
                    newList.Add(pairs[index]);
                    index++;
                }

                var t = new Thread(() => {

                    long opCount = 0;
                    long timeOfLastPing = 0;

                    foreach (var p in newList) {
                        if (sanity.ContainsKey(p.Item1))
                            throw new Exception("Must have unique lists");
                        Test.Assert(sanity.TryAdd(p.Item1, p.Item2));
                    }

                    while (true) {

                        // Try add rand.. try remove rand
                        var nextPair = newList[rand.Next() % newList.Count];
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

                        nextPair = newList[rand.Next() % newList.Count];
                        bool removed = tree.TryRemove(nextPair.Item1);

                        if (removed) {
                            Test.Assert(!tree.ContainsKey(nextPair.Item1));
                            opCount++;
                        }

                        opCount += 2;

                        var elaspsed = DateTimeOffset.Now.ToUnixTimeMilliseconds() - start;
                        if (elaspsed > ms) {
                            break;
                        }

                        if (DateTimeOffset.Now.ToUnixTimeMilliseconds() - timeOfLastPing > 1000) {
                            Console.WriteLine(".");
                            timeOfLastPing = DateTimeOffset.Now.ToUnixTimeMilliseconds();
                            pingTimes[id].Add(timeOfLastPing);
                        }
                    }

                    Console.WriteLine("Finished rand-read-write: [k:" + k + "] [" + pairs.Count + " items ] [" + opCount + " ops].");

                });
                t.Start();
                threads.Add(t);
            }

            foreach (var t in threads)
                t.Join();

            // check for deadlocks (or just really starved periods)
            var now = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            foreach (var l in pingTimes) {
                for (int i = 1; i < l.Count; i++) {
                    var diff = (l[i] - l[i-1]) / 1000.0f;
                    Test.Assert(diff < 10.0f);
                }
                Test.Assert((now - l[l.Count - 1]) / 1000.0f < 10.0f);
            }
        }

        private void iterateTreeOrder(Random rand, ConcurrentSortedDictionary<K, V> tree, ref long opCount) {
            K prev = default(K);
            bool prevInit = false;
            // Iterate through keys.. Either reversed or not reversed
            if (rand.Next() % 2 == 0) {
                foreach (var pair in tree) {
                    opCount++;
                    if (prevInit) {
                            Test.Assert(prev.CompareTo(pair.Key) < 0);
                    }
                    prev = pair.Key;
                    prevInit = true;
                }
            } else {
                foreach (var pair in tree.Reversed()) {
                    opCount++;
                    if (prevInit) {
                            Test.Assert(prev.CompareTo(pair.Key) > 0);
                    }
                    prev = pair.Key;
                    prevInit = true;
                }
            }
        }

        public void rand_add_remove_iterator_test(int k, List<ValueTuple<K, V>> pairs, int ms = 900000, int nThreads = 32, bool alwaysAssertTreeState = false) {
            var tree = new ConcurrentSortedDictionary<K, V>(k);
            var rand = new Random(k * pairs.Count/2);
            var start = DateTimeOffset.Now.ToUnixTimeMilliseconds();

            // deadlock testing
            List<long>[] pingTimes = new List<long>[nThreads];
            for (int i = 0; i < nThreads; i++) {
                pingTimes[i] = new List<long>();
            }

            int index = 0;

            var sanity = new ConcurrentDictionary<K, V>();

            var threads = new List<Thread>();
            for (int i = 0; i < nThreads; i++) {

                var newList = new List<ValueTuple<K, V>>();
                int stop = (i + 1) * (pairs.Count / nThreads);
                int id = i;
                while (index < stop) {
                    newList.Add(pairs[index]);
                    index++;
                }

                var t = new Thread(() => {

                    long opCount = 0;
                    long timeOfLastPing = 0;

                    foreach (var p in newList) {
                        if (sanity.ContainsKey(p.Item1))
                            throw new Exception("Must have unique lists");
                        Test.Assert(sanity.TryAdd(p.Item1, p.Item2));
                    }

                    while (true) {

                        // Try add rand.. try remove rand
                        var nextPair = newList[rand.Next() % newList.Count];
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

                        nextPair = newList[rand.Next() % newList.Count];
                        bool removed = tree.TryRemove(nextPair.Item1);

                        if (removed) {
                            Test.Assert(!tree.ContainsKey(nextPair.Item1));
                            opCount++;
                        }

                        opCount += 2;

                        var elaspsed = DateTimeOffset.Now.ToUnixTimeMilliseconds() - start;
                        if (elaspsed > ms) {
                            break;
                        }

                        if (opCount % 1000 == 0) {
                            iterateTreeOrder(rand, tree, ref opCount);
                        }

                        if (DateTimeOffset.Now.ToUnixTimeMilliseconds() - timeOfLastPing > 1000) {
                            Console.WriteLine(".");
                            timeOfLastPing = DateTimeOffset.Now.ToUnixTimeMilliseconds();
                            pingTimes[id].Add(timeOfLastPing);
                        }
                    }

                    Console.WriteLine("Finished rand-read-write: [k:" + k + "] [" + pairs.Count + " items ] [" + opCount + " ops].");

                });
                t.Start();
                threads.Add(t);
            }

            foreach (var t in threads)
                t.Join();

            // check for deadlocks (or just really starved periods)
            var now = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            foreach (var l in pingTimes) {
                for (int i = 1; i < l.Count; i++) {
                    var diff = (l[i] - l[i-1]) / 1000.0f;
                    Test.Assert(diff < 10.0f);
                }
                Test.Assert((now - l[l.Count - 1]) / 1000.0f < 10.0f);
            }
        }

        public void timeout_test(int k, List<ValueTuple<K, V>> pairs) {
            var tree = new ConcurrentSortedDictionary<K, V>(pairs);
            foreach (var pair in pairs) {
                foreach (var p in tree.Items(4000)) {
                    if (p.Key.Equals(pair.Item1)) {
                        var pollThread1 = new Thread(() => {
                            Test.AssertEqual(RemoveResult.timedOut, tree.TryRemove(pair.Item1, 1));
                            Test.AssertEqual(InsertResult.timedOut, tree.TryAdd(pair.Item1, pair.Item2, 1));
                            Test.AssertEqual(InsertResult.timedOut, tree.AddOrUpdate(pair.Item1, pair.Item2, 1));
                            V val;
                            Test.AssertEqual(InsertResult.timedOut, tree.GetOrAdd(pair.Item1, pair.Item2, 1, out val));
                        });
                        pollThread1.Start();
                        if (!pollThread1.Join(4000)) { throw new TimeoutException(); }
                    }
                }
                tree.AssertTreeState(pairs.Count);
            }
        }

        public void rand_add_remove_parity_test(int k, List<ValueTuple<K, V>> pairs, int ms = 60000, int nThreads = 32, bool alwaysAssertTreeState = false) {
            var tree = new ConcurrentSortedDictionary<K, V>(k);
            var dict = new ConcurrentDictionary<K, V>();
            var rand = new Random(k * pairs.Count/2);
            var start = DateTimeOffset.Now.ToUnixTimeMilliseconds();

            // no deadlock testing on the tests that use barriers because they require all threads to be running...
            // otherwise it will never finish (:

            int index = 0;

            var sanity = new ConcurrentDictionary<K, V>();
            var barrier = new Barrier(nThreads);

            var threads = new List<Thread>();
            for (int i = 0; i < nThreads; i++) {

                var newList = new List<ValueTuple<K, V>>();
                int stop = (i + 1) * (pairs.Count / nThreads);
                while (index < stop) {
                    newList.Add(pairs[index]);
                    index++;
                }

                var t = new Thread(() => {

                    long opCount = 0;
                    long timeOfLastPing = 0;

                    foreach (var p in newList) {
                        if (sanity.ContainsKey(p.Item1))
                            throw new Exception("Must have unique lists");
                        Test.Assert(sanity.TryAdd(p.Item1, p.Item2));
                    }

                    while (true) {

                        // Try add rand.. try remove rand
                        var nextPair = newList[rand.Next() % newList.Count];
                        bool added = true;
                        if (rand.Next() % 5 == 0) {
                            tree.AddOrUpdate(nextPair.Item1, nextPair.Item2);
                        } else {
                            added = tree.TryAdd(nextPair.Item1, nextPair.Item2);
                        }

                        if (added) {
                            Test.Assert(tree.ContainsKey(nextPair.Item1));
                            dict.TryAdd(nextPair.Item1, nextPair.Item2);
                            opCount++;
                        }

                        nextPair = newList[rand.Next() % newList.Count];
                        bool removed = tree.TryRemove(nextPair.Item1);

                        if (removed) {
                            Test.Assert(!tree.ContainsKey(nextPair.Item1));
                            Test.Assert(dict.TryRemove(new KeyValuePair<K,V>(nextPair.Item1, nextPair.Item2)));
                            opCount++;
                        }

                        opCount += 2;

                        var elaspsed = DateTimeOffset.Now.ToUnixTimeMilliseconds() - start;
                        if (elaspsed > ms) {
                            break;
                        }

                        if (opCount % 1000 == 0) {
                            iterateTreeOrder(rand, tree, ref opCount);
                        }

                        // every 1 second, check entire collection
                        if (DateTimeOffset.Now.ToUnixTimeMilliseconds() - timeOfLastPing > 1000) {
                            Console.WriteLine(".");
                            timeOfLastPing = DateTimeOffset.Now.ToUnixTimeMilliseconds();

                            barrier.SignalAndWait();

                            var l1 = tree.ToList();
                            var l2 = dict.OrderBy(x => x.Key).ToList();

                            // check forward
                            Test.Assert(l1.Count == l2.Count);
                            for (int i = 0; i < l1.Count; i++) {
                                Test.AssertEqual(l1[i].Key, l2[i].Key);
                                Test.AssertEqual(l1[i].Value, l2[i].Value);
                            }
                            // check reversed
                            l1 = tree.Reversed().ToList();
                            l2 = l2.OrderBy(x => x.Key).ToList();
                            for (int i = 0; i < l1.Count; i++) {
                                Test.AssertEqual(l1[i].Key, l2[i].Key);
                                Test.AssertEqual(l1[i].Value, l2[i].Value);
                            }

                            barrier.SignalAndWait();
                        }
                    }

                    Console.WriteLine("Finished rand-read-write: [k:" + k + "] [" + pairs.Count + " items ] [" + opCount + " ops].");

                });
                t.Start();
                threads.Add(t);
            }

            foreach (var t in threads)
                t.Join();
        }
    }
    

    public void run() {
        // timeout test
        {
            int count = 100;
            var intrange = Enumerable.Range(1, count);
            // int int tests
            var intint = new TypedTest<int, int>();
            var intint_pairs = intrange
                .Select(x => new ValueTuple<int, int>(x, -x))
                .ToList();
            intint.timeout_test(3, intint_pairs);
        }
        // linked list lock test
        {
            ConcurrentSortedDictionary<int, int>.LockTest(ms: 60000);
        }
        //parity test
        {
            int count = 20 * 32;
            var intrange = Enumerable.Range(1, count);
            // int int tests
            var intint = new TypedTest<int, int>();
            var intint_pairs = intrange
                .Select(x => new ValueTuple<int, int>(x, -x))
                .ToList();
            intint.rand_add_remove_parity_test(32, intint_pairs, ms: 60000);
        }
        // small tree cycle test
        {
            int count = 20 * 32;
            var intrange = Enumerable.Range(1, count);
            // int int tests
            var intint = new TypedTest<int, int>();
            var intint_pairs = intrange
                .Select(x => new ValueTuple<int, int>(x, -x))
                .ToList();
            intint.rand_add_remove_cycle_test(32, intint_pairs, ms: 120000);
        }
        // small tree test
        {
            int count = 20 * 32;
            var intrange = Enumerable.Range(1, count);
            // int int tests
            var intint = new TypedTest<int, int>();
            var intint_pairs = intrange
                .Select(x => new ValueTuple<int, int>(x, -x))
                .ToList();
            intint.rand_add_remove_test(32, intint_pairs, ms: 60000);
            intint.rand_add_remove_iterator_test(32, intint_pairs, ms: 60000);
        }
        // Big tree test
        {
            int count = 1000000 * 32;
            var intrange = Enumerable.Range(1, count);
            // int int tests
            var intint = new TypedTest<int, int>();
            var intint_pairs = intrange
                .Select(x => new ValueTuple<int, int>(x, -x))
                .ToList();
            //intint.rand_add_remove_test(32, intint_pairs);
            intint.rand_add_remove_iterator_test(32, intint_pairs);
        }
    }
}

#nullable restore

#endif

