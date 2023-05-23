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

            int index = 0;

            var sanity = new ConcurrentDictionary<K, V>();

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

        public void rand_add_remove_iterator_test(int k, List<ValueTuple<K, V>> pairs, int ms = 900000, int nThreads = 32, bool alwaysAssertTreeState = false) {
            var tree = new ConcurrentSortedDictionary<K, V>(k);
            var rand = new Random(k * pairs.Count/2);
            var start = DateTimeOffset.Now.ToUnixTimeMilliseconds();

            int index = 0;

            var sanity = new ConcurrentDictionary<K, V>();

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

                        K prev = default(K);
                        bool prevInit = false;
                        if (opCount % 1000 == 0) {
                            foreach (var pair in tree) {
                               opCount++;
                               if (prevInit) {
                                    Test.Assert(prev.CompareTo(pair.Key) < 0);
                               }
                               prev = pair.Key;
                               prevInit = true;
                            }
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
    }

    public void run() {
        //TODO: Deadlock test
        // iterator + reversed it test
        //concurrent dict parity test
        {
            ConcurrentSortedDictionary<int, int>.LockTest(ms: 60000);
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
            intint.rand_add_remove_test(32, intint_pairs);
            intint.rand_add_remove_iterator_test(32, intint_pairs);
        }
    }
}

#nullable restore

#endif

