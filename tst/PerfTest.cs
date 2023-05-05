#if DEBUG

namespace System.Collections.Concurrent.Extended;

public class PerfTest {

    private class TypedTest<K, V> where K: IComparable<K> {
        public long rand_add_remove_test_time(int k, List<ValueTuple<K, V>> pairs, ConcurrentSortedDictionary<long, string> times, int ops = 100000) {
            var tree = new ConcurrentSortedDictionary<K, V>(k);
            var rand = new Random(k * pairs.Count/2);
            var randPairs = pairs.OrderBy(pair => rand.Next()).ToList();
            var start = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            int opCount = 0;
            while (true) {

                // Try add rand.. try remove rand
                var nextPair = randPairs[rand.Next() % randPairs.Count];
                tree.TryAdd(nextPair.Item1, nextPair.Item2);
                nextPair = randPairs[rand.Next() % randPairs.Count];
                tree.TryRemove(nextPair.Item1);

                opCount += 2;

                if (opCount > ops) {
                    break;
                }
            }
            var elaspsedBPlusTree = DateTimeOffset.Now.ToUnixTimeMilliseconds() - start;

            while (!times.TryAdd(elaspsedBPlusTree, "k:" + k.ToString() + "-count:" + pairs.Count + "-key:" + typeof(K).Name + "-value:" + typeof(V).Name)) {
                elaspsedBPlusTree++;
            }
            return elaspsedBPlusTree;
        }

        public long rand_add_remove_test_time_sortedDict(List<ValueTuple<K, V>> pairs, ConcurrentSortedDictionary<long, string> times, int ops = 100000) {
            var sortedDict = new SortedDictionary<K, V>();
            var rand = new Random(pairs.Count/2);
            var randPairs = pairs.OrderBy(pair => rand.Next()).ToList();
            var start = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            var opCount = 0;
            while (true) {

                // Try add rand.. try remove rand
                var nextPair = randPairs[rand.Next() % randPairs.Count];
                sortedDict.TryAdd(nextPair.Item1, nextPair.Item2);
                nextPair = randPairs[rand.Next() % randPairs.Count];
                sortedDict.Remove(nextPair.Item1);

                opCount += 2;

                if (opCount > ops) {
                    break;
                }
            }
            var elaspsedSortedDict = DateTimeOffset.Now.ToUnixTimeMilliseconds() - start;
            while (!times.TryAdd(elaspsedSortedDict, "sortedDict"+ "-count:" + pairs.Count + "-key:" + typeof(K).Name + "-value:" + typeof(V).Name)) {
                elaspsedSortedDict++;
            }
            return elaspsedSortedDict;
        }
    }

    public void run() {
        var times = new ConcurrentSortedDictionary<long, string>();
        foreach (var k in Test.K_Range_Small) {
            foreach (var count in Test.Item_Count_Large) {
                var intrange = Enumerable.Range(1, count);
                // int int tests
                var intint = new TypedTest<int, int>();
                var intint_pairs = intrange
                    .Select(x => new ValueTuple<int, int>(x, -x))
                    .ToList();
                intint.rand_add_remove_test_time(k, intint_pairs, times);
                Console.WriteLine(".");
                var stringstring = new TypedTest<string, string>();
                var stringstring_pairs = intrange
                    .Select(x => new ValueTuple<string, string>(x.ToString(), (-x).ToString()))
                    .ToList();
                stringstring.rand_add_remove_test_time(k, stringstring_pairs, times);
                Console.WriteLine(".");
                var structclass = new TypedTest<CustomStruct, CustomClass>();
                var structclass_pairs = intrange
                    .Select(x => new ValueTuple<CustomStruct,CustomClass>(new CustomStruct(x), new CustomClass(-x)))
                    .ToList();
                structclass.rand_add_remove_test_time(k, structclass_pairs, times);
                Console.WriteLine(".");
                var classstruct = new TypedTest<CustomClass, CustomStruct>();
                var classstruct_pairs = intrange
                    .Select(x => new ValueTuple<CustomClass, CustomStruct>(new CustomClass(x), new CustomStruct(-x)))
                    .ToList();
                classstruct.rand_add_remove_test_time(k, classstruct_pairs, times);
                Console.WriteLine(".");
            }
        }

        foreach (var count in Test.Item_Count_Large)
        {
            var intrange = Enumerable.Range(1, count);
            // int int tests
            var intint = new TypedTest<int, int>();
            var intint_pairs = intrange
                .Select(x => new ValueTuple<int, int>(x, -x))
                .ToList();
            intint.rand_add_remove_test_time_sortedDict(intint_pairs, times);
            Console.WriteLine(".");
            var stringstring = new TypedTest<string, string>();
            var stringstring_pairs = intrange
                .Select(x => new ValueTuple<string, string>(x.ToString(), (-x).ToString()))
                .ToList();
            stringstring.rand_add_remove_test_time_sortedDict(stringstring_pairs, times);
            Console.WriteLine(".");
            var structclass = new TypedTest<CustomStruct, CustomClass>();
            var structclass_pairs = intrange
                .Select(x => new ValueTuple<CustomStruct,CustomClass>(new CustomStruct(x), new CustomClass(-x)))
                .ToList();
            structclass.rand_add_remove_test_time_sortedDict(structclass_pairs, times);
            Console.WriteLine(".");
            var classstruct = new TypedTest<CustomClass, CustomStruct>();
            var classstruct_pairs = intrange
                .Select(x => new ValueTuple<CustomClass, CustomStruct>(new CustomClass(x), new CustomStruct(-x)))
                .ToList();
            classstruct.rand_add_remove_test_time_sortedDict(classstruct_pairs, times);
            Console.WriteLine(".");
        }
        foreach (var count in Test.Item_Count_Large) {
            Console.WriteLine();Console.WriteLine();
            foreach (var pair in times) {
                if (pair.Value.Contains(count.ToString() + "-") && pair.Value.Contains("key:Int32"))
                    Console.WriteLine(pair.Value + ": " + pair.Key + " ms");
            }
            Console.WriteLine();Console.WriteLine();
            foreach (var pair in times) {
                if (pair.Value.Contains(count.ToString() + "-") && pair.Value.Contains("key:String"))
                    Console.WriteLine(pair.Value + ": " + pair.Key + " ms");
            }
            Console.WriteLine();Console.WriteLine();
            foreach (var pair in times) {
                if (pair.Value.Contains(count.ToString() + "-") && pair.Value.Contains("key:CustomStruct"))
                    Console.WriteLine(pair.Value + ": " + pair.Key + " ms");
            }
            Console.WriteLine();Console.WriteLine();
            foreach (var pair in times) {
                if (pair.Value.Contains(count.ToString() + "-") && pair.Value.Contains("key:CustomClass"))
                    Console.WriteLine(pair.Value + ": " + pair.Key + " ms");
            }
            Console.WriteLine();Console.WriteLine();
        }
    }
}

#endif
