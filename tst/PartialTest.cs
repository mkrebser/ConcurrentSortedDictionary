
#if DEBUG

#define ConcurrentSortedDictionary_DEBUG
#nullable disable

namespace System.Collections.Concurrent.Extended;

// Not using any mocking libraries, want to keep this solution 0 dependencies, so just creating partial class tests & compiling them out

public partial class ConcurrentSortedDictionary<Key, Value> where Key: IComparable<Key> {

    public static void LockTest(int ms = 60000) {
        ConcurrentKTreeNode<Key, Value>.LockTest(ms);
    }

    // Note** The test assumes that the user is not putting default(K), default(V) into the tree
    // these might be valid (eg for a struct)- but for test purposes it is assumed these specific values aren't here
    public void AssertTreeState(int numItems) {
        Test.Assert(this.Count == numItems);

        int actualItemCount = 0, minLeafDepth = int.MaxValue, maxLeafDepth = 0;
        HashSet<Key> keySet = new HashSet<Key>(); Key prev = default(Key); bool setPrev = false;
        List<Key> depthFirstKeys = new List<Key>();
        ConcurrentKTreeNode<Key, Value> prevNode = null, nextNode = null;
        this._root.assertStateAndCount(0, ref actualItemCount, ref minLeafDepth, ref maxLeafDepth, keySet, depthFirstKeys,
            ref prev, ref setPrev, ref prevNode, ref nextNode);
        Test.Assert(keySet.Count == actualItemCount); // assert global uniqueness
        Test.Assert(numItems == actualItemCount);
        Test.Assert(minLeafDepth == maxLeafDepth); // all leafs should be same depth
        Test.Assert(this._root.isLeaf || this._root.Count > 1); // can only have 1 child in leaf case
    }

    public void AssertTreeRoot(int numItems) {
        Test.Assert(this._root.isLeaf || this._root.Count > 1); // can only have 1 child in leaf case
        Test.Assert(this.Count == numItems);
    }

    private partial class ConcurrentKTreeNode<K, V> where K: IComparable<K> {

        public static void LockTest(int ms = 60000) {
            LeafSiblingNodes.LockTest(ms);
        }

        void assertArrayState<VType>(NodeData<K, VType>[] array, HashSet<K> allKeysSet, List<K> depthFirstKeys,
            bool isLeaf, ref K prev, ref bool setPrev,
            ref ConcurrentKTreeNode<K, V> prevLeaf, ref ConcurrentKTreeNode<K, V> nextLeaf
        ) {

            // Verify leaf refs
            if (this.isLeaf) {
                if (setPrev) {
                    Test.Assert(ReferenceEquals(prevLeaf, this.siblings.Prev));
                    Test.Assert(ReferenceEquals(this, nextLeaf));
                }
                prevLeaf = this;
                nextLeaf = this.siblings.Next;
            }

            // Assert count by checking for default...
            // the test assumes default values aren't used as valid keys only for test purposes
            HashSet<K> keySet = new HashSet<K>();
            for (int i = 0; i < this.Count; i++) {
                if (i != 0) Test.AssertNotEqual(default(K), array[i].key);
                else if (!isLeaf) Test.AssertEqual(default(K), array[i].key);
                Test.AssertNotEqual(default(VType), array[i].value);
                keySet.Add(array[i].key);
                if (allKeysSet != null) {
                    allKeysSet.Add(array[i].key);
                }
                if (isLeaf) {
                    if (!setPrev) {
                        prev = array[i].key;
                        setPrev = true;
                    } else {
                        Test.Assert(array[i].key.CompareTo(prev) > 0); // leaf values should always be globally increasing
                        prev = array[i].key;
                    }
                    depthFirstKeys.Add(array[i].key);
                }
            }
            Test.Assert(keySet.Count == this.Count); // assert all keys unique
            for (int i= this.Count; i < array.Length; i++) {
                Test.AssertEqual(default(K), array[i].key);
                Test.AssertEqual(default(VType), array[i].value);
            }
            for (int i = 0; i < this.Count - 1; i++) {
                Test.AssertLessThan(array[i].key, array[i + 1].key); // assert ordering
            }
        }

        public void assertStateAndCount(int depth, ref int itemCount, ref int minLeafDepth, ref int maxLeafDepth,
            HashSet<K> allKeysSet, List<K> depthFirstKeys, ref K prev, ref bool setPrev, ref ConcurrentKTreeNode<K, V> prevNode, ref ConcurrentKTreeNode<K, V> nextNode
        ) {
            if (!this.isRoot) Test.Assert(!this.canSplit() && !this.canMerge()); // all nodes should have appriate number of children (between k/2 and k-1)
            else Test.Assert(!this.canSplit());

            Test.Assert(this._rwLock.IsReadLockHeld == false && this._rwLock.IsWriteLockHeld == false && this._rwLock.IsUpgradeableReadLockHeld == false);

            if (this.isLeaf) {
                Test.AssertEqual(null, this._children);
                assertArrayState(this._values, allKeysSet, depthFirstKeys, true, ref prev, ref setPrev, ref prevNode, ref nextNode);
                itemCount += this.Count;
                minLeafDepth = Math.Min(minLeafDepth, depth);
                maxLeafDepth = Math.Max(maxLeafDepth, depth);
            } else {
                Test.AssertEqual(null, this._values);
                assertArrayState(this._children, null, depthFirstKeys, false, ref prev, ref setPrev, ref prevNode, ref nextNode);

                for (int i = 0; i < this.Count; i++) {
                    this._children[i].value.assertStateAndCount(depth + 1, ref itemCount, ref minLeafDepth, ref maxLeafDepth, allKeysSet,
                        depthFirstKeys, ref prev, ref setPrev, ref prevNode, ref nextNode);
                    Test.Assert(ReferenceEquals(this._children[i].value.Parent, this)); // assert correct parents
                    Test.Assert(i == 0 || this._children[i].value.MinTestKey.CompareTo(this._children[i].key) >= 0);
                }
            }
        }

        private K MinTestKey { get { return this.isLeaf ? this._values[0].key : this._children[1].key; }}

        #if ConcurrentSortedDictionary_DEBUG

        private int assertWriterLock(int version = -1, bool beginWrite = false) {
            assertWriterLockHeld();
            if (beginWrite) {
                return Interlocked.Increment(ref this._version);
            } else {
                Test.Assert(_version == version);
                return version;
            }
        }
        public void assertWriterLockHeld() {
            Test.Assert(this._rwLock.IsWriteLockHeld);
            Test.Assert(!this._rwLock.IsReadLockHeld);
            Test.Assert(!this._rwLock.IsUpgradeableReadLockHeld);
        }
        public void assertRootWriteLockHeld(ConcurrentSortedDictionary<K, V> tree) {
            Test.Assert(tree._rootLock.IsWriteLockHeld);
            Test.Assert(!tree._rootLock.IsReadLockHeld);
            Test.Assert(!tree._rootLock.IsUpgradeableReadLockHeld);
        }

        private int assertLatchLock(ref Latch<K, V> latch,  int version = -1, bool beginRead = false) {
            if (latch.isReadAccess || (latch.assumeLeafIsSafe && !this.isLeaf)) {
                Test.Assert(!this._rwLock.IsWriteLockHeld);
                Test.Assert(this._rwLock.IsReadLockHeld);
                Test.Assert(!this._rwLock.IsUpgradeableReadLockHeld);
            } else {
                Test.Assert(this._rwLock.IsWriteLockHeld);
                Test.Assert(!this._rwLock.IsReadLockHeld);
                Test.Assert(!this._rwLock.IsUpgradeableReadLockHeld);
            }

            if (beginRead) {
                return this._version;
            } else {
                Test.Assert(_version == version);
                return version;
            }
        }

        private int assertReadLock(int version = -1, bool beginRead = false) {
            Test.Assert(!this._rwLock.IsWriteLockHeld);
            Test.Assert(this._rwLock.IsReadLockHeld);
            Test.Assert(!this._rwLock.IsUpgradeableReadLockHeld);

            if (beginRead) {
                return this._version;
            } else {
                Test.Assert(_version == version);
                return version;
            }
        }

        private partial struct LeafSiblingNodes {

            public static void LockTest(int ms = 60000) {
                int nThreads = 32;
                int listLength = 10;
                int minLength = 3;
                Test.Assert(listLength > minLength);
                ConcurrentKTreeNode<K, V> init_node = new ConcurrentKTreeNode<K, V>(3, null, true);
                {
                    var next = init_node;
                    for (int i = 0; i < listLength; i++) {
                        var newNode = new ConcurrentKTreeNode<K, V>(3, null, true);
                        next.siblings.next = newNode;
                        newNode.siblings.prev = next;
                        next = newNode;
                    }
                }
                Console.WriteLine("Starting " + ms + "ms lock test");
                var threads = new List<Thread>();
                for (int i = 0; i < nThreads; i++) {
                    var t = new Thread(() => {

                        var start = DateTimeOffset.Now.ToUnixTimeMilliseconds();
                        long timeOfLastPing = 0;
                        var r = new Random();
                        while (true) {
                            // Try add...
                            {
                                // get random index
                                var index = r.Next() % listLength + 1;
                                var i = 1;
                                var node = init_node.siblings.next;
                                while (node != null) {
                                    if (i >= index) break;
                                    node = node.siblings.next;
                                    i++;
                                }
                                if (node == null) continue;

                                // Try adding a new node
                                var newNode = new ConcurrentKTreeNode<K, V>(3, null, true);
                                AtomicUpdateSplitNodes(in node, in newNode);
                            }

                            // Try deleting an existing node
                            {
                                var deleted = true;
                                while (deleted) {
                                    // get random index                                        
                                    var index = r.Next() % listLength + minLength;
                                    var i = 1;
                                    var node = init_node.siblings.next;
                                    while (node != null) {
                                        if (i >= index) break;
                                        node = node.siblings.next;
                                        i++;
                                    }
                                    if (node != null) {
                                        // Try removing node
                                        AtomicUpdateMergeNodes(in node);
                                    } else {
                                        break;
                                    }
                                }
                            }

                            var elaspsed = DateTimeOffset.Now.ToUnixTimeMilliseconds() - start;
                            if (elaspsed > ms) {
                                break;
                            }
                            if (DateTimeOffset.Now.ToUnixTimeMilliseconds() - timeOfLastPing > 1000) {
                                Console.WriteLine(".");
                                timeOfLastPing = DateTimeOffset.Now.ToUnixTimeMilliseconds();
                                var count = 0;
                                var next = init_node;
                                while (next != null) { next = next.siblings.next; count++; }
                                Test.Assert(count >= minLength);
                            }
                        }
                    });
                    t.Start();
                    threads.Add(t);
                }

                foreach (var t in threads)
                    t.Join();
            }

            private static void AssertMutexHeld(ref LeafSiblingNodes nodes) {
                int id = System.Environment.CurrentManagedThreadId;
                Test.Assert(nodes._mutex == id);
            }
            private static void assertStartWriter(ConcurrentKTreeNode<K, V> node1, ConcurrentKTreeNode<K, V> node2,
                ConcurrentKTreeNode<K, V> node3, out int version1, out int version2, out int version3
            ) {
                version1 = version2 = version3 = -1;
                if (!ReferenceEquals(null, node1)) {
                    AssertMutexHeld(ref node1.siblings);
                    version1 = Interlocked.Increment(ref node1.siblings._version);
                }
                if (!ReferenceEquals(null, node2)) {
                    AssertMutexHeld(ref node2.siblings);
                    version2 = Interlocked.Increment(ref node2.siblings._version);
                }
                if (!ReferenceEquals(null, node3)) {
                    AssertMutexHeld(ref node3.siblings);
                    version3 = Interlocked.Increment(ref node3.siblings._version);
                }
            }
            private static void assertEndWriter(ConcurrentKTreeNode<K, V> node1, ConcurrentKTreeNode<K, V> node2,
                ConcurrentKTreeNode<K, V> node3, int version1, int version2, int version3
            ) {
                if (!ReferenceEquals(null, node1)) {
                    AssertMutexHeld(ref node1.siblings);
                    Test.Assert(node1.siblings._version == version1);
                }
                if (!ReferenceEquals(null, node2)) {
                    AssertMutexHeld(ref node2.siblings);
                    Test.Assert(node2.siblings._version == version2);
                }
                if (!ReferenceEquals(null, node3)) {
                    AssertMutexHeld(ref node3.siblings);
                    Test.Assert(node3.siblings._version == version3);
                }
            }
        }

        #endif
    }
}

#nullable restore

#endif
