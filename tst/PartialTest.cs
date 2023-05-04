
#if DEBUG

namespace System.Collections.Concurrent.Extended;

#nullable disable

// Not using any mocking libraries, want to keep this solution 0 dependencies, so just creating partial class tests & compiling them out

public partial class ConcurrentSortedDictionary<Key, Value> where Key: IComparable<Key> {

    // Note** The test assumes that the user is not putting default(K), default(V) into the tree
    // these might be valid (eg for a struct)- but for test purposes it is assumed these specific values aren't here
    public void AssertTreeState(int numItems) {
        Test.Assert(this.Count == numItems);

        int actualItemCount = 0, minLeafDepth = int.MaxValue, maxLeafDepth = 0;
        HashSet<Key> keySet = new HashSet<Key>(); Key prev = default(Key); bool setPrev = false;
        List<Key> depthFirstKeys = new List<Key>();
        this._root.assertStateAndCount(0, ref actualItemCount, ref minLeafDepth, ref maxLeafDepth, keySet, depthFirstKeys, ref prev, ref setPrev);
        Test.Assert(keySet.Count == actualItemCount); // assert global uniqueness
        Test.Assert(numItems == actualItemCount);
        Test.Assert(minLeafDepth == maxLeafDepth); // all leafs should be same depth
    }

    private partial class ConcurrentKTreeNode<K, V> where K: IComparable<K> {

        void assertArrayState<VType>(NodeData<K, VType>[] array, HashSet<K> allKeysSet, List<K> depthFirstKeys, bool isLeaf, ref K prev, ref bool setPrev) {
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

        public void assertStateAndCount(int depth, ref int itemCount, ref int minLeafDepth, ref int maxLeafDepth, HashSet<K> allKeysSet, List<K> depthFirstKeys, ref K prev, ref bool setPrev) {
            if (!this.isRoot) Test.Assert(!this.canSplit() && !this.canMerge()); // all nodes should have appriate number of children (between k/2 and k-1)
            else Test.Assert(!this.canSplit());

            Test.Assert(this._rwLock.IsReadLockHeld == false && this._rwLock.IsWriteLockHeld == false && this._rwLock.IsUpgradeableReadLockHeld == false);

            if (this.isLeaf) {
                Test.AssertEqual(null, this._children);
                assertArrayState(this._values, allKeysSet, depthFirstKeys, true, ref prev, ref setPrev);
                itemCount += this.Count;
                minLeafDepth = Math.Min(minLeafDepth, depth);
                maxLeafDepth = Math.Max(maxLeafDepth, depth);
            } else {
                Test.AssertEqual(null, this._values);
                assertArrayState(this._children, null, depthFirstKeys, false, ref prev, ref setPrev);

                for (int i = 0; i < this.Count; i++) {
                    this._children[i].value.assertStateAndCount(depth + 1, ref itemCount, ref minLeafDepth, ref maxLeafDepth, allKeysSet, depthFirstKeys, ref prev, ref setPrev);
                    Test.Assert(ReferenceEquals(this._children[i].value.Parent, this)); // assert correct parents
                    Test.Assert(i == 0 || this._children[i].value.MinTestKey.CompareTo(this._children[i].key) >= 0);
                }
            }
        }

        private K MinTestKey { get { return this.isLeaf ? this._values[0].key : this._children[1].key; }}
    }
}

#nullable restore

#endif
