
#if DEBUG

namespace System.Collections.Concurrent.Extended;


// Not using any mocking libraries, want to keep this solution 0 dependencies, so just creating partial class tests & compiling them out

public partial class ConcurrentSortedDictionary<Key, Value> where Key: IComparable<Key> {

    // Note** The test assumes that the user is not putting default(K), default(V) into the tree
    // these might be valid (eg for a struct)- but for test purposes it is assumed these specific values aren't here
    public void AssertTreeState(int numItems) {
        Test.Assert(this.Count == numItems);

        int actualItemCount = 0, minLeafDepth = int.MaxValue, maxLeafDepth = 0;
        this._root.assertStateAndCount(0, ref actualItemCount, ref minLeafDepth, ref maxLeafDepth);
        Test.Assert(numItems == actualItemCount);
        Test.Assert(minLeafDepth == maxLeafDepth); // all leafs should be same depth
    }

    private partial class ConcurrentKTreeNode<K, V> where K: IComparable<K> {

        void assertArrayState<VType>(NodeData<K, VType>[] array) {
            // Assert count by checking for default...
            // the test assumes default values aren't used as valid keys only for test purposes
            HashSet<K> keySet = new HashSet<K>();
            for (int i = 0; i < this.Count; i++) {
                Test.AssertNotEqual(default(K), array[i].key);
                Test.AssertNotEqual(default(VType), array[i].value);
                keySet.Add(array[i].key);
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

        public void assertStateAndCount(int depth, ref int itemCount, ref int minLeafDepth, ref int maxLeafDepth) {
            if (!this.isRoot) Test.Assert(!this.canSplit() && !this.canMerge()); // all nodes should have appriate number of children (between k/2 and k-1)
            else Test.Assert(!this.canSplit());

            Test.Assert(this._rwLock.IsReadLockHeld == false && this._rwLock.IsWriteLockHeld == false && this._rwLock.IsUpgradeableReadLockHeld == false);

            if (this.isLeaf) {
                Test.AssertEqual(null, this._children);
                assertArrayState(this._values);
                itemCount += this.Count;
                minLeafDepth = Math.Min(minLeafDepth, depth);
                maxLeafDepth = Math.Max(maxLeafDepth, depth);
            } else {
                Test.AssertEqual(null, this._values);
                assertArrayState(this._children);
                for (int i = 0; i < this.Count; i++) {
                    this._children[i].value.assertStateAndCount(depth + 1, ref itemCount, ref minLeafDepth, ref maxLeafDepth);
                    Test.Assert(ReferenceEquals(this._children[i].value.Parent, this)); // assert correct parents
                }
            }
        }
    }

}

#endif