/*
MIT License

Copyright (c) 2023 Matthew Krebser (https://github.com/mkrebser)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

// Use ConcurrentSortedDictionary_DEBUG for all debugging assertions incase someone wants to change it...
//#define ConcurrentSortedDictionary_DEBUG



using System.Runtime.CompilerServices;

// Put this in the concurrent namespace but with 'Extended'
namespace System.Collections.Concurrent.Extended;

public enum InsertResult {
    /// <summary>
    /// Operation completed successfully.
    /// </summary>
    success = 0,
    /// <summary>
    /// Value was not inserted because it already exists.
    /// </summary>
    alreadyExists = 1,
    /// <summary>
    /// Value ws not inserted due to timeout.
    /// </summary>
    timedOut = 2
}

public enum RemoveResult {
    /// <summary>
    /// Successfully deleted.
    /// </summary>
    success = 0,
    /// <summary>
    /// key was not found. No deletion occured.
    /// </summary>
    notFound = 1,
    /// <summary>
    /// Value ws not deleted due to timeout.
    /// </summary>
    timedOut = 2,
}

public enum SearchResult {
    /// <summary>
    /// Successfully found key.
    /// </summary>
    success = 0,
    /// <summary>
    /// key was not found. 
    /// </summary>
    notFound = 1,
    /// <summary>
    /// Couldn't complete search due to timeout.
    /// </summary>
    timedOut = 2,
}

// not using the Nullable<T> notation (eg myType? ) because it adds a small overhead to everything
// eg, Nullable<int>[] vs int[] -> Nullable<int> will take up double the memory on many x64 systems due to extra boolean flag
#nullable disable

// Locking Scheme: Multiple Readers & Single Writer
// However, locks are at a granularity of each node- so multiple parts of the tree can be written concurrently
// 
// Read Access Scheme: While traversing down the tree hierarchy, reads will aquire the lock of the next
//                     node they move to before releasing the lock of their current node. This guarentees
//                     that no writers will skip ahead of any readers. It also guarentees readers will not
//                     skip ahead of any writers traversing down.
//
// Write Access Scheme: Writers use 'latching'. While traversing downwards.. They will only release a parent node
//                      if an insert/delete will not cause a spit or merge. (This means that the insert/delete won't
//                      need to traverse up the B+Tree)
//                      
// Locks: All locks used are ReaderWriterSlim locks which will prioritize writers
// https://learn.microsoft.com/en-us/dotnet/api/system.threading.readerwriterlockslim?view=net-7.0

// An excellent lecture on B+Trees:
// https://courses.cs.washington.edu/courses/cse332/20au/lectures/cse332-20au-lec09-BTrees.pdf

// slides that go over latching (latch-crabbing) https://15721.courses.cs.cmu.edu/spring2017/slides/06-latching.pdf

/// <summary>
/// Implementation of a concurrent B+Tree. https://en.wikipedia.org/wiki/B+tree#
/// </summary>
public partial class ConcurrentSortedDictionary<Key, Value> : IEnumerable<KeyValuePair<Key, Value>> where Key: IComparable<Key> {
    private volatile ConcurrentKTreeNode<Key, Value> _root;

    public void setRoot(object o) {
        this._root = (ConcurrentKTreeNode<Key, Value>)o;
    }

    private readonly ReaderWriterLockSlim _rootLock;
    private volatile int _count;
    /// <summary>
    /// Number of key-value pairs in the collection. Value may be stale in concurrent access.
    /// </summary>
    public int Count { get { return _count; } }
    /// <summary>
    /// Is collection empty? Value may be stale in concurrent access.
    /// </summary>
    public bool IsEmpty { get { return this.Count <= 0; }}

    private volatile int _depth;
    /// <summary>
    /// Approximate depth of the search tree. Value may be stale in concurrent access.
    /// </summary>
    public int Depth { get { return _depth + 1; } } // A tree with only root node has _depth = 0

    /// <summary>
    /// Width of each node in the tree.
    /// </summary>
    public readonly int k;

    /// <summary>
    /// Create a new instance of ConcurrentSortedDictionary
    /// </summary>
    /// <param name="k"> Number of children per node. </param>
    public ConcurrentSortedDictionary(int k = 32) {
        if (k < 3) // Don't allow '2', it creates potentially many leafs with only 1 item due to b+ tree requirements
            throw new ArgumentException("Invalid k specified");
        _rootLock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
        _root = new ConcurrentKTreeNode<Key, Value>(k, isLeaf: true);
        _count = 0;
        _depth = 0;
        this.k = k;
    }

    void assertTimeoutArg(in int timeoutMs) {
        if (timeoutMs < 0)
            throw new ArgumentException("Timeout cannot be negative!");
    }
    InsertResult ToInsertResult(in ConcurrentTreeResult_Extended result) {
        if (result == ConcurrentTreeResult_Extended.success) {
            return InsertResult.success;
        } else if (result == ConcurrentTreeResult_Extended.alreadyExists) {
            return InsertResult.alreadyExists;
        } else {
            return InsertResult.timedOut;
        }
    }
    RemoveResult ToRemoveResult(in ConcurrentTreeResult_Extended result) {
        if (result == ConcurrentTreeResult_Extended.success) {
            return RemoveResult.success;
        } else if (result == ConcurrentTreeResult_Extended.notFound) {
            return RemoveResult.notFound;
        } else {
            return RemoveResult.timedOut;
        }
    }

    /// <summary>
    /// Insert a Key-Value pair and overwrite if it already exists.
    /// </summary>
    public InsertResult AddOrUpdate(
        in Key key,
        in Value value,
        in int timeoutMs
    ) {
        assertTimeoutArg(timeoutMs);
        Value retrievedValue;
        return ToInsertResult(writeToTree(in key, in value, in timeoutMs, LatchAccessType.insert, out retrievedValue, true));
    }

    /// <summary>
    /// Insert a Key-Value pair and overwrite if it already exists. Waits forever for mutex.
    /// </summary>
    public void AddOrUpdate(
        in Key key,
        in Value value
    ) {
        Value retrievedValue;
        writeToTree(in key, in value, -1, LatchAccessType.insert, out retrievedValue, true);
    }

    /// <summary>
    /// Insert a Key-Value pair or return the existing pair if key already exists.
    /// </summary>
    public InsertResult TryAdd(
        in Key key,
        in Value value,
        in int timeoutMs
    ) {
        assertTimeoutArg(timeoutMs);
        Value retrievedValue;
        return ToInsertResult(writeToTree(in key, in value, in timeoutMs, LatchAccessType.insertTest, out retrievedValue, false));
    }

    /// <summary>
    /// Insert a Key-Value pair. Return false if not inserted due to existing value.
    /// </summary>
    public bool TryAdd(
        in Key key,
        in Value value
    ) {
        Value retrievedValue;
        return ToInsertResult(writeToTree(in key, in value, -1, LatchAccessType.insertTest, out retrievedValue, false)) == InsertResult.success;
    }

    /// <summary>
    /// Insert a Key-Value pair or output the existing pair if key already exists.
    /// </summary>
    public InsertResult GetOrAdd(
        in Key key,
        in Value value,
        in int timeoutMs,
        out Value retrievedValue
    ) {
        assertTimeoutArg(timeoutMs);
        return ToInsertResult(writeToTree(in key, in value, in timeoutMs, LatchAccessType.insertTest, out retrievedValue, false));
    }

    /// <summary>
    /// Insert a Key-Value pair if it doesn't exist and return the value. If it does exist, the existing value is returned.
    /// </summary>
    public Value GetOrAdd(
        in Key key,
        in Value value
    ) {
        Value retrievedValue;
        writeToTree(in key, in value, -1, LatchAccessType.insertTest, out retrievedValue, false);
        return retrievedValue;
    }

    void tryUpdateDepth(int newSearchDepth) {
        if (newSearchDepth >= 30) {
            throw new ArgumentException("Reached 31 tree limit depth. Only a max of "
                + (int)Math.Pow(this.k, 31) + " items is supported. Increasing 'k' will increase limit.");
        }
        this._depth = newSearchDepth;
    }

    /// <summary>
    /// Perform a insert or delete on the tree depending on the LatchAccessType.
    /// </summary>
    private ConcurrentTreeResult_Extended writeToTree(
        in Key key,
        in Value value,
        in int timeoutMs,
        in LatchAccessType accessType,
        out Value retrievedValue,
        in bool overwrite = false
    ) {
        if (!typeof(Key).IsValueType && ReferenceEquals(null, key)) {
            throw new ArgumentException("Cannot have null key");
        }

        SearchResultInfo<Key, Value> info = default(SearchResultInfo<Key, Value>);
        Value currentValue;
        // Optmistic latching
        var rwLatch = new Latch<Key, Value>(accessType, this._rootLock, assumeLeafIsSafe: true);
        var rwLockBuffer = new LockBuffer2<Key, Value>();
        long startTime = DateTimeOffset.Now.ToUnixTimeMilliseconds();
        int remainingMs = getRemainingMs(in startTime, in timeoutMs);
        var searchOptions = new ConcurrentKTreeNode<Key, Value>.SearchOptions(remainingMs, startTime: startTime);
        // Perform a query to recurse to the deepest node, latching downwards optimistically
        var getResult = ConcurrentKTreeNode<Key, Value>.TryGetValue(in key, out currentValue,
            ref info, ref rwLatch, ref rwLockBuffer, this, searchOptions);
        
        // Timeout!
        if (getResult == ConcurrentTreeResult_Extended.timedOut) {
            retrievedValue = default(Value);
            return ConcurrentTreeResult_Extended.timedOut;
        }

        // If this is a test-before-write- then determine if the test failed
        bool exitOnTest = false;
        if (accessType == LatchAccessType.insertTest) {
            exitOnTest = info.index > -1; // if it is found.. it must have a positive index
        } else if (accessType == LatchAccessType.deleteTest) {
            exitOnTest = info.index < 0; // if it is not found, it will have a negative index
        }

        try {
        // If we were able to optimistally acquire latch... (or test op was successful)
        // The write to tree
            if (getResult != ConcurrentTreeResult_Extended.notSafeToUpdateLeaf || exitOnTest) {
                #if ConcurrentSortedDictionary_DEBUG
                info.node.assertWriterLockHeld();
                #endif

                if (rwLatch.isInsertAccess) {
                    tryUpdateDepth(info.depth);
                    return writeInsertion(in key, in value, in info, in getResult, in overwrite, out retrievedValue, exitOnTest);
                } else {
                    retrievedValue = default(Value);
                    return writeDeletion(in key, in info, in getResult, exitOnTest);
                }
            }
        } finally {
            rwLatch.ExitLatchChain(ref rwLockBuffer);
        }

        // Otherwise, try to acquire write access using a full write latch chain
        // Note* forcing test to false
        var writeLatchAccessType = accessType == LatchAccessType.insertTest ? LatchAccessType.insert : accessType;
        writeLatchAccessType = accessType == LatchAccessType.deleteTest ? LatchAccessType.delete : accessType;
        var writeLatch = new Latch<Key, Value>(writeLatchAccessType , this._rootLock, assumeLeafIsSafe: false);
        var writeLockBuffer = new LockBuffer32<Key, Value>();
        remainingMs = getRemainingMs(in startTime, in timeoutMs);
        searchOptions = new ConcurrentKTreeNode<Key, Value>.SearchOptions(remainingMs, startTime: startTime);
        getResult = ConcurrentKTreeNode<Key, Value>.TryGetValue(in key, out currentValue,
            ref info, ref writeLatch, ref writeLockBuffer, this, searchOptions);

        // Check if timed out...
        if (getResult == ConcurrentTreeResult_Extended.timedOut) {
                retrievedValue = default(Value);
             return ConcurrentTreeResult_Extended.timedOut;
        }

        try {
            #if ConcurrentSortedDictionary_DEBUG
            info.node.assertWriterLockHeld();
            #endif

            if (writeLatch.isInsertAccess) {
                tryUpdateDepth(info.depth);
                return writeInsertion(in key, in value, in info, in getResult, in overwrite, out retrievedValue, false);
            } else {
                retrievedValue = default(Value);
                return writeDeletion(in key, in info, in getResult, false);
            }
        } finally {
            writeLatch.ExitLatchChain(ref writeLockBuffer);
        }
    }

    private ConcurrentTreeResult_Extended writeInsertion(
        in Key key,
        in Value value,
        in SearchResultInfo<Key, Value> info,
        in ConcurrentTreeResult_Extended getResult,
        in bool overwrite,
        out Value retrievedValue,
        in bool exitOnTest
    ) {
        // If the vaue already exists...
        if (getResult == ConcurrentTreeResult_Extended.success || exitOnTest) {
            if (overwrite && !exitOnTest) {
                info.node.SetValue(info.index, in key, in value);
                retrievedValue = value;
                return ConcurrentTreeResult_Extended.success;
            }
            retrievedValue = info.node.GetValue(info.index).value;
            return ConcurrentTreeResult_Extended.alreadyExists;
        }
        info.node.sync_InsertAtThisNode(in key, in value, this);
        Interlocked.Increment(ref this._count); // increase count
        retrievedValue = value;
        return ConcurrentTreeResult_Extended.success;
    }

    private ConcurrentTreeResult_Extended writeDeletion(
        in Key key,
        in SearchResultInfo<Key, Value> info,
        in ConcurrentTreeResult_Extended getResult,
        in bool exitOnTest
    ) {
        if (getResult == ConcurrentTreeResult_Extended.notFound || exitOnTest) {
            return ConcurrentTreeResult_Extended.notFound;
        }
        info.node.sync_DeleteAtThisNode(in key, this);
        Interlocked.Decrement(ref this._count); // decrement count
        return ConcurrentTreeResult_Extended.success;
    }

    /// <summary>
    /// Remove a key-value pair from the tree.
    /// </summary>
    public RemoveResult TryRemove(in Key key, int timeoutMs) {
        assertTimeoutArg(timeoutMs);
        Value v = default(Value);
        return ToRemoveResult(writeToTree(in key, in v, in timeoutMs, LatchAccessType.deleteTest, out v));
    }

    /// <summary>
    /// Remove a key-value pair from the tree. Waits forever until mutex(s) are acquired.
    /// </summary>
    public bool TryRemove(in Key key) {
        Value v = default(Value);
        return ToRemoveResult(writeToTree(in key, in v, -1, LatchAccessType.deleteTest, out v)) == RemoveResult.success;
    }

    /// <summary>
    /// Search for input key and outputs the value. Returns false if not found. Waits forever until search mutex(s) are acquired.
    /// </summary>
    public bool TryGetValue(in Key key, out Value value) {
        return tryGetValue(in key, out value, -1) == SearchResult.success;
    }

    /// <summary>
    /// Search for input key and outputs the value. Returns if it was successful.
    /// </summary>
    public SearchResult TryGetValue(in Key key, out Value value, in int timeoutMs) {
        assertTimeoutArg(timeoutMs);
        return tryGetValue(in key, out value, timeoutMs);
    }

    /// <summary>
    /// Check if the input key is in this collection.
    /// </summary>
    public SearchResult ContainsKey(in Key key, in int timeoutMs) {
        assertTimeoutArg(timeoutMs);
        Value value;
        return tryGetValue(in key, out value, in timeoutMs);
    }
    /// <summary>
    /// Check if the input key is in this collection. Wait forever to acquire mutex(s)
    /// </summary>
    public bool ContainsKey(in Key key) {
        Value value;
        return tryGetValue(in key, out value, -1)== SearchResult.success;
    }

    /// <summary>
    /// Search for input key and output the value.
    /// </summary>
    private SearchResult tryGetValue(in Key key, out Value value, in int timeoutMs = -1) {
        if (!typeof(Key).IsValueType && ReferenceEquals(null, key)) {
            throw new ArgumentException("Cannot have null key");
        }
        SearchResultInfo<Key, Value> searchInfo = default(SearchResultInfo<Key, Value>);
        var searchOptions = new ConcurrentKTreeNode<Key, Value>.SearchOptions(timeoutMs);
        var latch = new Latch<Key, Value> (LatchAccessType.read, this._rootLock);
        var readLockBuffer = new LockBuffer2<Key, Value>();
        var result = ConcurrentKTreeNode<Key, Value>.TryGetValue(in key, out value,
            ref searchInfo, ref latch, ref readLockBuffer, this, searchOptions);
        if (result == ConcurrentTreeResult_Extended.timedOut) {
            return SearchResult.timedOut;
        }
        this._depth = searchInfo.depth; // Note* Int32 read/write is atomic
        return result == ConcurrentTreeResult_Extended.success ?
            SearchResult.success :
            SearchResult.notFound;
    }

    public Value this[in Key key]
    {
        get {
            Value value;
            if (!this.TryGetValue(in key, out value)) {
                throw new ArgumentException("Input key does not exist!");
            }
            return value;
        }
        set {
            this.AddOrUpdate(in key, in value);
        }
    }

    /// <summary>
    /// Can be used to iterate though all items in the Dictionary with optional timeout and subtree depth.
    /// </summary>
    public IEnumerable<KeyValuePair<Key, Value>> Items(int itemTimeoutMs = -1, int subTreeDepth = 2) {
        using (var it = GetEnumerator(itemTimeoutMs, subTreeDepth)) {
            while (it.MoveNext()) {
                yield return it.Current;
            }
        };
    }

    public IEnumerable<Key> Keys { get { foreach (var pair in this) { yield return pair.Key; } } }
    public IEnumerable<Value> Values { get { foreach (var pair in this) { yield return pair.Value; } } }

    public IEnumerator<KeyValuePair<Key, Value>> GetEnumerator() {
        return GetEnumerator(-1, 2);
    }

    public IEnumerator<KeyValuePair<Key, Value>> GetEnumerator(int itemTimeoutMs = -1, int subTreeDepth = 2) {
        return ConcurrentKTreeNode<Key, Value>.AllItems(this, subTreeDepth, itemTimeoutMs).GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator() {
        return this.GetEnumerator();
    }

    public void Clear() {
        clear();
    }
    public bool Clear(int timeoutMs) {
        assertTimeoutArg(timeoutMs);
        return clear();
    }

    private bool clear(int timeoutMs = -1) {
        // Try to enter the root lock
        var latch = new Latch<Key, Value> (LatchAccessType.delete, this._rootLock, assumeLeafIsSafe: false);
        if (!latch.TryEnterRootLock(timeoutMs)) {
            return false;
        }
        try {
            // Make a new root...
            var newRoot = new ConcurrentKTreeNode<Key, Value>(_root.k, parent: null, isLeaf: true);
            this.setRoot(newRoot);
            this._count = 0;
            this._depth = 0;
            return true;
        } finally {
            latch.ExitRootLock();
        }
    }

    /// <summary>
    /// Struct that contains meta data about the TryGetValue search attempt.
    /// </summary>
    private struct SearchResultInfo<K, V> where K : IComparable<K> {
        /// <summary>
        /// Index of found item. -1 if not found.
        /// </summary>
        public int index;
        /// <summary>
        /// Node that the search stopped at.
        /// </summary>
        public ConcurrentKTreeNode<K, V> node;
        /// <summary>
        /// Depth where search stopped
        /// </summary>
        public int depth;
        /// <summary>
        /// Next key of next node
        /// </summary>
        public K nextSubTreeKey;
        /// <summary>
        /// Is there a subtree after this one?
        /// </summary>
        public bool hasNextSubTree;
    }

    /// <summary>
    /// Key-Value pair that is used to store all items in the tree.
    /// </summary>
    private struct NodeData<K, V> where K: IComparable<K> {
        public readonly V value;
        public readonly K key;
        public NodeData(in K key, in V value) {
            this.value = value;
            this.key = key;
        }
    }

    private enum ConcurrentTreeResult_Extended {
        success = 0,
        notFound = 1,
        timedOut = 2,
        alreadyExists = 3,
        notSafeToUpdateLeaf = 4
    }

    private enum LatchAccessType {
        read = 0,
        insert = 1,
        delete = 2,
        insertTest = 3,
        deleteTest = 4
    }

    private enum LatchAccessResult {
        timedOut = 0,
        acquired = 1,
        notSafeToUpdateLeaf = 2,
        // Test Result.. This is returned when it is not safe to update the leaf
        // but we want to retain the lock on the leaf for the purpose of testing if the
        // desired key is present
        notSafeToUpdateLeafTest = 3
    }

    private interface ILockBuffer<K, V> where K: IComparable<K> {
        public ConcurrentKTreeNode<K, V> peek();
        public void push(in ConcurrentKTreeNode<K, V> node);
        public ConcurrentKTreeNode<K, V> pop();
    }

    private struct LockBuffer2<K, V> : ILockBuffer<K, V> where K: IComparable<K> {
        public ConcurrentKTreeNode<K, V> peek() {
            if (!ReferenceEquals(null, r1)) return r1;
            else return r0;
        }
        public void push(in ConcurrentKTreeNode<K, V> node) {
            if (ReferenceEquals(null, r0)) r0 = node;
            else if (ReferenceEquals(null, r1)) r1 = node;
            else throw new ArgumentException("Lock stack is full");
        }
        public ConcurrentKTreeNode<K, V> pop() {
            //Note* pop returns null on empty, this is intentional
            // See Latch.ExitLatchChain- it will just iterate until pop() returns null
            if (!ReferenceEquals(null, r1)) {
                var result = r1;
                r1 = null;
                return result;
            } else {
                var result = r0;
                r0 = null;
                return result;
            }
        }

        private ConcurrentKTreeNode<K, V> r0; private ConcurrentKTreeNode<K, V> r1;
    }

    // Doing this nonsense because c# doesn't allow stacalloc of reference type arrays.
    // -And don't want to force users to use unsafe if they arent compiling with it.
    // Another alternative is to create a pool<buffers> or linked list nodes-
    // but this would potentially create unexpected memory usage by this data structure.
    // This struct should be 256 bytes and is used whenever a write forces changing the tree structure.
    private struct LockBuffer32<K, V> : ILockBuffer<K, V> where K: IComparable<K> {
        public ConcurrentKTreeNode<K, V> peek() {
            if (this.Count <= 0)
                return null;
            return get(this.Count - 1);
        }
        public int Count { get; private set; }
        public void push(in ConcurrentKTreeNode<K, V> node) {
            if (this.Count >= 32)
                throw new ArgumentException("Cannot push, reach lock buffer limit");
            set(this.Count, in node);
            this.Count++;
        }
        public ConcurrentKTreeNode<K, V> pop() {
            //Note* pop returns null on empty, this is intentional
            if (this.Count <= 0)
                return null;
            var topNode = get(this.Count - 1);
            set(this.Count - 1, null);
            this.Count--;
            return topNode;
        }

        private ConcurrentKTreeNode<K, V> r0; private ConcurrentKTreeNode<K, V> r1; private ConcurrentKTreeNode<K, V> r2; private ConcurrentKTreeNode<K, V> r3;
        private ConcurrentKTreeNode<K, V> r4; private ConcurrentKTreeNode<K, V> r5; private ConcurrentKTreeNode<K, V> r6; private ConcurrentKTreeNode<K, V> r7;
        private ConcurrentKTreeNode<K, V> r8; private ConcurrentKTreeNode<K, V> r9; private ConcurrentKTreeNode<K, V> r10; private ConcurrentKTreeNode<K, V> r11;
        private ConcurrentKTreeNode<K, V> r12; private ConcurrentKTreeNode<K, V> r13; private ConcurrentKTreeNode<K, V> r14; private ConcurrentKTreeNode<K, V> r15;
        private ConcurrentKTreeNode<K, V> r16; private ConcurrentKTreeNode<K, V> r17; private ConcurrentKTreeNode<K, V> r18; private ConcurrentKTreeNode<K, V> r19;
        private ConcurrentKTreeNode<K, V> r20; private ConcurrentKTreeNode<K, V> r21; private ConcurrentKTreeNode<K, V> r22; private ConcurrentKTreeNode<K, V> r23;
        private ConcurrentKTreeNode<K, V> r24; private ConcurrentKTreeNode<K, V> r25; private ConcurrentKTreeNode<K, V> r26; private ConcurrentKTreeNode<K, V> r27;
        private ConcurrentKTreeNode<K, V> r28; private ConcurrentKTreeNode<K, V> r29; private ConcurrentKTreeNode<K, V> r30; private ConcurrentKTreeNode<K, V> r31;

        private void set(in int i, in ConcurrentKTreeNode<K, V> value) {
            switch (i) {
                case 0: this.r0 = value; return; case 1: this.r1 = value; return; case 2: this.r2 = value; return; case 3: this.r3 = value; return;
                case 4: this.r4 = value; return; case 5: this.r5 = value; return; case 6: this.r6 = value; return; case 7: this.r7 = value; return;
                case 8: this.r8 = value; return; case 9: this.r9 = value; return; case 10: this.r10 = value; return; case 11: this.r11 = value; return;
                case 12: this.r12 = value; return; case 13: this.r13 = value; return; case 14: this.r14 = value; return; case 15: this.r15 = value; return;
                case 16: this.r16 = value; return; case 17: this.r17 = value; return; case 18: this.r18 = value; return; case 19: this.r19 = value; return;
                case 20: this.r20 = value; return; case 21: this.r21 = value; return; case 22: this.r22 = value; return; case 23: this.r23 = value; return;
                case 24: this.r24 = value; return; case 25: this.r25 = value; return; case 26: this.r26 = value; return; case 27: this.r27 = value; return;
                case 28: this.r28 = value; return; case 29: this.r29 = value; return; case 30: this.r30 = value; return; case 31: this.r31 = value; return;
            }
        }
        private ConcurrentKTreeNode<K, V> get(in int i) {
            switch (i) {
                case 0: return this.r0; case 1: return this.r1; case 2: return this.r2; case 3: return this.r3;
                case 4: return this.r4; case 5: return this.r5; case 6: return this.r6; case 7: return this.r7;
                case 8: return this.r8; case 9: return this.r9; case 10: return this.r10; case 11: return this.r11;
                case 12: return this.r12; case 13: return this.r13; case 14: return this.r14; case 15: return this.r15;
                case 16: return this.r16; case 17: return this.r17; case 18: return this.r18; case 19: return this.r19;
                case 20: return this.r20; case 21: return this.r21; case 22: return this.r22; case 23: return this.r23;
                case 24: return this.r24; case 25: return this.r25; case 26: return this.r26; case 27: return this.r27;
                case 28: return this.r28; case 29: return this.r29; case 30: return this.r30; case 31: return this.r31;
                default: throw new IndexOutOfRangeException();
            }
        }
    }

    private struct Latch<K, V> where K : IComparable<K> {
        /// <summary>
        /// if 'true', write operations will acquire read locks all the way to the leaf- and then acquire
        /// a write lock only on the leaf. if 'false' then write locks will be used to traverse down the
        /// while latching.
        /// </summary>
        public readonly bool assumeLeafIsSafe;
        /// <summary>
        /// type of latch
        /// </summary>
        private readonly LatchAccessType accessType;
        /// <summary>
        /// Retain the reader lock on the found node after finishing a tree search?
        /// </summary>
        public readonly bool retainReaderLock;

        private ReaderWriterLockSlim _rootLock;

        public bool isInsertAccess { get { return this.accessType == LatchAccessType.insert || this.accessType == LatchAccessType.insertTest; } }
        public bool isDeleteAccess { get { return this.accessType == LatchAccessType.delete || this.accessType == LatchAccessType.deleteTest; } }
        public bool isReadAccess { get { return this.accessType == LatchAccessType.read; } }
        
        public bool TryEnterRootLock(int timeoutMs = -1) {
            if (this.isReadAccess || this.assumeLeafIsSafe) {
                return this._rootLock.TryEnterReadLock(timeoutMs);
            } else {
                return this._rootLock.TryEnterWriteLock(timeoutMs);
            }
        }

        /// <summary>
        /// exits the rootLock or does nothing if it was already exited.
        /// </summary>
        public void ExitRootLock() {
            if (!ReferenceEquals(null, this._rootLock)) {
                if (this.isReadAccess || this.assumeLeafIsSafe) {
                    this._rootLock.ExitReadLock();
                } else {
                    this._rootLock.ExitWriteLock();
                }
                this._rootLock = null;
            }
        }

        /// <summary>
        /// Exit the latch at this level and every parent including the rootLock
        /// </summary>
        public void ExitLatchChain<LockBuffer>(ref LockBuffer lockBuffer) where LockBuffer : ILockBuffer<K, V> {
            ConcurrentKTreeNode<K, V> node = lockBuffer.pop();
            while (node != null) {
                if (this.isReadAccess ||
                    (this.assumeLeafIsSafe && !node.isLeaf)
                ) {
                    node._rwLock.ExitReadLock();
                } else {
                    node._rwLock.ExitWriteLock();
                }
                node = lockBuffer.pop();
            }
            ExitRootLock();
        }

        public LatchAccessResult TryEnterLatch<LockBuffer>(
            ref LockBuffer lockBuffer,
            in ConcurrentKTreeNode<K, V> node,
            in int timeoutMs
        ) where LockBuffer : ILockBuffer<K, V> {
            if (this.isReadAccess ||
                (this.assumeLeafIsSafe && !node.isLeaf)
            ) {
                // Try to acquire read lock...
                bool acquired = node._rwLock.TryEnterReadLock(timeoutMs);
                // Always release existing locks, even if failed to acquire
                ExitLatchChain(ref lockBuffer);
                if (acquired) {
                    lockBuffer.push(node);
                }
                return acquired ? LatchAccessResult.acquired : LatchAccessResult.timedOut;
            } else {
                // try to acquire a write lock...
                if (!node._rwLock.TryEnterWriteLock(timeoutMs)) {
                    // If failed to get the lock.. release locks
                    ExitLatchChain(ref lockBuffer);
                    return LatchAccessResult.timedOut;
                }

                // Check if it is safe to update node
                if (node.NodeIsSafe(this.isInsertAccess, this.isDeleteAccess)) {
                    ExitLatchChain(ref lockBuffer); // Exit existing locks
                    lockBuffer.push(node); // push newly acquired lock to chain
                    return LatchAccessResult.acquired;
                }

                // Not safe to update..
                if (this.assumeLeafIsSafe) {
                    lockBuffer.push(node); // push newly acquired lock to chain

                    // if test... retain the write lock! (this way we can read the leaf to test it)
                    if (this.accessType == LatchAccessType.insertTest || this.accessType == LatchAccessType.deleteTest) {
                        return LatchAccessResult.notSafeToUpdateLeafTest;
                    }

                    // if assumingLeafIsafe, then exit latch and return not safe
                    ExitLatchChain(ref lockBuffer);
                    return LatchAccessResult.notSafeToUpdateLeaf;
                }

                // Otherwise... return acquired and don't release any locks
                lockBuffer.push(node);
                return LatchAccessResult.acquired;
            }
        }

        public Latch(
            LatchAccessType type,
            ReaderWriterLockSlim rootLock,
            bool assumeLeafIsSafe = true,
            bool retainReaderLock = false
        ) {
            this.accessType = type;
            this._rootLock = rootLock;
            this.assumeLeafIsSafe = assumeLeafIsSafe;
            this.retainReaderLock = retainReaderLock;
        }
    }

    /// <summary>
    /// Tree Node with N children. Can be a leaf or an internal node.
    /// </summary>
    private partial class ConcurrentKTreeNode<K, V> where K: IComparable<K> {
        public ConcurrentKTreeNode(int k, ConcurrentKTreeNode<K, V> parent = null, bool isLeaf = false) {
            if (isLeaf) {
                this._values = new NodeData<K, V>[k+1];
                this._children = null;
            } else {
                this._children = new NodeData<K, ConcurrentKTreeNode<K, V>>[k+1];
                this._values = null;
            }
            this._rwLock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
            this._count = 0;
            this._parent = parent;
        }
        private NodeData<K, V>[] _values;
        private NodeData<K, ConcurrentKTreeNode<K, V>>[] _children;
        private volatile ConcurrentKTreeNode<K, V> _parent;
        private volatile int _count;
        public ReaderWriterLockSlim _rwLock { get; private set; } // Each node has its own lock

        public bool isLeaf {
            get { return ReferenceEquals(null, this._children); }
        }

        public int k {
            get { return this.isLeaf ? this._values.Length - 1 : this._children.Length - 1; }
        }

        public bool isRoot {
            get { return ReferenceEquals(null, this._parent); }
        }

        public void SetValue(in int index, in K key, in V value) {
            this._values[index] = new NodeData<K, V>(key, value);
        }

        public ref NodeData<K, V> GetValue(in int index) {
            return ref this._values[index];
        }

        public int Count {
            get {
                return _count;
            }
            set {
                this._count = value;
            }
        }
        public ConcurrentKTreeNode<K, V> Parent {
            get {
                return this._parent;
            }
            private set {
                this._parent = value;
            }
        }

        /// <summary>
        /// Perform insert starting at leaf and recurse up.
        /// **WARNING**. This method assumes the calling thread has acquired all write locks needed
        /// for this write.
        /// </summary>
        public void sync_InsertAtThisNode(
            in K key,
            in V value,
            in ConcurrentSortedDictionary<K, V> tree
        ) {
            if (!this.isLeaf) {
                throw new Exception("Can only insert at leaf node");
            }
            this.orderedInsert(in key, in this._values, in value);
            this.trySplit(in tree);
        }

        /// <summary>
        /// Returns index in the array for the input key
        /// </summary>
        
        private int searchRangeIndex<VType>(in K key, in NodeData<K, VType>[] array, out int compareResult) {
            // Perform a modified binary search to find the 'key' in the array
            // This binary search will return the index of the last bucket that 'key' is greater than or equal to
            // eg, for key = 5, and arr = 1, 2, 4, 6, 7, the result is index=2, for key = 5 and arr = 1, 2, 4, 5, 7, 8, the reuslt is index = 3
            // this search function expects unique array entries!
            int lo = 0;
            int hi = this.Count - 1;
            compareResult = -1;
            if (hi < 0) return 0;
            int index = lo;
            while (lo <= hi) {
                index = lo + ((hi - lo) >> 1);
                compareResult = key.CompareTo(array[index].key);
                if (compareResult == 0) return index;
                if (compareResult > 0) {
                    lo = index + 1;
                } else {
                    hi = index - 1;
                }
            }
            if (compareResult < 0 && index > 0) {
                compareResult = key.CompareTo(array[index - 1].key);
                return index - 1;
            }
            return index;
        }

        /// <summary>
        /// Returns index in the array for the input key
        /// </summary>
        
        private int searchRangeIndex<VType>(in K key, in NodeData<K, VType>[] array) {
            int cmp;
            return searchRangeIndex(in key, in array, out cmp);
        }

        private int indexOfNode(in ConcurrentKTreeNode<K, V> node) {
            int cmp;
            // The index of node in 'this' can be found via a bsearch by using any key that is underneath 'node'
            if (node.isLeaf) { return searchRangeIndex(in node._values[0].key, this._children, out cmp); }
            else {
                K key;
                if (node.Count <= 1) {
                    // In the event that node recently had a merge... It could have a count of '1'
                    // so we need to fetch from any of node's children keys
                    // the children of node are guarenteed to be well formed! (eg have atleast k/2 children)
                    key = node._children[0].value.isLeaf ? node._children[0].value._values[0].key : node._children[0].value._children[1].key;
                } else {
                    key = node._children[1].key;
                }
                return searchRangeIndex(in key, in this._children, out cmp);
            }
        }

        /// <summary>
        /// Insert item into array and shift array right. Returns index of inserted item.
        /// </summary>
        /// <param name="key"> insert in the bucket this key belongs to </param>
        /// <param name="array"> array to insert into </param>
        /// <param name="value"> value to insert </param>
        private int orderedInsert<VType>(
            in K key,
            in NodeData<K, VType>[] array,
            in VType value
        )  {
            // Get index that key belongs in
            int compareResult;
            int index = this.searchRangeIndex(in key, in array, out compareResult);
            if (compareResult > 0 && this.Count > 0)
                index++; // insertion happens after found index
            // shift the array right
            return indexInsert(in index, in key, in array, in value);
        }
        /// <summary>
        /// Insert key-value at index. Shift array right after index.
        /// </summary>
        /// <param name="key"> insert in the bucket this key belongs to </param>
        /// <param name="array"> array to insert into </param>
        /// <param name="value"> value to insert </param>
        
        private int indexInsert<VType>(
            in int index,
            in K key,
            in NodeData<K, VType>[] array,
            in VType value
        )  {
            if (index < this.Count) {
                // shift the array right
                Array.Copy(array, index, array, index + 1, this.Count - index);
            }
            //assign value at index
            array[index] = new NodeData<K, VType>(key, value);
            this.Count++;
            return index;
        }

        /// <summary>
        /// Try to split this node into two nodes. The new node will have k/2 highest children.
        /// The current node (this) will keep k/2 lowest children.
        /// The new node will have the same parent as this node.
        /// </summary>
        private void trySplit(in ConcurrentSortedDictionary<K, V> tree) {
            /// <summary>
            /// copy right half of array from -> to and zero-initialize copied indices in 'from'
            /// </summary>
            void splitCopy(
                in ConcurrentKTreeNode<K, V> from,
                in ConcurrentKTreeNode<K, V> to,
                out K newNodeMinKey
            ) {
                int arrLength = from.k + 1;
                int half = arrLength / 2;
                if (from.isLeaf) {
                    for (int i = half; i < arrLength; i++) {
                        to._values[i - half] = from._values[i];
                        from._values[i] = default(NodeData<K, V>); // 0 init
                    }
                    newNodeMinKey = to._values[0].key;
                } else {
                    // set default key for new node
                    newNodeMinKey = from._children[half].key;
                    to._children[0] = new NodeData<K, ConcurrentKTreeNode<K, V>>(default(K), from._children[half].value);
                    to._children[0].value.Parent = to;
                    from._children[half] = default(NodeData<K, ConcurrentKTreeNode<K, V>>); // default
                    // set the rest
                    for (int i = half + 1; i < arrLength; i++) {
                        to._children[i - half] = new NodeData<K, ConcurrentKTreeNode<K, V>>(
                            from._children[i].key, from._children[i].value);
                        to._children[i - half].value.Parent = to; // copy parent ref
                        from._children[i] = default(NodeData<K, ConcurrentKTreeNode<K, V>>); // 0 init
                    }
                }
                from.Count = half;
                to.Count = arrLength - half;
            }

            #if ConcurrentSortedDictionary_DEBUG
            int version = assertWriterLock(beginWrite: true);
            #endif

            // Check if this node needs to split
            if (!canSplit()) {
                #if ConcurrentSortedDictionary_DEBUG
                assertWriterLock(version);
                #endif

                return;
            }

            // 1. Make empty new node with the same parent as this node
            var newNode = new ConcurrentKTreeNode<K, V>(this.k, this.Parent, this.isLeaf);

            // 2. Copy k/2 largest from this node to the new node
            K newNodeMinKey;
            splitCopy(this, in newNode, out newNodeMinKey);

            // 3a. Handle root edge case
            if (this.isRoot) {
                var newRoot = new ConcurrentKTreeNode<K, V>(this.k, null, false);
                newRoot._children[0] = new NodeData<K, ConcurrentKTreeNode<K, V>>(default(K), this);
                newRoot._children[1] = new NodeData<K, ConcurrentKTreeNode<K, V>>(newNodeMinKey, in newNode);
                newRoot.Count = 2;
                newNode.Parent = newRoot;
                this.Parent = newRoot;
                tree.setRoot(newRoot); // Note* newRoot is not locked.. but noone else has ref to it since the root ptr is locked

                #if ConcurrentSortedDictionary_DEBUG
                assertRootWriteLockHeld(tree);
                #endif
            // 3b. Otherwise, handle internal node parent
            } else {
                var thisNodeIndex = this.Parent.indexOfNode(this);
                // Insert new node into the parent
                this.Parent.indexInsert(thisNodeIndex + 1, newNodeMinKey, in this.Parent._children, in newNode);
                // Try recurse on parent
                this.Parent.trySplit(in tree);
            }
        }

        /// <summary>
        /// Perform deletion starting at this node and recurse up.
        /// **WARNING**. This method assumes the calling thread has acquired all write locks needed
        /// for this write.
        /// </summary>
        public void sync_DeleteAtThisNode(
            in K key,
            in ConcurrentSortedDictionary<K, V> tree
        ) {
            if (!this.isLeaf) {
                throw new Exception("Can only delete at leaf node");
            }
            this.orderedDelete(in key, in this._values);
            this.tryMerge(in tree);
        }

        /// <summary>
        /// This node will merge/adopt from siblings to maintain tree balane
        /// </summary>
        private void tryMerge(in ConcurrentSortedDictionary<K, V> tree) {
            /// <summary>
            /// Merge 'left' into 'right'. Update parent accordingly.
            /// </summary>
            void mergeLeft(
                in ConcurrentKTreeNode<K, V> left,
                in ConcurrentKTreeNode<K, V> right,
                in int leftNodeIndex,
                in int rightNodeIndex
            ) {
                var leftAncestorKey = left.Parent._children[leftNodeIndex].key;
                var rightAncestorKey = right.Parent._children[rightNodeIndex].key;

                // Perform Copy
                if (left.isLeaf) {
                    for (int i = right.Count - 1; i >= 0; i--) { // Shift rightArray right by left.Count
                        right._values[i + left.Count] = right._values[i];
                    }
                    for (int i = 0; i < left.Count; i++) {
                        right._values[i] = left._values[i];
                        left._values[i] = default(NodeData<K, V>);
                    }
                } else {
                    // Update right key to be non default in preparation for being (not in the front anymore)
                    right._children[0] = new NodeData<K, ConcurrentKTreeNode<K, V>>(rightAncestorKey, right._children[0].value);
                    for (int i = right.Count - 1; i >= 0; i--) {
                        right._children[i + left.Count] = right._children[i];
                    }
                    for (int i = 0; i < left.Count; i++) {
                        right._children[i] = new NodeData<K, ConcurrentKTreeNode<K, V>>(
                            left._children[i].key, left._children[i].value);
                        right._children[i].value.Parent = right; // update parent ref
                        left._children[i] = default(NodeData<K, ConcurrentKTreeNode<K, V>>); // clear
                    }
                }
                // Update Counts
                right.Count += left.Count;
                left.Count = 0;
                // De-parent the left node
                left.Parent.deleteIndex(in leftNodeIndex, left.Parent._children);
                left._parent = null;
                // right is shifted left by one
                right.Parent._children[rightNodeIndex - 1] = new NodeData<K, ConcurrentKTreeNode<K, V>>(leftAncestorKey, right);
            }
            /// <summary>
            /// Merge 'right' into 'left'. Upate parent accordingly.
            /// </summary>
            void mergeRight(
                in ConcurrentKTreeNode<K, V> left,
                in ConcurrentKTreeNode<K, V> right,
                in int leftNodeIndex,
                in int rightNodeIndex
            ) {
                var rightAncestorKey = right.Parent._children[rightNodeIndex].key;

                // Perform copy
                if (right.isLeaf) {
                    for (int i = 0; i < right.Count; i++) {
                        left._values[left.Count + i] = right._values[i];
                        right._values[i] = default(NodeData<K, V>);
                    }
                } else {
                    // Update right key to be non default in preparation for being (not in the front anymore)
                    right._children[0] = new NodeData<K, ConcurrentKTreeNode<K, V>>(rightAncestorKey, right._children[0].value);
                    for (int i = 0; i < right.Count; i++) {
                        left._children[left.Count + i] = new NodeData<K, ConcurrentKTreeNode<K, V>>(
                            right._children[i].key, right._children[i].value);
                        left._children[left.Count + i].value.Parent = left; // update parent
                        right._children[i] = default(NodeData<K, ConcurrentKTreeNode<K,V>>); // clear
                    }
                }
                // Update Counts
                left.Count += right.Count;
                right.Count = 0;
                // De-Parent the right node
                right.Parent.deleteIndex(in rightNodeIndex, in right.Parent._children);
                right._parent = null;
            }
            /// <summary>
            /// Adopt left to right. 
            /// </summary>
            void adoptLeft(
                in ConcurrentKTreeNode<K, V> left,
                in ConcurrentKTreeNode<K, V> right,
                in int leftNodeIndex,
                in int rightNodeIndex
            ) {
                int leftArrayIndex = left.Count - 1; // (index of max in left node)

                K newParentMin;
                var rightAncestorKey = right.Parent._children[rightNodeIndex].key;

                if (this.isLeaf) { // copy from left[count-1] to right [0]
                    newParentMin = left._values[leftArrayIndex].key;
                    right.indexInsert(0, in left._values[leftArrayIndex].key, in right._values, in left._values[leftArrayIndex].value);
                    left._values[leftArrayIndex] = default(NodeData<K, V>);
                } else {
                    newParentMin = left._children[leftArrayIndex].key;
                    // Update right key to be non default in preparation for being (not in the front anymore)
                    right._children[0] = new NodeData<K, ConcurrentKTreeNode<K, V>>(rightAncestorKey, right._children[0].value);
                    int insertedIndex = right.indexInsert(0, default(K),
                        in right._children, in left._children[leftArrayIndex].value);
                    right._children[insertedIndex].value.Parent = right; // update parent ref
                    left._children[leftArrayIndex] = default(NodeData<K, ConcurrentKTreeNode<K, V>>);
                }
                // Update parent keys
                right.Parent._children[rightNodeIndex] = new NodeData<K, ConcurrentKTreeNode<K, V>>(newParentMin, right);
                // Update counts
                left.Count--;
            }
            /// <summary>
            /// Adopt right into left. 
            /// </summary>
            void adoptRight(
                in ConcurrentKTreeNode<K, V> left,
                in ConcurrentKTreeNode<K, V> right,
                in int leftNodeIndex,
                in int rightNodeIndex
            ) {
                K newParentMin;
                var rightAncestorKey = right.Parent._children[rightNodeIndex].key;

                if (right.isLeaf) { // copy right[0] to left[count]
                    newParentMin = right._values[1].key; // new parent min will be the next key...
                    left._values[left.Count] = right._values[0];
                    right.deleteIndex(0, right._values);
                } else {
                    newParentMin = right._children[1].key;
                    left._children[left.Count] = new NodeData<K, ConcurrentKTreeNode<K, V>>(rightAncestorKey,
                        right._children[0].value);
                    left._children[left.Count].value.Parent = left; // update parent ref
                    right.deleteIndex(0, right._children);
                    // reset the key on the first index in the right array
                    right._children[0] = new NodeData<K, ConcurrentKTreeNode<K, V>>(default(K), right._children[0].value);
                }
                // Update parent keys
                right.Parent._children[rightNodeIndex] = new NodeData<K, ConcurrentKTreeNode<K, V>>(newParentMin, right);
                // update counts
                left.Count++;
            }

            #if ConcurrentSortedDictionary_DEBUG
            int version = assertWriterLock(beginWrite: true);
            #endif

            // 1. Check if this node needs to merge or adopt
            if (!canMerge()) {
                #if ConcurrentSortedDictionary_DEBUG
                assertWriterLock(version);
                #endif

                return;
            }
            bool isLeaf = this.isLeaf;
            var parent = this.Parent;

            // 2. Handle root edge case
            if (this.isRoot) {
                // Try to select new root if this root only has 1 child
                if (!isLeaf && this.Count <= 1) {
                    this._children[0].value._parent = null; // remove parent ref (child will become root)
                    tree.setRoot(this._children[0].value); // set new root

                    #if ConcurrentSortedDictionary_DEBUG
                    assertWriterLock(version);
                    assertRootWriteLockHeld(tree);
                    #endif

                    return;
                }
                #if ConcurrentSortedDictionary_DEBUG
                assertWriterLock(version);
                #endif

                // Otherwise, root remains...
                return;
            }

            int nodeIndex = this.Parent.indexOfNode(this);
            int leftIndex = nodeIndex - 1;
            int rightIndex = nodeIndex + 1;
            var left = nodeIndex > 0 ? this.Parent._children[leftIndex].value : null;
            var right = nodeIndex < this.Parent.Count - 1 ? this.Parent._children[rightIndex].value : null;

            // 3. Try to Adopt from left
            if (!ReferenceEquals(null, left) && left.canSafelyDelete()) {
                adoptLeft(in left, this, in leftIndex, in nodeIndex);

                #if ConcurrentSortedDictionary_DEBUG
                assertWriterLock(version);
                #endif

                return;
            }
            // 4. Try to Adopt from right
            if (!ReferenceEquals(null, right) && right.canSafelyDelete()) {
                adoptRight(this, in right, in nodeIndex, in rightIndex);

                #if ConcurrentSortedDictionary_DEBUG
                assertWriterLock(version);
                #endif

                return;
            }
            // 5a. Merge Right if possible
            if (!ReferenceEquals(null, right)) mergeRight(this, in right, in nodeIndex, in rightIndex);
            // 5b. Otherwise Merge left
            else mergeLeft(in left, this, in leftIndex, in nodeIndex);
 
            // 6. Try to merge recurse on parent
            parent.tryMerge(in tree);
        }

        /// <summary>
        /// Removes the key from the array.
        /// </summary>
        private void orderedDelete<VType>(
            in K key,
            in NodeData<K, VType>[] array
        )  {
            // Get index of key
            int index = this.searchRangeIndex(in key, in array);
            // shift the array left
            this.deleteIndex(in index, in array);
        }
        /// <summary>
        /// Remove index from array and shift left starting at removed index
        /// </summary>
        
        private void deleteIndex<VType>(
            in int index,
            in NodeData<K, VType>[] array
        )  {
            // shift the array left
            if (index < this.Count) {
                Array.Copy(array, index + 1, array, index, this.Count - index);
            }
            // unassign value in the array
            array[this.Count] = default(NodeData<K, VType>);
            this.Count--;
        }

        public struct SearchOptions {
            private const int kDefaultMaxDepth = int.MaxValue - 1;
            public readonly int timeoutMs;
            public readonly int maxDepth;
            public readonly long startTime;
            public bool getMin;
            public SearchOptions(in int timeoutMs = -1, in int maxDepth = kDefaultMaxDepth, in bool getMin = false, in long startTime = -1) {
                this.timeoutMs = timeoutMs; this.maxDepth = maxDepth; this.getMin = getMin;
                this.startTime = startTime < 0 ? DateTimeOffset.Now.ToUnixTimeMilliseconds() : startTime;
            }
            public void assertValid(bool isReadAccess) {
                if (this.maxDepth != kDefaultMaxDepth && !isReadAccess)
                    throw new ArgumentException("Can only set maxDepth for read access");
                if (maxDepth > kDefaultMaxDepth || maxDepth < 0)
                    throw new ArgumentException("Invalid maxDepth specified");
            }
        }

        /// <summary>
        /// Recurse down the tree searching for a value starting at the root.
        /// </summary>
        /// <param name="key"> key of the item to be inserted </param>
        /// <param name="value"> item to be inserted </param>
        /// <param name="info"> contains search meta data </param>
        /// <param name="latch"> latch used to secure concurrent node access. TryGetValue will always release the entire latch upon exiting except in the case where it is in (insert|delete) and returned notFound or success. </param>
        /// <param name="timeoutMs"> optional timeout in milliseconds </param>
        /// <param name="startTime"> time in milliseconds since 1970 when call was started. </param>
        /// <returns> success, notFound => (Latch may not be released). all others => Latch is fully released </returns>
        public static ConcurrentTreeResult_Extended TryGetValue<LockBuffer>(
            in K key,
            out V value,
            ref SearchResultInfo<K, V> info,
            ref Latch<K, V> latch,
            ref LockBuffer lockBuffer,
            in ConcurrentSortedDictionary<Key, Value> tree,
            in SearchOptions options = new SearchOptions()
        ) where LockBuffer: ILockBuffer<K, V> {
            options.assertValid(latch.isReadAccess);

            // Try to enter the root lock
            if (!latch.TryEnterRootLock(options.timeoutMs)) {
                value = default(V);
                info.index = -1;
                return ConcurrentTreeResult_Extended.timedOut;
            }

            // Init
            info.depth = 0;
            info.node = tree._root as ConcurrentKTreeNode<K, V>;
            info.index = 0;
            info.nextSubTreeKey = default(K);
            info.hasNextSubTree = false;
            int remainingMs = getRemainingMs(in options.startTime, in options.timeoutMs);

            // Try enter latch on this (ie the root node)
            LatchAccessResult result = latch.TryEnterLatch(ref lockBuffer, in info.node, in remainingMs);
            if (result == LatchAccessResult.timedOut || result == LatchAccessResult.notSafeToUpdateLeaf) {
                 value = default(V);
                info.index = -1;
                return result == LatchAccessResult.timedOut ?
                    ConcurrentTreeResult_Extended.timedOut :
                    ConcurrentTreeResult_Extended.notSafeToUpdateLeaf;
            }

            #if ConcurrentSortedDictionary_DEBUG
            int version = info.node.assertLatchLock(ref latch, beginRead: true);
            #endif

            for (int depth = 0; depth < options.maxDepth; depth++) {
                if (info.node.isLeaf) {
                    int compareResult;
                    info.index = info.node.searchRangeIndex(key, info.node._values, out compareResult);
                    info.depth = depth;
                    var searchResult = ConcurrentTreeResult_Extended.success;
                    if (compareResult == 0) {
                        value = info.node._values[info.index].value;
                    } else {
                        value = default(V);
                        info.index = -1;
                        searchResult = ConcurrentTreeResult_Extended.notFound;
                    }

                    #if ConcurrentSortedDictionary_DEBUG
                    info.node.assertLatchLock(ref latch, version);
                    #endif

                    // Exit latch if reading and not retaining
                    if (latch.isReadAccess && !latch.retainReaderLock) {
                        latch.ExitLatchChain(ref lockBuffer);
                    }
                    if (result == LatchAccessResult.notSafeToUpdateLeafTest) {
                        return ConcurrentTreeResult_Extended.notSafeToUpdateLeaf;
                    }
                    return searchResult;
                } else {
                    if (result == LatchAccessResult.notSafeToUpdateLeafTest)
                        throw new Exception("Failed sanity test");

                    #if ConcurrentSortedDictionary_DEBUG
                    info.node.assertLatchLock(ref latch, version);
                    #endif

                    int nextIndex = options.getMin ? 0 : info.node.searchRangeIndex(in key, in info.node._children);
                    // get next sibling subtree
                    if (nextIndex + 1 < info.node.Count) {
                        info.hasNextSubTree = true;
                        info.nextSubTreeKey = info.node._children[nextIndex + 1].key;
                    }
                    // Move to next node
                    info.node = info.node._children[nextIndex].value;
                    info.depth = depth + 1;

                    // Try Enter latch on next node (which will also atomically exit latch on parent)
                    result = latch.TryEnterLatch(ref lockBuffer, in info.node, in remainingMs);
                    if (result == LatchAccessResult.timedOut || result == LatchAccessResult.notSafeToUpdateLeaf) {
                        value = default(V);
                        info.index = -1;
                        return result == LatchAccessResult.timedOut ?
                            ConcurrentTreeResult_Extended.timedOut :
                            ConcurrentTreeResult_Extended.notSafeToUpdateLeaf;
                    }

                    #if ConcurrentSortedDictionary_DEBUG
                    version = info.node.assertLatchLock(ref latch, beginRead: true);
                    #endif
                }

                remainingMs = getRemainingMs(in options.startTime, in options.timeoutMs);
            }

            // Sanity check
            if (info.depth >= int.MaxValue - 1) {
                throw new Exception("Bad Tree State, reached integer max depth limit");
            }
            // maxDepth was reached before finding a result!
            if (latch.isReadAccess && !latch.retainReaderLock) {
                latch.ExitLatchChain(ref lockBuffer);
            }
            value = default(V);
            return ConcurrentTreeResult_Extended.notFound;
        }

        /// <summary>
        /// read lock entire tree under this node and return key-values in order
        /// </summary>
        /// <param name="largerThan"> only return a pair if its key is larger than this </param>
        /// <param name="doLargerThanCheck"> do larger than comparison? </param>
        /// <param name="itemTimeoutMs"> how long to wait before timeout on each node </param>
        private IEnumerable<KeyValuePair<K, V>> recurseTree(
            K largerThan = default(K),
            bool doLargerThanCheck = false,
            int itemTimeoutMs = -1
        ) {
            bool enteredMutex = false;
            try {
                if (!this._rwLock.TryEnterReadLock(itemTimeoutMs)) {
                    throw new TimeoutException();
                }
                enteredMutex = true;

                if (this.isLeaf) {
                    for (int i = 0; i < this.Count; i++) {
                        if (!doLargerThanCheck || largerThan.CompareTo(this._values[i].key) < 0)
                            yield return new KeyValuePair<K, V>(this._values[i].key, this._values[i].value);
                    }
                } else {
                    for (int i = 0; i < this.Count; i++) {
                        var nextNode = this._children[i].value;
                        foreach (var pair in nextNode.recurseTree(largerThan, doLargerThanCheck, itemTimeoutMs)) {
                            yield return pair;
                        }
                    }
                }
            } finally {
                if (enteredMutex) this._rwLock.ExitReadLock();
            }
        }

        /// <summary>
        /// Get all items starting from this node. This method will not read lock the entire tree.
        /// It will instead lock subtrees as it iterates through the entire tree.
        /// </summary>
        /// <param name="tree"> tree reference </param>
        /// <param name="hasAcquiredRootLock"> bool value used for acknowledging root is locked. </param>
        /// <param name="subTreeDepth"> depth subtrees which get read locked. (eg 1=k values locked, 2=k^2 locked, 3=k^3 locked), etc.. </param>
        /// <param name="itemTimeoutMs"> key of the item to be inserted </param>
        public static IEnumerable<KeyValuePair<K, V>> AllItems(
            ConcurrentSortedDictionary<Key, Value> tree,
            int subTreeDepth = 2,
            int itemTimeoutMs = -1
        ) {
            // General idea of this iterator:
            // it will search for all subtrees and return their items until done
            // searching for subtrees N times is preferred because it avoids read-locking the root
            // for the entire duration of the iterator
            //
            // foreach (subtree in tree.recurseToNextSubtree())
            //     yield return subtree.items()

            // init. Note* we are not passing the latch in by ref because enumerable doesn't allow it
            //       however, there isn't a difference if we just create it in read mode using the same root

            SearchResultInfo<K, V> subtree = default(SearchResultInfo<K, V>);
            V _ = default(V);
            K maxKey = default(K);
            bool doLargerThanCheck = false;
            int maxDepth = Math.Max(0, tree.Depth - subTreeDepth);
            var searchOptions = new SearchOptions(itemTimeoutMs, maxDepth, true);
            var latch = new Latch<K, V>(LatchAccessType.read, tree._rootLock, retainReaderLock: true);
            var readLockBuffer = new LockBuffer2<K, V>();
            // flag for checking if the final subtree has been returned
            bool searchFinalSubtree = false;

            // Get Min subtree (eg starting point)
            var searchResult = TryGetValue(subtree.nextSubTreeKey, out _, ref subtree,
                ref latch, ref readLockBuffer, in tree, searchOptions);
            if (searchResult == ConcurrentTreeResult_Extended.timedOut) {
                throw new TimeoutException();
            } else if (searchResult == ConcurrentTreeResult_Extended.notSafeToUpdateLeaf) {
                throw new Exception("Bad Tree State, unexpected search result");
            }

            do {
                // Iterate all in current locked tree
                try {
                    if (subtree.node.isLeaf) { // subtree is leaf
                        for (int i = 0; i < subtree.node.Count; i++) {
                            yield return new KeyValuePair<K, V>(subtree.node._values[i].key,
                                subtree.node._values[i].value);
                        }
                        // If leaf node is also the root...
                        if (subtree.node.isRoot) {
                            yield break;
                        }
                    } else {
                        // If not at a leaf... then recurse on the subtree
                        for (int i = 0; i < subtree.node.Count; i++) {
                            // We already locked the subtree node.. So recurse on each child
                            // to avoid recursive locking of the same lock
                            var childNode = subtree.node._children[i].value;
                            foreach (var pair in childNode.recurseTree(maxKey, doLargerThanCheck, itemTimeoutMs)) {
                                maxKey = pair.Key;
                                yield return pair;
                            }
                        }
                    }
                } finally {
                    // Release Latch on subtree
                    latch.ExitLatchChain(ref readLockBuffer);
                }

                // No need to search again.. this was the final node... just exit
                if (searchFinalSubtree) {
                    yield break;
                }

                // Get next tree
                searchOptions = new SearchOptions(itemTimeoutMs, maxDepth, false);
                latch = new Latch<K, V>(LatchAccessType.read, tree._rootLock, retainReaderLock: true);
                readLockBuffer = new LockBuffer2<K, V>();
                K nextKey = subtree.nextSubTreeKey; // Note* make copy due to pass-by-reference
                searchResult = TryGetValue(
                    nextKey, out _, ref subtree, ref latch, ref readLockBuffer, in tree, searchOptions);
                // If failed due to timout.. or there is no next key
                if (!subtree.hasNextSubTree) {
                    // If the final node was already searched....
                    if (searchFinalSubtree) {
                        latch.ExitLatchChain(ref readLockBuffer);
                        yield break;
                    } else { // otherwise.. set flag and do one more pass
                        searchFinalSubtree = true;
                    }
                } else if (searchResult == ConcurrentTreeResult_Extended.timedOut) {
                    throw new TimeoutException();
                } else if (searchResult == ConcurrentTreeResult_Extended.notSafeToUpdateLeaf) {
                    throw new Exception("Bad Tree State, unexpected search result");
                }
                doLargerThanCheck = true;
            } while (true);
        }

        
        private bool canSafelyInsert() {
            return this.Count < this.k; // it is safe to insert if (count + 1 <= k)
        }
        
        private bool canSplit() {
            return this.Count > this.k; // split if we exceeded allowed count
        }
        
        private bool canSafelyDelete() {
            int k = this.k;
            // Example: (L=3, safe to release at C=3), (L=4, C=4,3), (L=5, C=5,4), (L=6, C=6,5,4) etc...
            int checkLength = k % 2 == 0 ? k / 2 : k / 2 + 1;
            return this.Count > checkLength;
        }
        
        private bool canMerge() {
            int k = this.k; // merge if less than k/2 items in array
            int checkLength = k % 2 == 0 ? k / 2 : k / 2 + 1;
            return this.Count < checkLength;
        }

        /// <summary>
        /// Check if inserting/deleting on this node will cause a split or merge to parent
        /// </summary>
        
        public bool NodeIsSafe(bool isInsertAccess, bool isDeleteAccess) {
            if (isInsertAccess) {
                return canSafelyInsert();
            } else if (isDeleteAccess) {
                return canSafelyDelete();
            } else {
                throw new ArgumentException("Unsupported latch access type");
            }
        }
    }

    private static int getRemainingMs(in long startTime, in int timeoutMs) {
        return timeoutMs <= 0 ? -1 : (int)(DateTimeOffset.Now.ToUnixTimeMilliseconds() - startTime);
    }
}

#nullable restore
