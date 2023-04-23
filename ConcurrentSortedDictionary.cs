/*
MIT License

Copyright (c) 2023 Matthew Krebser (mkrebser)

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

// nullable isn't really mandated here. null may or may not be allowed depending on the type of
// <T> (eg is it valueType? refType? it is allowed to be both)
#pragma warning disable CS8600
#pragma warning disable CS8601
#pragma warning disable CS8602
#pragma warning disable CS8603
#pragma warning disable CS8604
#pragma warning disable CA2200 // catch and rethrow an exception for purpose of releasing mutex

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
public class ConcurrentSortedDictionary<Key, Value> : IEnumerable<KeyValuePair<Key, Value>> where Key: IComparable<Key> {
    private volatile ConcurrentKTreeNode<Key, Value> _root;
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
    public int Depth { get { return _depth; }}

    /// <summary>
    /// Create a new instance of ConcurrentSortedDictionary
    /// </summary>
    /// <param name="k"> Number of children per node. </param>
    public ConcurrentSortedDictionary(int k = 8) {
        if (k < 2)
            throw new ArgumentException("Invalid k specified");
        _rootLock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
        _root = new ConcurrentKTreeNode<Key, Value>(k, isLeaf: true);
        _count = 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void assertTimeoutArg(in int timeoutMs) {
        if (timeoutMs < 0)
            throw new ArgumentException("Timeout cannot be negative!");
    }

    /// <summary>
    /// Returns true if writer latch is successfully acquired. False if not.
    /// </summary>
    /// <param name="key"> key of the item for latch search </param>
    /// <param name="timeoutMs"> optional timeout in milliseconds </param>
    /// <param name="startTime"> time in milliseconds since 1970 when call was started. </param>
    /// <param name="accessType"> type of latch to acquire </param>
    /// <param name="latch"> output latch that is acquired </param>
    /// <param name="currentValue"> deepest node at which the seatch ended </param>
    /// <param name="info"> search result metadata </param>
    /// <param name="getResult"> search result. guarenteed 'notFound' or 'success if latch was successfully acquired. </param>
    private bool tryAcquireWriterLatch(
        in Key key,
        in int timeoutMs,
        in LatchAccessType accessType,
        out Latch latch,
        out Value? currentValue,
        ref SearchResultInfo<Key, Value> info,
        out ConcurrentTreeResult_Extended getResult
    ) {
        // Create a latch that will acquire read locks on all internal nodes
        // and then only try to writeLock the leaf. 
        // (We optimistically assume the value can be trivially modified)
        latch = new Latch(accessType, this._rootLock, assumeLeafIsSafe: true);
        long startTime = DateTimeOffset.Now.ToUnixTimeMilliseconds();
        int remainingMs = getRemainingMs(in startTime, in timeoutMs);
        var searchOptions = new ConcurrentKTreeNode<Key, Value>.SearchOptions(remainingMs, startTime: startTime);
        // Perform a query to recurse to the deepest node, latching downwards optimistically
        getResult = ConcurrentKTreeNode<Key, Value>.unsafe_TryGetValue(in key, out currentValue,
            ref info, ref latch, this, searchOptions);

        // If the leaf wasn't safe to update.. Try again but with write locks all the way down. 
        if (getResult == ConcurrentTreeResult_Extended.notSafeToUpdateLeaf) {
            // TryGetValue will completely exit the latch if it was determined notSafe
            latch = new Latch(accessType, this._rootLock, assumeLeafIsSafe: false);
            remainingMs = getRemainingMs(in startTime, in timeoutMs);
            searchOptions = new ConcurrentKTreeNode<Key, Value>.SearchOptions(remainingMs, startTime: searchOptions.startTime);
            getResult = ConcurrentKTreeNode<Key, Value>.unsafe_TryGetValue(in key, out currentValue,
                ref info, ref latch, this, searchOptions);
        }

        // Check if timed out...
        if (getResult == ConcurrentTreeResult_Extended.timedOut) {
            currentValue = default(Value);
            info = default(SearchResultInfo<Key, Value>);
            return false;
        }

        // sanity check
        if (getResult != ConcurrentTreeResult_Extended.success &&
        getResult != ConcurrentTreeResult_Extended.notFound) {
            throw new Exception("Bad Tree State: " + getResult.ToString());
        }

        // Write latch acquired!
        return true;
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
        return insert(in key, in value, out retrievedValue, in timeoutMs, true);
    }

    /// <summary>
    /// Insert a Key-Value pair and overwrite if it already exists. Waits forever for mutex.
    /// </summary>
    public void AddOrUpdate(
        in Key key,
        in Value value
    ) {
        Value retrievedValue;
        insert(in key, in value, out retrievedValue, -1, true);
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
        return insert(in key, in value, out retrievedValue, in timeoutMs, false);
    }

    /// <summary>
    /// Insert a Key-Value pair. Return false if not inserted due to existing value.
    /// </summary>
    public bool TryAdd(
        in Key key,
        in Value value
    ) {
        Value retrievedValue;
        return insert(in key, in value, out retrievedValue, -1, false) == InsertResult.success;
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
        return insert(in key, in value, out retrievedValue, in timeoutMs, false);
    }

    /// <summary>
    /// Insert a Key-Value pair if it doesn't exist and return the value. If it does exist, the existing value is returned.
    /// </summary>
    public Value GetOrAdd(
        in Key key,
        in Value value
    ) {
        Value retrievedValue;
        insert(in key, in value, out retrievedValue, -1, false);
        return retrievedValue;
    }

    /// <summary>
    /// Insert a value into the tree starting at this node.
    /// </summary>
    /// <param name="key"> key of the item to be inserted </param>
    /// <param name="value"> item to be inserted </param>
    /// <param name="timeoutMs"> optional timeout in milliseconds </param>
    /// <param name="overwrite"> overwrite existing value. default is false. </param>
    /// <returns> success if inserted. alreadyExists if key already exists & overwrite=false. timedOut if timed out. </returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private InsertResult insert(
        in Key key,
        in Value value,
        out Value retrievedValue,
        in int timeoutMs = -1,
        in bool overwrite = false
    ) {
        if (!typeof(Key).IsValueType && ReferenceEquals(null, key)) {
            throw new ArgumentException("Cannot have null key");
        }
        Value? currentValue; SearchResultInfo<Key, Value> info = default(SearchResultInfo<Key, Value>);
        Latch latch; ConcurrentTreeResult_Extended getResult;
        if (!tryAcquireWriterLatch(in key, in timeoutMs, LatchAccessType.insert,
        out latch, out currentValue, ref info, out getResult)) {
            retrievedValue = default(Value);
            return InsertResult.timedOut;
        }
        this._depth = info.depth; // Note* Int32 read/write is atomic

        // Perform write.
        try {
            // If the vaue already exists...
            if (getResult == ConcurrentTreeResult_Extended.success) {
                if (overwrite) {
                    info.node.SetValue(info.index, in key, in value);
                    retrievedValue = value;
                    return InsertResult.success;
                }
                retrievedValue = info.node.GetValue(info.index).value;
                return InsertResult.alreadyExists;
            }
            info.node.unsafe_InsertAtThisNode(in key, in value, this);
            Interlocked.Increment(ref this._count); // increase count
            retrievedValue = value;
            return InsertResult.success;
        } finally {
            // Exit the latch after writing
            info.node.ExitLatchChain(ref latch);
        }
    }

    /// <summary>
    /// Remove a key-value pair from the tree.
    /// </summary>
    public RemoveResult TryRemove(in Key key, int timeoutMs) {
        assertTimeoutArg(timeoutMs);
        return remove(in key, in timeoutMs);
    }

    /// <summary>
    /// Remove a key-value pair from the tree. Waits forever until mutex(s) are acquired.
    /// </summary>
    public bool TryRemove(in Key key) {
        return remove(in key, -1) == RemoveResult.success;
    }

    /// <summary>
    /// Remove a key-value pair from the tree.
    /// </summary>
    /// <param name="key"> key of the item to be inserted </param>
    /// <param name="timeoutMs"> optional timeout in milliseconds </param>
    /// <param name="startTime"> time in milliseconds since 1970 when call was started. </param>
    /// <param name="rootLock"> lock used to protect access to the tree root pointer. (Edge case for making new root node) </param>
    /// <returns> success if removed. notFound if not found. timedOut if timed out. </returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private RemoveResult remove(
        in Key key,
        in int timeoutMs = -1
    ) {
        if (!typeof(Key).IsValueType && ReferenceEquals(null, key)) {
            throw new ArgumentException("Cannot have null key");
        }
        Value? currentValue; SearchResultInfo<Key, Value> info = default(SearchResultInfo<Key, Value>);
        Latch latch; ConcurrentTreeResult_Extended getResult;
        if (!tryAcquireWriterLatch(in key, in timeoutMs, LatchAccessType.delete,
        out latch, out currentValue, ref info, out getResult)) {
            return RemoveResult.timedOut;
        }
        this._depth = info.depth; // Note* Int32 read/write is atomic

        // Perform write.
        try {
            // If the vaue already exists...
            if (getResult == ConcurrentTreeResult_Extended.notFound) {
                return RemoveResult.notFound;
            }
            info.node.unsafe_DeleteAtThisNode(in key, this);
            Interlocked.Decrement(ref this._count); // decrement count
            return RemoveResult.success;
        } finally {
            // Exit the latch after writing
            info.node.ExitLatchChain(ref latch);
        }
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
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private SearchResult tryGetValue(in Key key, out Value value, in int timeoutMs = -1) {
        if (!typeof(Key).IsValueType && ReferenceEquals(null, key)) {
            throw new ArgumentException("Cannot have null key");
        }
        SearchResultInfo<Key, Value> searchInfo = default(SearchResultInfo<Key, Value>);
        var searchOptions = new ConcurrentKTreeNode<Key, Value>.SearchOptions(timeoutMs);
        Latch latch = new Latch(LatchAccessType.read, this._rootLock);
        var result = ConcurrentKTreeNode<Key, Value>.unsafe_TryGetValue(in key, out value,
            ref searchInfo, ref latch, this, searchOptions);
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
        return ConcurrentKTreeNode<Key, Value>.unsafe_AllItems(this, subTreeDepth, itemTimeoutMs).GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator() {
        return this.GetEnumerator();
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
        delete = 2
    }

    private enum LatchAccessResult {
        timedOut = 0,
        acquired = 1,
        notSafeToUpdateLeaf = 2
    }

    private struct Latch {
        /// <summary>
        /// Length of the latch chain
        /// </summary>
        public int latchLength;
        /// <summary>
        /// if 'true', write operations will acquire read locks all the way to the leaf- and then acquire
        /// a write lock only on the leaf. if 'false' then write locks will be used to traverse down the
        /// while latching.
        /// </summary>
        public readonly bool assumeLeafIsSafe;
        /// <summary>
        /// type of latch
        /// </summary>
        public readonly LatchAccessType accessType;
        /// <summary>
        /// Retain the reader lock on the found node after finishing a tree search?
        /// </summary>
        public readonly bool retainReaderLock;

        private ReaderWriterLockSlim? _rootLock;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool tryEnterRootLock(int timeoutMs = -1) {
            if (this.accessType == LatchAccessType.read || this.assumeLeafIsSafe) {
                return this._rootLock.TryEnterReadLock(timeoutMs);
            } else {
                return this._rootLock.TryEnterWriteLock(timeoutMs);
            }
        }

        /// <summary>
        /// exits the rootLock or does nothing if it was already exited.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void tryExitRootLock(bool isLeaf) {
            if (!ReferenceEquals(null, this._rootLock)) {
                if (this.accessType == LatchAccessType.read ||
                    (this.assumeLeafIsSafe && !isLeaf)) {
                        this._rootLock.ExitReadLock();
                } else {
                    this._rootLock.ExitWriteLock();
                }
                this._rootLock = null;
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
    private class ConcurrentKTreeNode<K, V> where K: IComparable<K> {
        public ConcurrentKTreeNode(int k, ConcurrentKTreeNode<K, V>? parent = null, bool isLeaf = false) {
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
        private NodeData<K, V>[]? _values;
        private NodeData<K, ConcurrentKTreeNode<K, V>>[]? _children;
        private volatile ConcurrentKTreeNode<K, V>? _parent;
        private volatile int _count;
        private volatile ReaderWriterLockSlim _rwLock; // Each node has its own lock

        public bool isLeaf {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return ReferenceEquals(null, this._children); }
        }

        public int k {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return this.isLeaf ? this._values.Length - 1 : this._children.Length - 1; }
        }

        public bool isRoot {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return ReferenceEquals(null, this._parent); }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetValue(in int index, in K key, in V value) {
            this._values[index] = new NodeData<K, V>(key, value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref NodeData<K, V> GetValue(in int index) {
            return ref this._values[index];
        }

        public K MinKey {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get {
                return this.isLeaf ? this._values[0].key : this._children[0].key;
            }
        }

        public int Count {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get {
                return _count;
            }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set {
                this._count = value;
            }
        }
        public ConcurrentKTreeNode<K, V> Parent {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get {
                return this._parent;
            }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private set {
                this._parent = value;
            }
        }

        /// <summary>
        /// Perform insert starting at leaf and recurse up.
        /// **WARNING**. This method assumes the calling thread has acquired all write locks needed
        /// for this write.
        /// </summary>
        public void unsafe_InsertAtThisNode(
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int searchRangeIndex<VType>(in K key, in NodeData<K, VType>[] array, out int compareResult, in bool overflow = false) {
            int index = 0;
            int count = this.Count;
            while ((compareResult = key.CompareTo(array[index].key)) >= 0) {
                if (index + 1 >= count) {
                    return overflow ? count : index;
                }
                index++;
            }
            return index;
        }
        /// <summary>
        /// Returns index in the array for the input key
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int searchRangeIndex<VType>(in K key, in NodeData<K, VType>[] array, in bool overflow = false) {
            int compareResult;
            return this.searchRangeIndex(in key, in array, out compareResult, in overflow);
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
            int index = this.searchRangeIndex(in key, in array, overflow: true);
            // shift the array right
            return indexInsert(in index, in key, in array, in value);
        }
        /// <summary>
        /// Insert key-value at index. Shift array right after index.
        /// </summary>
        /// <param name="key"> insert in the bucket this key belongs to </param>
        /// <param name="array"> array to insert into </param>
        /// <param name="value"> value to insert </param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int indexInsert<VType>(
            in int index,
            in K key,
            in NodeData<K, VType>[] array,
            in VType value
        )  {
            // shift the array right
            for (int i = array.Length - 1; i > index; i--) {
                array[i] = array[i-1];
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
            void splitCopy<VType>(
                in NodeData<K, VType>[] from,
                in NodeData<K, VType>[] to,
                in ConcurrentKTreeNode<K, V> fromNode,
                in ConcurrentKTreeNode<K, V> toNode
            ) {
                int half = from.Length/2;
                for (int i = half; i < from.Length; i++) {
                    to[i] = from[i];
                    from[i] = default(NodeData<K, VType>); // 0 init
                }
                fromNode.Count = from.Length - half;
                toNode.Count = half;
            }

            // Check if this node needs to split
            if (!canSplit()) {
                return;
            }

            // 1. Make empty new node with the same parent as this node
            var newNode = new ConcurrentKTreeNode<K, V>(this.k, this.Parent, this.isLeaf);

            // 2. Copy k/2 largest from this node to the new node
            if (this.isLeaf) {
                splitCopy(in this._values, in newNode._values, this, in newNode);
            } else {
                splitCopy(in this._children, in newNode._children, this, in newNode);
            }

            // 3a. Handle root edge case
            if (this.isRoot) {
                var newRoot = new ConcurrentKTreeNode<K, V>(this.k, null, false);
                newRoot._children[0] = new NodeData<K, ConcurrentKTreeNode<K, V>>(this.MinKey, this);
                newRoot._children[1] = new NodeData<K, ConcurrentKTreeNode<K, V>>(newNode.MinKey, in newNode);
                newRoot.Count = 2;
                newRoot.Parent = newRoot;
                this.Parent = newRoot;
                tree.setRoot(newRoot); // Note* newRoot is not locked.. but noone else has ref to it since the root ptr is locked
            // 3b. Otherwise, handle internal node parent
            } else {
                // Update the parent node key to latest min value (the index of this node will not change)
                // because it contain the k/2 smallest items
                int thisNodeIndex = this.Parent.searchRangeIndex(this.MinKey, in this.Parent._children);
                this.Parent._children[thisNodeIndex] = new NodeData<K, ConcurrentKTreeNode<K, V>>(this.MinKey, this);
                // Insert new node into the parent
                this.Parent.orderedInsert(newNode.MinKey, in this.Parent._children, in newNode);
                // Try recurse on parent
                this.Parent.trySplit(in tree);
            }
        }

        /// <summary>
        /// Perform deletion starting at this node and recurse up.
        /// **WARNING**. This method assumes the calling thread has acquired all write locks needed
        /// for this write.
        /// </summary>
        public void unsafe_DeleteAtThisNode(
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
            void mergeLeft<VType>(
                in ConcurrentKTreeNode<K, V> left,
                in ConcurrentKTreeNode<K, V> right,
                in NodeData<K, VType>[] leftArray,
                in NodeData<K, VType>[] rightArray,
                in int leftNodeIndex
            ) {
                // De-parent the left node
                left.Parent.deleteIndex(in leftNodeIndex, left.Parent._children);
                left._parent = null;
                // Shift rightArray right by left.Count
                for (int i = right.Count - 1; i >= 0; i--) {
                    rightArray[i + left.Count] = rightArray[i];
                }
                // Perform Copy
                for (int i = 0; i < left.Count; i++) {
                    rightArray[i] = leftArray[i];
                    leftArray[i] = default(NodeData<K, VType>);
                }
                // Update Counts
                right.Count += left.Count;
                left.Count = 0;
            }
            /// <summary>
            /// Merge 'right' into 'left'. Upate parent accordingly.
            /// </summary>
            void mergeRight<VType>(
                in ConcurrentKTreeNode<K, V> left,
                in ConcurrentKTreeNode<K, V> right,
                in NodeData<K, VType>[] leftArray,
                in NodeData<K, VType>[] rightArray,
                in int rightNodeIndex
            ) {
                // De-Parent the right node
                right.Parent.deleteIndex(in rightNodeIndex, in right.Parent._children);
                right._parent = null;
                // Perform copy
                for (int rightArrayIndex = 0; rightArrayIndex < right.Count; rightArrayIndex++) {
                    leftArray[left.Count + rightArrayIndex] = rightArray[rightArrayIndex];
                    rightArray[rightArrayIndex] = default(NodeData<K, VType>);
                }
                // Update Counts
                left.Count += right.Count;
                right.Count = 0;
            }
            /// <summary>
            /// Adopt left to right. Update parent accordingly.
            /// </summary>
            void adoptLeft<VType>(
                in ConcurrentKTreeNode<K, V> left,
                in ConcurrentKTreeNode<K, V> right,
                in NodeData<K, VType>[] leftArray,
                in NodeData<K, VType>[] rightArray,
                in int rightNodeIndex
            ) {
                // Insert into right & delete from left
                int leftArrayIndex = left.Count - 1;
                right.indexInsert(0, in leftArray[leftArrayIndex].key, in rightArray, in leftArray[leftArrayIndex].value);
                leftArray[leftArrayIndex] = default(NodeData<K, VType>);
                left.Count--;

                // Update the key for 'right' in the parent children array
                right.Parent._children[rightNodeIndex] = new NodeData<K, ConcurrentKTreeNode<K, V>>(right.MinKey, right);
            }
            /// <summary>
            /// Adopt right into left. Update parent accordingly.
            /// </summary>
            void adoptRight<VType>(
                in ConcurrentKTreeNode<K, V> left,
                in ConcurrentKTreeNode<K, V> right,
                in NodeData<K, VType>[] leftArray,
                in NodeData<K, VType>[] rightArray,
                in int rightIndex
            ) {
                // Insert into left & delete from right
                leftArray[left.Count] = rightArray[0]; // copy from right
                left.Count++;
                right.deleteIndex(0, rightArray);

                // Update the key for 'right' in the parent children array
                right.Parent._children[rightIndex] = new NodeData<K, ConcurrentKTreeNode<K, V>>(right.MinKey, right);
            }

            // 1. Check if this node needs to merge or adopt
            if (!canMerge()) {
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
                    return;
                }
                // Otherwise, root remains...
                return;
            }

            int nodeIndex = isLeaf ?
                this.Parent.searchRangeIndex(this.MinKey, this._values) :
                this.Parent.searchRangeIndex(this.MinKey, this._children);
            var left = nodeIndex > 0 ?
                this.Parent._children[nodeIndex - 1].value : null;
            var right = nodeIndex < this.Parent.Count - 1 ?
                this.Parent._children[nodeIndex + 1].value : null;

            // 3. Try to Adopt from left
            if (!ReferenceEquals(null, left) && left.canSafelyDelete()) {
                if (isLeaf) adoptLeft(left, this, left._values, this._values, nodeIndex);
                else adoptLeft(left, this, left._children, this._children, nodeIndex);
                return;
            }

            // 4. Try to Adopt from right
            if (!ReferenceEquals(null, right) && right.canSafelyDelete()) {
                if (isLeaf) adoptRight(this, right, this._values, right._values, nodeIndex + 1);
                else adoptRight(this, right, this._children, right._children, nodeIndex + 1);
                return;
            }

            // 5a. Merge Right if possible
            if (!ReferenceEquals(null, right)) {
                if (isLeaf) mergeRight(this, right, this._values, right._values, nodeIndex + 1);
                else mergeRight(this, right, this._children, right._children, nodeIndex + 1);
            // 5b. Otherwise Merge left
            } else {
                if (isLeaf) mergeLeft(left, this, left._values, this._values, nodeIndex - 1);
                else mergeLeft(left, this, left._children, this._children, nodeIndex - 1);
            }

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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void deleteIndex<VType>(
            in int index,
            in NodeData<K, VType>[] array
        )  {
            // shift the array left
            for (int i = index; i < array.Length - 1; i++) {
                array[i] = array[i+1];
            }
            // unassign value in the array
            array[array.Length - 1] = default(NodeData<K, VType>);
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
            public void assertValid(in LatchAccessType accessType) {
                if (this.maxDepth != kDefaultMaxDepth && accessType != LatchAccessType.read)
                    throw new ArgumentException("Can only set maxDepth for read access");
                if (maxDepth > kDefaultMaxDepth || maxDepth < 0)
                    throw new ArgumentException("Invalid maxDepth specified");
            }
        }

        /// <summary>
        /// Recurse down the tree searching for a value starting at the root.
        /// **WARNING**. This method assumes the calling thread has acquired the root lock.
        /// </summary>
        /// <param name="key"> key of the item to be inserted </param>
        /// <param name="value"> item to be inserted </param>
        /// <param name="info"> contains search meta data </param>
        /// <param name="latch"> latch used to secure concurrent node access. TryGetValue will always release the entire latch upon exiting except in the case where it is in (insert|delete) and returned notFound or success. </param>
        /// <param name="timeoutMs"> optional timeout in milliseconds </param>
        /// <param name="startTime"> time in milliseconds since 1970 when call was started. </param>
        /// <returns> success, notFound => (Latch may not be released). all others => Latch is fully released </returns>
        public static ConcurrentTreeResult_Extended unsafe_TryGetValue(
            in K key,
            out V value,
            ref SearchResultInfo<K, V> info,
            ref Latch latch,
            in ConcurrentSortedDictionary<Key, Value> tree,
            in SearchOptions options = new SearchOptions()
        ) {
            options.assertValid(in latch.accessType);

            // Try to enter the root lock
            if (!latch.tryEnterRootLock(options.timeoutMs)) {
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
            LatchAccessResult result = info.node.TryEnterLatch(ref latch, in remainingMs);
            if (result != LatchAccessResult.acquired) {
                value = default(V);
                info.index = -1;
                return result == LatchAccessResult.timedOut ?
                    ConcurrentTreeResult_Extended.timedOut :
                    ConcurrentTreeResult_Extended.notSafeToUpdateLeaf;
            }

            try {
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
                        // Exit latch if reading and not retaining
                        if (latch.accessType == LatchAccessType.read && !latch.retainReaderLock) {
                            info.node.ExitLatchChain(ref latch);
                        }
                        return searchResult;
                    } else {
                        int nextIndex = info.node.searchRangeIndex(in key, in info.node._children);
                        // get next sibling subtree
                        if (nextIndex + 1 < info.node.Count) {
                            info.hasNextSubTree = true;
                            info.nextSubTreeKey = info.node._children[nextIndex + 1].key;
                        }
                        // Move to next node
                        info.node = info.node._children[nextIndex].value;
                        info.depth = depth + 1;
                        // Try Enter latch on next node (which will also atomically exit latch on parent)
                        if (info.node.TryEnterLatch(ref latch, in remainingMs) != LatchAccessResult.acquired) {
                            value = default(V);
                            info.index = -1;
                            return result == LatchAccessResult.timedOut ?
                                ConcurrentTreeResult_Extended.timedOut :
                                ConcurrentTreeResult_Extended.notSafeToUpdateLeaf;
                        }
                    }

                    remainingMs = getRemainingMs(in options.startTime, in options.timeoutMs);
                }

                // Sanity check
                if (info.depth >= int.MaxValue - 1) {
                    throw new Exception("Bad Tree State, reached integer max depth limit");
                }
                // maxDepth was reached before finding a result!
                if (latch.accessType == LatchAccessType.read && !latch.retainReaderLock) {
                    info.node.ExitLatchChain(ref latch);
                }
                value = default(V);
                return ConcurrentTreeResult_Extended.notFound;
            } catch (Exception e) {
                // catching to unlock- then rethrow
                info.node.ExitLatchChain(ref latch);
                throw e;
            }
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
        public static IEnumerable<KeyValuePair<K, V>> unsafe_AllItems(
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
            Latch latch = new Latch(LatchAccessType.read, tree._rootLock);

            // Get Min subtree (eg starting point)
            var searchResult = unsafe_TryGetValue(subtree.nextSubTreeKey, out _, ref subtree,
                ref latch, in tree, searchOptions);
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
                    subtree.node.ExitLatchChain(ref latch);
                }

                // Get next tree
                searchOptions = new SearchOptions(itemTimeoutMs, maxDepth, false);
                latch = new Latch(LatchAccessType.read, tree._rootLock);
                searchResult = unsafe_TryGetValue(
                    subtree.nextSubTreeKey, out _, ref subtree, ref latch, in tree, searchOptions);
                // If failed due to timout.. or there is no next key
                if (!subtree.hasNextSubTree) {
                    yield break;
                } else if (searchResult == ConcurrentTreeResult_Extended.timedOut) {
                    throw new TimeoutException();
                } else if (searchResult == ConcurrentTreeResult_Extended.notSafeToUpdateLeaf) {
                    throw new Exception("Bad Tree State, unexpected search result");
                }
                doLargerThanCheck = true;
            } while (true);
        }

        public LatchAccessResult TryEnterLatch(ref Latch latch, in int timeoutMs) {

            if (latch.accessType == LatchAccessType.read ||
                (latch.assumeLeafIsSafe && !this.isLeaf)
            ) {
                // Try to acquire read lock...
                bool acquired = this._rwLock.TryEnterReadLock(timeoutMs);
                if (acquired) {
                    latch.latchLength++;
                }
                // Release parent lock after acquiring next lock
                // Even if we failed to gain access, it still needs released
                ExitParentLatchChain(ref latch);
                return acquired ? LatchAccessResult.acquired : LatchAccessResult.timedOut;
            } else {
                // try to acquire a write lock...
                if (!this._rwLock.TryEnterWriteLock(timeoutMs)) {
                    // If failed to get the lock.. release parent and return
                    ExitParentLatchChain(ref latch);
                    return LatchAccessResult.timedOut;
                }
                // Write lock acquired... increment
                latch.latchLength++;

                // Check if it is safe to update node
                if (NodeIsSafe(ref latch)) {
                    // If it is.. Release parent & return acquired
                    ExitParentLatchChain(ref latch);
                    return LatchAccessResult.acquired;
                }

                // Not safe to update..
                if (latch.assumeLeafIsSafe) {
                    // if assumingLeafIsafe, then exit latch and return not safe
                    ExitLatchChain(ref latch);
                    return LatchAccessResult.notSafeToUpdateLeaf;
                }

                // Otherwise... return acquired and don't release parent
                return LatchAccessResult.acquired;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool canSafelyInsert() {
            return this.Count < this.k; // it is safe to insert if (count + 1 <= k)
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool canSplit() {
            return this.Count > this.k; // split if we exceeded allowed count
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool canSafelyDelete() {
            int k = this.k;
            // Example: (L=3, safe to release at C=3), (L=4, C=4,3), (L=5, C=5,4), (L=6, C=6,5,4) etc...
            int checkLength = k % 2 == 0 ? k / 2 : k / 2 + 1;
            return this.Count > checkLength;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool canMerge() {
            int k = this.k; // merge if less than k/2 items in array
            int checkLength = k % 2 == 0 ? k / 2 : k / 2 + 1;
            return this.Count < checkLength;
        }

        /// <summary>
        /// Check if inserting/deleting on this node will cause a split or merge to parent
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool NodeIsSafe(ref Latch latch) {
            if (latch.accessType == LatchAccessType.insert) {
                return canSafelyInsert();
            } else if (latch.accessType == LatchAccessType.delete) {
                return canSafelyDelete();
            } else {
                throw new ArgumentException("Unsupported latch access type");
            }
        }

        /// <summary>
        /// Exit the latch at this level and every parent including the rootLock
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ExitLatchChain(ref Latch latch) {
            var next = this;
            while (next != null && latch.latchLength > 0) {
                latch.latchLength--;
                if (latch.accessType == LatchAccessType.read ||
                    (latch.assumeLeafIsSafe && !this.isLeaf)
                ) {
                    next._rwLock.ExitReadLock();
                } else {
                    next._rwLock.ExitWriteLock();
                }
                next = next.Parent;
            }
            latch.tryExitRootLock(this.isLeaf);
        }

        /// <summary>
        /// Exit the latch chain starting at the parent node of this root
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ExitParentLatchChain(ref Latch latch) {
            if (!this.isRoot) {
                this.Parent.ExitLatchChain(ref latch);
            } else {
                latch.tryExitRootLock(this.isLeaf);
            }
        }
    }

    private static int getRemainingMs(in long startTime, in int timeoutMs) {
        return timeoutMs <= 0 ? -1 : (int)(DateTimeOffset.Now.ToUnixTimeMilliseconds() - startTime);
    }
}

#pragma warning restore CS8600
#pragma warning restore CS8601
#pragma warning restore CS8602
#pragma warning restore CS8603
#pragma warning restore CS8604
#pragma warning disable CA2200
