# ConcurrentSortedDictionary

### [Learn about Concurrent B+ Trees here! [LINK]](https://medium.com/@mkrebser/concurrent-b-trees-concurrentsorteddictionary-c-net-f7c1c2a84141)


ConcurrentSortedDictionary<Key, Value> implementation in (c#, .NET 7).

- The interface is based on [.NET ConcurrentDictionary:](https://learn.microsoft.com/en-us/dotnet/api/system.collections.concurrent.concurrentdictionary-2?view=net-7.0)
- The ConcurrentSortedDictionary class is entirely implemented in a single file: [ConcurrentSortedDictionary.cs.](https://github.com/mkrebser/ConcurrentSortedDictionary/blob/master/ConcurrentSortedDictionary.cs)
- The ConcurrentSortedDictionary is [implemented using a B+tree.](https://en.wikipedia.org/wiki/B%2B_tree#)
- Mutual Exclusion is guarenteed via latching using Reader-Writer locks on each tree node.
  - Writers can write concurrently to the tree *on different nodes in the tree*
  - Writers cannot write concurrently to the *same node in the tree*
  - Readers have concurrent access with other readers- but not writers *on the same node in the tree*
  - Readers and writers can always access *different* tree nodes concurrently.
  - Nodes in the tree have 'k' children determined by constructor arguments.

## Properties
### `long Count { get; }`
 - Number of items in the dictionary
### `int Depth { get; }`
 - Depth of the underlying B+Tree
### `bool IsEmpty { get; }`
- Check if the Dictionary is empty.

## Methods
### `bool TryAdd(K key, V value)`
```
if (!myDict.TryAdd("key1", 345)) {
  Console.WriteLine("Failed to add because the input key already exists!");
}
```
- Returns true if the key-value pair was added.
- Returns false if it already exists.
### `InsertResult TryAdd(K key, V value, int timeoutMs)`
 - `InsertResult.success` if they key-value pair was successfully inserted.
 - `InsertResult.alreadyExists` if the key already exists.
 - `InsertResult.timedOut` if the operation timed out when acquiring locks.
### `V GetOrAdd(K key, V value)`
```
int myVal = myDict.GetOrAdd("key1", -1);
```
- Inserts a new item. Or if it already exists, returns the existing value.
### `InsertResult GetOrAdd(K key, V value, int timeoutMs, out V val)`
 - `InsertResult.success` if they key-value pair was successfully inserted.
 - `InsertResult.alreadyExists` if the key already exists.
 - `InsertResult.timedOut` if the operation timed out when acquiring locks.
### `void AddOrUpdate(Key key, Value value)`
```
myDict.AddOrUpdate("key1", 100);
```
- Insert a new item or overwrite if it already exists.
### `InsertResult AddOrUpdate(Key key, Value value, int timeoutMs)`
 - `InsertResult.success` if they key-value pair was successfully inserted/updated.
 - `InsertResult.timedOut` if the operation timed out when acquiring locks.
### `bool TryGetValue(Key key, out Value value)`
```
int myValue;
if (myDict.TryGetValue("key1", out value)) {
  Console.WriteLine("Key Exists!");
}
```
- Returns true if the input key exists and outputs the value.
- Returns false if the input key did not exist in the collection.
### `SearchResult TryGetValue(Key key, out Value value, int timeoutMs)`
- `SearchResult.success` if the input key is found.
- `SearchResult.notFound` if the input key is not found.
- `SearchResult.timedOut`  if the operation timed out when acquiring locks.
### `bool TryRemove(Key key) `
```
if (!myDict.TryRemove) {
  throw new Exception();
}
```
- Returns true if the input key was removed from the collection.
- Returns false if the input key did not exist in the collection.
### `RemoveResult TryRemove(Key key, int timeoutMs) `
- `RemoveResult.success` if the input key was removed.
- `RemoveResult.notFound` if the input key is not found.
- `RemoveResult.timedOut`  if the operation timed out when acquiring locks.
### `bool ContainsKey(Key key)`
```
if (myDict.ContainsKey("key1")) {
  return true;
}
```
- Returns true if the key exists in the collection.
### `SearchResult ContainsKey(Key key)`
- `SearchResult.success` if the input key is found.
- `SearchResult.notFound` if the input key is not found.
- `SearchResult.timedOut`  if the operation timed out when acquiring locks.
### `IEnumerator<KeyValuePair<Key, Value>> GetEnumerator()`
```
foreach (KeyValuePair<string, int> pair in myDict) {
  // Do something
}
foreach (KeyValuePair<string, int> pair in myDict.Reversed()) {
  // Do something but iterating in reverse direction
}
foreach (KeyValuePair<string, int> pair in myDict.Range(5, 10)) {
  // iterate through all nodes within the range (inclusive, exclusive)
}
foreach (KeyValuePair<string, int> pair in myDict.Range(99, 10)) {
  // iterate through all nodes within the range (reverse direction)
}
foreach (KeyValuePair<string, int> pair in myDict.StartingWith(55, reverse: true)) {
  // iterate through all nodes starting with a key in the reverse direction
}
foreach (KeyValuePair<string, int> pair in myDict.EndingWith(40)) {
  // iterate through all nodes ending with an exclusive key
}
```
 - **Warning** 
   - Thread Safety: Do not perform read/write operations inside iterator blocks. Iterator blocks maintain a shared non-recursive read-lock on the tree node for the current item. You cannot access or mutate a different part of the tree inside the iterator scope. Additionally, always make sure to dispose of the iterator
   because it maintains a read-lock while iterating. Failing to dispose of the enumerator may cause deadlocks!
   - It is generally not recommended to use the enumerators in nested LINQ statements. While this can be done safely, it is easy to deadlock because- again, readlocks are held until the iterator finishes. Lazy execution of LINQ functions can make it difficult to understand when locks are being held.
### `IEnumerator<KeyValuePair<Key, Value>> GetEnumerator(int timeoutMs)`
- throws a `System.TimeoutException` if the timeout is reached for any individual node in the tree.
### `Value this[Key key] { get; set; }`
```
myDict["key1"] = 123;
Console.WriteLine(myDict["key1"]);
```

### Timeout
 - A timeout of '0' will immediately return timeout result without making the calling thread wait
 - A timeout of '-1' will make the calling thread wait forever to acquire locks
 - A timeout > 0 will make the calling wait for the specified milliseconds before timeout occurs
 - Methods that don't allow specifying a timeout will automatically use '-1'

### Limitations
 - The tree will allow at most k^30 nodes. This means the default (k=32) will allow 32^30 items in the tree.
