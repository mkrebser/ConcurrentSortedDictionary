# ConcurrentSortedDictionary
ConcurrentSortedDictionary<Key, Value> implementation in (c#, .NET 7).

- The interface is based on [.NET ConcurrentDictionary:](https://learn.microsoft.com/en-us/dotnet/api/system.collections.concurrent.concurrentdictionary-2?view=net-7.0)
- The ConcurrentSortedDictionary class is entirely implemented in a single file: [ConcurrentSortedDictionary.cs.](https://github.com/mkrebser/ConcurrentSortedDictionary/blob/master/ConcurrentSortedDictionary.cs)
- The ConcurrentSortedDictionary is [implemented using a B+tree.](https://en.wikipedia.org/wiki/B%2B_tree#)
- Mutual Exclusion is guarenteed via latching using Reader-Writer locks on each tree node.
  - Writers can write concurrently to the tree *on different nodes in the tree*
  - Writers cannot write concurrently to the *same node in the tree*
  - Readers have concurrent access with other readers- but not writers.
  - Nodes in the tree have 'k' children.

### Properties
#### `Count`
 - Number of items in the dictionary
#### `Depth`
 - Depth of the underlying B+Tree
#### `IsEmpty`
- Check if the Dictionary is empty.

### Methods
#### `bool TryAdd(K key, V value)`
```
if (!myDict.TryAdd("key1", 345)) {
  Console.WriteLine("Failed to add because the input key already exists!");
}
```
#### `V GetOrAdd(K key, V value)`
```
int myVal = myDict.GetOrAdd("key1", -1);
```
#### `AddOrUpdate(Key key, Value value)`
```
myDict.AddOrUpdate("key1", 100);
```
#### `TryGetValue(Key key, out Value value)`
```
int myValue;
if (myDict.TryGetValue("key1", out value)) {
  Console.WriteLine("Key Exists!");
}
```
#### `TryRemove(Key key) `
```
if (!myDict.TryRemove) {
  throw new Exception();
}
```
#### `bool ContainsKey(Key key)`
```
if (myDict.ContainsKey("key1")) {
  return true;
}
```
#### `IEnumerator<KeyValuePair<Key, Value>> GetEnumerator()`
```
foreach (KeyValuePair<string, int> pair in myDict) {
  // Do something
}
```
#### `Value this[Key key] { get; set; }`
```
myDict["key1"] = 123;
Console.WriteLine(myDict["key1"]);
```

### Other overloaded methods
- There is an overload of every method from the above interface that allows specifying a mutex timeout!
- The enumerable overload will throw a TimeoutException() if a positive timeout is specified.
