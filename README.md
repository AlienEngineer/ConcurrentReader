ConcurrentReader
================

Wrapper that enables concurrent reading from a database or any other IDataReader implementations.

```C#
// Returns some IDataReader implementation...
IDataReader reader = GetReader();
// Wraps the IDataReader making it Thread-Safe
var cReader = reader.AsParallel(); 
```
