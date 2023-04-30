using System.Collections.Concurrent.Extended;

Console.WriteLine("Starting...");


// The #DEBUG directive is used for conditionally compiling out test & assertion code...
// dotnet run --configuration Debug
// dotnet run --configuration Release

#if DEBUG

new BPlusTreeCorrectnessTest().run();

#endif


Console.WriteLine("Success!");
