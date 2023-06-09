﻿#if DEBUG

using System.Collections.Concurrent.Extended;

Console.WriteLine("Starting...");


// The #DEBUG directive is used for conditionally compiling out test & assertion code...
// dotnet run --configuration Debug
// dotnet run --configuration Release


new BPlusTreeCorrectnessTest().run();
new ConcurrencyTest().run();
new PerfTest().run();



Console.WriteLine("Success!");


#endif