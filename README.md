![alt tag](https://github.com/jchristn/watsondedupe/blob/master/assets/watson.ico)

# Watson Deduplication Library

[![NuGet Version](https://img.shields.io/nuget/v/WatsonDedupe.svg?style=flat)](https://www.nuget.org/packages/WatsonDedupe/) [![NuGet](https://img.shields.io/nuget/dt/WatsonDedupe.svg)](https://www.nuget.org/packages/WatsonDedupe) 

Self-contained C# library for data deduplication targeted to .NET Core, .NET Standard, and .NET Framework.

![alt tag](https://github.com/jchristn/WatsonDedupe/blob/master/assets/diagram_half.png)

## New in v2.0.x

- Major internal refactor and breaking changes
- Consolidated settings, callbacks, and statistics into new objects
- Paginated enumeration including prefix-based search
- Refactored many APIs for simplification
- Internal table structure extended
- TryGet APIs
- Additional properties on ```DedupeObject``` including compressed length and number of chunks

## Help and Support

Please contact me or file an issue here if you encounter any problems with the library or have suggestions! 

## Working with Sqlite on .NET Framework

Sqlite has shown to have issues with .NET Framework.  You may need to clone and build, then copy the ```runtimes``` directory from the ```bin/debug/netcoreapp#.#``` directory into your working directory.  You may also need to download and add sqlite3.dll to your project manually.  Set it to copy to output always.

## Under the Hood

The Watson Dedupe library will take an incoming byte array or stream (which you assign a unique key) and utilize a sliding window performing MD5 calculations over the data in the window to identify breakpoints in the data (this is called 'chunking').  Each chunk of data is assigned a chunk key (based on the SHA256 of the data).  MD5 is used to dynamically identify chunk boundaries, and when a chunk boundary is identified, SHA256 is used to assign a unique key to each chunk.  

Tables in a database (Sqlite by default, or, bring your own by implementing the ```DbProvider``` class) are maintained to indicate which objects (```dedupeobject``` table) map to which chunks (```dedupechunk``` table) and their ordering/position (```dedupeobjmap``` table).  Chunks are stored as flat files in a directory you specify using a sanitized version of the chunk key as the filename.  

On retrieval, the object map is retrieved from the index, the appropriate chunks are retrieved, and the object is reconstructed.  As long as the chunk data is consistent across analyzed data sets, identical chunk keys will be created, meaning duplicate data chunks are only stored once.  Further, each chunk key has a separate associated reference count to ensure that chunks are not garbage collected when a referencing object is deleted should another object also hold that reference.
 
## Test App 

Refer to the Test project which will help you exercise DedupeLibrary.

For an example of how to use WatsonDedupe using your own database, refer to the ```Test.External``` project along with the sample implementation of the ```WatsonDedupe.Database.DbProvider``` class found in ```Database.cs```.

## CLI

Refer to the CLI project which provide a binary that can be used in a shell or terminal window to interact with an index for object storage, retrieval, removal, and statistics.  

## Library Example

To use WatsonDedupe, instantiate the ```DedupeSettings``` class and the ```DedupeCallbacks``` class.  ```DedupeSettings``` dictates the inner working of the chunk identification algorithn, and ```DedupeCallbacks``` defines the functions in your application that are invoked for writing, reading, and deleting chunk data.  In this way, your application dictates how chunk data is stored, accessed, and managed.

The example below is from the ```SampleApp``` project and shows how to use WatsonDedupe with a managed internal Sqlite database.
 
```csharp
using System;
using System.IO;
using WatsonDedupe;

namespace SampleApp
{
    class Program
    { 
        static void Main(string[] args)
        {
            // Create chunk directory
            if (!Directory.Exists("chunks")) Directory.CreateDirectory("chunks");

            // Define settings, callbacks, and initialize
            DedupeSettings  settings  = new DedupeSettings(32768, 262144, 2048, 2);
            DedupeCallbacks callbacks = new DedupeCallbacks(WriteChunk, ReadChunk, DeleteChunk);
            DedupeLibrary   dedupe    = new DedupeLibrary("test.db", settings, callbacks);

            // Store objects in the index
            dedupe.Write("kjv1", File.ReadAllBytes("samplefiles/kjv.txt"));
            dedupe.Write("kjv2", File.ReadAllBytes("samplefiles/kjv.txt"));
            dedupe.Write("kjv3", File.ReadAllBytes("samplefiles/kjv.txt"));

            // Check existence and retrieve an object from the index
            if (dedupe.Exists("kjv2")) Console.WriteLine("Exists");
            DedupeObject obj = dedupe.Get("kjv1");

            // List all objects
            Console.WriteLine(dedupe.ListObjects().ToTabularString());

            // Display index statistics
            Console.WriteLine(dedupe.IndexStats().ToString());

            // Delete an object from the index
            dedupe.Delete("kjv1");
        }

        // Called during store operations, consider using FileStream with FileOptions.WriteThrough to ensure crash consistency
        static void WriteChunk(DedupeChunk data)
        {
            File.WriteAllBytes("Chunks\\" + data.Key, data.Data); 
        }

        // Called during read operations
        static byte[] ReadChunk(string key)
        {
            return File.ReadAllBytes("Chunks\\" + key);
        }

        // Called during delete operations
        static void DeleteChunk(string key)
        {
            File.Delete("Chunks\\" + key); 
        }
    }
}
```

## CLI Example

Example taken using the ```Cli``` project.

```
// Initialize new index
> dedupe test.idx create --params=32768,262144,2048,2

// Store an object
> dedupe test.idx write --chunks=chunks --key=obj1 < obj1.txt

// Retrieve an object
> dedupe test.idx get --chunks=chunks --key=obj1 > obj1_new.txt

// Delete an object
> dedupe test.idx del --chunks=chunks --key=obj1

// List objects
> dedupe test.idx list

// Check object existence
> dedupe test.idx exists --key=obj1

// Gather index stats
> dedupe test.idx stats
```

## Index Settings

Four parameters are used when creating the index: minimum chunk size, maximum chunk size, shift count, and boundary check bytes.  They are defined as follows:

- Minimum chunk size: the smallest amount of data that can be considered a chunk of data
- Maximum chunk size: the largest amount of data that can be considered a chunk of data
- Shift count: the number of bytes to move the sliding window while evaluating data in the window for a chunk boundary
- Boundary check bytes: the number of bytes in the MD5 hash of the data in the sliding window that is evaluated to identify a chunk boundary

The index parameters should be set in such a way to balance performance vs the frequency with which duplicate data is identified.  With smaller values, it is more likely the library will find repeated data patterns, but processing and storage will take longer and create more chunk records and files.  With larger values, the opposite is true.

In some cases, it is assumed that duplicate data will not be found within a file and that the library should only be used to identify large chunks of redundant data across larger files (for instance, copies of files with minor changes or backup files).  In such cases, use large values for the index settings.

In other cases, where it is assumed that duplicate data will be found within a file and across files, smaller values can be used.

Recommended settings for most environments (min, max, shift, boundary):

- For small file environments, use 2048, 16384, 128, and 2
- For large file environments, use 32768, 262144, 512, and 2 

## External Databases

External databases can be used with WatsonDedupe if you implement the ```WatsonDedupe.Database.DbProvider``` class.  You are free to use whatever table and column names you wish, since your code will be responsible for issuing queries and returning results using the WatsonDedupe classes.  Refer to the ```Test.External``` project for an example.

## Version History

Please refer to CHANGELOG.md.
