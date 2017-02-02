# Watson Deduplication Library

[![][nuget-img]][nuget]

[nuget]:     https://www.nuget.org/packages/WatsonDedupe/
[nuget-img]: https://badge.fury.io/nu/Object.svg

Self-contained C# library for data deduplication using Sqlite. 

## Help and Support
Please contact me for any issues or enhancement requests!  I'm at joel at maraudersoftware dot com.  This is an early release and it works well, however, I need to spend more time on performance.  

## Installing with NuGet
Due to some unforseen issues with NuGet, you must download and add sqlite3.dll to your project manually.  Set it to copy to output always.

## Under the Hood
The Watson Dedupe library will take an incoming byte array (which you give an object key name) and utilize a sliding window performing MD5 calculations over the data in the window to identify breakpoints in the data (this is called 'chunking').  Each chunk of data is assigned a chunk key (based on the SHA256 of the data).  MD5 is used for breakpoint identification for speed, and SHA256 is used for key assignment to practically eliminate the likelihood of hash collisions.  Tables in a Sqlite database are maintained to indicate which object keys map to which chunk keys and their ordering/position.  Chunks are stored in a directory you specify.  On retrieval, the object key data is retrieved from the index, the appropriate chunk keys are retrieved, and the object is reconstructed.  As long as the chunk data is consistent across analyzed data sets, identical chunk keys will be created, meaning duplicate data chunks are only stored once.  Further, each chunk key has a separate associated reference count to ensure that chunks are not garbage collected when a referencing object is deleted should another object also hold that reference.

## Build process
Copy either the x86 or x64 SQLite.Interop.Dll from the DedupeLibrary\bin\debug or DedupeLibrary\bin\release into your project folder.  If you clean the solution, you'll have to do this again.  This will also need to be done for the test application and CLI application.

## Test App
A test project is included which will help you exercise the class library.

## CLI
A CLI project is also included which provides a binary that can be used to interact with the index for object storage, retrieval, removal, and statistics.  CLI examples are shown below.

## Library Example
The library requires that you implement three functions within your app for managing chunk data, specifically, writing, reading, and deleting.  This was done to provide you with flexibility on where you store chunk data and how you manage it.
```
using WatsonDedupe;

static DedupeLibrary Dedupe;
static List<Chunk> Chunks;
static string Key;
static List<string> Keys;
static byte[] Data;

static bool DebugDedupe = false;
static bool DebugSql = false;
static int NumObjects;
static int NumChunks;
static long LogicalBytes;
static long PhysicalBytes;
static decimal DedupeRatioX;
static decimal DedupeRatioPercent;

static void Main(string[] args)
{
	// Initialize existing index from file
	Dedupe = new DedupeLibrary("Test.idx", WriteChunk, ReadChunk, DeleteChunk, DebugDedupe, DebugSql);

	// Or, create a new index
	Dedupe = new DedupeLibrary("Test.idx", 1024, 32768, 64, 2, WriteChunk, ReadChunk, DeleteChunk, DebugDedupe, DebugSql);

	// Store an object in the index
	if (Dedupe.StoreObject(Key, Data, out Chunks)) Console.WriteLine("Success");

	// Retrieve an object from the index
	if (Dedupe.RetrieveObject(Key, out Data)) Console.WriteLine("Success");

	// Delete an object from the index
	if (Dedupe.DeleteObject(Key)) Console.WriteLine("Success");

	// Check if object exists in the index
	if (Dedupe.ObjectExists(Key)) Console.WriteLine("Exists");

	// List all objects
	Dedupe.ListObjects(out Keys);

	// Gather index and dedupe stats
	if (Dedupe.IndexStats(out NumObjects, out NumChunks, out LogicalBytes, out PhysicalBytes, out DedupeRatioX, out DedupeRatioPercent))
	{
	    Console.WriteLine("Statistics:");
	    Console.WriteLine("  Number of objects : " + NumObjects);
	    Console.WriteLine("  Number of chunks  : " + NumChunks);
	    Console.WriteLine("  Logical bytes     : " + LogicalBytes + " bytes");
	    Console.WriteLine("  Physical bytes    : " + PhysicalBytes + " bytes");
	    Console.WriteLine("  Dedupe ratio      : " + DedupeRatioX + "X, " + DedupeRatioPercent + "%");
	    Console.WriteLine("");
	}
}

// Called during store operations, consider using FileStream with FileOptions.WriteThrough to ensure crash consistency
static bool WriteChunk(Chunk data)
{
	File.WriteAllBytes("Chunks\\" + data.Key, data.Value);
    return true;
}

// Called during read operations
static byte[] ReadChunk(string key)
{
    return File.ReadAllBytes("Chunks\\" + key);
}

// Called during delete operations
static bool DeleteChunk(string key)
{
	File.Delete("Chunks\\" + key);
    return true;
}
```

## CLI Example
```
// Initialize new index
> dedupecli test.idx create --params=128,4096,16,2

// Store an object
> dedupecli test.idx store --chunks=chunks --key=obj1 < obj1.txt

// Retrieve an object
> dedupecli test.idx retrieve --chunks=chunks --key=obj1 > obj1_new.txt

// Delete an object
> dedupecli test.idx delete --chunks=chunks --key=obj1

// List objects
> dedupecli test.idx list

// Check object existence
> dedupecli test.idx exists --key=obj1

// Gather index stats
> dedupecli test.idx stats
```

## Index Settings
Four parameters are used when creating the index: minimum chunk size, maximum chunk size, shift count, and boundary check bytes.  They are defined as follows:

- Minimum chunk size: the smallest amount of data that can be considered a chunk of data
- Maximum chunk size: the largest amount of data that can be considered a chunk of data
- Shift count: the number of bytes to move the sliding window while evaluating data in the window for a chunk boundary
- Boundary check bytes: the number of bytes in the MD5 hash of the data in the sliding window that is evaluated to identify a chunk boundary

The index parameters should be set in such a way to balance performance vs the frequency with which duplicate data is identified.  Generally speaking, the smaller the values for these, the more likely the library will be in finding repeated data patterns, and, the larger the values for these, the less likely the library will be in finding repeated data patterns.

Similarly, the smaller the value for these, the slower the library will perform, and the larger the value for these, the faster the library will perform.  Smaller values for these settings requires that more data be evaluated (via MD5) and more records be inserted into Sqlite.  Larger values for these settings requires less data analysis and fewer records inserted into Sqlite.

In some cases, it is assumed that duplicate data will not be found within a file and that the library should only be used to identify large chunks of redundant data across large objects (for instance, copies of files with minor changes).  In such cases, use large values for the index settings.

In other cases, where it is assumed that duplicate data will be found within a file and across files, smaller values can be used.

Recommended settings for most environments (min, max, shift, boundary):
- For small file environments, use 256, 4096, 16, and 2
- For large file environments, use 8192, 65536, 512, and 3

## Running under Mono
This library uses Mono.Data.Sqlite which requires sqlite3.dll.  sqlite3.dll has been manually added to each project with its copy setting set to "always copy".  You may want to use the Mono AOT (ahead of time) compiler prior to using any binary that includes this library on Mono.
