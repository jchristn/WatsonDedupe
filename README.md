# Watson Deduplication Library

[![][nuget-img]][nuget]

[nuget]:     https://www.nuget.org/packages/WatsonDedupe/
[nuget-img]: https://badge.fury.io/nu/Object.svg

Self-contained C# library for data deduplication in C# using Sqlite. 

## Help and Support
Please contact me for any issues or enhancement requests!  I'm at joel at maraudersoftware dot com.  This is an early release and it works well, however, I need to spend more time on performance.  

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
	if (Dedupe.IndexStats(out numChunks, out logicalBytes, out physicalBytes, out dedupeRatioX, out dedupeRatioPercent))
	{
	    Console.WriteLine("Statistics:");
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

## Running under Mono
Watson works well in Mono environments to the extent that we have tested it. It is recommended that when running under Mono, you execute the containing EXE using --server and after using the Mono Ahead-of-Time Compiler (AOT).  You may want to use the AOT on the CLI app as well.
```
mono --aot=nrgctx-trampolines=8096,nimt-trampolines=8096,ntrampolines=4048 --server myapp.exe
mono --server myapp.exe
```