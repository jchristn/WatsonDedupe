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