using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WatsonDedupe;

namespace Test
{
    class Program
    {
        static DedupeLibrary Dedupe;
        static bool DebugDedupe = true;
        static bool DebugSql = true;

        static void Main(string[] args)
        {
            bool runForever = true;
            string userInput = "";
            string filename = "";
            string key = "";
            long contentLength = 0;
            Stream stream = null; 
            List<Chunk> chunks;
            List<string> keys;
            ObjectMetadata md;

            int numObjects;
            int numChunks;
            long logicalBytes;
            long physicalBytes;
            decimal dedupeRatioX;
            decimal dedupeRatioPercent;

            Initialize();

            while (runForever)
            {
                Console.Write("Command [? for help] > ");
                userInput = Console.ReadLine();
                if (String.IsNullOrEmpty(userInput)) continue;

                switch (userInput)
                {
                    case "?":
                        Console.WriteLine("Available commands:");
                        Console.WriteLine("  q          quit");
                        Console.WriteLine("  cls        clear the screen");
                        Console.WriteLine("  store      store an object");
                        Console.WriteLine("  retrieve   retrieve an object");
                        Console.WriteLine("  delete     delete an object");
                        Console.WriteLine("  metadata   retrieve object metadata");
                        Console.WriteLine("  list       list objects in the index");
                        Console.WriteLine("  exists     check if object exists in the index");
                        Console.WriteLine("  stats      list index stats");
                        Console.WriteLine("");
                        break;

                    case "q":
                    case "Q":
                        runForever = false;
                        break;

                    case "cls":
                        Console.Clear();
                        break;

                    case "store":
                        filename = DedupeCommon.InputString("Input filename:", null, false);
                        key = DedupeCommon.InputString("Object key:", null, false);
                        contentLength = GetContentLength(filename);
                        using (FileStream fs = new FileStream(filename, FileMode.Open))
                        {
                            if (Dedupe.StoreObject(key, contentLength, fs, out chunks))
                            {
                                if (chunks != null && chunks.Count > 0)
                                { 
                                    Console.WriteLine("Success: " + chunks.Count + " chunks");
                                }
                                else
                                {
                                    Console.WriteLine("Success (no chunks)");
                                }
                            }
                            else
                            {
                                Console.WriteLine("Failed");
                            }
                        }
                        break;

                    case "retrieve":
                        key = DedupeCommon.InputString("Object key:", null, false);
                        filename = DedupeCommon.InputString("Output filename:", null, false);
                        if (Dedupe.RetrieveObject(key, out contentLength, out stream))
                        {
                            if (contentLength > 0)
                            {
                                using (FileStream fs = new FileStream(filename, FileMode.OpenOrCreate))
                                {
                                    int bytesRead = 0;
                                    long bytesRemaining = contentLength;
                                    byte[] readBuffer = new byte[65536];

                                    while (bytesRemaining > 0)
                                    {
                                        bytesRead = stream.Read(readBuffer, 0, readBuffer.Length);
                                        if (bytesRead > 0)
                                        {
                                            fs.Write(readBuffer, 0, bytesRead);
                                            bytesRemaining -= bytesRead;
                                        }
                                    }
                                } 

                                Console.WriteLine("Success");
                            }
                            else
                            {
                                Console.WriteLine("Success, (no data)");
                            }
                        }
                        else
                        {
                            Console.WriteLine("Failed");
                        }
                        break;

                    case "delete":
                        key = DedupeCommon.InputString("Object key:", null, false);
                        if (Dedupe.DeleteObject(key))
                        {
                            Console.WriteLine("Success");
                        }
                        else
                        {
                            Console.WriteLine("Failed");
                        }
                        break;

                    case "metadata":
                        key = DedupeCommon.InputString("Object key:", null, false);
                        if (Dedupe.RetrieveObjectMetadata(key, out md))
                        {
                            Console.WriteLine("Success");
                            Console.WriteLine(md.ToString());
                        }
                        else
                        {
                            Console.WriteLine("Failed");
                        }
                        break;

                    case "list":
                        Dedupe.ListObjects(out keys);
                        if (keys != null && keys.Count > 0)
                        {
                            Console.WriteLine("Objects: ");
                            foreach (string curr in keys) Console.WriteLine("  " + curr);
                            Console.WriteLine(keys.Count + " objects listed");
                        }
                        break;

                    case "exists":
                        key = DedupeCommon.InputString("Object name:", null, false);
                        if (Dedupe.ObjectExists(key))
                        {
                            Console.WriteLine("Object exists");
                        }
                        else
                        {
                            Console.WriteLine("Object does not exist");
                        }
                        break;

                    case "stats":
                        if (Dedupe.IndexStats(out numObjects, out numChunks, out logicalBytes, out physicalBytes, out dedupeRatioX, out dedupeRatioPercent))
                        {
                            Console.WriteLine("Statistics:");
                            Console.WriteLine("  Number of objects : " + numObjects);
                            Console.WriteLine("  Number of chunks  : " + numChunks);
                            Console.WriteLine("  Logical bytes     : " + logicalBytes + " bytes");
                            Console.WriteLine("  Physical bytes    : " + physicalBytes + " bytes");
                            Console.WriteLine("  Dedupe ratio      : " + DedupeCommon.DecimalToString(dedupeRatioX) + "X, " + DedupeCommon.DecimalToString(dedupeRatioPercent) + "%");
                            Console.WriteLine("");
                        }
                        else
                        {
                            Console.WriteLine("Failed");
                        }
                        break;

                    default:
                        break;
                } 
            }
        }

        static void Initialize()
        {
            if (!Directory.Exists("Chunks")) Directory.CreateDirectory("Chunks");
            if (File.Exists("Test.db"))
            {
                Dedupe = new DedupeLibrary("Test.db", WriteChunk, ReadChunk, DeleteChunk, DebugDedupe, DebugSql);
            }
            else
            {
                Dedupe = new DedupeLibrary("Test.db", 2048, 16384, 64, 2, WriteChunk, ReadChunk, DeleteChunk, DebugDedupe, DebugSql);
            }
        }

        static bool WriteChunk(Chunk data)
        {
            File.WriteAllBytes("Chunks\\" + data.Key, data.Value);
            using (var fs = new FileStream(
                "Chunks\\" + data.Key,
                FileMode.Create,
                FileAccess.Write,
                FileShare.None,
                0x1000,
                FileOptions.WriteThrough))
            {
                fs.Write(data.Value, 0, data.Value.Length);
            }
            return true;
        }

        static byte[] ReadChunk(string key)
        {
            return File.ReadAllBytes("Chunks\\" + key);
        }

        static bool DeleteChunk(string key)
        {
            try
            {
                File.Delete("Chunks\\" + key);
            }
            catch (Exception)
            {

            }
            return true;
        }

        static long GetContentLength(string filename)
        {
            FileInfo fi = new FileInfo(filename);
            return fi.Length;
        }
    }
}
