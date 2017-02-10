using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WatsonDedupe;

namespace DedupeTestXL
{
    class Test
    {
        static DedupeLibraryXL Dedupe;
        static bool DebugDedupe = false;
        static bool DebugSql = false;

        static void Main(string[] args)
        {
            bool runForever = true;
            string userInput = "";
            string filename = "";
            string objectIndex = "";
            string key = "";
            byte[] data;
            List<Chunk> chunks;
            List<string> keys;

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
                        filename = Common.InputString("Input filename:", null, false);
                        objectIndex = Common.InputString("Object index:", null, false);
                        key = Common.InputString("Object key:", null, false);
                        data = File.ReadAllBytes(filename);
                        if (Dedupe.StoreObject(key, objectIndex, data, out chunks))
                        {
                            if (chunks != null && chunks.Count > 0)
                            {
                                Console.WriteLine("MD5: " + Common.BytesToBase64(Common.Md5(data)));
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
                        break;

                    case "retrieve":
                        key = Common.InputString("Object key:", null, false);
                        objectIndex = Common.InputString("Object index:", null, false);
                        filename = Common.InputString("Output filename:", null, false);
                        if (Dedupe.RetrieveObject(key, objectIndex, out data))
                        {
                            if (data != null && data.Length > 0)
                            {
                                Console.WriteLine("MD5: " + Common.BytesToBase64(Common.Md5(data)));
                                File.WriteAllBytes(filename, data);
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
                        key = Common.InputString("Object key:", null, false);
                        objectIndex = Common.InputString("Object index:", null, false);
                        if (Dedupe.DeleteObject(key, objectIndex))
                        {
                            Console.WriteLine("Success");
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
                        key = Common.InputString("Object name:", null, false);
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
                            Console.WriteLine("  Dedupe ratio      : " + Common.DecimalToString(dedupeRatioX) + "X, " + Common.DecimalToString(dedupeRatioPercent) + "%");
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
            if (File.Exists("Test.idx"))
            {
                Dedupe = new DedupeLibraryXL("Test.idx", WriteChunk, ReadChunk, DeleteChunk, DebugDedupe, DebugSql);
            }
            else
            {
                Dedupe = new DedupeLibraryXL("Test.idx", 1024, 32768, 64, 2, WriteChunk, ReadChunk, DeleteChunk, DebugDedupe, DebugSql);
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
    }
}
