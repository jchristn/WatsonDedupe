using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using WatsonDedupe;

namespace Test.ReadStream
{
    class Program
    {
        static DedupeLibrary _Dedupe;
        static bool _DebugDedupe = true;
        static bool _DebugSql = true;

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
                        Console.WriteLine("  stream     open read stream on an object");
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
                        filename = InputString("Input filename:", null, false);
                        key = InputString("Object key:", null, false);
                        contentLength = GetContentLength(filename);
                        using (FileStream fs = new FileStream(filename, FileMode.Open))
                        {
                            if (_Dedupe.StoreObject(key, contentLength, fs, out chunks))
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
                        key = InputString("Object key:", null, false);
                        filename = InputString("Output filename:", null, false);
                        if (_Dedupe.RetrieveObject(key, out contentLength, out stream))
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
                        key = InputString("Object key:", null, false);
                        if (_Dedupe.DeleteObject(key))
                        {
                            Console.WriteLine("Success");
                        }
                        else
                        {
                            Console.WriteLine("Failed");
                        }
                        break;

                    case "metadata":
                        key = InputString("Object key:", null, false);
                        if (_Dedupe.RetrieveObjectMetadata(key, true, out md))
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
                        _Dedupe.ListObjects(out keys);
                        if (keys != null && keys.Count > 0)
                        {
                            Console.WriteLine("Objects: ");
                            foreach (string curr in keys) Console.WriteLine("  " + curr);
                            Console.WriteLine(keys.Count + " objects listed");
                        }
                        break;

                    case "exists":
                        key = InputString("Object name:", null, false);
                        if (_Dedupe.ObjectExists(key))
                        {
                            Console.WriteLine("Object exists");
                        }
                        else
                        {
                            Console.WriteLine("Object does not exist");
                        }
                        break;

                    case "stats":
                        if (_Dedupe.IndexStats(out numObjects, out numChunks, out logicalBytes, out physicalBytes, out dedupeRatioX, out dedupeRatioPercent))
                        {
                            Console.WriteLine("Statistics:");
                            Console.WriteLine("  Number of objects : " + numObjects);
                            Console.WriteLine("  Number of chunks  : " + numChunks);
                            Console.WriteLine("  Logical bytes     : " + logicalBytes + " bytes");
                            Console.WriteLine("  Physical bytes    : " + physicalBytes + " bytes");
                            Console.WriteLine("  Dedupe ratio      : " + DecimalToString(dedupeRatioX) + "X, " + DecimalToString(dedupeRatioPercent) + "%");
                            Console.WriteLine("");
                        }
                        else
                        {
                            Console.WriteLine("Failed");
                        }
                        break;

                    case "stream":
                        ReadStream();
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
                _Dedupe = new DedupeLibrary("Test.db", WriteChunk, ReadChunk, DeleteChunk, _DebugDedupe, _DebugSql);
            }
            else
            {
                // Dedupe = new DedupeLibrary("Test.db", 2048, 16384, 64, 2, WriteChunk, ReadChunk, DeleteChunk, DebugDedupe, DebugSql);
                _Dedupe = new DedupeLibrary("Test.db", 32768, 262144, 2048, 2, WriteChunk, ReadChunk, DeleteChunk, _DebugDedupe, _DebugSql);
            }
        }

        static void ReadStream()
        {
            string key = InputString("Object name:", null, false);
            if (!_Dedupe.ObjectExists(key))
            {
                Console.WriteLine("Object does not exist");
                return;
            }

            DedupeStream stream = null;
            if (!_Dedupe.RetrieveObjectStream(key, out stream))
            {
                Console.WriteLine("Unable to retrieve stream");
                return;
            }

            bool exiting = false;
            string userInput = null;
            byte[] buffer = null;
            int count = 0;
            int bytesRead = 0;

            while (!exiting)
            {
                Console.Write("Stream :: " + key + " [? for help] > ");
                userInput = Console.ReadLine();
                if (String.IsNullOrEmpty(userInput)) continue;

                switch (userInput)
                {
                    case "?":
                        Console.WriteLine("Available stream commands:");
                        Console.WriteLine("  q          exit stream menu");
                        Console.WriteLine("  cls        clear the screen");
                        Console.WriteLine("  pos        display stream position");
                        Console.WriteLine("  jump       jump to specific position");
                        Console.WriteLine("  begin      move to beginning of stream");
                        Console.WriteLine("  end        move to end of stream");
                        Console.WriteLine("  read       read a specified number of bytes");
                        Console.WriteLine("");
                        break;
                    case "q":
                        exiting = true;
                        break;
                    case "c":
                    case "cls":
                        Console.Clear();
                        break;
                    case "pos":
                        Console.WriteLine(stream.Position);
                        break;
                    case "jump":
                        Console.Write("Position: ");
                        stream.Position = Convert.ToInt64(Console.ReadLine());
                        break;
                    case "begin":
                        stream.Seek(0, SeekOrigin.Begin);
                        break;
                    case "end":
                        stream.Seek(0, SeekOrigin.End);
                        break;
                    case "read":
                        Console.Write("Count: ");
                        count = Convert.ToInt32(Console.ReadLine());
                        buffer = new byte[count];
                        bytesRead = stream.Read(buffer, 0, count);
                        if (bytesRead > 0)
                        {
                            Console.WriteLine(bytesRead + " bytes: " + Encoding.UTF8.GetString(buffer));
                        }
                        else
                        {
                            Console.WriteLine("0 bytes read");
                        }
                        break;
                }
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

        static string InputString(string question, string defaultAnswer, bool allowNull)
        {
            while (true)
            {
                Console.Write(question);

                if (!String.IsNullOrEmpty(defaultAnswer))
                {
                    Console.Write(" [" + defaultAnswer + "]");
                }

                Console.Write(" ");

                string userInput = Console.ReadLine();

                if (String.IsNullOrEmpty(userInput))
                {
                    if (!String.IsNullOrEmpty(defaultAnswer)) return defaultAnswer;
                    if (allowNull) return null;
                    else continue;
                }

                return userInput;
            }
        }

        static string DecimalToString(object obj)
        {
            if (obj == null) return null;
            string ret = string.Format("{0:N2}", obj);
            ret = ret.Replace(",", "");
            return ret;
        }
    }
}
