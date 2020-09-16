using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using WatsonDedupe;

namespace Test.ReadStream
{
    class Program
    {
        static DedupeSettings _Settings;
        static DedupeCallbacks _Callbacks;
        static DedupeLibrary _Dedupe;
        static IndexStatistics _Stats;
        static EnumerationResult _EnumResult;

        static void Main(string[] args)
        {
            bool runForever = true; 
            string filename = "";
            string key = "";
            long contentLength = 0;  
            DedupeObject obj = null; 
             
            Initialize();

            while (runForever)
            {
                Console.Write("Command [? for help] > ");
                string userInput = Console.ReadLine();
                if (String.IsNullOrEmpty(userInput)) continue;

                switch (userInput)
                {
                    case "?":
                        Console.WriteLine("Available commands:");
                        Console.WriteLine("  q          quit");
                        Console.WriteLine("  cls        clear the screen");
                        Console.WriteLine("  write      store an object");
                        Console.WriteLine("  get        retrieve an object");
                        Console.WriteLine("  del        delete an object");
                        Console.WriteLine("  md         retrieve object metadata");
                        Console.WriteLine("  list       list 100 objects in the index");
                        Console.WriteLine("  listp      paginated list objects"); 
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

                    case "write":
                        filename = InputString("Input filename:", null, false);
                        key = InputString("Object key:", null, false);
                        contentLength = GetContentLength(filename);
                        using (FileStream fs = new FileStream(filename, FileMode.Open))
                        {
                            _Dedupe.Write(key, contentLength, fs);
                        }
                        break;

                    case "get":
                        key = InputString("Object key:", null, false);
                        filename = InputString("Output filename:", null, false);
                        obj = _Dedupe.Get(key);
                        if (obj != null)
                        {
                            if (obj.Length > 0)
                            {
                                using (FileStream fs = new FileStream(filename, FileMode.OpenOrCreate))
                                {
                                    int bytesRead = 0;
                                    long bytesRemaining = obj.Length;
                                    byte[] readBuffer = new byte[65536];

                                    while (bytesRemaining > 0)
                                    {
                                        bytesRead = obj.DataStream.Read(readBuffer, 0, readBuffer.Length);
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

                    case "del":
                        key = InputString("Object key:", null, false);
                        _Dedupe.Delete(key);
                        break;

                    case "md":
                        key = InputString("Object key:", null, false);
                        obj = _Dedupe.GetMetadata(key);
                        if (obj != null)
                        {
                            Console.WriteLine("Success");
                            Console.WriteLine(obj.ToString());
                        }
                        else
                        {
                            Console.WriteLine("Failed");
                        }
                        break;

                    case "list":
                        _EnumResult = _Dedupe.ListObjects();
                        if (_EnumResult == null)
                        {
                            Console.WriteLine("No objects");
                        }
                        else
                        {
                            Console.WriteLine(_EnumResult.ToTabularString());
                        }
                        break;

                    case "exists":
                        key = InputString("Object key:", null, false);
                        if (_Dedupe.Exists(key))
                        {
                            Console.WriteLine("Object exists");
                        }
                        else
                        {
                            Console.WriteLine("Object does not exist");
                        }
                        break;

                    case "stats":
                        _Stats = _Dedupe.IndexStats();
                        if (_Stats != null)
                        {
                            Console.WriteLine("Statistics:");
                            Console.WriteLine("  Number of objects : " + _Stats.Objects);
                            Console.WriteLine("  Number of chunks  : " + _Stats.Chunks);
                            Console.WriteLine("  Logical bytes     : " + _Stats.LogicalBytes + " bytes");
                            Console.WriteLine("  Physical bytes    : " + _Stats.PhysicalBytes + " bytes");
                            Console.WriteLine("  Dedupe ratio      : " + DecimalToString(_Stats.RatioX) + "X, " + DecimalToString(_Stats.RatioPercent) + "%");
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
            _Settings = new DedupeSettings(32768, 262144, 2048, 2);
            _Callbacks = new DedupeCallbacks(WriteChunk, ReadChunk, DeleteChunk);
            _Dedupe = new DedupeLibrary("test.db", _Settings, _Callbacks); 
        }

        static void ReadStream()
        {
            string key = InputString("Object key:", null, false);
            if (!_Dedupe.Exists(key))
            {
                Console.WriteLine("Object does not exist");
                return;
            }

            DedupeStream stream = _Dedupe.GetStream(key);
            if (stream == null)
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

        static void WriteChunk(DedupeChunk data)
        {
            // File.WriteAllBytes("Chunks\\" + data.Key, data.Value);
            using (var fs = new FileStream(
                "Chunks\\" + data.Key,
                FileMode.Create,
                FileAccess.Write,
                FileShare.None,
                0x1000,
                FileOptions.WriteThrough))
            {
                fs.Write(data.Data, 0, data.Data.Length);
            }
        }

        static byte[] ReadChunk(string key)
        {
            return File.ReadAllBytes("Chunks\\" + key);
        }

        static void DeleteChunk(string key)
        {
            File.Delete("Chunks\\" + key);
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
