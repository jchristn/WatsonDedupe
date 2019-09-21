using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using WatsonDedupe;

namespace Cli
{
    class Program
    {
        static string Command;
        static string IndexFile;
        static string ChunkDirectory;
        static string ObjectKey;
        static string CreateParams;
        static int MinChunkSize = 0;
        static int MaxChunkSize = 0;
        static int ShiftCount = 0;
        static int BoundaryCheckBytes = 0;

        static bool DebugDedupe = false;
        static bool DebugSql = false;
        static DedupeLibrary Dedupe;

        static int NumObjects;
        static int NumChunks;
        static long LogicalBytes;
        static long PhysicalBytes;
        static decimal DedupeRatioX;
        static decimal DedupeRatioPercent;
        static byte[] ResponseData;
        static byte[] RequestData;
        static List<Chunk> Chunks;
        static List<string> Keys;
        static ObjectMetadata Metadata;

        static void Main(string[] args)
        {
            try
            {
                #region Parse-Arguments

                if (args == null || args.Length < 2)
                {
                    Usage("No arguments specified");
                    return;
                }

                IndexFile = args[0];
                Command = args[1];

                for (int i = 2; i < args.Length; i++)
                {
                    if (String.IsNullOrEmpty(args[i])) continue;
                    if (args[i].StartsWith("--chunks=") && args[i].Length > 9)
                    {
                        ChunkDirectory = args[i].Substring(9);
                        if (!ChunkDirectory.EndsWith("\\")) ChunkDirectory += "\\";
                        if (!Directory.Exists(ChunkDirectory)) Directory.CreateDirectory(ChunkDirectory);
                    }
                    else if (args[i].StartsWith("--key=") && args[i].Length > 6)
                    {
                        ObjectKey = args[i].Substring(6);
                    }
                    else if (String.Compare(args[i], "--debug") == 0)
                    {
                        DebugDedupe = true;
                    }
                    else if (String.Compare(args[i], "--debugsql") == 0)
                    {
                        DebugSql = true;
                    }
                    else if (args[i].StartsWith("--params=") && args[i].Length > 9)
                    {
                        CreateParams = args[i].Substring(9);
                        if (new Regex(@"^\d+,\d+,\d+,\d+$").IsMatch(CreateParams))
                        {
                            string[] currParams = CreateParams.Split(',');
                            if (currParams.Length != 4)
                            {
                                Usage("Value for 'params' is invalid");
                                return;
                            }

                            if (!Int32.TryParse(currParams[0], out MinChunkSize)
                                || !Int32.TryParse(currParams[1], out MaxChunkSize)
                                || !Int32.TryParse(currParams[2], out ShiftCount)
                                || !Int32.TryParse(currParams[3], out BoundaryCheckBytes)
                                )
                            {
                                Usage("Value for 'params' is not of the form int,int,int,int");
                                return;
                            }
                        }
                        else
                        {
                            Usage("Value for 'params' is not of the form int,int,int,int");
                            return;
                        }
                    }
                    else
                    {
                        Usage("Unknown argument: " + args[i]);
                        return;
                    }
                }

                #endregion

                #region Verify-Values

                List<string> validCommands = new List<string>() { "create", "stats", "store", "retrieve", "delete", "list", "exists", "metadata" };
                if (!validCommands.Contains(Command))
                {
                    Usage("Invalid command: " + Command);
                    return;
                }

                #endregion

                #region Enumerate

                if (DebugDedupe)
                {
                    Console.WriteLine("Command         : " + Command);
                    Console.WriteLine("Index File      : " + IndexFile);
                    Console.WriteLine("Chunk Directory : " + ChunkDirectory);
                    Console.WriteLine("Object Key      : " + ObjectKey);
                    if (MinChunkSize > 0) Console.WriteLine("Min Chunk Size  : " + MinChunkSize);
                    if (MaxChunkSize > 0) Console.WriteLine("Max Chunk Size  : " + MaxChunkSize);
                    if (ShiftCount > 0) Console.WriteLine("Shift Count     : " + ShiftCount);
                    if (BoundaryCheckBytes > 0) Console.WriteLine("Boundary Bytes  : " + BoundaryCheckBytes);
                }

                #endregion

                #region Create

                if (String.Compare(Command, "create") == 0)
                {
                    Dedupe = new DedupeLibrary(IndexFile, MinChunkSize, MaxChunkSize, ShiftCount, BoundaryCheckBytes, WriteChunk, ReadChunk, DeleteChunk, DebugDedupe, DebugSql);
                    if (DebugDedupe) Console.WriteLine("Successfully wrote new index: " + IndexFile);
                    return;
                }

                #endregion

                #region Initialize-Index

                if (!File.Exists(IndexFile))
                {
                    Console.WriteLine("*** Index file " + IndexFile + " not found");
                }

                Dedupe = new DedupeLibrary(IndexFile, WriteChunk, ReadChunk, DeleteChunk, DebugDedupe, DebugSql);

                #endregion

                #region Process-by-Command

                switch (Command)
                {
                    case "stats":
                        if (Dedupe.IndexStats(out NumObjects, out NumChunks, out LogicalBytes, out PhysicalBytes, out DedupeRatioX, out DedupeRatioPercent))
                        {
                            Console.WriteLine("Statistics:");
                            Console.WriteLine("  Number of objects : " + NumObjects);
                            Console.WriteLine("  Number of chunks  : " + NumChunks);
                            Console.WriteLine("  Logical bytes     : " + LogicalBytes + " bytes");
                            Console.WriteLine("  Physical bytes    : " + PhysicalBytes + " bytes");
                            Console.WriteLine("  Dedupe ratio      : " + DecimalToString(DedupeRatioX) + "X, " + DecimalToString(DedupeRatioPercent) + "%");
                            return;
                        }
                        else
                        {
                            Console.WriteLine("Failed");
                        }
                        return;

                    case "retrieve":
                        if (String.IsNullOrEmpty(ObjectKey))
                        {
                            Usage("Object key must be supplied");
                        }
                        else
                        {
                            if (!Dedupe.RetrieveObject(ObjectKey, out ResponseData))
                            {
                                Console.WriteLine("Failed");
                            }
                            else
                            {
                                WriteConsoleData(ResponseData);
                            }
                        }
                        return;

                    case "store":
                        if (String.IsNullOrEmpty(ObjectKey))
                        {
                            Usage("Object key must be supplied");
                        }
                        else
                        {
                            if (Dedupe.ObjectExists(ObjectKey))
                            {
                                Console.WriteLine("Already exists");
                            }
                            else
                            {
                                ReadConsoleData();
                                if (!Dedupe.StoreObject(ObjectKey, RequestData, out Chunks))
                                {
                                    Console.WriteLine("Failed");
                                }
                                else
                                {
                                    Console.WriteLine("Success");
                                }
                            }
                        }
                        return;

                    case "delete":
                        if (String.IsNullOrEmpty(ObjectKey))
                        {
                            Usage("Object key must be supplied");
                        }
                        else
                        {
                            if (!Dedupe.DeleteObject(ObjectKey))
                            {
                                Console.WriteLine("Failed");
                            }
                            else
                            {
                                Console.WriteLine("Success");
                            }
                        }
                        return;

                    case "metadata":
                        if (String.IsNullOrEmpty(ObjectKey))
                        {
                            Usage("Object key must be supplied");
                        }
                        else
                        {
                            if (!Dedupe.RetrieveObjectMetadata(ObjectKey, out Metadata))
                            {
                                Console.WriteLine("Failed");
                            }
                            else
                            {
                                Console.WriteLine(Metadata.ToString());
                            }
                        }
                        return;

                    case "list":
                        Dedupe.ListObjects(out Keys);
                        if (Keys == null || Keys.Count < 1)
                        {
                            Console.WriteLine("No objects");
                        }
                        else
                        {
                            Console.WriteLine("Objects:");
                            foreach (string curr in Keys) Console.WriteLine("  " + curr);
                            Console.WriteLine(Keys.Count + " objects in index");
                        }
                        return;

                    case "exists":
                        if (String.IsNullOrEmpty(ObjectKey))
                        {
                            Usage("Object key must be supplied");
                            return;
                        }
                        else
                        {
                            Console.WriteLine(Dedupe.ObjectExists(ObjectKey));
                        }
                        return;

                    default:
                        Usage("Unknown command: " + Command);
                        return;
                }

                #endregion
            }
            catch (Exception e)
            {
                ExceptionConsole("Dedupe", "Outer exception", e);
            }
        }

        static void ExceptionConsole(string method, string text, Exception e)
        {
            var st = new StackTrace(e, true);
            var frame = st.GetFrame(0);
            int line = frame.GetFileLineNumber();
            string filename = frame.GetFileName();

            Console.WriteLine("---");
            Console.WriteLine("An exception was encountered which triggered this message.");
            Console.WriteLine("  Method: " + method);
            Console.WriteLine("  Text: " + text);
            Console.WriteLine("  Type: " + e.GetType().ToString());
            Console.WriteLine("  Data: " + e.Data);
            Console.WriteLine("  Inner: " + e.InnerException);
            Console.WriteLine("  Message: " + e.Message);
            Console.WriteLine("  Source: " + e.Source);
            Console.WriteLine("  StackTrace: " + e.StackTrace);
            Console.WriteLine("  Stack: " + StackToString());
            Console.WriteLine("  Line: " + line);
            Console.WriteLine("  File: " + filename);
            Console.WriteLine("  ToString: " + e.ToString());
            Console.WriteLine("---");

            return;
        }

        static string StackToString()
        {
            string ret = "";

            StackTrace t = new StackTrace();
            for (int i = 0; i < t.FrameCount; i++)
            {
                if (i == 0)
                {
                    ret += t.GetFrame(i).GetMethod().Name;
                }
                else
                {
                    ret += " <= " + t.GetFrame(i).GetMethod().Name;
                }
            }

            return ret;
        }

        static void WriteConsoleData(byte[] data)
        {
            if (data == null)
            {
                data = new byte[1];
                data[0] = 0x00;
            }

            using (Stream stdout = Console.OpenStandardOutput())
            {
                stdout.Write(data, 0, data.Length);
            }
        }

        static void ReadConsoleData()
        {
            using (Stream stdin = Console.OpenStandardInput())
            {
                byte[] buffer = new byte[2048];
                int bytes;
                while ((bytes = stdin.Read(buffer, 0, buffer.Length)) > 0)
                {
                    if (RequestData == null)
                    {
                        RequestData = new byte[bytes];
                        Buffer.BlockCopy(buffer, 0, RequestData, 0, bytes);
                    }
                    else
                    {
                        byte[] tempData = new byte[RequestData.Length + bytes];
                        Buffer.BlockCopy(RequestData, 0, tempData, 0, RequestData.Length);
                        Buffer.BlockCopy(buffer, 0, tempData, RequestData.Length, bytes);
                        RequestData = tempData;
                    }
                }
            }
        }

        static bool WriteChunk(Chunk data)
        {
            using (var fs = new FileStream(
                ChunkDirectory + data.Key,
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
            return File.ReadAllBytes(ChunkDirectory + key);
        }

        static bool DeleteChunk(string key)
        {
            try
            {
                File.Delete(ChunkDirectory + key);
            }
            catch (Exception)
            {

            }
            return true;
        }

        static void Usage(string msg)
        {
            if (!String.IsNullOrEmpty(msg))
            {
                Console.WriteLine("*** " + msg);
                Console.WriteLine("");
            }

            //          1         2         3         4         5         6         7        
            // 12345678901234567890123456789012345678901234567890123456789012345678901234567890
            Console.WriteLine("Dedupe CLI v" + Version());
            Console.WriteLine("Usage:");
            Console.WriteLine("$ dedupe [index] [command] [options]");
            Console.WriteLine("");
            Console.WriteLine("Where [index] is the deduplication index database, and command is one of:");
            Console.WriteLine("  create              Create the index (supply --params)");
            Console.WriteLine("  stats               Gather deduplication stats from the index");
            Console.WriteLine("  store               Write an object to the index");
            Console.WriteLine("  retrieve            Retrieve an object from the index");
            Console.WriteLine("  metadata            Retrieve metadata about an object");
            Console.WriteLine("  delete              Delete an object from the index");
            Console.WriteLine("  list                List the objects in the index");
            Console.WriteLine("  exists              Check if an object exists in the index");
            Console.WriteLine("");
            Console.WriteLine("Where [options] are:");
            Console.WriteLine("  --chunks=[dir]      Directory where chunks are stored");
            Console.WriteLine("  --key=[name]        The object key to store or retrieve");
            Console.WriteLine("  --debug             Enable dedupe debug logging to the console");
            Console.WriteLine("  --debugsql          Enable SQL debug logging to the console");
            Console.WriteLine("  --params=[params]   Index creation parameters");
            Console.WriteLine("");
            Console.WriteLine("Creating an index");
            Console.WriteLine("  When creating a container, use the following value for --params:");
            Console.WriteLine("  [minchunksize],[maxchunksize],[shiftcount],[boundarycheckbytes]");
            Console.WriteLine("  Where: ");
            Console.WriteLine("    minchunksize        Minimum length of data to be considered a chunk");
            Console.WriteLine("    maxchunksize        Maximum length of data to be considered a chunk");
            Console.WriteLine("    shiftcount          Number of bytes to shift while locating a chunk");
            Console.WriteLine("    boundarycheckbytes  Number of bytes to compare while locating a chunk");
            Console.WriteLine("");
            Console.WriteLine("Storing an object:");
            Console.WriteLine("  $ dedupe [index] store --key=[key] --chunks=[dir] < file.txt");
            Console.WriteLine("  $ echo Some data! | dedupe [index] store --key=[key] --chunks=[dir]");
            Console.WriteLine("");
            Console.WriteLine("Retrieving an object:");
            Console.WriteLine("  $ dedupe [index] retrieve --key=[key] --chunks=[dir] > file.txt");
            Console.WriteLine("");
        }

        static string Version()
        {
            Assembly assembly = System.Reflection.Assembly.GetExecutingAssembly();
            FileVersionInfo fvi = FileVersionInfo.GetVersionInfo(assembly.Location);
            return fvi.FileVersion;
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
