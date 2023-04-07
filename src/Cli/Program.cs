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
        static string _Command = null;
        static string _IndexFile = null;
        static string _ChunkDir = null;
        static string _Key = null;
        static string _CreateParams = null;
        static int _MinChunkSize = 0;
        static int _MaxChunkSize = 0;
        static int _ShiftCount = 0;
        static int _BoundaryCheckBytes = 0;
        static int _IndexStart = 0;
        static int _MaxResults = 100;

        static DedupeSettings _Settings = null;
        static DedupeCallbacks _Callbacks = null;
        static DedupeLibrary _Dedupe = null;
        static IndexStatistics _Stats = null;
         
        static byte[] _Request = null;
        static EnumerationResult _EnumResult = null;
        static DedupeObject _Object = null;

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

                _IndexFile = args[0];
                _Command = args[1];

                for (int i = 2; i < args.Length; i++)
                {
                    if (String.IsNullOrEmpty(args[i])) continue;
                    if (args[i].StartsWith("--chunks=") && args[i].Length > 9)
                    {
                        _ChunkDir = args[i].Substring(9);
                        if (!_ChunkDir.EndsWith("\\")) _ChunkDir += "\\";
                        if (!Directory.Exists(_ChunkDir)) Directory.CreateDirectory(_ChunkDir);
                    }
                    else if (args[i].StartsWith("--key=") && args[i].Length > 6)
                    {
                        _Key = args[i].Substring(6);
                    }
                    else if (args[i].StartsWith("--idxstart=") && args[i].Length > 11)
                    {
                        if (!Int32.TryParse(args[i].Substring(11), out _IndexStart))
                        {
                            Usage("Index start must be an integer value.");
                            return;
                        }
                        else
                        {
                            if (_IndexStart < 0)
                            {
                                Usage("Index start must be greater than zero.");
                                return;
                            }
                        }
                    }
                    else if (args[i].StartsWith("--results=") && args[i].Length > 10)
                    {
                        if (!Int32.TryParse(args[i].Substring(10), out _MaxResults))
                        {
                            Usage("Max results must be an integer value.");
                            return;
                        }
                        else
                        {
                            if (_MaxResults < 1 || _MaxResults > 100)
                            {
                                Usage("Max results must be greater than zero and less than or equal to 100.");
                                return;
                            }
                        }
                    }
                    else if (args[i].StartsWith("--params=") && args[i].Length > 9)
                    {
                        _CreateParams = args[i].Substring(9);
                        if (new Regex(@"^\d+,\d+,\d+,\d+$").IsMatch(_CreateParams))
                        {
                            string[] currParams = _CreateParams.Split(',');
                            if (currParams.Length != 4)
                            {
                                Usage("Value for 'params' is invalid");
                                return;
                            }

                            if (!Int32.TryParse(currParams[0], out _MinChunkSize)
                                || !Int32.TryParse(currParams[1], out _MaxChunkSize)
                                || !Int32.TryParse(currParams[2], out _ShiftCount)
                                || !Int32.TryParse(currParams[3], out _BoundaryCheckBytes)
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

                List<string> validCommands = new List<string>() { "create", "stats", "write", "get", "del", "list", "exists", "md" };
                if (!validCommands.Contains(_Command))
                {
                    Usage("Invalid command: " + _Command);
                    return;
                }

                #endregion
                 
                #region Create

                if (String.Compare(_Command, "create") == 0)
                {
                    _Settings = new DedupeSettings(_MinChunkSize, _MaxChunkSize, _ShiftCount, _BoundaryCheckBytes);
                    _Callbacks = new DedupeCallbacks(WriteChunk, ReadChunk, DeleteChunk);
                    _Dedupe = new DedupeLibrary(_IndexFile, _Settings, _Callbacks);
                    return;
                }

                #endregion

                #region Initialize-Index

                if (!File.Exists(_IndexFile))
                {
                    Console.WriteLine("*** Index file " + _IndexFile + " not found");
                }

                _Settings = new DedupeSettings();
                _Callbacks = new DedupeCallbacks(WriteChunk, ReadChunk, DeleteChunk);
                _Dedupe = new DedupeLibrary(_IndexFile, _Settings, _Callbacks);

                #endregion

                #region Process-by-Command

                switch (_Command)
                {
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
                            return;
                        }
                        else
                        {
                            Console.WriteLine("Failed");
                        }
                        return;

                    case "get":
                        if (String.IsNullOrEmpty(_Key))
                        {
                            Usage("Object key must be supplied");
                        }
                        else
                        {
                            _Object = _Dedupe.Get(_Key);
                            if (_Object == null)
                            {
                                Console.WriteLine("Failed");
                            }
                            else
                            {
                                WriteConsoleData(_Object.Data);
                            }
                        }
                        return;

                    case "write":
                        if (String.IsNullOrEmpty(_Key))
                        {
                            Usage("Object key must be supplied");
                        }
                        else
                        {
                            if (_Dedupe.Exists(_Key))
                            {
                                Console.WriteLine("Already exists");
                            }
                            else
                            {
                                ReadConsoleData();
                                _Dedupe.Write(_Key, _Request);
                            }
                        }
                        return;

                    case "del":
                        if (String.IsNullOrEmpty(_Key))
                        {
                            Usage("Object key must be supplied");
                        }
                        else
                        {
                            _Dedupe.Delete(_Key);
                        }
                        return;

                    case "md":
                        if (String.IsNullOrEmpty(_Key))
                        {
                            Usage("Object key must be supplied");
                        }
                        else
                        {
                            _Object = _Dedupe.GetMetadata(_Key);
                            if (_Object == null)
                            {
                                Console.WriteLine("Failed");
                            }
                            else
                            {
                                Console.WriteLine(_Object.ToString());
                            }
                        }
                        return;

                    case "list":
                        _EnumResult = _Dedupe.ListObjects(_Key, _IndexStart, _MaxResults);
                        if (_EnumResult == null)
                        {
                            Console.WriteLine("No objects");
                        }
                        else
                        {
                            Console.WriteLine(_EnumResult.ToTabularString());
                        }
                        return;

                    case "exists":
                        if (String.IsNullOrEmpty(_Key))
                        {
                            Usage("Object key must be supplied");
                            return;
                        }
                        else
                        {
                            Console.WriteLine(_Dedupe.Exists(_Key));
                        }
                        return;

                    default:
                        Usage("Unknown command: " + _Command);
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
                    if (_Request == null)
                    {
                        _Request = new byte[bytes];
                        Buffer.BlockCopy(buffer, 0, _Request, 0, bytes);
                    }
                    else
                    {
                        byte[] tempData = new byte[_Request.Length + bytes];
                        Buffer.BlockCopy(_Request, 0, tempData, 0, _Request.Length);
                        Buffer.BlockCopy(buffer, 0, tempData, _Request.Length, bytes);
                        _Request = tempData;
                    }
                }
            }
        }

        static void WriteChunk(DedupeChunk data)
        {
            using (var fs = new FileStream(
                _ChunkDir + data.Key,
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
            return File.ReadAllBytes(_ChunkDir + key);
        }

        static void DeleteChunk(string key)
        {
            File.Delete(_ChunkDir + key);
        }

        static void Usage(string msg)
        {
            //          1         2         3         4         5         6         7        
            // 12345678901234567890123456789012345678901234567890123456789012345678901234567890
            Console.WriteLine("Dedupe CLI v" + Version());
            Console.WriteLine("Usage:");
            Console.WriteLine("$ dedupe [index] [command] [options]");
            Console.WriteLine("");
            Console.WriteLine("Where [index] is the deduplication index database, and command is one of:");
            Console.WriteLine("  create              Create the index (supply --params)");
            Console.WriteLine("  stats               Gather deduplication stats from the index");
            Console.WriteLine("  write               Write an object to the index");
            Console.WriteLine("  get                 Retrieve an object from the index");
            Console.WriteLine("  md                  Retrieve metadata about an object");
            Console.WriteLine("  del                 Delete an object from the index");
            Console.WriteLine("  list                List the objects in the index");
            Console.WriteLine("  exists              Check if an object exists in the index");
            Console.WriteLine("");
            Console.WriteLine("Where [options] are:");
            Console.WriteLine("  --chunks=[dir]      Directory where chunks are stored");
            Console.WriteLine("  --key=[name]        Object key to store, retrieve, or prefix to filter enumeration"); 
            Console.WriteLine("  --params=[params]   Index creation parameters");
            Console.WriteLine("  --idxstart=[#]      Object index from which to start enumeration");
            Console.WriteLine("  --results=[#]       Maximum number of results to retrieve (up to 100)");
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
            Console.WriteLine("  $ dedupe [index] write --key=[key] --chunks=[dir] < file.txt");
            Console.WriteLine("  $ echo Some data! | dedupe [index] write --key=[key] --chunks=[dir]");
            Console.WriteLine("");
            Console.WriteLine("Retrieving an object:");
            Console.WriteLine("  $ dedupe [index] get --key=[key] --chunks=[dir] > file.txt");
            Console.WriteLine("");

            if (!String.IsNullOrEmpty(msg))
            {
                Console.WriteLine("*** " + msg);
                Console.WriteLine("");
            }
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
