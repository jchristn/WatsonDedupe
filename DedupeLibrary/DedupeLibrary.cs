using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WatsonDedupe
{
    /// <summary>
    /// Library for deduplication of objects.
    /// </summary>
    public class DedupeLibrary
    {
        #region Public-Members

        /// <summary>
        /// Enable or disable console logging for deduplication operations.
        /// </summary>
        public bool DebugDedupe;

        /// <summary>
        /// Enable or disable console logging for SQL operations.
        /// </summary>
        public bool DebugSql;

        /// <summary>
        /// Callback methods used by the dedupe library to read, write, and delete chunks.
        /// </summary>
        public CallbackMethods Callbacks = new CallbackMethods();

        #endregion

        #region Private-Members

        private string _IndexFile;
        private int _MinChunkSize;
        private int _MaxChunkSize;
        private int _ShiftCount;
        private int _BoundaryCheckBytes;
        private readonly object _ChunkLock;

        private SqliteWrapper _Sqlite;
         
        #endregion

        #region Constructor

        /// <summary>
        /// Initialize from an existing index.
        /// </summary>
        /// <param name="indexFile">Path and filename.</param>
        /// <param name="writeChunkMethod">Method to call to write a chunk to storage.</param>
        /// <param name="readChunkMethod">Method to call to read a chunk from storage.</param>
        /// <param name="deleteChunkMethod">Method to call to delete a chunk from storage.</param>
        /// <param name="debugDedupe">Enable console logging for deduplication operations.</param>
        /// <param name="debugSql">Enable console logging for SQL operations.</param>
        public DedupeLibrary(string indexFile, Func<Chunk, bool> writeChunkMethod, Func<string, byte[]> readChunkMethod, Func<string, bool> deleteChunkMethod, bool debugDedupe, bool debugSql)
        {
            if (String.IsNullOrEmpty(indexFile)) throw new ArgumentNullException(nameof(indexFile));
            if (!File.Exists(indexFile)) throw new FileNotFoundException("Index file not found.");
            if (writeChunkMethod == null) throw new ArgumentNullException(nameof(writeChunkMethod));
            if (readChunkMethod == null) throw new ArgumentNullException(nameof(readChunkMethod));
            if (deleteChunkMethod == null) throw new ArgumentNullException(nameof(deleteChunkMethod));

            _IndexFile = Common.SanitizeString(indexFile);

            Callbacks = new CallbackMethods();
            Callbacks.WriteChunk = writeChunkMethod;
            Callbacks.ReadChunk = readChunkMethod;
            Callbacks.DeleteChunk = deleteChunkMethod;

            DebugDedupe = debugDedupe;
            DebugSql = debugSql;
            _ChunkLock = new object();

            _Sqlite = new SqliteWrapper(_IndexFile, DebugSql);

            InitFromExistingIndex();
        }

        /// <summary>
        /// Create a new index.
        /// </summary>
        /// <param name="indexFile">Path and filename.</param>
        /// <param name="minChunkSize">Minimum chunk size, must be divisible by 8, divisible by 64, and 128 or greater.</param>
        /// <param name="maxChunkSize">Maximum chunk size, must be divisible by 8, divisible by 64, and at least 8 times larger than minimum chunk size.</param>
        /// <param name="shiftCount">Number of bytes to shift while identifying chunk boundaries, must be less than or equal to minimum chunk size.</param>
        /// <param name="boundaryCheckBytes">Number of bytes to examine while checking for a chunk boundary, must be 8 or fewer.</param>
        /// <param name="writeChunkMethod">Method to call to write a chunk to storage.</param>
        /// <param name="readChunkMethod">Method to call to read a chunk from storage.</param>
        /// <param name="deleteChunkMethod">Method to call to delete a chunk from storage.</param>
        /// <param name="debugDedupe">Enable console logging for deduplication operations.</param>
        /// <param name="debugSql">Enable console logging for SQL operations.</param>
        public DedupeLibrary(
            string indexFile,
            int minChunkSize,
            int maxChunkSize,
            int shiftCount, 
            int boundaryCheckBytes,
            Func<Chunk, bool> writeChunkMethod, 
            Func<string, byte[]> readChunkMethod,
            Func<string, bool> deleteChunkMethod,
            bool debugDedupe,
            bool debugSql)
        {
            if (String.IsNullOrEmpty(indexFile)) throw new ArgumentNullException(nameof(indexFile));
            if (minChunkSize % 8 != 0) throw new ArgumentException("Value for minChunkSize must be evenly divisible by 8.");
            if (maxChunkSize % 8 != 0) throw new ArgumentException("Value for maxChunkSize must be evenly divisible by 8.");
            if (minChunkSize % 64 != 0) throw new ArgumentException("Value for minChunkSize must be evenly divisible by 64.");
            if (maxChunkSize % 64 != 0) throw new ArgumentException("Value for maxChunkSize must be evenly divisible by 64.");
            if (minChunkSize < 128) throw new ArgumentOutOfRangeException("Value for minChunkSize must be 128 or greater.");
            if (maxChunkSize <= minChunkSize) throw new ArgumentOutOfRangeException("Value for maxChunkSize must be greater than minChunkSize and " + (8 * minChunkSize) + " or less.");
            if (maxChunkSize < (8 * minChunkSize)) throw new ArgumentOutOfRangeException("Value for maxChunkSize must be " + (8 * minChunkSize) + " or greater.");
            if (shiftCount > minChunkSize) throw new ArgumentOutOfRangeException("Value for shiftCount must be less than or equal to minChunkSize.");
            if (writeChunkMethod == null) throw new ArgumentNullException(nameof(writeChunkMethod));
            if (readChunkMethod == null) throw new ArgumentNullException(nameof(readChunkMethod));
            if (deleteChunkMethod == null) throw new ArgumentNullException(nameof(deleteChunkMethod));
            if (boundaryCheckBytes < 1 || boundaryCheckBytes > 8) throw new ArgumentNullException(nameof(boundaryCheckBytes));

            if (File.Exists(indexFile)) throw new IOException("Index file already exists.");

            _IndexFile = Common.SanitizeString(indexFile);
            _MinChunkSize = minChunkSize;
            _MaxChunkSize = maxChunkSize;
            _ShiftCount = shiftCount;
            _BoundaryCheckBytes = boundaryCheckBytes;

            Callbacks = new CallbackMethods();
            Callbacks.WriteChunk = writeChunkMethod;
            Callbacks.ReadChunk = readChunkMethod;
            Callbacks.DeleteChunk = deleteChunkMethod; 

            DebugDedupe = debugDedupe;
            DebugSql = debugSql;
            _ChunkLock = new object();

            _Sqlite = new SqliteWrapper(_IndexFile, DebugSql);

            InitNewIndex();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Store an object in the deduplication index.
        /// </summary>
        /// <param name="objectName">The name of the object.  Must be unique in the index.</param>
        /// <param name="data">The byte data for the object.</param>
        /// <param name="chunks">The list of chunks identified during the deduplication operation.</param>
        /// <returns>True if successful.</returns>
        public bool StoreObject(string objectName, byte[] data, out List<Chunk> chunks)
        {
            return StoreObject(objectName, Callbacks, data, out chunks);
        }

        /// <summary>
        /// Store an object in the deduplication index.
        /// This method will use the callbacks supplied in the method signature.
        /// </summary>
        /// <param name="objectName">The name of the object.  Must be unique in the index.</param>
        /// <param name="callbacks">CallbackMethods object containing callback methods.</param>
        /// <param name="data">The byte data for the object.</param>
        /// <param name="chunks">The list of chunks identified during the deduplication operation.</param>
        /// <returns>True if successful.</returns>
        public bool StoreObject(string objectName, CallbackMethods callbacks, byte[] data, out List<Chunk> chunks)
        {
            #region Initialize

            chunks = new List<Chunk>();
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            if (callbacks == null) throw new ArgumentNullException(nameof(callbacks));
            if (callbacks.WriteChunk == null) throw new ArgumentException("WriteChunk callback must be specified.");
            if (callbacks.DeleteChunk == null) throw new ArgumentException("DeleteChunk callback must be specified.");
            if (data == null || data.Length < 1) return false;
            objectName = Common.SanitizeString(objectName);

            if (_Sqlite.ObjectExists(objectName))
            {
                if (DebugDedupe) Console.WriteLine("Object already exists");
                return false;
            }

            #endregion

            #region Chunk-Data

            if (!ChunkObject(data, out chunks))
            {
                if (DebugDedupe) Console.WriteLine("Unable to chunk supplied data");
                return false;
            }

            if (chunks == null || chunks.Count < 1)
            {
                if (DebugDedupe) Console.WriteLine("No chunks found in supplied data");
                return false;
            }

            #endregion

            #region Add-Object-Map

            lock (_ChunkLock)
            {
                if (!_Sqlite.AddObjectChunks(objectName, data.Length, chunks))
                {
                    if (DebugDedupe) Console.WriteLine("Unable to add object");
                    return false;
                }

                bool storageSuccess = true;
                foreach (Chunk curr in chunks)
                {
                    if (!callbacks.WriteChunk(curr))
                    {
                        if (DebugDedupe) Console.WriteLine("Unable to store chunk " + curr.Key);
                        storageSuccess = false;
                        break;
                    }
                }

                if (!storageSuccess)
                {
                    List<string> garbageCollectKeys;
                    _Sqlite.DeleteObjectChunks(objectName, out garbageCollectKeys);

                    if (garbageCollectKeys != null && garbageCollectKeys.Count > 0)
                    {
                        foreach (string key in garbageCollectKeys)
                        {
                            if (!callbacks.DeleteChunk(key))
                            {
                                if (DebugDedupe) Console.WriteLine("Unable to delete chunk: " + key);
                            }
                        }
                    }
                    return false;
                }
            }

            return true;

            #endregion
        }

        /// <summary>
        /// Store an object within a container in the deduplication index if it doesn't already exist, or, replace the object if it does.
        /// </summary>
        /// <param name="objectName">The name of the object.  Must be unique in the index.</param>
        /// <param name="data">The byte data for the object.</param>
        /// <param name="chunks">The list of chunks identified during the deduplication operation.</param>
        /// <returns>True if successful.</returns>
        public bool StoreOrReplaceObject(string objectName, byte[] data, out List<Chunk> chunks)
        {
            return StoreOrReplaceObject(objectName, Callbacks, data, out chunks); 
        }

        /// <summary>
        /// Store an object within a container in the deduplication index if it doesn't already exist, or, replace the object if it does.
        /// This method will use the callbacks supplied in the method signature.
        /// </summary>
        /// <param name="objectName">The name of the object.  Must be unique in the index.</param>
        /// <param name="callbacks">CallbackMethods object containing callback methods.</param>
        /// <param name="data">The byte data for the object.</param>
        /// <param name="chunks">The list of chunks identified during the deduplication operation.</param>
        /// <returns>True if successful.</returns>
        public bool StoreOrReplaceObject(string objectName, CallbackMethods callbacks, byte[] data, out List<Chunk> chunks)
        {
            #region Initialize

            chunks = new List<Chunk>();
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            if (callbacks == null) throw new ArgumentNullException(nameof(callbacks));
            if (callbacks.WriteChunk == null) throw new ArgumentException("WriteChunk callback must be specified.");
            if (callbacks.DeleteChunk == null) throw new ArgumentException("DeleteChunk callback must be specified.");
            if (data == null || data.Length < 1) return false;
            objectName = Common.SanitizeString(objectName);

            if (_Sqlite.ObjectExists(objectName))
            {
                if (DebugDedupe) Console.WriteLine("Object already exists, deleting");
                if (!DeleteObject(objectName))
                {
                    if (DebugDedupe) Console.WriteLine("Unable to delete existing object");
                    return false;
                }
                else
                {
                    if (DebugDedupe) Console.WriteLine("Successfully deleted object for replacement");
                }
            }

            #endregion

            #region Chunk-Data

            if (!ChunkObject(data, out chunks))
            {
                if (DebugDedupe) Console.WriteLine("Unable to chunk supplied data");
                return false;
            }

            if (chunks == null || chunks.Count < 1)
            {
                if (DebugDedupe) Console.WriteLine("No chunks found in supplied data");
                return false;
            }

            #endregion

            #region Add-Object-Map

            lock (_ChunkLock)
            {
                if (!_Sqlite.AddObjectChunks(objectName, data.Length, chunks))
                {
                    if (DebugDedupe) Console.WriteLine("Unable to add object");
                    return false;
                }

                bool storageSuccess = true;
                foreach (Chunk curr in chunks)
                {
                    if (!callbacks.WriteChunk(curr))
                    {
                        if (DebugDedupe) Console.WriteLine("Unable to store chunk " + curr.Key);
                        storageSuccess = false;
                        break;
                    }
                }

                if (!storageSuccess)
                {
                    List<string> garbageCollectKeys;
                    _Sqlite.DeleteObjectChunks(objectName, out garbageCollectKeys);

                    if (garbageCollectKeys != null && garbageCollectKeys.Count > 0)
                    {
                        foreach (string key in garbageCollectKeys)
                        {
                            if (!callbacks.DeleteChunk(key))
                            {
                                if (DebugDedupe) Console.WriteLine("Unable to delete chunk: " + key);
                            }
                        }
                    }
                    return false;
                }
            }

            return true;

            #endregion
        }

        /// <summary>
        /// Retrieve metadata about an object from the deduplication index.
        /// </summary>
        /// <param name="objectName">The name of the object.</param>
        /// <param name="md">Object metadata.</param>
        /// <returns>True if successful.</returns>
        public bool RetrieveObjectMetadata(string objectName, out ObjectMetadata md)
        {
            md = null;
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            objectName = Common.SanitizeString(objectName);

            lock (_ChunkLock)
            {
                return _Sqlite.GetObjectMetadata(objectName, out md);
            }
        }

        /// <summary>
        /// Retrieve an object from the deduplication index.
        /// </summary>
        /// <param name="objectName">The name of the object.</param>
        /// <param name="data">The byte data from the object.</param>
        /// <returns>True if successful.</returns>
        public bool RetrieveObject(string objectName, out byte[] data)
        {
            return RetrieveObject(objectName, Callbacks, out data);
        }

        /// <summary>
        /// Retrieve an object from the deduplication index.
        /// This method will use the callbacks supplied in the method signature.
        /// </summary>
        /// <param name="objectName">The name of the object.</param>
        /// <param name="callbacks">CallbackMethods object containing callback methods.</param>
        /// <param name="data">The byte data from the object.</param>
        /// <returns>True if successful.</returns>
        public bool RetrieveObject(string objectName, CallbackMethods callbacks, out byte[] data)
        {
            data = null;
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            if (callbacks == null) throw new ArgumentNullException(nameof(callbacks));
            if (callbacks.ReadChunk == null) throw new ArgumentException("ReadChunk callback must be specified."); 
            objectName = Common.SanitizeString(objectName);

            ObjectMetadata md = null;

            lock (_ChunkLock)
            {
                if (!_Sqlite.GetObjectMetadata(objectName, out md))
                {
                    if (DebugDedupe) Console.WriteLine("Unable to retrieve object metadata");
                    return false;
                }

                if (md.Chunks == null || md.Chunks.Count < 1)
                {
                    if (DebugDedupe) Console.WriteLine("No chunks returned");
                    return false;
                }

                data = Common.InitBytes(md.ContentLength, 0x00);

                foreach (Chunk curr in md.Chunks)
                {
                    byte[] chunkData = callbacks.ReadChunk(curr.Key);
                    if (chunkData == null || chunkData.Length < 1)
                    {
                        if (DebugDedupe) Console.WriteLine("Unable to read chunk " + curr.Key);
                        return false;
                    }

                    Buffer.BlockCopy(chunkData, 0, data, (int)curr.Address, chunkData.Length);
                }
            }

            return true;
        }

        /// <summary>
        /// Delete an object stored in the deduplication index.
        /// </summary>
        /// <param name="objectName">The name of the object.</param>
        /// <returns>True if successful.</returns>
        public bool DeleteObject(string objectName)
        {
            return DeleteObject(objectName, Callbacks);
        }

        /// <summary>
        /// Delete an object stored in the deduplication index.
        /// This method will use the callbacks supplied in the method signature.
        /// </summary>
        /// <param name="objectName">The name of the object.</param>
        /// <param name="callbacks">CallbackMethods object containing callback methods.</param>
        /// <returns>True if successful.</returns>
        public bool DeleteObject(string objectName, CallbackMethods callbacks)
        {
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            if (callbacks == null) throw new ArgumentNullException(nameof(callbacks));
            if (callbacks.DeleteChunk == null) throw new ArgumentException("DeleteChunk callback must be specified.");
            objectName = Common.SanitizeString(objectName);

            List<string> garbageCollectChunks = null;

            lock (_ChunkLock)
            {
                _Sqlite.DeleteObjectChunks(objectName, out garbageCollectChunks);
                if (garbageCollectChunks != null && garbageCollectChunks.Count > 0)
                {
                    foreach (string key in garbageCollectChunks)
                    {
                        if (!callbacks.DeleteChunk(key))
                        {
                            if (DebugDedupe) Console.WriteLine("Unable to delete chunk: " + key);
                        }
                    }
                }
            }

            return true;
        }

        /// <summary>
        /// List the objects stored in the deduplication index.
        /// </summary>
        /// <param name="keys">List of object names.</param>
        public void ListObjects(out List<string> keys)
        {
            _Sqlite.ListObjects(out keys);
            return;
        }

        /// <summary>
        /// Determine if an object exists in the index.
        /// </summary>
        /// <param name="objectName">The name of the object.</param>
        /// <returns>Boolean indicating if the object exists.</returns>
        public bool ObjectExists(string objectName)
        {
            return _Sqlite.ObjectExists(objectName);
        }

        /// <summary>
        /// Determine if a chunk exists in the index.
        /// </summary>
        /// <param name="chunkKey">The chunk's key.</param>
        /// <returns>Boolean indicating if the chunk exists.</returns>
        public bool ChunkExists(string chunkKey)
        {
            return _Sqlite.ChunkExists(chunkKey);
        }

        /// <summary>
        /// Retrieve deduplication index statistics.
        /// </summary>
        /// <param name="numObjects">The number of objects stored in the index.</param>
        /// <param name="numChunks">Number of chunks referenced in the index.</param>
        /// <param name="logicalBytes">The amount of data stored in the index, i.e. the full size of the original data.</param>
        /// <param name="physicalBytes">The number of bytes consumed by chunks of data, i.e. the deduplication set size.</param>
        /// <param name="dedupeRatioX">Deduplication ratio represented as a multiplier.</param>
        /// <param name="dedupeRatioPercent">Deduplication ratio represented as a percentage.</param>
        /// <returns>True if successful.</returns>
        public bool IndexStats(out int numObjects, out int numChunks, out long logicalBytes, out long physicalBytes, out decimal dedupeRatioX, out decimal dedupeRatioPercent)
        {
            return _Sqlite.IndexStats(out numObjects, out numChunks, out logicalBytes, out physicalBytes, out dedupeRatioX, out dedupeRatioPercent);
        }

        /// <summary>
        /// Copies the index database to another file.
        /// </summary>
        /// <param name="destination">The destination file.</param>
        /// <returns>True if successful.</returns>
        public bool BackupIndex(string destination)
        {
            return _Sqlite.BackupIndex(destination);
        }

        #endregion

        #region Private-Methods

        private void InitFromExistingIndex()
        {
            string tempVal;
            if (!_Sqlite.GetConfigData("min_chunk_size", out tempVal))
            {
                throw new Exception("Configuration table has invalid value for 'min_chunk_size'.");
            }
            else
            {
                if (DebugDedupe) Console.WriteLine("MinChunkSize set to " + tempVal);
                _MinChunkSize = Convert.ToInt32(tempVal);
            }

            if (!_Sqlite.GetConfigData("max_chunk_size", out tempVal))
            {
                throw new Exception("Configuration table has invalid value for 'max_chunk_size'.");
            }
            else
            {
                if (DebugDedupe) Console.WriteLine("MaxChunkSize set to " + tempVal);
                _MaxChunkSize = Convert.ToInt32(tempVal);
            }

            if (!_Sqlite.GetConfigData("shift_count", out tempVal))
            {
                throw new Exception("Configuration table has invalid value for 'shift_count'.");
            }
            else
            {
                if (DebugDedupe) Console.WriteLine("ShiftCount set to " + tempVal);
                _ShiftCount = Convert.ToInt32(tempVal);
            }

            if (!_Sqlite.GetConfigData("boundary_check_bytes", out tempVal))
            {
                throw new Exception("Configuration table has invalid value for 'boundary_check_bytes'.");
            }
            else
            {
                if (DebugDedupe) Console.WriteLine("BoundaryCheckBytes set to " + tempVal);
                _BoundaryCheckBytes = Convert.ToInt32(tempVal);
            }
        }

        private void InitNewIndex()
        {
            _Sqlite.AddConfigData("min_chunk_size", _MinChunkSize.ToString());
            _Sqlite.AddConfigData("max_chunk_size", _MaxChunkSize.ToString());
            _Sqlite.AddConfigData("shift_count", _ShiftCount.ToString());
            _Sqlite.AddConfigData("boundary_check_bytes", _BoundaryCheckBytes.ToString());
            _Sqlite.AddConfigData("index_per_object", "false");
        }

        private bool ChunkObject(byte[] data, out List<Chunk> chunks)
        {
            #region Initialize

            chunks = new List<Chunk>();
            Chunk c;

            if (data == null || data.Length < 1) return false;

            if (data.Length <= _MinChunkSize)
            {
                c = new Chunk(
                    Common.BytesToBase64(Common.Sha256(data)),
                    data.Length,
                    0,
                    0,
                    data);
                chunks.Add(c);
                return true;
            }

            int currPosition = 0;
            int chunkStart = 0;
            byte[] window = new byte[_MinChunkSize];

            #endregion

            #region Setup-First-Window

            Buffer.BlockCopy(data, 0, window, 0, _MinChunkSize);
            currPosition = _MinChunkSize;

            #endregion

            #region Process

            int chunksFound = 0;
            int bytesTotal = 0;
            
            if (DebugDedupe) Console.WriteLine("Chunking " + data.Length + " bytes of data");
            while (currPosition < data.Length)
            {
                byte[] md5Hash = Common.Md5(window);
                if (DebugDedupe)
                {
                    if (currPosition % 1000 == 0) Console.Write("Chunk start " + chunkStart + " window end " + currPosition + " hash: " + Common.BytesToBase64(md5Hash) + "\r");
                }

                if (Common.IsZeroBytes(md5Hash, _BoundaryCheckBytes))
                {
                    #region New-Chunk-Identified

                    if (DebugDedupe)
                    {
                        Common.ClearCurrentLine();
                        Console.Write
                            ("\rChunk identified from " + chunkStart + " to " + currPosition + " (" + (currPosition - chunkStart) + " bytes)");
                    }

                    // create chunk
                    byte[] chunk = new byte[(currPosition - chunkStart)];
                    Buffer.BlockCopy(data, chunkStart, chunk, 0, (currPosition - chunkStart));

                    // add to chunk list
                    c = new Chunk(
                        Common.BytesToBase64(Common.Sha256(chunk)),
                        chunk.Length,
                        chunksFound,
                        chunkStart,
                        chunk);
                    chunks.Add(c);
                    chunksFound++;
                    bytesTotal += (currPosition - chunkStart);

                    chunkStart = currPosition;

                    // initialize new window
                    if (data.Length - currPosition >= _MinChunkSize)
                    {
                        #region Min-Size-or-More-Remaining

                        window = new byte[_MinChunkSize];
                        Buffer.BlockCopy(data, currPosition, window, 0, _MinChunkSize);
                        currPosition += _MinChunkSize;
                        continue;

                        #endregion
                    }
                    else
                    {
                        #region Less-Than-Min-Size-Remaining

                        // end of data
                        if (DebugDedupe)
                        {
                            Common.ClearCurrentLine();
                            Console.WriteLine("Less than MinChunkSize remaining, adding chunk (" + (data.Length - currPosition) + " bytes)");
                        }

                        chunk = new byte[(data.Length - currPosition)];
                        Buffer.BlockCopy(data, currPosition, chunk, 0, (data.Length - currPosition));

                        // add to chunk list
                        c = new Chunk(
                            Common.BytesToBase64(Common.Sha256(chunk)),
                            chunk.Length,
                            chunksFound,
                            currPosition,
                            chunk);
                        chunks.Add(c);
                        chunksFound++;
                        bytesTotal += (data.Length - currPosition);

                        window = null;

                        // end processing
                        break;

                        #endregion
                    }

                    #endregion
                }
                else
                {
                    #region Not-a-Chunk-Boundary

                    if ((currPosition - chunkStart) >= _MaxChunkSize)
                    {
                        #region Max-Size-Reached

                        // create chunk
                        byte[] chunk = new byte[(currPosition - chunkStart)];
                        Buffer.BlockCopy(data, chunkStart, chunk, 0, (currPosition - chunkStart));
                        // if (Debug) Console.WriteLine("chunk identified due to max size from " + chunk_start + " to " + curr_pos + " (" + (curr_pos - chunk_start) + " bytes)");

                        // add to chunk list
                        c = new Chunk(
                            Common.BytesToBase64(Common.Sha256(chunk)),
                            chunk.Length,
                            chunksFound,
                            chunkStart,
                            chunk);
                        chunks.Add(c);
                        chunksFound++;
                        bytesTotal += (currPosition - chunkStart);

                        chunkStart = currPosition;

                        // initialize new window
                        if (data.Length - currPosition >= _MinChunkSize)
                        {
                            #region Min-Size-or-More-Remaining

                            window = new byte[_MinChunkSize];
                            Buffer.BlockCopy(data, currPosition, window, 0, _MinChunkSize);
                            currPosition += _MinChunkSize;
                            continue;

                            #endregion
                        }
                        else
                        {
                            #region Less-Than-Min-Size-Remaining

                            // end of data
                            if (DebugDedupe) Console.WriteLine("Less than MinChunkSize remaining, adding chunk (" + (data.Length - currPosition) + " bytes)");
                            chunk = new byte[(data.Length - currPosition)];
                            Buffer.BlockCopy(data, currPosition, chunk, 0, (data.Length - currPosition));

                            // add to chunk list
                            c = new Chunk(
                                Common.BytesToBase64(Common.Sha256(chunk)),
                                chunk.Length,
                                chunksFound,
                                currPosition,
                                chunk);
                            chunks.Add(c);
                            chunksFound++;
                            bytesTotal += (data.Length - currPosition);

                            window = null;

                            // end processing
                            break;

                            #endregion
                        }

                        #endregion
                    }
                    else
                    {
                        #region Shift-Window

                        // shift the window
                        window = Common.ShiftLeft(window, _ShiftCount, 0x00);

                        // add the next set of bytes to the window
                        if (currPosition + _ShiftCount > data.Length)
                        {
                            //
                            // set current position to the end and break
                            //
                            currPosition = data.Length;
                            break;
                        }
                        else
                        {
                            Buffer.BlockCopy(data, currPosition, window, (_MinChunkSize - _ShiftCount), _ShiftCount);
                        }

                        // increment the current position
                        currPosition = currPosition + _ShiftCount;

                        #endregion
                    }

                    #endregion
                }
            }

            if (window != null)
            {
                #region Last-Chunk

                if (DebugDedupe)
                {
                    Common.ClearCurrentLine();
                    Console.WriteLine("\rChunk identified (end of input) from " + chunkStart + " to " + currPosition + " (" + (currPosition - chunkStart) + " bytes)");
                }

                // if (Debug) Console.WriteLine("adding leftover chunk (" + (data.Length - chunk_start) + " bytes)");
                byte[] chunk = new byte[(data.Length - chunkStart)];
                Buffer.BlockCopy(data, chunkStart, chunk, 0, (data.Length - chunkStart));

                // add to chunk list
                c = new Chunk(
                    Common.BytesToBase64(Common.Sha256(chunk)),
                    chunk.Length,
                    chunksFound,
                    chunkStart,
                    chunk);
                chunks.Add(c);
                chunksFound++;
                bytesTotal += (currPosition - chunkStart);

                #endregion
            }

            #endregion

            #region Respond

            if (DebugDedupe)
            {
                Common.ClearCurrentLine();
                Console.WriteLine("Returning " + chunks.Count + " chunks (" + bytesTotal + " bytes)");
            }

            return true;

            #endregion
        }

        #endregion
    }
}
