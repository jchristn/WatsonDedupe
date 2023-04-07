using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SlidingWindow;

using WatsonDedupe.Database;

namespace WatsonDedupe
{
    /// <summary>
    /// Library for deduplication of objects.
    /// </summary>
    public class DedupeLibrary
    {
        #region Public-Members
          
        /// <summary>
        /// Callback methods used by the dedupe library to read, write, and delete chunks.
        /// </summary>
        public DedupeCallbacks Callbacks
        {
            get
            {
                return _Callbacks;
            }
            set
            {
                if (value == null) throw new ArgumentNullException(nameof(Callbacks));
                else _Callbacks = value;
            }
        }

        /// <summary>
        /// Deduplication settings.
        /// </summary>
        public DedupeSettings Settings
        {
            get
            {
                return _Settings;
            }            
        }

        /// <summary>
        /// Method to invoke when sending log messages.
        /// </summary>
        public Action<string> Logger = null; 

        /// <summary>
        /// Database provider.
        /// </summary>
        public DbProvider Database
        {
            get
            {
                return _Database;
            } 
        }

        #endregion

        #region Private-Members

        private string _Header = "[Dedupe] "; 
        private string _IndexFile;
        private DedupeSettings _Settings = new DedupeSettings();
        private DedupeCallbacks _Callbacks = new DedupeCallbacks(); 
        private DbProvider _Database = null;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initialize a new or existing index using an internal Sqlite database.
        /// </summary>
        /// <param name="indexFile">Path and filename.</param>
        /// <param name="settings">Deduplication settings.</param>
        /// <param name="callbacks">Object containing callback functions for writing, reading, and deleting chunks.</param>
        public DedupeLibrary(string indexFile, DedupeSettings settings, DedupeCallbacks callbacks)
        { 
            if (String.IsNullOrEmpty(indexFile)) throw new ArgumentNullException(nameof(indexFile));
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (callbacks == null) throw new ArgumentNullException(nameof(callbacks));

            _Settings = settings;
            _Callbacks = callbacks;
            _IndexFile = DedupeCommon.SanitizeString(indexFile); 
            _Database = new SqliteProvider(_IndexFile); 
            InitializeIndex(); 
        }

        /// <summary>
        /// Initialize an existing index using an external database.  Tables must be created ahead of time.
        /// </summary>
        /// <param name="database">Database provider implemented using the Database.DbProvider class.</param>
        /// <param name="settings">Deduplication settings.</param>
        /// <param name="callbacks">Object containing callback functions for writing, reading, and deleting chunks.</param>
        public DedupeLibrary(DbProvider database, DedupeSettings settings, DedupeCallbacks callbacks)
        {
            if (database == null) throw new ArgumentNullException(nameof(database));
            if (settings == null) throw new ArgumentNullException(nameof(settings));
            if (callbacks == null) throw new ArgumentNullException(nameof(callbacks));
             
            _Database = database;
            _Settings = settings;
            _Callbacks = callbacks;

            InitializeIndex();
        }
         
        #endregion

        #region Public-Methods

        #region Write-Methods

        /// <summary>
        /// Write an object to the deduplication index.
        /// </summary>
        /// <param name="key">The object key.  Must be unique in the index.</param>
        /// <param name="data">The string data for the object.</param> 
        public void Write(string key, string data)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (String.IsNullOrEmpty(data)) throw new ArgumentNullException(nameof(data));
            byte[] bytes = Encoding.UTF8.GetBytes(data);
            Write(key, Callbacks, bytes.Length, DedupeCommon.BytesToStream(bytes));
        }

        /// <summary>
        /// Write an object to the deduplication index.
        /// </summary>
        /// <param name="key">The object key.  Must be unique in the index.</param>
        /// <param name="bytes">The byte data for the object.</param> 
        public void Write(string key, byte[] bytes)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (bytes == null || bytes.Length < 1) throw new ArgumentNullException(nameof(bytes));
            Write(key, Callbacks, bytes.Length, DedupeCommon.BytesToStream(bytes));
        }

        /// <summary>
        /// Write an object to the deduplication index.
        /// </summary>
        /// <param name="key">The object key.  Must be unique in the index.</param>
        /// <param name="contentLength">The length of the data.</param>
        /// <param name="stream">The stream containing the data.</param> 
        public void Write(string key, long contentLength, Stream stream)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (contentLength < 1) throw new ArgumentOutOfRangeException("Content length must be greater than zero.");
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            if (!stream.CanRead) throw new InvalidOperationException("Cannot read from the supplied stream.");
            Write(key, Callbacks, contentLength, stream);
        }

        /// <summary>
        /// Write an object to the deduplication index.
        /// This method will use the callbacks supplied in the method signature.
        /// </summary>
        /// <param name="key">The object key.  Must be unique in the index.</param>
        /// <param name="callbacks">CallbackMethods object containing callback methods.</param>
        /// <param name="data">The string data for the object.</param> 
        public void Write(string key, DedupeCallbacks callbacks, string data)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (String.IsNullOrEmpty(data)) throw new ArgumentNullException(nameof(data));
            byte[] bytes = Encoding.UTF8.GetBytes(data);
            Write(key, callbacks, bytes.Length, DedupeCommon.BytesToStream(bytes));
        }

        /// <summary>
        /// Write an object to the deduplication index.
        /// This method will use the callbacks supplied in the method signature.
        /// </summary>
        /// <param name="key">The object key.  Must be unique in the index.</param>
        /// <param name="callbacks">CallbackMethods object containing callback methods.</param>
        /// <param name="bytes">The byte data for the object.</param> 
        public void Write(string key, DedupeCallbacks callbacks, byte[] bytes)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (bytes == null || bytes.Length < 1) throw new ArgumentNullException(nameof(bytes));
            Write(key, callbacks, bytes.Length, DedupeCommon.BytesToStream(bytes));
        }

        /// <summary>
        /// Write an object to the deduplication index.
        /// This method will use the callbacks supplied in the method signature.
        /// </summary>
        /// <param name="key">The object key.  Must be unique in the index.</param>
        /// <param name="callbacks">CallbackMethods object containing callback methods.</param>
        /// <param name="contentLength">The length of the data.</param>
        /// <param name="stream">The stream containing the data.</param> 
        public void Write(string key, DedupeCallbacks callbacks, long contentLength, Stream stream)
        {
            #region Initialize
             
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (_Database.Exists(key)) throw new ArgumentException("An object with key '" + key + "' already exists.");
            if (callbacks == null) throw new ArgumentNullException(nameof(callbacks));
            if (callbacks.WriteChunk == null) throw new ArgumentException("WriteChunk callback must be specified.");
            if (callbacks.DeleteChunk == null) throw new ArgumentException("DeleteChunk callback must be specified.");
            if (contentLength < 1) throw new ArgumentException("Content length must be at least one byte.");
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            if (!stream.CanRead) throw new InvalidOperationException("Cannot read from the supplied stream.");
            key = DedupeCommon.SanitizeString(key);
             
            bool garbageCollectionRequired = false;

            #endregion

            #region Chunk-Data

            List<DedupeChunk> chunks = new List<DedupeChunk>();

            try
            {
                Action<DedupeChunk, DedupeObjectMap> processChunk = delegate (DedupeChunk chunk, DedupeObjectMap map)
                {
                    if (chunk == null || map == null) return;
                     
                    _Database.IncrementChunkRefcount(chunk.Key, chunk.Length);
                    _Database.AddObjectMap(key, chunk.Key, chunk.Length, map.ChunkPosition, map.ChunkAddress);
                    callbacks.WriteChunk(chunk); 
                };

                chunks = ChunkStream(key, contentLength, stream, processChunk);

                _Database.AddObject(key, contentLength, chunks.Sum(item => item.Length), chunks.Count);
            }
            finally
            {
                if (garbageCollectionRequired)
                {
                    List<string> garbageCollectKeys = _Database.Delete(key); 
                    if (garbageCollectKeys != null && garbageCollectKeys.Count > 0)
                    {
                        foreach (string gcKey in garbageCollectKeys)
                        {
                            callbacks.DeleteChunk(gcKey);
                        }
                    }
                }
            }

            #endregion 
        }

        /// <summary>
        /// Write an object to the deduplication index if it doesn't already exist, or, replace the object if it does.
        /// </summary>
        /// <param name="key">The object key.  Must be unique in the index.</param>
        /// <param name="data">The byte data for the object.</param> 
        public void WriteOrReplace(string key, byte[] data)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (data == null || data.Length < 1) throw new ArgumentNullException(nameof(data));
            WriteOrReplace(key, Callbacks, data.Length, DedupeCommon.BytesToStream(data));
        }

        /// <summary>
        /// Write an object to the deduplication index if it doesn't already exist, or, replace the object if it does.
        /// </summary>
        /// <param name="key">The object key.  Must be unique in the index.</param>
        /// <param name="contentLength">The length of the data.</param>
        /// <param name="stream">The stream containing the data.</param> 
        public void WriteOrReplace(string key, long contentLength, Stream stream)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            if (!stream.CanRead) throw new InvalidOperationException("Cannot read from the supplied stream.");
            WriteOrReplace(key, Callbacks, contentLength, stream);
        }

        /// <summary>
        /// Write an object to the deduplication index if it doesn't already exist, or, replace the object if it does.
        /// This method will use the callbacks supplied in the method signature.
        /// </summary>
        /// <param name="key">The object key.  Must be unique in the index.</param>
        /// <param name="callbacks">CallbackMethods object containing callback methods.</param>
        /// <param name="data">The byte data for the object.</param> 
        public void WriteOrReplace(string key, DedupeCallbacks callbacks, byte[] data)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (data == null || data.Length < 1) throw new ArgumentNullException(nameof(data));
            WriteOrReplace(key, callbacks, data.Length, DedupeCommon.BytesToStream(data)); 
        }

        /// <summary>
        /// Write an object to the deduplication index if it doesn't already exist, or, replace the object if it does.
        /// This method will use the callbacks supplied in the method signature.
        /// </summary>
        /// <param name="key">The object key.  Must be unique in the index.</param>
        /// <param name="callbacks">CallbackMethods object containing callback methods.</param>
        /// <param name="contentLength">The length of the data.</param>
        /// <param name="stream">The stream containing the data.</param> 
        public void WriteOrReplace(string key, DedupeCallbacks callbacks, long contentLength, Stream stream)
        { 
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (callbacks == null) throw new ArgumentNullException(nameof(callbacks));
            if (callbacks.WriteChunk == null) throw new ArgumentException("WriteChunk callback must be specified.");
            if (callbacks.DeleteChunk == null) throw new ArgumentException("DeleteChunk callback must be specified.");
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            if (!stream.CanRead) throw new ArgumentException("Cannot read from the supplied stream.");
            key = DedupeCommon.SanitizeString(key);
             
            if (_Database.Exists(key))
            {
                Logger?.Invoke(_Header + "Object " + key + " already exists, deleting");
                Delete(key);
            }
             
            Write(key, callbacks, contentLength, stream); 
        }

        #endregion

        #region Get-Methods

        /// <summary>
        /// Retrieve metadata about an object from the deduplication index.
        /// </summary>
        /// <param name="key">The object key.</param>
        /// <returns>Object metadata.</returns>
        public DedupeObject GetMetadata(string key)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            key = DedupeCommon.SanitizeString(key);
            return _Database.GetObjectMetadata(key);
        }
         
        /// <summary>
        /// Retrieve an object from the deduplication index.
        /// </summary>
        /// <param name="key">The object key.</param>
        /// <returns>Object data.</returns>
        public DedupeObject Get(string key)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            return Get(key, Callbacks);
        }

        /// <summary>
        /// Retrieve an object from the deduplication index.
        /// </summary>
        /// <param name="key">The object key.</param>
        /// <param name="data">Object data.</param>
        /// <returns>True if successful.</returns>
        public bool TryGet(string key, out DedupeObject data)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            data = null;

            try
            {
                data = Get(key, Callbacks);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// Retrieve an object from the deduplication index.
        /// This method will use the callbacks supplied in the method signature.
        /// </summary>
        /// <param name="key">The object key.</param>
        /// <param name="callbacks">CallbackMethods object containing callback methods.</param>
        /// <returns>Object data.</returns>
        public DedupeObject Get(string key, DedupeCallbacks callbacks)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (callbacks == null) throw new ArgumentNullException(nameof(callbacks));
            if (callbacks.ReadChunk == null) throw new ArgumentException("ReadChunk callback must be specified.");
            key = DedupeCommon.SanitizeString(key);
             
            DedupeObject md = _Database.GetObjectMetadata(key);
            if (md == null) throw new KeyNotFoundException("Object key '" + key + "' not found.");
            if (md.Chunks == null || md.Chunks.Count < 1) throw new IOException("No chunks returned for object key '" + key + "'.");

            MemoryStream stream = new MemoryStream();
            long contentLength = 0;

            foreach (DedupeObjectMap curr in md.ObjectMap)
            {
                byte[] chunkData = callbacks.ReadChunk(curr.ChunkKey);
                if (chunkData == null || chunkData.Length < 1) throw new IOException("Unable to read chunk '" + curr.ChunkKey + "'.");

                stream.Write(chunkData, 0, chunkData.Length);
                contentLength += chunkData.Length;
            }

            if (contentLength > 0) stream.Seek(0, SeekOrigin.Begin);

            md.DataStream = stream;
            return md;
        }
         
        /// <summary>
        /// Retrieve a read-only stream over an object that has been stored.
        /// </summary>
        /// <param name="key">The object key.</param>
        /// <returns>Read-only stream.</returns>
        public DedupeStream GetStream(string key)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            return GetStream(key, Callbacks);
        }

        /// <summary>
        /// Retrieve a read-only stream over an object that has been stored.
        /// </summary>
        /// <param name="key">The object key.</param>
        /// <param name="callbacks">CallbackMethods object containing callback methods.</param>
        /// <returns>Read-only stream.</returns>
        public DedupeStream GetStream(string key, DedupeCallbacks callbacks)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (callbacks == null) throw new ArgumentNullException(nameof(callbacks));
            if (callbacks.ReadChunk == null) throw new ArgumentException("ReadChunk callback must be specified.");
            key = DedupeCommon.SanitizeString(key);

            DedupeObject md = GetMetadata(key);
            if (md == null) throw new KeyNotFoundException("Object key '" + key + "' not found.");

            return new DedupeStream(md, _Database, callbacks);
        }

        /// <summary>
        /// Retrieve a read-only stream over an object that has been stored.
        /// </summary>
        /// <param name="key">The object key.</param>
        /// <param name="data">Read-only stream.</param>
        /// <returns>True if successful.</returns>
        public bool TryGetStream(string key, out DedupeStream data)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            key = DedupeCommon.SanitizeString(key);

            data = null;

            try
            {
                data = GetStream(key, Callbacks);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// Retrieve a read-only stream over an object that has been stored.
        /// </summary>
        /// <param name="key">The object key.</param>
        /// <param name="callbacks">CallbackMethods object containing callback methods.</param>
        /// <param name="data">Read-only stream.</param>
        /// <returns>True if successful.</returns>
        public bool TryGetStream(string key, DedupeCallbacks callbacks, out DedupeStream data)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (callbacks == null) throw new ArgumentNullException(nameof(callbacks));
            if (callbacks.ReadChunk == null) throw new ArgumentException("ReadChunk callback must be specified.");
            key = DedupeCommon.SanitizeString(key);

            data = null;

            try
            {
                data = GetStream(key, callbacks);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        #endregion

        #region Delete-Methods

        /// <summary>
        /// Delete an object stored in the deduplication index.
        /// </summary>
        /// <param name="key">The object key.</param>
        public void Delete(string key)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            Delete(key, Callbacks);
        }

        /// <summary>
        /// Delete an object stored in the deduplication index.
        /// This method will use the callbacks supplied in the method signature.
        /// </summary>
        /// <param name="key">The object key.</param>
        /// <param name="callbacks">CallbackMethods object containing callback methods.</param>
        public void Delete(string key, DedupeCallbacks callbacks)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (callbacks == null) throw new ArgumentNullException(nameof(callbacks));
            if (callbacks.DeleteChunk == null) throw new ArgumentException("DeleteChunk callback must be specified.");
            key = DedupeCommon.SanitizeString(key);
             
            List<string> garbageCollectChunks = _Database.Delete(key);
            if (garbageCollectChunks != null && garbageCollectChunks.Count > 0)
            {
                foreach (string gcKey in garbageCollectChunks)
                {
                    callbacks.DeleteChunk(gcKey);
                }
            } 
        }

        #endregion

        #region Other-Methods

        /// <summary>
        /// List the object keys stored in the deduplication index.
        /// </summary>
        /// <returns>Enumeration result.</returns>
        public EnumerationResult ListObjects()
        {
            return ListObjects(null, 0, 100);
        }

        /// <summary>
        /// List the object keys stored in the deduplication index.
        /// </summary>
        /// <returns>Enumeration result.</returns>
        public EnumerationResult ListObjects(string prefix)
        {
            return ListObjects(prefix, 0, 100);
        }

        /// <summary>
        /// List the object keys stored in the deduplication index.
        /// </summary>
        /// <returns>Enumeration result.</returns>
        public EnumerationResult ListObjects(string prefix, int indexStart, int maxResults)
        {
            if (indexStart < 0) throw new ArgumentException("Index start must be greater than or equal to zero.");
            if (maxResults < 1) throw new ArgumentException("Max results must be greater than zero.");
            return _Database.ListObjects(prefix, indexStart, maxResults);
        }

        /// <summary>
        /// Determine if an object exists in the index.
        /// </summary>
        /// <param name="key">The object key.</param>
        /// <returns>Boolean indicating if the object exists.</returns>
        public bool Exists(string key)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            return _Database.Exists(key);
        }
         
        /// <summary>
        /// Retrieve deduplication index statistics.
        /// </summary>
        /// <returns>Index statistics.</returns>
        public IndexStatistics IndexStats()
        {
            return _Database.GetStatistics();
        }
          
        #endregion

        #endregion

        #region Private-Methods

        private void InitializeIndex()
        {
            if (!_Database.IsInitialized())
            {
                Logger?.Invoke(_Header + "Initializing new index");

                _Database.AddConfigValue("min_chunk_size", _Settings.MinChunkSize.ToString());
                _Database.AddConfigValue("max_chunk_size", _Settings.MaxChunkSize.ToString());
                _Database.AddConfigValue("shift_count", _Settings.ShiftCount.ToString());
                _Database.AddConfigValue("boundary_check_bytes", _Settings.BoundaryCheckBytes.ToString()); 
            }
            else
            {
                Logger?.Invoke(_Header + "Initializing existing index");

                _Settings.MinChunkSize = Convert.ToInt32(_Database.GetConfigValue("min_chunk_size"));
                _Settings.MaxChunkSize = Convert.ToInt32(_Database.GetConfigValue("max_chunk_size"));
                _Settings.ShiftCount = Convert.ToInt32(_Database.GetConfigValue("shift_count"));
                _Settings.BoundaryCheckBytes = Convert.ToInt32(_Database.GetConfigValue("boundary_check_bytes"));
            }
        }
         
        private List<DedupeChunk> ChunkStream(string key, long contentLength, Stream stream, Action<DedupeChunk, DedupeObjectMap> processChunk)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (contentLength < 1) throw new ArgumentException("Content length must be greater than zero.");
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            if (!stream.CanRead) throw new ArgumentException("Cannot read from supplied stream.");
            if (processChunk == null) throw new ArgumentNullException(nameof(processChunk));

            #region Initialize

            List<DedupeChunk> chunks = new List<DedupeChunk>();
            DedupeObjectMap map = null;
            DedupeChunk chunk = null;
            long bytesRead = 0;
            string chunkKey = null;
            
            #endregion

            if (contentLength <= _Settings.MinChunkSize)
            {
                #region Single-Chunk

                byte[] chunkData = DedupeCommon.ReadBytesFromStream(stream, contentLength, out bytesRead);
                chunkKey = DedupeCommon.BytesToBase64(DedupeCommon.Sha256(chunkData));
                chunk = new DedupeChunk(chunkKey, chunkData.Length, 1, chunkData);
                chunks.Add(chunk);

                map = new DedupeObjectMap(key, chunkKey, chunk.Length, 0, 0);
                processChunk(chunk, map);
                return chunks;

                #endregion
            }
            else
            {
                #region Sliding-Window

                Streams streamWindow = new Streams(stream, contentLength, _Settings.MinChunkSize, _Settings.ShiftCount);
                byte[] chunkData = null;
                long chunkAddress = 0;     // should only be set at the beginning of a new chunk

                while (true)
                {
                    byte[] newData = null;
                    bool finalChunk = false;

                    long tempPosition = 0;
                    byte[] window = streamWindow.GetNextChunk(out tempPosition, out newData, out finalChunk);
                    if (window == null) return chunks;
                    if (chunkData == null) chunkAddress = tempPosition;

                    if (chunkData == null)
                    {
                        // starting a new chunk
                        chunkData = new byte[window.Length];
                        Buffer.BlockCopy(window, 0, chunkData, 0, window.Length);
                    }
                    else
                    {
                        // append new data
                        chunkData = DedupeCommon.AppendBytes(chunkData, newData);
                    }

                    byte[] md5Hash = DedupeCommon.Md5(window);
                    if (DedupeCommon.IsZeroBytes(md5Hash, _Settings.BoundaryCheckBytes)
                        || chunkData.Length >= _Settings.MaxChunkSize)
                    {
                        #region Chunk-Boundary

                        chunkKey = DedupeCommon.BytesToBase64(DedupeCommon.Sha256(chunkData));

                        chunk = new DedupeChunk(chunkKey, chunkData.Length, 1, chunkData);
                        map = new DedupeObjectMap(key, chunk.Key, chunkData.Length, chunks.Count, chunkAddress);
                        processChunk(chunk, map);
                        chunk.Data = null;
                        chunks.Add(chunk);

                        chunk = null;
                        chunkData = null;

                        streamWindow.AdvanceToNewChunk();

                        #endregion
                    }
                    else
                    {
                        // do nothing, continue; 
                    }

                    if (finalChunk)
                    {
                        #region Final-Chunk

                        if (chunkData != null)
                        {
                            chunkKey = DedupeCommon.BytesToBase64(DedupeCommon.Sha256(chunkData));
                            chunk = new DedupeChunk(chunkKey, chunkData.Length, 1, chunkData);
                            map = new DedupeObjectMap(key, chunk.Key, chunk.Length, chunks.Count, chunkAddress);
                            processChunk(chunk, map);
                            chunk.Data = null;
                            chunks.Add(chunk);
                            break;
                        }

                        #endregion
                    }
                }

                #endregion
            }

            return chunks; 
        } 

        #endregion
    }
}
