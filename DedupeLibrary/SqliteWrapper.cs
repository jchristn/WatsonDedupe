using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;

namespace WatsonDedupe
{
    /// <summary>
    /// Sqlite wrapper for DedupeLibrary.
    /// </summary>
    public class SqliteWrapper
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private string _IndexFile;
        private string _ConnectionString;
        private SQLiteConnection _SqliteConnection;
        private bool _Debug;

        private readonly object _ConfigLock;
        private readonly object _ChunkRefcountLock;
        private readonly object _ObjectLock;

        #endregion

        #region Constructor

        /// <summary>
        /// Instantiates the object
        /// </summary>
        /// <param name="indexFile">The index database file.</param>
        /// <param name="debug">Enable or disable console logging.</param>
        public SqliteWrapper(string indexFile, bool debug)
        {
            if (String.IsNullOrEmpty(indexFile)) throw new ArgumentNullException(nameof(indexFile));

            _IndexFile = indexFile;

            _ConnectionString = "Data Source=" + _IndexFile + ";Version=3;";

            CreateFile(_IndexFile);
            Connect();
            CreateConfigTable();
            CreateObjectMapTable();
            CreateChunkRefcountTable();
            _Debug = debug;

            _ConfigLock = new object();
            _ChunkRefcountLock = new object();
            _ObjectLock = new object();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Execute a SQL query.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="result">DataTable containing results.</param>
        /// <returns>True if successful.</returns>
        public bool Query(string query, out DataTable result)
        {
            result = new DataTable();
            
            try
            {
                if (String.IsNullOrEmpty(query)) return false;

                using (SQLiteCommand cmd = new SQLiteCommand(query, _SqliteConnection))
                {
                    using (SQLiteDataReader rdr = cmd.ExecuteReader())
                    {
                        result.Load(rdr);
                        return true;
                    }
                }
            }
            catch (Exception)
            {
                return false;
            }
            finally
            {
                if (_Debug)
                {
                    if (result != null)
                    {
                        Console.WriteLine(result.Rows.Count + " rows, query: " + query);
                    }
                }
            }
        }

        /// <summary>
        /// Add a configuration key-value pair.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="val">The value.</param>
        public void AddConfigData(string key, string val)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (String.IsNullOrEmpty(val)) throw new ArgumentNullException(nameof(val));

            key = Common.SanitizeString(key);
            val = Common.SanitizeString(val);

            string keyCheckQuery = "SELECT * FROM DedupeConfig WHERE Key = '" + key + "'";
            DataTable keyCheckResult;

            string keyDeleteQuery = "DELETE FROM DedupeConfig WHERE Key = '" + key + "'";
            DataTable keyDeleteResult;

            string keyInsertQuery = "INSERT INTO DedupeConfig (Key, Val) VALUES ('" + key + "', '" + val + "')";
            DataTable keyInsertResult;

            lock (_ConfigLock)
            {
                if (Query(keyCheckQuery, out keyCheckResult))
                {
                    Query(keyDeleteQuery, out keyDeleteResult);
                }

                Query(keyInsertQuery, out keyInsertResult);
            }

            return;
        }

        /// <summary>
        /// Retrieve a configuration value.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="val">The value.</param>
        /// <returns>True if successful.</returns>
        public bool GetConfigData(string key, out string val)
        {
            val = null;
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            key = Common.SanitizeString(key);

            string keyQuery = "SELECT Val FROM DedupeConfig WHERE Key = '" + key + "' LIMIT 1";
            DataTable result;

            lock (_ConfigLock)
            {
                if (Query(keyQuery, out result))
                {
                    if (result != null && result.Rows.Count > 0)
                    {
                        foreach (DataRow curr in result.Rows)
                        {
                            val = curr["Val"].ToString();
                            if (_Debug) Console.WriteLine("Returning " + key + ": " + val);
                            return true;
                        }
                    }
                }
            }

            return false;
        }

        /// <summary>
        /// Determine if an object exists in the index.
        /// </summary>
        /// <param name="objectName">The name of the object.</param>
        /// <returns>Boolean indicating if the object exists.</returns>
        public bool ObjectExists(string objectName)
        {
            if (String.IsNullOrEmpty(objectName)) return false;

            objectName = Common.SanitizeString(objectName);

            string query = "SELECT * FROM ObjectMap WHERE Name = '" + objectName + "' LIMIT 1";
            DataTable result;
            
            lock (_ObjectLock)
            {
                if (Query(query, out result))
                {
                    if (result != null && result.Rows.Count > 0) return true;
                }
            }

            return false;
        }
        
        /// <summary>
        /// List the objects stored in the index.
        /// </summary>
        /// <param name="keys">List of object keys.</param>
        public void ListObjects(out List<string> keys)
        {
            keys = new List<string>();

            string query = "SELECT DISTINCT Name FROM ObjectMap";
            DataTable result;

            lock (_ObjectLock)
            {
                if (Query(query, out result))
                {
                    if (result != null && result.Rows.Count > 0)
                    {
                        foreach (DataRow curr in result.Rows)
                        {
                            keys.Add(curr["Name"].ToString());
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Add chunks from an object to the index.
        /// </summary>
        /// <param name="objectName">The name of the object.</param>
        /// <param name="totalLen">The total length of the object.</param>
        /// <param name="chunks">The chunks from the object.</param>
        /// <returns>True if successful.</returns>
        public bool AddObjectChunks(string objectName, int totalLen, List<Chunk> chunks)
        {
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            if (totalLen < 1) throw new ArgumentException("Total length must be greater than zero.");
            if (chunks == null || chunks.Count < 1) throw new ArgumentException("No chunk data supplied.");

            objectName = Common.SanitizeString(objectName);

            if (ObjectExists(objectName)) return false;

            DataTable result;
            List<string> addObjectChunksQueries = BatchAddObjectChunksQuery(objectName, totalLen, chunks);

            lock (_ObjectLock)
            {
                foreach (string query in addObjectChunksQueries)
                {
                    if (!Query(query, out result))
                    {
                        if (_Debug) Console.WriteLine("Insert query failed: " + query);
                        return false;
                    }
                }

                foreach (Chunk currChunk in chunks)
                {
                    if (!IncrementChunkRefcount(currChunk.Key, currChunk.Length))
                    {
                        if (_Debug) Console.WriteLine("Unable to increment refcount for chunk: " + currChunk.Key);
                        return false;
                    }
                }
            }
            
            return true;
        }
         
        /// <summary>
        /// Retrieve metadata for a given object.
        /// </summary>
        /// <param name="objectName">The name of the object.</param>
        /// <param name="metadata">Object metadata.</param>
        /// <returns>True if successful.</returns>
        public bool GetObjectMetadata(string objectName, out ObjectMetadata metadata)
        {
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));

            objectName = Common.SanitizeString(objectName);
            metadata = null;

            string query = "SELECT * FROM ObjectMap WHERE Name = '" + objectName + "'";
            DataTable result;
            bool success = false;
            lock (_ObjectLock)
            {
                success = Query(query, out result);
            }

            if (result == null || result.Rows.Count < 1) return false;
            if (!success) return false;

            metadata = ObjectMetadata.FromDataTable(result);
            return true;
        }

        /// <summary>
        /// Delete an object and dereference the associated chunks.
        /// </summary>
        /// <param name="objectName">The name of the object.</param>
        /// <param name="garbageCollectChunks">List of chunk keys that should be garbage collected.</param>
        public void DeleteObjectChunks(string objectName, out List<string> garbageCollectChunks)
        {
            garbageCollectChunks = new List<string>();
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));

            objectName = Common.SanitizeString(objectName);

            string selectQuery = "SELECT * FROM ObjectMap WHERE Name = '" + objectName + "'";
            string deleteObjectMapQuery = "DELETE FROM ObjectMap WHERE Name = '" + objectName + "'";
            DataTable result;
            bool garbageCollect = false;

            lock (_ObjectLock)
            {
                if (!Query(selectQuery, out result))
                {
                    if (_Debug)
                    {
                        Console.WriteLine("Unable to retrieve object map for object: " + objectName);
                    }
                }

                if (result == null || result.Rows.Count < 1) return;

                foreach (DataRow curr in result.Rows)
                {
                    Chunk c = Chunk.FromDataRow(curr);
                    DecrementChunkRefcount(c.Key, out garbageCollect);
                    if (garbageCollect) garbageCollectChunks.Add(c.Key);
                }

                if (!Query(deleteObjectMapQuery, out result))
                {
                    if (_Debug)
                    {
                        Console.WriteLine("Unable to delete object map entries for object: " + objectName);
                    }
                }
            }
        }

        /// <summary>
        /// Increment the reference count of a chunk key, or insert the key.
        /// </summary>
        /// <param name="chunkKey">The chunk key.</param>
        /// <param name="len">The length of the chunk.</param>
        /// <returns>True if successful.</returns>
        public bool IncrementChunkRefcount(string chunkKey, long len)
        {
            if (String.IsNullOrEmpty(chunkKey)) throw new ArgumentNullException(nameof(chunkKey));

            chunkKey = Common.SanitizeString(chunkKey);

            string selectQuery = "";
            string updateQuery = "";
            string insertQuery = "";

            DataTable selectResult;
            DataTable updateResult;
            DataTable insertResult;

            selectQuery = "SELECT * FROM ChunkRefcount WHERE ChunkKey = '" + chunkKey + "'";
            insertQuery = "INSERT INTO ChunkRefcount (ChunkKey, ChunkLength, RefCount) VALUES ('" + chunkKey + "', '" + len + "', 1)";

            lock (_ChunkRefcountLock)
            {
                if (Query(selectQuery, out selectResult))
                {
                    if (selectResult == null || selectResult.Rows.Count < 1)
                    {
                        #region New-Entry

                        return Query(insertQuery, out insertResult);

                        #endregion
                    }
                    else
                    {
                        #region Update

                        int currCount = 0;
                        foreach (DataRow curr in selectResult.Rows)
                        {
                            currCount = Convert.ToInt32(curr["RefCount"]);
                        }

                        currCount++;

                        updateQuery = "UPDATE ChunkRefcount SET RefCount = '" + currCount + "' WHERE ChunkKey = '" + chunkKey + "'";
                        return Query(updateQuery, out updateResult);

                        #endregion
                    }
                }
                else
                {
                    return false;
                }
            }

            throw new NotImplementedException();
        }

        /// <summary>
        /// Decrement the reference count of a chunk key, or delete the key.
        /// </summary>
        /// <param name="chunkKey">The chunk key.</param>
        /// <param name="garbageCollect">Boolean indicating if the chunk should be garbage collected.</param>
        /// <returns>True if successful.</returns>
        public bool DecrementChunkRefcount(string chunkKey, out bool garbageCollect)
        {
            garbageCollect = false;
            if (String.IsNullOrEmpty(chunkKey)) throw new ArgumentNullException(nameof(chunkKey));

            chunkKey = Common.SanitizeString(chunkKey);

            string selectQuery = "";
            string updateQuery = "";
            string deleteQuery = "";

            DataTable selectResult;
            DataTable updateResult;
            DataTable deleteResult;

            selectQuery = "SELECT * FROM ChunkRefcount WHERE ChunkKey = '" + chunkKey + "'";
            deleteQuery = "DELETE FROM ChunkRefcount WHERE ChunkKey = '" + chunkKey + "'";

            lock (_ChunkRefcountLock)
            {
                if (Query(selectQuery, out selectResult))
                {
                    if (selectResult == null || selectResult.Rows.Count < 1)
                    {
                        return false;
                    }
                    else
                    {
                        int currCount = 0;
                        foreach (DataRow curr in selectResult.Rows)
                        {
                            currCount = Convert.ToInt32(curr["RefCount"]);
                        }

                        currCount--;
                        if (currCount == 0)
                        {
                            garbageCollect = true;
                            return Query(deleteQuery, out deleteResult);
                        }
                        else
                        {
                            updateQuery = "UPDATE ChunkRefcount SET RefCount = '" + currCount + "' WHERE ChunkKey = '" + chunkKey + "'";
                            return Query(updateQuery, out updateResult);
                        }
                    }
                }
                else
                {
                    return false;
                }
            }
        }

        /// <summary>
        /// Retrieve index statistics.
        /// </summary>
        /// <param name="numObjects">The number of objects stored in the index.</param>
        /// <param name="numChunks">The number of chunks stored in the index.</param>
        /// <param name="logicalBytes">The amount of data stored in the index, i.e. the full size of the original data.</param>
        /// <param name="physicalBytes">The number of bytes consumed by chunks of data, i.e. the deduplication set size.</param>
        /// <param name="dedupeRatioX">Deduplication ratio represented as a multiplier.</param>
        /// <param name="dedupeRatioPercent">Deduplication ratio represented as a percentage.</param>
        /// <returns>True if successful.</returns>
        public bool IndexStats(out int numObjects, out int numChunks, out long logicalBytes, out long physicalBytes, out decimal dedupeRatioX, out decimal dedupeRatioPercent)
        {
            numObjects = 0;
            numChunks = 0;
            logicalBytes = 0;
            physicalBytes = 0;
            dedupeRatioX = 0m;
            dedupeRatioPercent = 0m;

            string query =
                "SELECT * FROM " +
                "(" +
                "  (SELECT COUNT(*) AS NumObjects FROM " + 
                "    (SELECT DISTINCT(Name) FROM ObjectMap) Names " +
                "  ) NumObjects, " +
                "  (SELECT COUNT(*) AS NumChunks FROM ChunkRefcount) NumChunks, " +
                "  (SELECT SUM(ChunkLength * RefCount) AS LogicalBytes FROM ChunkRefcount) LogicalBytes, " +
                "  (SELECT SUM(ChunkLength) AS PhysicalBytes FROM ChunkRefcount) PhysicalBytes " +
                ")";

            DataTable result;

            lock (_ChunkRefcountLock)
            {
                if (!Query(query, out result))
                {
                    if (_Debug) Console.WriteLine("Unable to retrieve index stats");
                    return false;
                }

                if (result == null || result.Rows.Count < 1) return true;
            }

            foreach (DataRow curr in result.Rows)
            {
                if (curr["NumObjects"] != DBNull.Value) numObjects = Convert.ToInt32(curr["NumObjects"]);
                if (curr["NumChunks"] != DBNull.Value) numChunks = Convert.ToInt32(curr["NumChunks"]);
                if (curr["LogicalBytes"] != DBNull.Value) logicalBytes = Convert.ToInt32(curr["LogicalBytes"]);
                if (curr["PhysicalBytes"] != DBNull.Value) physicalBytes = Convert.ToInt32(curr["PhysicalBytes"]);

                if (physicalBytes > 0 && logicalBytes > 0)
                {
                    dedupeRatioX = (decimal)logicalBytes / (decimal)physicalBytes;
                    dedupeRatioPercent = 100 * (1 - ((decimal)physicalBytes / (decimal)logicalBytes));
                }
            }

            return true;
        }

        /// <summary>
        /// Copies the index database to another file.
        /// </summary>
        /// <param name="destination">The destination file.</param>
        /// <returns>True if successful.</returns>
        public bool BackupIndex(string destination)
        {
            if (String.IsNullOrEmpty(destination)) throw new ArgumentNullException(nameof(destination));
            
            bool copySuccess = false;
            using (SQLiteCommand cmd = new SQLiteCommand("BEGIN IMMEDIATE;", _SqliteConnection))
            {
                cmd.ExecuteNonQuery();
            }

            try
            {
                File.Copy(_IndexFile, destination, true);
                copySuccess = true;
            }
            catch (Exception)
            {
            }

            using (SQLiteCommand cmd = new SQLiteCommand("ROLLBACK;", _SqliteConnection))
            {
                cmd.ExecuteNonQuery();
            }

            return copySuccess;
        }

        #endregion

        #region Private-Methods

        private void CreateFile(string filename)
        {
            if (!File.Exists(filename))
            {
                SQLiteConnection.CreateFile(filename);
            }
        }

        private void Connect()
        {
            _SqliteConnection = new SQLiteConnection(_ConnectionString);
            _SqliteConnection.Open();
        }

        private void CreateConfigTable()
        {
            using (SQLiteCommand cmd = _SqliteConnection.CreateCommand())
            {
                cmd.CommandText =
                    @"CREATE TABLE IF NOT EXISTS DedupeConfig " +
                    "(" +
                    " Key VARCHAR(128), " +
                    " Val VARCHAR(1024) " +
                    ")";
                cmd.ExecuteNonQuery();
            }
        }
        
        private void CreateObjectMapTable()
        {
            using (SQLiteCommand cmd = _SqliteConnection.CreateCommand())
            {
                cmd.CommandText =
                    @"CREATE TABLE IF NOT EXISTS ObjectMap " +
                    "(" +
                    " ObjectMapId INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    " Name VARCHAR(1024), " +
                    " ContentLength INTEGER, " +
                    " ChunkKey VARCHAR(128), " +
                    " ChunkLength INTEGER, " +
                    " ChunkPosition INTEGER, " +
                    " ChunkAddress INTEGER " +
                    ")";
                 
                cmd.ExecuteNonQuery();
            }
        }

        private void CreateChunkRefcountTable()
        {
            using (SQLiteCommand cmd = _SqliteConnection.CreateCommand())
            {
                cmd.CommandText =
                    @"CREATE TABLE IF NOT EXISTS ChunkRefcount " +
                    "(" +
                    " ChunkRefcountId INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    " ChunkKey VARCHAR(128), " +
                    " ChunkLength INTEGER, " +
                    " RefCount INTEGER" +
                    ")";
                cmd.ExecuteNonQuery();
            }
        }

        private List<string> BatchAddObjectChunksQuery(string objectName, int totalLen, List<Chunk> chunks)
        {
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));

            List<string> ret = new List<string>();

            bool moreRecords = true;
            int batchMaxSize = 32;
            int totalRecords = chunks.Count;
            int currPosition = 0;
            int remainingRecords = 0;

            while (moreRecords)
            {
                string query = "INSERT INTO ObjectMap (Name, ContentLength, ChunkKey, ChunkLength, ChunkPosition, ChunkAddress) VALUES ";

                remainingRecords = totalRecords - currPosition;
                if (remainingRecords > batchMaxSize)
                {
                    #region Max-Size-Records

                    for (int i = 0; i < batchMaxSize; i++)
                    {
                        if (i > 0) query += ", ";
                        query +=
                            "(" +
                            "'" + objectName + "', " +
                            "'" + totalLen + "', " +
                            "'" + Common.SanitizeString(chunks[i + currPosition].Key) + "', " +
                            "'" + chunks[i + currPosition].Length + "', " +
                            "'" + chunks[i + currPosition].Position + "', " +
                            "'" + chunks[i + currPosition].Address + "'" +
                            ")";
                    }

                    currPosition += batchMaxSize;
                    ret.Add(query);

                    #endregion
                }
                else if (remainingRecords > 0)
                {
                    #region N-Records

                    for (int i = 0; i < remainingRecords; i++)
                    {
                        if (i > 0) query += ", ";
                        query +=
                            "(" +
                            "'" + objectName + "', " +
                            "'" + totalLen + "', " +
                            "'" + Common.SanitizeString(chunks[i + currPosition].Key) + "', " +
                            "'" + chunks[i + currPosition].Length + "', " +
                            "'" + chunks[i + currPosition].Position + "', " +
                            "'" + chunks[i + currPosition].Address + "'" +
                            ")";
                    }

                    currPosition += remainingRecords;
                    ret.Add(query);

                    #endregion
                }
                else
                {
                    moreRecords = false;
                }
            }

            return ret;
        }

        #endregion

        #region Public-Static-Methods

        #endregion

        #region Private-Static-Methods

        #endregion
    }
}
