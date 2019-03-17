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
    public class SqliteWrapper
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private string IndexFile;
        private string ConnStr;
        private SQLiteConnection Conn;
        private bool Debug;

        private readonly object ConfigLock;
        private readonly object ChunkRefcountLock;
        private readonly object ObjectLock;

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

            IndexFile = indexFile;

            ConnStr = "Data Source=" + IndexFile + ";Version=3;";

            CreateFile(IndexFile);
            Connect();
            CreateConfigTable();
            CreateObjectMapTable();
            CreateChunkRefcountTable();
            Debug = debug;

            ConfigLock = new object();
            ChunkRefcountLock = new object();
            ObjectLock = new object();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Execute a SQL query.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="result">DataTable containing results.</param>
        /// <returns>Boolean indicating success or failure.</returns>
        public bool Query(string query, out DataTable result)
        {
            result = new DataTable();
            
            try
            {
                if (String.IsNullOrEmpty(query)) return false;

                using (SQLiteCommand cmd = new SQLiteCommand(query, Conn))
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
                if (Debug)
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

            string keyCheckQuery = "SELECT * FROM dedupe_config WHERE key = '" + key + "'";
            DataTable keyCheckResult;

            string keyDeleteQuery = "DELETE FROM dedupe_config WHERE key = '" + key + "'";
            DataTable keyDeleteResult;

            string keyInsertQuery = "INSERT INTO dedupe_config (key, val) VALUES ('" + key + "', '" + val + "')";
            DataTable keyInsertResult;

            lock (ConfigLock)
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
        /// <returns>Boolean indicating success.</returns>
        public bool GetConfigData(string key, out string val)
        {
            val = null;
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            key = Common.SanitizeString(key);

            string keyQuery = "SELECT val FROM dedupe_config WHERE key = '" + key + "' LIMIT 1";
            DataTable result;

            lock (ConfigLock)
            {
                if (Query(keyQuery, out result))
                {
                    if (result != null && result.Rows.Count > 0)
                    {
                        foreach (DataRow curr in result.Rows)
                        {
                            val = curr["val"].ToString();
                            if (Debug) Console.WriteLine("Returning " + key + ": " + val);
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

            string query = "SELECT * FROM object_map WHERE object_name = '" + objectName + "' LIMIT 1";
            DataTable result;
            
            lock (ObjectLock)
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

            string query = "SELECT DISTINCT object_name FROM object_map";
            DataTable result;

            lock (ObjectLock)
            {
                if (Query(query, out result))
                {
                    if (result != null && result.Rows.Count > 0)
                    {
                        foreach (DataRow curr in result.Rows)
                        {
                            keys.Add(curr["object_name"].ToString());
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
        /// <returns>Boolean indicating success.</returns>
        public bool AddObjectChunks(string objectName, int totalLen, List<Chunk> chunks)
        {
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            if (totalLen < 1) throw new ArgumentException("Total length must be greater than zero.");
            if (chunks == null || chunks.Count < 1) throw new ArgumentException("No chunk data supplied.");

            objectName = Common.SanitizeString(objectName);

            if (ObjectExists(objectName)) return false;

            DataTable result;
            List<string> addObjectChunksQueries = BatchAddObjectChunksQuery(objectName, totalLen, chunks);

            lock (ObjectLock)
            {
                foreach (string query in addObjectChunksQueries)
                {
                    if (!Query(query, out result))
                    {
                        if (Debug) Console.WriteLine("Insert query failed: " + query);
                        return false;
                    }
                }

                foreach (Chunk currChunk in chunks)
                {
                    if (!IncrementChunkRefcount(currChunk.Key, currChunk.Length))
                    {
                        if (Debug) Console.WriteLine("Unable to increment refcount for chunk: " + currChunk.Key);
                        return false;
                    }
                }
            }
            
            return true;
        }

        /// <summary>
        /// Retrieve chunks associated with an object.
        /// </summary>
        /// <param name="objectName">The name of the object.</param>
        /// <param name="chunks">The chunks from the object.</param>
        /// <returns>Boolean indicating success.</returns>
        public bool GetObjectChunks(string objectName, out List<Chunk> chunks)
        {
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));

            objectName = Common.SanitizeString(objectName);

            chunks = new List<Chunk>();

            string query = "SELECT * FROM object_map WHERE object_name = '" + objectName + "'";
            DataTable result;
            bool success = false;
            lock (ObjectLock)
            {
                success = Query(query, out result);
            }

            if (result == null || result.Rows.Count < 1) return false;
            if (!success) return false;

            foreach (DataRow curr in result.Rows)
            {
                chunks.Add(Chunk.FromDataRow(curr));
            }

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

            string selectQuery = "SELECT * FROM object_map WHERE object_name = '" + objectName + "'";
            string deleteObjectMapQuery = "DELETE FROM object_map WHERE object_name = '" + objectName + "'";
            DataTable result;
            bool garbageCollect = false;

            lock (ObjectLock)
            {
                if (!Query(selectQuery, out result))
                {
                    if (Debug)
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
                    if (Debug)
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
        /// <returns>Boolean indicating success.</returns>
        public bool IncrementChunkRefcount(string chunkKey, int len)
        {
            if (String.IsNullOrEmpty(chunkKey)) throw new ArgumentNullException(nameof(chunkKey));

            chunkKey = Common.SanitizeString(chunkKey);

            string selectQuery = "";
            string updateQuery = "";
            string insertQuery = "";

            DataTable selectResult;
            DataTable updateResult;
            DataTable insertResult;

            selectQuery = "SELECT * FROM chunk_refcount WHERE chunk_key = '" + chunkKey + "'";
            insertQuery = "INSERT INTO chunk_refcount (chunk_key, chunk_len, ref_count) VALUES ('" + chunkKey + "', '" + len + "', 1)";

            lock (ChunkRefcountLock)
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
                            currCount = Convert.ToInt32(curr["ref_count"]);
                        }

                        currCount++;

                        updateQuery = "UPDATE chunk_refcount SET ref_count = '" + currCount + "' WHERE chunk_key = '" + chunkKey + "'";
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
        /// <returns>Boolean indicating success.</returns>
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

            selectQuery = "SELECT * FROM chunk_refcount WHERE chunk_key = '" + chunkKey + "'";
            deleteQuery = "DELETE FROM chunk_refcount WHERE chunk_key = '" + chunkKey + "'";

            lock (ChunkRefcountLock)
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
                            currCount = Convert.ToInt32(curr["ref_count"]);
                        }

                        currCount--;
                        if (currCount == 0)
                        {
                            garbageCollect = true;
                            return Query(deleteQuery, out deleteResult);
                        }
                        else
                        {
                            updateQuery = "UPDATE chunk_refcount SET ref_count = '" + currCount + "' WHERE chunk_key = '" + chunkKey + "'";
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
        /// <returns>Boolean indicating success.</returns>
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
                "  (SELECT COUNT(*) AS num_objects FROM " + 
                "    (SELECT DISTINCT(object_name) FROM object_map) object_names " +
                "  ) num_objects, " +
                "  (SELECT COUNT(*) AS num_chunks FROM chunk_refcount) num_chunks, " +
                "  (SELECT SUM(chunk_len * ref_count) AS logical_bytes FROM chunk_refcount) logical_bytes, " +
                "  (SELECT SUM(chunk_len) AS physical_bytes FROM chunk_refcount) physical_bytes " +
                ")";

            DataTable result;

            lock (ChunkRefcountLock)
            {
                if (!Query(query, out result))
                {
                    if (Debug) Console.WriteLine("Unable to retrieve index stats");
                    return false;
                }

                if (result == null || result.Rows.Count < 1) return true;
            }

            foreach (DataRow curr in result.Rows)
            {
                if (curr["num_objects"] != DBNull.Value) numObjects = Convert.ToInt32(curr["num_objects"]);
                if (curr["num_chunks"] != DBNull.Value) numChunks = Convert.ToInt32(curr["num_chunks"]);
                if (curr["logical_bytes"] != DBNull.Value) logicalBytes = Convert.ToInt32(curr["logical_bytes"]);
                if (curr["physical_bytes"] != DBNull.Value) physicalBytes = Convert.ToInt32(curr["physical_bytes"]);

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
        /// <returns>Boolean indicating success.</returns>
        public bool BackupIndex(string destination)
        {
            if (String.IsNullOrEmpty(destination)) throw new ArgumentNullException(nameof(destination));
            
            bool copySuccess = false;
            using (SQLiteCommand cmd = new SQLiteCommand("BEGIN IMMEDIATE;", Conn))
            {
                cmd.ExecuteNonQuery();
            }

            try
            {
                File.Copy(IndexFile, destination, true);
                copySuccess = true;
            }
            catch (Exception)
            {
            }

            using (SQLiteCommand cmd = new SQLiteCommand("ROLLBACK;", Conn))
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
            Conn = new SQLiteConnection(ConnStr);
            Conn.Open();
        }

        private void CreateConfigTable()
        {
            using (SQLiteCommand cmd = Conn.CreateCommand())
            {
                cmd.CommandText =
                    @"CREATE TABLE IF NOT EXISTS dedupe_config " +
                    "(" +
                    " key VARCHAR(128), " +
                    " val VARCHAR(1024) " +
                    ")";
                cmd.ExecuteNonQuery();
            }
        }
        
        private void CreateObjectMapTable()
        {
            using (SQLiteCommand cmd = Conn.CreateCommand())
            {
                cmd.CommandText =
                    @"CREATE TABLE IF NOT EXISTS object_map " +
                    "(" +
                    " object_map_id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    " object_name VARCHAR(1024), " +
                    " object_len INTEGER, " +
                    " chunk_key VARCHAR(128), " +
                    " chunk_len INTEGER, " +
                    " chunk_position INTEGER, " +
                    " chunk_address INTEGER " +
                    ")";
                cmd.ExecuteNonQuery();
            }
        }

        private void CreateChunkRefcountTable()
        {
            using (SQLiteCommand cmd = Conn.CreateCommand())
            {
                cmd.CommandText =
                    @"CREATE TABLE IF NOT EXISTS chunk_refcount " +
                    "(" +
                    " chunk_refcount_id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    " chunk_key VARCHAR(128), " +
                    " chunk_len INTEGER, " +
                    " ref_count INTEGER" +
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
                string query = "INSERT INTO object_map (object_name, object_len, chunk_key, chunk_len, chunk_position, chunk_address) VALUES ";

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
