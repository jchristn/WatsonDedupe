using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mono.Data.Sqlite;

namespace WatsonDedupe
{
    public class SqlitePoolWrapperXL
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private string IndexFile;
        private string ConnStr;
        private SqliteConnection Conn;
        private bool Debug;

        private readonly object ConfigLock;
        private readonly object ChunkRefcountLock;

        #endregion

        #region Constructor

        /// <summary>
        /// Instantiates the object
        /// </summary>
        /// <param name="indexFile">The index database file.</param>
        /// <param name="debug">Enable or disable console logging.</param>
        public SqlitePoolWrapperXL(string indexFile, bool debug)
        {
            if (String.IsNullOrEmpty(indexFile)) throw new ArgumentNullException(nameof(indexFile));

            IndexFile = indexFile;

            ConnStr = "Data Source=" + IndexFile + ";Version=3;";

            CreateFile(IndexFile);
            ConnectPoolIndex();
            CreatePoolIndexConfigTable();
            CreateContainerFileMapTable();
            CreatePoolIndexChunkRefcountTable();
            Debug = debug;

            ConfigLock = new object();
            ChunkRefcountLock = new object();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Execute a SQL query against the pool index.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="result">DataTable containing results.</param>
        /// <returns>Boolean indicating success or failure.</returns>
        public bool QueryPoolIndex(string query, out DataTable result)
        {
            result = new DataTable();

            try
            {
                if (String.IsNullOrEmpty(query)) return false;

                using (SqliteCommand cmd = new SqliteCommand(query, Conn))
                {
                    using (SqliteDataReader rdr = cmd.ExecuteReader())
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
                        Console.WriteLine(result.Rows.Count + " rows [pool], query: " + query);
                    }
                }
            }
        }

        /// <summary>
        /// Execute a SQL query against a container index.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="containerIndexFile">The path to the index file for the container.</param>
        /// <param name="result">DataTable containing results.</param>
        /// <returns>Boolean indicating success or failure.</returns>
        public bool QueryContainerIndex(string query, string containerIndexFile, out DataTable result)
        {
            result = new DataTable();
            if (String.IsNullOrEmpty(containerIndexFile)) throw new ArgumentNullException(nameof(containerIndexFile));

            string connStr = "Data Source=" + containerIndexFile + ";Version=3;";
            
            using (SqliteConnection conn = new SqliteConnection(connStr))
            {
                conn.Open();

                try
                {
                    if (String.IsNullOrEmpty(query)) return false;

                    using (SqliteCommand cmd = new SqliteCommand(query, conn))
                    {
                        using (SqliteDataReader rdr = cmd.ExecuteReader())
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
                            Console.WriteLine(result.Rows.Count + " rows [container], query: " + query);
                        }
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

            string keyCheckQuery = "SELECT * FROM dedupe_config WHERE key = '" + key + "'";
            DataTable keyCheckResult;

            string keyDeleteQuery = "DELETE FROM dedupe_config WHERE key = '" + key + "'";
            DataTable keyDeleteResult;

            string keyInsertQuery = "INSERT INTO dedupe_config (key, val) VALUES ('" + key + "', '" + val + "')";
            DataTable keyInsertResult;

            lock (ConfigLock)
            {
                if (QueryPoolIndex(keyCheckQuery, out keyCheckResult))
                {
                    QueryPoolIndex(keyDeleteQuery, out keyDeleteResult);
                }

                QueryPoolIndex(keyInsertQuery, out keyInsertResult);
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

            string keyQuery = "SELECT val FROM dedupe_config WHERE key = '" + key + "' LIMIT 1";
            DataTable result;

            lock (ConfigLock)
            {
                if (QueryPoolIndex(keyQuery, out result))
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
        /// Determine if a container exists in the index.
        /// </summary>
        /// <param name="containerName">The name of the container.</param>
        /// <returns>Boolean indicating if the container exists.</returns>
        public bool ContainerExists(string containerName)
        {
            if (String.IsNullOrEmpty(containerName)) return false;

            string query = "SELECT * FROM container_file_map WHERE container_name = '" + containerName + "' LIMIT 1";
            DataTable result;

            if (QueryPoolIndex(query, out result))
            {
                if (result != null && result.Rows.Count > 0) return true;
            }

            return false;
        }

        /// <summary>
        /// Determine if an object exists within a container.
        /// </summary>
        /// <param name="objectName">The name of the object.</param>
        /// <param name="containerName">The name of the container.</param>
        /// <param name="containerIndexFile">The path to the index file for the container.</param>
        /// <returns>Boolean indicating if the object exists within the container.</returns>
        public bool ObjectExists(string objectName, string containerName, string containerIndexFile)
        {
            if (String.IsNullOrEmpty(objectName)) return false;
            if (String.IsNullOrEmpty(containerName)) return false;
            if (String.IsNullOrEmpty(containerIndexFile)) return false;

            if (!ContainerExists(containerName)) return false;

            string query = "SELECT * FROM object_map WHERE object_name = '" + objectName + "' AND container_name = '" + containerName + "' LIMIT 1";
            DataTable result;

            if (QueryContainerIndex(query, containerIndexFile, out result))
            {
                if (result != null && result.Rows.Count > 0) return true;
            }

            return false;
        }

        /// <summary>
        /// List the containers stored in the index.
        /// </summary>
        /// <param name="keys">List of container keys.</param>
        public void ListContainers(out List<string> keys)
        {
            keys = new List<string>();

            string query = "SELECT DISTINCT container_name FROM container_file_map";
            DataTable result;

            if (QueryPoolIndex(query, out result))
            {
                if (result != null && result.Rows.Count > 0)
                {
                    foreach (DataRow curr in result.Rows)
                    {
                        keys.Add(curr["container_name"].ToString());
                    }
                }
            }
        }

        /// <summary>
        /// List the objects stored in a container.
        /// </summary>
        /// <param name="containerName">The name of the container.</param>
        /// <param name="containerIndexFile">The path to the index file for the container.</param>
        /// <param name="keys">List of container keys.</param>
        public void ListObjects(string containerName, string containerIndexFile, out List<string> keys)
        {
            keys = new List<string>();

            string query = "SELECT DISTINCT object_name FROM object_map";
            DataTable result;

            if (QueryContainerIndex(query, containerIndexFile, out result))
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

        /// <summary>
        /// Add chunks from an object to the container and to the index.
        /// </summary>
        /// <param name="objectName">The name of the object.  Must be unique in the container.</param>
        /// <param name="containerName">The name of the container.  Must be unique in the index.</param>
        /// <param name="containerIndexFile">The path to the index file for the object.</param>
        /// <param name="totalLen">The total length of the object.</param>
        /// <param name="chunks">The chunks from the object.</param>
        /// <returns>Boolean indicating success.</returns>
        public bool AddObjectChunks(string objectName, string containerName, string containerIndexFile, int totalLen, List<Chunk> chunks)
        {
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            if (String.IsNullOrEmpty(containerName)) throw new ArgumentNullException(nameof(containerName));
            if (String.IsNullOrEmpty(containerIndexFile)) throw new ArgumentNullException(nameof(containerIndexFile));
            if (totalLen < 1) throw new ArgumentException("Total length must be greater than zero.");
            if (chunks == null || chunks.Count < 1) throw new ArgumentException("No chunk data supplied.");

            if (ObjectExists(objectName, containerName, containerIndexFile)) throw new IOException("Object already exists in container");

            DataTable result;
            List<string> addContainerChunksQuery = BatchAddContainerChunksQuery(objectName, containerName, totalLen, chunks);

            CreateFile(containerIndexFile);
            CreateContainerObjectMapTable(containerIndexFile);
            
            foreach (string query in addContainerChunksQuery)
            {
                if (!QueryContainerIndex(query, containerIndexFile, out result))
                {
                    if (Debug) Console.WriteLine("Container insert query failed: " + query);
                    return false;
                }
            }

            lock (ChunkRefcountLock)
            {
                foreach (Chunk currChunk in chunks)
                {
                    if (!IncrementRefcount(currChunk.Key, currChunk.Length))
                    {
                        if (Debug) Console.WriteLine("Unable to increment refcount for chunk " + currChunk.Key + " in container " + containerName);
                        return false;
                    }
                }
            }

            AddContainerFileMap(containerName, containerIndexFile);
            return true;
        }

        /// <summary>
        /// Retrieve chunks associated with an object within a container.
        /// </summary>
        /// <param name="objectName">The name of the object.</param>
        /// <param name="containerName">The name of the container.</param>
        /// <param name="containerIndexFile">The path to the index file for the container.</param>
        /// <param name="chunks">The chunks from the object.</param>
        /// <returns>Boolean indicating success.</returns>
        public bool GetObjectChunks(string objectName, string containerName, string containerIndexFile, out List<Chunk> chunks)
        {
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            if (String.IsNullOrEmpty(containerName)) throw new ArgumentNullException(nameof(containerName));
            if (String.IsNullOrEmpty(containerIndexFile)) throw new ArgumentNullException(nameof(containerIndexFile));
            chunks = new List<Chunk>();
            
            string query = "SELECT * FROM object_map WHERE object_name = '" + objectName + "' AND container_name = '" + containerName + "'";
            DataTable result;
            bool success = QueryContainerIndex(query, containerIndexFile, out result);

            if (result == null || result.Rows.Count < 1) return false;
            if (!success) return false;

            foreach (DataRow curr in result.Rows)
            {
                chunks.Add(Chunk.FromDataRow(curr));
            }

            return true;
        }

        /// <summary>
        /// Delete an object within a container and dereference the associated chunks.
        /// </summary>
        /// <param name="objectName">The name of the object.</param>
        /// <param name="containerName">The name of the container.</param>
        /// <param name="containerIndexFile">The path to the index file for the object.</param>
        /// <param name="garbageCollectChunks">List of chunk keys that should be garbage collected.</param>
        public void DeleteObjectChunks(string objectName, string containerName, string containerIndexFile, out List<string> garbageCollectChunks)
        {
            garbageCollectChunks = new List<string>();
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            if (String.IsNullOrEmpty(containerName)) throw new ArgumentNullException(nameof(containerName));
            if (String.IsNullOrEmpty(containerIndexFile)) throw new ArgumentNullException(nameof(containerIndexFile));

            string selectQuery = "SELECT * FROM object_map WHERE object_name = '" + objectName + "' AND container_name = '" + containerName + "'";
            string deleteObjectMapQuery = "DELETE FROM object_map WHERE object_name = '" + objectName + "' AND container_name = '" + containerName + "'";
            
            DataTable result;
            bool garbageCollect = false;
            
            if (!QueryContainerIndex(selectQuery, containerIndexFile, out result))
            {
                if (Debug)
                {
                    Console.WriteLine("Unable to retrieve object map for object " + objectName + " in container " + containerName);
                }
            }

            if (result == null || result.Rows.Count < 1) return;

            lock (ChunkRefcountLock)
            {
                foreach (DataRow curr in result.Rows)
                {
                    Chunk c = Chunk.FromDataRow(curr);
                    DecrementRefcount(c.Key, out garbageCollect);
                    if (garbageCollect) garbageCollectChunks.Add(c.Key);
                }
            }

            if (!QueryContainerIndex(deleteObjectMapQuery, containerIndexFile, out result))
            {
                if (Debug)
                {
                    Console.WriteLine("Unable to delete object map entries for object " + objectName + " in container " + containerName);
                }
            }

            if (GetObjectRowCount(containerIndexFile, null) == 0)
            {
                DeleteContainerFileMap(containerName, containerIndexFile);
                File.Delete(containerIndexFile);
            }
        }

        /// <summary>
        /// Increment the reference count of a chunk key, or insert the key.
        /// </summary>
        /// <param name="chunkKey">The chunk key.</param>
        /// <param name="len">The length of the chunk.</param>
        /// <returns>Boolean indicating success.</returns>
        public bool IncrementRefcount(string chunkKey, int len)
        {
            if (String.IsNullOrEmpty(chunkKey)) throw new ArgumentNullException(nameof(chunkKey));

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
                if (QueryPoolIndex(selectQuery, out selectResult))
                {
                    if (selectResult == null || selectResult.Rows.Count < 1)
                    {
                        #region New-Entry

                        return QueryPoolIndex(insertQuery, out insertResult);

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
                        return QueryPoolIndex(updateQuery, out updateResult);

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
        public bool DecrementRefcount(string chunkKey, out bool garbageCollect)
        {
            garbageCollect = false;
            if (String.IsNullOrEmpty(chunkKey)) throw new ArgumentNullException(nameof(chunkKey));

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
                if (QueryPoolIndex(selectQuery, out selectResult))
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
                            return QueryPoolIndex(deleteQuery, out deleteResult);
                        }
                        else
                        {
                            updateQuery = "UPDATE chunk_refcount SET ref_count = '" + currCount + "' WHERE chunk_key = '" + chunkKey + "'";
                            return QueryPoolIndex(updateQuery, out updateResult);
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
        /// <param name="numContainers">The number of containers stored in the index.</param>
        /// <param name="numChunks">The number of chunks stored in the index.</param>
        /// <param name="logicalBytes">The amount of data stored in the index, i.e. the full size of the original data.</param>
        /// <param name="physicalBytes">The number of bytes consumed by chunks of data, i.e. the deduplication set size.</param>
        /// <param name="dedupeRatioX">Deduplication ratio represented as a multiplier.</param>
        /// <param name="dedupeRatioPercent">Deduplication ratio represented as a percentage.</param>
        /// <returns>Boolean indicating success.</returns>
        public bool IndexStats(out int numContainers, out int numChunks, out long logicalBytes, out long physicalBytes, out decimal dedupeRatioX, out decimal dedupeRatioPercent)
        {
            numContainers = 0;
            numChunks = 0;
            logicalBytes = 0;
            physicalBytes = 0;
            dedupeRatioX = 0m;
            dedupeRatioPercent = 0m;

            string query =
                "SELECT * FROM " +
                "(" +
                "  (SELECT COUNT(*) AS num_containers FROM " + 
                "    (SELECT DISTINCT(container_name) FROM container_file_map) container_names " +
                "  ) num_containers, " +
                "  (SELECT COUNT(*) AS num_chunks FROM chunk_refcount) num_chunks, " +
                "  (SELECT SUM(chunk_len * ref_count) AS logical_bytes FROM chunk_refcount) logical_bytes, " +
                "  (SELECT SUM(chunk_len) AS physical_bytes FROM chunk_refcount) physical_bytes " +
                ")";

            DataTable result;

            lock (ChunkRefcountLock)
            {
                if (!QueryPoolIndex(query, out result))
                {
                    if (Debug) Console.WriteLine("Unable to retrieve index stats");
                    return false;
                }

                if (result == null || result.Rows.Count < 1) return true;
            }

            foreach (DataRow curr in result.Rows)
            {
                if (curr["num_containers"] != DBNull.Value) numContainers = Convert.ToInt32(curr["num_containers"]);
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
        /// Copies the pool index database to another file.
        /// </summary>
        /// <param name="destination">The destination file.</param>
        /// <returns>Boolean indicating success.</returns>
        public bool BackupPoolIndex(string destination)
        {
            if (String.IsNullOrEmpty(destination)) throw new ArgumentNullException(nameof(destination));

            bool copySuccess = false;
            using (SqliteCommand cmd = new SqliteCommand("BEGIN IMMEDIATE;", Conn))
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

            using (SqliteCommand cmd = new SqliteCommand("ROLLBACK;", Conn))
            {
                cmd.ExecuteNonQuery();
            }

            return copySuccess;
        }

        /// <summary>
        /// Copies a container index database to another file.
        /// </summary>
        /// <param name="containerIndexFile">The path to the index file for the container.</param>
        /// <param name="destination">The destination file.</param>
        /// <param name="incrementRefCount">Indicate if chunk reference counts should be incremented after copy.</param>
        /// <returns>Boolean indicating success.</returns>
        public bool BackupContainerIndex(string containerIndexFile, string destination, bool incrementRefCount)
        {
            if (String.IsNullOrEmpty(containerIndexFile)) throw new ArgumentNullException(nameof(containerIndexFile));
            if (String.IsNullOrEmpty(destination)) throw new ArgumentNullException(nameof(destination));

            bool copySuccess = false;
            DataTable result;

            using (SqliteConnection conn = new SqliteConnection("Data Source=" + containerIndexFile + ";Version=3;"))
            {
                conn.Open();
                using (SqliteCommand cmd = new SqliteCommand("BEGIN IMMEDIATE;", conn))
                {
                    cmd.ExecuteNonQuery();
                }

                try
                {
                    File.Copy(containerIndexFile, destination, true);
                    copySuccess = true;
                }
                catch (Exception)
                {
                }

                using (SqliteCommand cmd = new SqliteCommand("ROLLBACK;", conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            if (incrementRefCount)
            {
                string query = "SELECT * FROM object_map";
                if (!QueryContainerIndex(query, containerIndexFile, out result))
                {
                    if (Debug) Console.WriteLine("Unable to retrieve object map from container " + containerIndexFile);
                    return false;
                }

                if (result != null && result.Rows.Count > 0)
                {
                    bool incrementSuccess = false;
                    foreach (DataRow curr in result.Rows)
                    {
                        incrementSuccess = IncrementRefcount(curr["chunk_key"].ToString(), Convert.ToInt32(curr["chunk_len"]));
                    }
                }
            }

            return copySuccess;
        }

        /// <summary>
        /// Imports a container index into the deduplication index.
        /// </summary>
        /// <param name="containerName">The name of the container.</param>
        /// <param name="containerIndexFile">The path to the index file for the object.</param>
        /// <param name="incrementRefCount">Indicate if chunk reference counts should be incremented after copy.</param>
        /// <returns>Boolean indicating success.</returns>
        public bool ImportContainerIndex(string containerName, string containerIndexFile, bool incrementRefCount)
        {
            if (String.IsNullOrEmpty(containerName)) throw new ArgumentNullException(nameof(containerName));
            if (String.IsNullOrEmpty(containerIndexFile)) throw new ArgumentNullException(nameof(containerIndexFile));
            if (!File.Exists(containerIndexFile)) throw new FileNotFoundException("Container index file does not exist");

            if (ContainerExists(containerName))
            {
                if (Debug) Console.WriteLine("Container " + containerName + " already exists");
                return false;
            }

            AddContainerFileMap(containerName, containerIndexFile);

            string selectQuery = "SELECT * FROM object_map WHERE container_name = '" + containerName + "'";
            DataTable selectResult;
            if (!QueryContainerIndex(selectQuery, containerIndexFile, out selectResult))
            {
                if (Debug) Console.WriteLine("Unable to query container " + containerName);
                return false;
            }

            if (selectResult == null || selectResult.Rows.Count < 1)
            {
                if (Debug) Console.WriteLine("No rows in container " + containerName);
                return true;
            }

            if (incrementRefCount)
            {
                lock (ChunkRefcountLock)
                {
                    foreach (DataRow curr in selectResult.Rows)
                    {
                        Chunk c = Chunk.FromDataRow(curr);
                        if (!IncrementRefcount(c.Key, c.Length))
                        {
                            if (Debug) Console.WriteLine("Unable to increment refcount for chunk " + c.Key);
                            return false;
                        }
                    }
                }
            }

            return true;
        }

        #endregion

        #region Private-Methods

        private void CreateFile(string filename)
        {
            if (!File.Exists(filename))
            {
                SqliteConnection.CreateFile(filename);
            }
        }

        private void ConnectPoolIndex()
        {
            Conn = new SqliteConnection(ConnStr);
            Conn.Open();
        }

        private void CreatePoolIndexConfigTable()
        {
            using (SqliteCommand cmd = Conn.CreateCommand())
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
        
        private void CreateContainerFileMapTable()
        {
            using (SqliteCommand cmd = Conn.CreateCommand())
            {
                cmd.CommandText =
                    @"CREATE TABLE IF NOT EXISTS container_file_map " +
                    "(" +
                    " container_file_map_id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    " container_name VARCHAR(1024), " +
                    " container_file VARCHAR(1024) " +
                    ")";
                cmd.ExecuteNonQuery();
            }
        }

        private void CreateContainerObjectMapTable(string containerIndexFile)
        {
            string connStr = "Data Source=" + containerIndexFile + ";Version=3;";
            SqliteConnection conn = new SqliteConnection(connStr);
            conn.Open();
            
            using (SqliteCommand cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    @"CREATE TABLE IF NOT EXISTS object_map " +
                    "(" +
                    " object_map_id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    " container_name VARCHAR(1024), " +
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

        private void CreatePoolIndexChunkRefcountTable()
        {
            using (SqliteCommand cmd = Conn.CreateCommand())
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

        private List<string> BatchAddContainerChunksQuery(string objectName, string containerName, int totalLen, List<Chunk> chunks)
        {
            List<string> ret = new List<string>();

            bool moreRecords = true;
            int batchMaxSize = 32;
            int totalRecords = chunks.Count;
            int currPosition = 0;
            int remainingRecords = 0;

            while (moreRecords)
            {
                string query = "INSERT INTO object_map (container_name, object_name, object_len, chunk_key, chunk_len, chunk_position, chunk_address) VALUES ";

                remainingRecords = totalRecords - currPosition;
                if (remainingRecords > batchMaxSize)
                {
                    #region Max-Size-Records

                    for (int i = 0; i < batchMaxSize; i++)
                    {
                        if (i > 0) query += ", ";
                        query +=
                            "(" +
                            "'" + containerName + "', " +
                            "'" + objectName + "', " +
                            "'" + totalLen + "', " +
                            "'" + chunks[i + currPosition].Key + "', " +
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
                            "'" + containerName + "', " +
                            "'" + objectName + "', " +
                            "'" + totalLen + "', " +
                            "'" + chunks[i + currPosition].Key + "', " +
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

        private void AddContainerFileMap(string containerName, string containerIndexFile)
        {
            if (String.IsNullOrEmpty(containerName)) throw new ArgumentNullException(nameof(containerName));
            if (String.IsNullOrEmpty(containerIndexFile)) throw new ArgumentNullException(nameof(containerIndexFile));

            string selectQuery = "SELECT * FROM container_file_map WHERE container_name = '" + containerName + "'";
            DataTable selectResult;

            if (!QueryPoolIndex(selectQuery, out selectResult))
            {
                if (Debug) Console.WriteLine("Unable to retrieve container file list");
                throw new IOException("Unable to access container file map table");
            }

            if (selectResult == null || selectResult.Rows.Count < 1)
            {
                string insertQuery =
                    "INSERT INTO container_file_map (container_name, container_file) VALUES " +
                    "('" + containerName + "', '" + containerIndexFile + "')";
                DataTable insertResult;

                if (!QueryPoolIndex(insertQuery, out insertResult))
                {
                    if (Debug) Console.WriteLine("Unable to add container file map for container: " + containerName);
                    throw new IOException("Unable to add container file map entry");
                }
            }
        }
        
        private void DeleteContainerFileMap(string containerName, string containerIndexFile)
        {
            if (String.IsNullOrEmpty(containerName)) throw new ArgumentNullException(nameof(containerName));
            if (String.IsNullOrEmpty(containerIndexFile)) throw new ArgumentNullException(nameof(containerIndexFile));

            string deleteQuery = "DELETE FROM container_file_map WHERE container_name = '" + containerName + "'";
            DataTable deleteResult;

            if (!QueryPoolIndex(deleteQuery, out deleteResult))
            {
                if (Debug) Console.WriteLine("Unable to delete container file map");
                throw new IOException("Unable to access container file map table");
            }
        }

        private int GetObjectRowCount(string containerIndexFile, string objectName)
        {
            string selectQuery = "SELECT COUNT(*) AS num_rows FROM object_map ";
            if (!String.IsNullOrEmpty(objectName)) selectQuery += "WHERE object_name = '" + objectName + "'";
            DataTable selectResult;

            if (!QueryContainerIndex(selectQuery, containerIndexFile, out selectResult))
            {
                if (Debug) Console.WriteLine("Unable to access container object map");
                throw new IOException("Unable to access container object map table");
            }

            if (selectResult == null || selectResult.Rows.Count < 1) return 0;
            foreach (DataRow curr in selectResult.Rows)
            {
                int ret = Convert.ToInt32(curr["num_rows"]);
                if (Debug)
                {
                    if (String.IsNullOrEmpty(objectName)) Console.WriteLine(ret + " row(s) in " + containerIndexFile);
                    else Console.WriteLine(ret + " row(s) in " + containerIndexFile + " for object " + objectName);
                }
                return ret;
            }
            return 0;
        }

        #endregion

        #region Public-Static-Methods

        #endregion

        #region Private-Static-Methods

        #endregion
    }
}
