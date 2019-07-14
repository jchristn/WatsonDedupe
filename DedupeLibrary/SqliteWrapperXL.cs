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
    /// Sqlite wrapper for DedupeLibraryXL.
    /// </summary>
    internal class SqliteWrapperXL
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
 
        #endregion

        #region Constructor

        /// <summary>
        /// Instantiates the object
        /// </summary>
        /// <param name="indexFile">The index database file.</param>
        /// <param name="debug">Enable or disable console logging.</param>
        public SqliteWrapperXL(string indexFile, bool debug)
        {
            if (String.IsNullOrEmpty(indexFile)) throw new ArgumentNullException(nameof(indexFile));

            _IndexFile = indexFile;

            _ConnectionString = "Data Source=" + _IndexFile + ";Version=3;";

            CreateFile(_IndexFile);
            ConnectPoolIndex();
            CreatePoolIndexConfigTable();
            CreateContainerFileMapTable();
            CreatePoolIndexChunkRefcountTable();
            _Debug = debug;

            _ConfigLock = new object();
            _ChunkRefcountLock = new object();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Execute a SQL query against the pool index.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="result">DataTable containing results.</param>
        /// <returns>True if successful.</returns>
        public bool QueryPoolIndex(string query, out DataTable result)
        {
            result = new DataTable();
            if (String.IsNullOrEmpty(query)) throw new ArgumentNullException(nameof(query));

            try
            {
                using (SQLiteCommand cmd = new SQLiteCommand(query, _SqliteConnection))
                {
                    using (SQLiteDataReader rdr = cmd.ExecuteReader())
                    {
                        result.Load(rdr);
                        return true;
                    }
                }
            }
            catch (Exception e)
            {
                if (_Debug) Console.WriteLine("Query exception [pool]: " + e.Message);
                return false;
            }
            finally
            {
                if (_Debug)
                {
                    if (result != null)
                    {
                        Console.WriteLine(result.Rows.Count + " rows [pool], query: " + query);
                    }
                    else
                    {
                        Console.WriteLine("No ros [pool], query: " + query);
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
        /// <returns>True if successful.</returns>
        public bool QueryContainerIndex(string query, string containerIndexFile, out DataTable result)
        {
            result = new DataTable();
            if (String.IsNullOrEmpty(query)) throw new ArgumentNullException(nameof(query));
            if (String.IsNullOrEmpty(containerIndexFile)) throw new ArgumentNullException(nameof(containerIndexFile));

            string connStr = "Data Source=" + containerIndexFile + ";Version=3;";
            
            using (SQLiteConnection conn = new SQLiteConnection(connStr))
            {
                conn.Open();

                try
                {
                    using (SQLiteCommand cmd = new SQLiteCommand(query, conn))
                    {
                        using (SQLiteDataReader rdr = cmd.ExecuteReader())
                        {
                            result.Load(rdr);
                            return true;
                        }
                    }
                }
                catch (Exception e)
                {
                    if (_Debug) Console.WriteLine("Query exception [container]: " + e.Message);
                    return false;
                }
                finally
                {
                    if (_Debug)
                    {
                        if (result != null)
                        {
                            Console.WriteLine(result.Rows.Count + " rows [container], query: " + query);
                        }
                        else
                        {
                            Console.WriteLine("No rows [container], query: " + query);
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

            key = DedupeCommon.SanitizeString(key);
            val = DedupeCommon.SanitizeString(val);

            string keyCheckQuery = "SELECT * FROM DedupeConfig WHERE Key = '" + key + "'";
            DataTable keyCheckResult;

            string keyDeleteQuery = "DELETE FROM DedupeConfig WHERE Key = '" + key + "'";
            DataTable keyDeleteResult;

            string keyInsertQuery = "INSERT INTO DedupeConfig (Key, Val) VALUES ('" + key + "', '" + val + "')";
            DataTable keyInsertResult;

            lock (_ConfigLock)
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
        /// <returns>True if successful.</returns>
        public bool GetConfigData(string key, out string val)
        {
            val = null;
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            key = DedupeCommon.SanitizeString(key);

            string keyQuery = "SELECT Val FROM DedupeConfig WHERE Key = '" + key + "' LIMIT 1";
            DataTable result;

            lock (_ConfigLock)
            {
                if (QueryPoolIndex(keyQuery, out result))
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
        /// Determine if a container exists in the index.
        /// </summary>
        /// <param name="containerName">The name of the container.</param>
        /// <returns>Boolean indicating if the container exists.</returns>
        public bool ContainerExists(string containerName)
        {
            if (String.IsNullOrEmpty(containerName)) return false;

            containerName = DedupeCommon.SanitizeString(containerName);

            string query = "SELECT * FROM ContainerFileMap WHERE ContainerName = '" + containerName + "' LIMIT 1";
            DataTable result;

            if (QueryPoolIndex(query, out result))
            {
                if (result != null && result.Rows.Count > 0) return true;
            }

            return false;
        }

        /// <summary>
        /// Add a container and create the required database file if needed.
        /// </summary>
        /// <param name="containerName">The name of the container.</param>
        /// <param name="containerIndexFile">The path to the index file for the container.</param>
        public void AddContainer(string containerName, string containerIndexFile)
        {
            if (String.IsNullOrEmpty(containerName)) throw new ArgumentNullException(nameof(containerName));
            if (String.IsNullOrEmpty(containerIndexFile)) throw new ArgumentNullException(nameof(containerIndexFile));

            containerName = DedupeCommon.SanitizeString(containerName);
            containerIndexFile = DedupeCommon.SanitizeString(containerIndexFile);

            string selectQuery = "SELECT * FROM ContainerFileMap WHERE ContainerName = '" + containerName + "'";
            DataTable selectResult;

            if (!QueryPoolIndex(selectQuery, out selectResult))
            {
                if (_Debug) Console.WriteLine("Unable to retrieve container file list");
                throw new IOException("Unable to access container file map table");
            }

            if (selectResult == null || selectResult.Rows.Count < 1)
            {
                string insertQuery =
                    "INSERT INTO ContainerFileMap (ContainerName, ContainerFile) VALUES " +
                    "('" + containerName + "', '" + containerIndexFile + "')";
                DataTable insertResult;

                if (!QueryPoolIndex(insertQuery, out insertResult))
                {
                    if (_Debug) Console.WriteLine("Unable to add container file map for container: " + containerName);
                    throw new IOException("Unable to add container file map entry");
                }
            }

            if (!File.Exists(containerIndexFile))
            {
                CreateContainerObjectMapTable(containerIndexFile);
            }
        }

        /// <summary>
        /// Determine if a chunk exists in the index.
        /// </summary>
        /// <param name="chunkKey">Chunk key.</param>
        /// <param name="containerName">The name of the container.</param>
        /// <param name="containerIndexFile">The path to the index file for the container.</param>
        /// <returns>True if the chunk exists.</returns>
        public bool ChunkExists(string chunkKey, string containerName, string containerIndexFile)
        {
            if (String.IsNullOrEmpty(chunkKey)) return false;
            if (String.IsNullOrEmpty(containerName)) return false;
            if (String.IsNullOrEmpty(containerIndexFile)) return false;

            chunkKey = DedupeCommon.SanitizeString(chunkKey);
            containerName = DedupeCommon.SanitizeString(containerName);
            containerIndexFile = DedupeCommon.SanitizeString(containerIndexFile);

            string query = "SELECT * FROM ObjectMap WHERE ChunkKey = '" + chunkKey + "' AND ContainerName = '" + containerName + "' LIMIT 1";
            DataTable result;

            if (QueryContainerIndex(query, containerIndexFile, out result))
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

            objectName = DedupeCommon.SanitizeString(objectName);
            containerName = DedupeCommon.SanitizeString(containerName);
            containerIndexFile = DedupeCommon.SanitizeString(containerIndexFile);

            if (!ContainerExists(containerName)) return false;

            string query = "SELECT * FROM ObjectMap WHERE Name = '" + objectName + "' AND ContainerName = '" + containerName + "' LIMIT 1";
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

            string query = "SELECT DISTINCT ContainerName FROM ContainerFileMap";
            DataTable result;

            if (QueryPoolIndex(query, out result))
            {
                if (result != null && result.Rows.Count > 0)
                {
                    foreach (DataRow curr in result.Rows)
                    {
                        keys.Add(curr["ContainerName"].ToString());
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
            if (String.IsNullOrEmpty(containerName)) throw new ArgumentNullException(nameof(containerName));
            if (String.IsNullOrEmpty(containerIndexFile)) throw new ArgumentNullException(nameof(containerIndexFile));

            containerName = DedupeCommon.SanitizeString(containerName);

            keys = new List<string>();

            string query = "SELECT DISTINCT Name FROM ObjectMap";
            DataTable result;

            if (QueryContainerIndex(query, containerIndexFile, out result))
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

        /// <summary>
        /// Add chunks from an object to the container and to the index.
        /// </summary>
        /// <param name="objectName">The name of the object.  Must be unique in the container.</param>
        /// <param name="containerName">The name of the container.  Must be unique in the index.</param>
        /// <param name="containerIndexFile">The path to the index file for the object.</param>
        /// <param name="totalLen">The total length of the object.</param>
        /// <param name="chunk">Chunk from the object..</param>
        /// <returns>True if successful.</returns>
        public bool AddObjectChunk(string objectName, string containerName, string containerIndexFile, long totalLen, Chunk chunk)
        {
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            if (String.IsNullOrEmpty(containerName)) throw new ArgumentNullException(nameof(containerName));
            if (String.IsNullOrEmpty(containerIndexFile)) throw new ArgumentNullException(nameof(containerIndexFile));
            if (totalLen < 1) throw new ArgumentException("Total length must be greater than zero.");
            if (chunk == null) throw new ArgumentNullException(nameof(chunk));

            objectName = DedupeCommon.SanitizeString(objectName);
            containerName = DedupeCommon.SanitizeString(containerName);

            if (ObjectExists(objectName, containerName, containerIndexFile)) return false;

            DataTable result = null;
            string query = AddContainerChunkQuery(objectName, containerName, totalLen, chunk);

            CreateFile(containerIndexFile);
            CreateContainerObjectMapTable(containerIndexFile);
             
            if (!QueryContainerIndex(query, containerIndexFile, out result))
            {
                if (_Debug) Console.WriteLine("Container insert query failed: " + query);
                return false;
            } 

            lock (_ChunkRefcountLock)
            { 
                if (!IncrementRefcount(chunk.Key, chunk.Length))
                {
                    if (_Debug) Console.WriteLine("Unable to increment refcount for chunk " + chunk.Key + " in container " + containerName);
                    return false;
                } 
            }

            AddContainer(containerName, containerIndexFile);
            return true;
        }

        /// <summary>
        /// Add chunks from an object to the container and to the index.
        /// </summary>
        /// <param name="objectName">The name of the object.  Must be unique in the container.</param>
        /// <param name="containerName">The name of the container.  Must be unique in the index.</param>
        /// <param name="containerIndexFile">The path to the index file for the object.</param>
        /// <param name="totalLen">The total length of the object.</param>
        /// <param name="chunks">The chunks from the object.</param>
        /// <returns>True if successful.</returns>
        public bool AddObjectChunks(string objectName, string containerName, string containerIndexFile, long totalLen, List<Chunk> chunks)
        {
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            if (String.IsNullOrEmpty(containerName)) throw new ArgumentNullException(nameof(containerName));
            if (String.IsNullOrEmpty(containerIndexFile)) throw new ArgumentNullException(nameof(containerIndexFile));
            if (totalLen < 1) throw new ArgumentException("Total length must be greater than zero.");
            if (chunks == null || chunks.Count < 1) throw new ArgumentException("No chunk data supplied.");

            objectName = DedupeCommon.SanitizeString(objectName);
            containerName = DedupeCommon.SanitizeString(containerName);

            if (ObjectExists(objectName, containerName, containerIndexFile)) throw new IOException("Object " + objectName + " already exists in container");

            DataTable result;
            List<string> addContainerChunksQuery = BatchAddContainerChunksQuery(objectName, containerName, totalLen, chunks);

            CreateFile(containerIndexFile);
            CreateContainerObjectMapTable(containerIndexFile);

            foreach (string query in addContainerChunksQuery)
            {
                if (!QueryContainerIndex(query, containerIndexFile, out result))
                {
                    if (_Debug) Console.WriteLine("Container insert query failed: " + query);
                    return false;
                }
            }

            lock (_ChunkRefcountLock)
            {
                foreach (Chunk currChunk in chunks)
                {
                    if (!IncrementRefcount(currChunk.Key, currChunk.Length))
                    {
                        if (_Debug) Console.WriteLine("Unable to increment refcount for chunk " + currChunk.Key + " in container " + containerName);
                        return false;
                    }
                }
            }

            AddContainer(containerName, containerIndexFile);
            return true;
        }

        /// <summary>
        /// Retrieve metadata for a given object.
        /// </summary>
        /// <param name="objectName">The name of the object.</param>
        /// <param name="containerName">The name of the container.</param>
        /// <param name="containerIndexFile">The path to the index file for the container.</param>
        /// <param name="metadata">Object metadata.</param>
        /// <returns>True if successful.</returns>
        public bool GetObjectMetadata(string objectName, string containerName, string containerIndexFile, out ObjectMetadata metadata)
        {
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            if (String.IsNullOrEmpty(containerName)) throw new ArgumentNullException(nameof(containerName));
            if (String.IsNullOrEmpty(containerIndexFile)) throw new ArgumentNullException(nameof(containerIndexFile));

            objectName = DedupeCommon.SanitizeString(objectName);
            metadata = null;

            string query = "SELECT * FROM ObjectMap WHERE Name = '" + objectName + "' AND ContainerName = '" + containerName + "'";
            DataTable result;
            bool success = QueryContainerIndex(query, containerIndexFile, out result);

            if (result == null || result.Rows.Count < 1)
            {
                if (_Debug) Console.WriteLine("No rows returned");
                return false;
            }

            if (!success)
            {
                if (_Debug) Console.WriteLine("Query failed");
                return false;
            }

            metadata = ObjectMetadata.FromDataTable(result);
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

            objectName = DedupeCommon.SanitizeString(objectName);
            containerName = DedupeCommon.SanitizeString(containerName);

            string selectQuery = "SELECT * FROM ObjectMap WHERE Name = '" + objectName + "' AND ContainerName = '" + containerName + "'";
            string deleteObjectMapQuery = "DELETE FROM ObjectMap WHERE Name = '" + objectName + "' AND ContainerName = '" + containerName + "'";
            
            DataTable result;
            bool garbageCollect = false;
            
            if (!QueryContainerIndex(selectQuery, containerIndexFile, out result))
            {
                if (_Debug)
                {
                    Console.WriteLine("Unable to retrieve object map for object " + objectName + " in container " + containerName);
                }
            }

            if (result == null || result.Rows.Count < 1) return;

            lock (_ChunkRefcountLock)
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
                if (_Debug)
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
        /// <returns>True if successful.</returns>
        public bool IncrementRefcount(string chunkKey, long len)
        {
            if (String.IsNullOrEmpty(chunkKey)) throw new ArgumentNullException(nameof(chunkKey));

            chunkKey = DedupeCommon.SanitizeString(chunkKey);

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
                            currCount = Convert.ToInt32(curr["RefCount"]);
                        }

                        currCount++;

                        updateQuery = "UPDATE ChunkRefcount SET RefCount = '" + currCount + "' WHERE ChunkKey = '" + chunkKey + "'";
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
        /// <returns>True if successful.</returns>
        public bool DecrementRefcount(string chunkKey, out bool garbageCollect)
        {
            garbageCollect = false;
            if (String.IsNullOrEmpty(chunkKey)) throw new ArgumentNullException(nameof(chunkKey));

            chunkKey = DedupeCommon.SanitizeString(chunkKey);

            string selectQuery = "SELECT * FROM ChunkRefcount WHERE ChunkKey = '" + chunkKey + "'";
            string deleteQuery = "DELETE FROM ChunkRefcount WHERE ChunkKey = '" + chunkKey + "'";
            string updateQuery = "";

            DataTable selectResult;
            DataTable updateResult;
            DataTable deleteResult;

            lock (_ChunkRefcountLock)
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
                            currCount = Convert.ToInt32(curr["RefCount"]);
                        }

                        currCount--;
                        if (currCount == 0)
                        {
                            garbageCollect = true;
                            return QueryPoolIndex(deleteQuery, out deleteResult);
                        }
                        else
                        {
                            updateQuery = "UPDATE ChunkRefcount SET RefCount = '" + currCount + "' WHERE ChunkKey = '" + chunkKey + "'";
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
        /// <returns>True if successful.</returns>
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
                "  (SELECT COUNT(*) AS NumContainers FROM " + 
                "    (SELECT DISTINCT(ContainerName) FROM ContainerFileMap) ContainerNames " +
                "  ) NumContainers, " +
                "  (SELECT COUNT(*) AS NumChunks FROM ChunkRefcount) NumChunks, " +
                "  (SELECT SUM(ChunkLength * RefCount) AS LogicalBytes FROM ChunkRefcount) LogicalBytes, " +
                "  (SELECT SUM(ChunkLength) AS PhysicalBytes FROM ChunkRefcount) PhysicalBytes " +
                ")";

            DataTable result;

            lock (_ChunkRefcountLock)
            {
                if (!QueryPoolIndex(query, out result))
                {
                    if (_Debug) Console.WriteLine("Unable to retrieve index stats");
                    return false;
                }

                if (result == null || result.Rows.Count < 1) return true;
            }

            foreach (DataRow curr in result.Rows)
            {
                if (curr["NumContainers"] != DBNull.Value) numContainers = Convert.ToInt32(curr["NumContainers"]);
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
        /// Copies the pool index database to another file.
        /// </summary>
        /// <param name="destination">The destination file.</param>
        /// <returns>True if successful.</returns>
        public bool BackupPoolIndex(string destination)
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

        /// <summary>
        /// Copies a container index database to another file.
        /// </summary>
        /// <param name="containerIndexFile">The path to the index file for the container.</param>
        /// <param name="destinationIndexFile">The destination file.</param>
        /// <param name="incrementRefCount">Indicate if chunk reference counts should be incremented after copy.</param>
        /// <returns>True if successful.</returns>
        public bool BackupContainerIndex(string containerIndexFile, string destinationIndexFile, string newContainerName, bool incrementRefCount)
        {
            if (String.IsNullOrEmpty(containerIndexFile)) throw new ArgumentNullException(nameof(containerIndexFile));
            if (String.IsNullOrEmpty(destinationIndexFile)) throw new ArgumentNullException(nameof(destinationIndexFile));
            if (String.IsNullOrEmpty(newContainerName)) throw new ArgumentNullException(nameof(newContainerName));

            newContainerName = DedupeCommon.SanitizeString(newContainerName);

            bool copySuccess = false;
            DataTable result;
            string query = "";

            using (SQLiteConnection conn = new SQLiteConnection("Data Source=" + containerIndexFile + ";Version=3;"))
            {
                conn.Open();
                using (SQLiteCommand cmd = new SQLiteCommand("BEGIN IMMEDIATE;", conn))
                {
                    cmd.ExecuteNonQuery();
                }

                try
                {
                    File.Copy(containerIndexFile, destinationIndexFile, true);
                    copySuccess = true;
                }
                catch (Exception)
                {
                }

                using (SQLiteCommand cmd = new SQLiteCommand("ROLLBACK;", conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            query = "UPDATE ObjectMap SET ContainerName = '" + newContainerName + "'";
            if (!QueryContainerIndex(query, destinationIndexFile, out result))
            {
                if (_Debug) Console.WriteLine("Unable to update container name in destination index file");
                return false;
            }

            if (incrementRefCount)
            {
                query = "SELECT * FROM ObjectMap";
                if (!QueryContainerIndex(query, containerIndexFile, out result))
                {
                    if (_Debug) Console.WriteLine("Unable to retrieve object map from container " + containerIndexFile);
                    return false;
                }

                if (result != null && result.Rows.Count > 0)
                {
                    bool incrementSuccess = false;
                    foreach (DataRow curr in result.Rows)
                    {
                        incrementSuccess = IncrementRefcount(curr["ChunkKey"].ToString(), Convert.ToInt32(curr["ChunkLength"]));
                    }
                }
            }

            AddContainer(newContainerName, destinationIndexFile);
            return copySuccess;
        }

        /// <summary>
        /// Imports a container index into the deduplication index.
        /// </summary>
        /// <param name="containerName">The name of the container.</param>
        /// <param name="containerIndexFile">The path to the index file for the object.</param>
        /// <param name="incrementRefCount">Indicate if chunk reference counts should be incremented after copy.</param>
        /// <returns>True if successful.</returns>
        public bool ImportContainerIndex(string containerName, string containerIndexFile, bool incrementRefCount)
        {
            if (String.IsNullOrEmpty(containerName)) throw new ArgumentNullException(nameof(containerName));
            if (String.IsNullOrEmpty(containerIndexFile)) throw new ArgumentNullException(nameof(containerIndexFile));
            if (!File.Exists(containerIndexFile)) throw new FileNotFoundException("Container index file does not exist");

            containerName = DedupeCommon.SanitizeString(containerName);

            if (ContainerExists(containerName))
            {
                if (_Debug) Console.WriteLine("Container " + containerName + " already exists");
                return true;
            }

            AddContainer(containerName, containerIndexFile);

            string selectQuery = "SELECT * FROM ObjectMap WHERE ContainerName = '" + containerName + "'";
            DataTable selectResult;
            if (!QueryContainerIndex(selectQuery, containerIndexFile, out selectResult))
            {
                if (_Debug) Console.WriteLine("Unable to query container " + containerName);
                return false;
            }

            if (selectResult == null || selectResult.Rows.Count < 1)
            {
                if (_Debug) Console.WriteLine("No rows in container " + containerName);
                return true;
            }

            if (incrementRefCount)
            {
                lock (_ChunkRefcountLock)
                {
                    foreach (DataRow curr in selectResult.Rows)
                    {
                        Chunk c = Chunk.FromDataRow(curr);
                        if (!IncrementRefcount(c.Key, c.Length))
                        {
                            if (_Debug) Console.WriteLine("Unable to increment refcount for chunk " + c.Key);
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
                SQLiteConnection.CreateFile(filename);
            }
        }

        private void ConnectPoolIndex()
        {
            _SqliteConnection = new SQLiteConnection(_ConnectionString);
            _SqliteConnection.Open();
        }

        private void CreatePoolIndexConfigTable()
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
        
        private void CreateContainerFileMapTable()
        {
            using (SQLiteCommand cmd = _SqliteConnection.CreateCommand())
            {
                cmd.CommandText =
                    @"CREATE TABLE IF NOT EXISTS ContainerFileMap " +
                    "(" +
                    " ContainerFileMapId INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    " ContainerName VARCHAR(1024), " +
                    " ContainerFile VARCHAR(1024) " +
                    ")";
                cmd.ExecuteNonQuery();
            }
        }

        private void CreateContainerObjectMapTable(string containerIndexFile)
        {
            string connStr = "Data Source=" + containerIndexFile + ";Version=3;";
            SQLiteConnection conn = new SQLiteConnection(connStr);
            conn.Open();
            
            using (SQLiteCommand cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    @"CREATE TABLE IF NOT EXISTS ObjectMap " +
                    "(" +
                    " ObjectMapId INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    " ContainerName VARCHAR(1024), " +
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

        private void CreatePoolIndexChunkRefcountTable()
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

        private string AddContainerChunkQuery(string objectName, string containerName, long totalLen, Chunk chunk)
        {
            string query =
                "INSERT INTO ObjectMap " +
                "(" +
                "  ContainerName, " +
                "  Name, " +
                "  ContentLength, " +
                "  ChunkKey, " +
                "  ChunkLength, " +
                "  ChunkPosition, " +
                "  ChunkAddress) " +
                "VALUES " +
                "(" +
                "  '" + DedupeCommon.SanitizeString(containerName) + "', " +
                "  '" + DedupeCommon.SanitizeString(objectName) + "', " +
                "  '" + totalLen + "', " +
                "  '" + DedupeCommon.SanitizeString(chunk.Key) + "', " +
                "  '" + chunk.Length + "', " +
                "  '" + chunk.Position + "', " +
                "  '" + chunk.Address + "'" +
                ")";

            return query;
        }

        private List<string> BatchAddContainerChunksQuery(string objectName, string containerName, long totalLen, List<Chunk> chunks)
        {
            if (String.IsNullOrEmpty(objectName)) throw new ArgumentNullException(nameof(objectName));
            if (String.IsNullOrEmpty(containerName)) throw new ArgumentNullException(nameof(containerName));

            containerName = DedupeCommon.SanitizeString(containerName);

            List<string> ret = new List<string>();

            bool moreRecords = true;
            int batchMaxSize = 32;
            int totalRecords = chunks.Count;
            int currPosition = 0;
            int remainingRecords = 0;

            while (moreRecords)
            {
                string query = "INSERT INTO ObjectMap (ContainerName, Name, ContentLength, ChunkKey, ChunkLength, ChunkPosition, ChunkAddress) VALUES ";

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
                            "'" + DedupeCommon.SanitizeString(chunks[i + currPosition].Key) + "', " +
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
                            "'" + DedupeCommon.SanitizeString(chunks[i + currPosition].Key) + "', " +
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

        private void DeleteContainerFileMap(string containerName, string containerIndexFile)
        {
            if (String.IsNullOrEmpty(containerName)) throw new ArgumentNullException(nameof(containerName));
            if (String.IsNullOrEmpty(containerIndexFile)) throw new ArgumentNullException(nameof(containerIndexFile));

            containerName = DedupeCommon.SanitizeString(containerName);

            string deleteQuery = "DELETE FROM ContainerFileMap WHERE ContainerName = '" + containerName + "'";
            DataTable deleteResult;

            if (!QueryPoolIndex(deleteQuery, out deleteResult))
            {
                if (_Debug) Console.WriteLine("Unable to delete container file map");
                throw new IOException("Unable to access container file map table");
            }
        }

        private int GetObjectRowCount(string containerIndexFile, string objectName)
        {
            if (!String.IsNullOrEmpty(objectName)) objectName = DedupeCommon.SanitizeString(objectName);

            string selectQuery = "SELECT COUNT(*) AS NumRows FROM ObjectMap ";
            if (!String.IsNullOrEmpty(objectName)) selectQuery += "WHERE Name = '" + objectName + "'";
            DataTable selectResult;

            if (!QueryContainerIndex(selectQuery, containerIndexFile, out selectResult))
            {
                if (_Debug) Console.WriteLine("Unable to access container object map");
                throw new IOException("Unable to access container object map table");
            }

            if (selectResult == null || selectResult.Rows.Count < 1) return 0;
            foreach (DataRow curr in selectResult.Rows)
            {
                int ret = Convert.ToInt32(curr["NumRows"]);
                if (_Debug)
                {
                    if (String.IsNullOrEmpty(objectName)) Console.WriteLine(ret + " row(s) in " + containerIndexFile);
                    else Console.WriteLine(ret + " row(s) in " + containerIndexFile + " for object " + objectName);
                }
                return ret;
            }
            return 0;
        }

        #endregion 
    }
}
