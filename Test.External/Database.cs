using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using DatabaseWrapper;
using WatsonDedupe;
using WatsonDedupe.Database;

namespace Test.External
{
    public class Database : DbProvider
    {
        private bool _Debug;
        private readonly object _ConfigLock = new object();
        private readonly object _ChunkRefcountLock = new object();
        private readonly object _ObjectLock = new object();

        private DatabaseClient _Database = null;

        public Database(DatabaseClient database, bool debug)
        {
            _Debug = debug;
            _Database = database;
        }

        public override bool IsInitialized()
        {
            string query = "SELECT * FROM dedupeconfig;";
            DataTable result = _Database.Query(query);
            if (result != null && result.Rows.Count > 1)
            {
                return true;
            }
            return false;
        }

        public override void AddConfigData(string key, string val)
        {
            key = DedupeCommon.SanitizeString(key);
            val = DedupeCommon.SanitizeString(val);

            string keyCheckQuery = "SELECT * FROM DedupeConfig WHERE configkey = '" + key + "'";
            DataTable result;

            string keyDeleteQuery = "DELETE FROM DedupeConfig WHERE configkey = '" + key + "'";  
            string keyInsertQuery = "INSERT INTO DedupeConfig (configkey, configval) VALUES ('" + key + "', '" + val + "')"; 

            lock (_ConfigLock)
            {
                result = _Database.Query(keyCheckQuery);
                if (result != null && result.Rows.Count > 0) 
                    _Database.Query(keyDeleteQuery); 

                _Database.Query(keyInsertQuery);
            }

            return;
        }

        public override bool GetConfigData(string key, out string val)
        {
            val = null; 

            key = DedupeCommon.SanitizeString(key);

            string keyQuery = "SELECT configval FROM DedupeConfig WHERE configkey = '" + key + "' LIMIT 1";
            DataTable result;

            lock (_ConfigLock)
            {
                result = _Database.Query(keyQuery);
                if (result != null && result.Rows.Count > 0)
                {
                    foreach (DataRow curr in result.Rows)
                    {
                        val = curr["configval"].ToString();
                        if (_Debug) Console.WriteLine("Returning " + key + ": " + val);
                        return true;
                    }
                }
            }

            return false;
        }

        public override bool ChunkExists(string key)
        { 
            key = DedupeCommon.SanitizeString(key);

            string query = "SELECT * FROM ObjectMap WHERE ChunkKey = '" + key + "' LIMIT 1";
            DataTable result;

            lock (_ObjectLock)
            {
                result = _Database.Query(query);
                if (result != null && result.Rows.Count > 0) return true;
            }

            return false;
        }

        public override bool ObjectExists(string name)
        { 
            name = DedupeCommon.SanitizeString(name);

            string query = "SELECT * FROM ObjectMap WHERE Name = '" + name + "' LIMIT 1";
            DataTable result;

            lock (_ObjectLock)
            {
                result = _Database.Query(query);
                if (result != null && result.Rows.Count > 0) return true;
            }

            return false;
        }

        public override void ListObjects(out List<string> names)
        {
            names = new List<string>();

            string query = "SELECT DISTINCT Name FROM ObjectMap";
            DataTable result;

            lock (_ObjectLock)
            {
                result = _Database.Query(query);
                if (result != null && result.Rows.Count > 0)
                {
                    foreach (DataRow curr in result.Rows)
                    {
                        names.Add(curr["Name"].ToString());
                    }
                }
            }
        }

        public override bool AddObjectChunk(string name, long totalLen, Chunk chunk)
        {
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));
            if (totalLen < 1) throw new ArgumentException("Total length must be greater than zero.");
            if (chunk == null) throw new ArgumentNullException(nameof(chunk));

            name = DedupeCommon.SanitizeString(name);

            DataTable result = null;
            string query = AddObjectChunkQuery(name, totalLen, chunk);

            lock (_ObjectLock)
            {
                result = _Database.Query(query);
                return IncrementChunkRefcount(chunk.Key, chunk.Length);
            } 
        }
         
        public override bool AddObjectChunks(string name, long totalLen, List<Chunk> chunks)
        {
            name = DedupeCommon.SanitizeString(name);

            if (ObjectExists(name)) return false;

            DataTable result;
            List<string> addObjectChunksQueries = BatchAddObjectChunksQuery(name, totalLen, chunks);

            lock (_ObjectLock)
            {
                foreach (string query in addObjectChunksQueries)
                {
                    result = _Database.Query(query);
                }

                foreach (Chunk currChunk in chunks)
                {
                    IncrementChunkRefcount(currChunk.Key, currChunk.Length);
                }
            }

            return true;
        }

        public override bool GetObjectMetadata(string name, out ObjectMetadata metadata)
        { 
            name = DedupeCommon.SanitizeString(name);
            metadata = null;

            string query = "SELECT * FROM ObjectMap WHERE Name = '" + name + "'";
            DataTable result; 
            lock (_ObjectLock)
            {
                result = _Database.Query(query);
            }

            if (result == null || result.Rows.Count < 1) return false; 

            metadata = ObjectMetadata.FromDataTable(result);
            return true;
        }

        public override bool GetObjectChunks(string name, out List<Chunk> chunks)
        { 
            name = DedupeCommon.SanitizeString(name);
            chunks = new List<Chunk>();

            string query = "SELECT * FROM ObjectMap WHERE Name = '" + name + "'";
            DataTable result; 
            lock (_ObjectLock)
            {
                result = _Database.Query(query);
            }

            if (result == null || result.Rows.Count < 1) return false; 

            foreach (DataRow row in result.Rows)
            {
                chunks.Add(Chunk.FromDataRow(row));
            }

            return true;
        }

        public override void DeleteObjectChunks(string name, out List<string> garbageCollectChunks)
        {
            garbageCollectChunks = new List<string>(); 

            name = DedupeCommon.SanitizeString(name);

            string selectQuery = "SELECT * FROM ObjectMap WHERE Name = '" + name + "'";
            string deleteObjectMapQuery = "DELETE FROM ObjectMap WHERE Name = '" + name + "'";
            DataTable result;
            bool garbageCollect = false;

            lock (_ObjectLock)
            {
                result = _Database.Query(selectQuery); 
                if (result == null || result.Rows.Count < 1) return;

                foreach (DataRow curr in result.Rows)
                {
                    Chunk c = Chunk.FromDataRow(curr);
                    DecrementChunkRefcount(c.Key, out garbageCollect);
                    if (garbageCollect) garbageCollectChunks.Add(c.Key);
                }

                result = _Database.Query(deleteObjectMapQuery);
            }
        }

        public override bool IncrementChunkRefcount(string key, long len)
        { 
            key = DedupeCommon.SanitizeString(key);

            string selectQuery = "";
            string updateQuery = "";
            string insertQuery = "";

            DataTable selectResult;
            DataTable updateResult;
            DataTable insertResult;

            selectQuery = "SELECT * FROM ChunkRefcount WHERE ChunkKey = '" + key + "'";
            insertQuery = "INSERT INTO ChunkRefcount (ChunkKey, ChunkLength, RefCount) VALUES ('" + key + "', '" + len + "', 1)";

            lock (_ChunkRefcountLock)
            {
                selectResult = _Database.Query(selectQuery); 
                if (selectResult == null || selectResult.Rows.Count < 1)
                {
                    insertResult = _Database.Query(insertQuery);
                    return true;
                }
                else
                {
                    // update 
                    int currCount = 0;
                    foreach (DataRow curr in selectResult.Rows)
                    {
                        currCount = Convert.ToInt32(curr["RefCount"]);
                    }

                    currCount++;

                    updateQuery = "UPDATE ChunkRefcount SET RefCount = '" + currCount + "' WHERE ChunkKey = '" + key + "'";
                    updateResult = _Database.Query(updateQuery);
                    return true;
                }  
            }
        }

        public override bool DecrementChunkRefcount(string key, out bool garbageCollect)
        {
            garbageCollect = false; 

            key = DedupeCommon.SanitizeString(key);

            string selectQuery = "";
            string updateQuery = "";
            string deleteQuery = "";

            DataTable selectResult;
            DataTable updateResult;
            DataTable deleteResult;

            selectQuery = "SELECT * FROM ChunkRefcount WHERE ChunkKey = '" + key + "'";
            deleteQuery = "DELETE FROM ChunkRefcount WHERE ChunkKey = '" + key + "'";

            lock (_ChunkRefcountLock)
            {
                selectResult = _Database.Query(selectQuery);
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
                        deleteResult = _Database.Query(deleteQuery);
                        return true;
                    }
                    else
                    {
                        updateQuery = "UPDATE ChunkRefcount SET RefCount = '" + currCount + "' WHERE ChunkKey = '" + key + "'";
                        updateResult = _Database.Query(updateQuery);
                        return true;
                    }
                }
            }
        }

        public override bool IndexStats(out int numObjects, out int numChunks, out long logicalBytes, out long physicalBytes, out decimal dedupeRatioX, out decimal dedupeRatioPercent)
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
                result = _Database.Query(query);
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

        public override bool BackupDatabase(string filename)
        {
            throw new NotImplementedException(); 
        }
         
        private string AddObjectChunkQuery(string objectName, long totalLen, Chunk chunk)
        {
            string query =
                "INSERT INTO ObjectMap " +
                "(" +
                "  Name, " +
                "  ContentLength, " +
                "  ChunkKey, " +
                "  ChunkLength, " +
                "  ChunkPosition, " +
                "  ChunkAddress) " +
                "VALUES " +
                "(" +
                "  '" + DedupeCommon.SanitizeString(objectName) + "', " +
                "  '" + totalLen + "', " +
                "  '" + DedupeCommon.SanitizeString(chunk.Key) + "', " +
                "  '" + chunk.Length + "', " +
                "  '" + chunk.Position + "', " +
                "  '" + chunk.Address + "'" +
                ")";

            return query;
        }

        private List<string> BatchAddObjectChunksQuery(string objectName, long totalLen, List<Chunk> chunks)
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
    }
}
