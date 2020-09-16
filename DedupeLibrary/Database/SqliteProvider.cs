using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Watson.ORM.Core;
using Watson.ORM.Sqlite;

namespace WatsonDedupe.Database
{
    /// <summary>
    /// Built-in Sqlite provider for WatsonDedupe.
    /// </summary>
    public class SqliteProvider : DbProvider
    {
        #region Public-Members

        #endregion

        #region Private-Members

        private string _IndexFile = null;
        private DatabaseSettings _Settings = null;
        private WatsonORM _ORM = null;

        private readonly object _ConfigLock = new object();
        private readonly object _ChunkLock = new object();

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiates the object
        /// </summary>
        /// <param name="indexFile">The index database file.</param>
        public SqliteProvider(string indexFile)
        { 
            if (String.IsNullOrEmpty(indexFile)) throw new ArgumentNullException(nameof(indexFile));
             
            _IndexFile = indexFile;
            _Settings = new DatabaseSettings(_IndexFile);
            _ORM = new WatsonORM(_Settings);
            _ORM.InitializeDatabase();

            _ORM.InitializeTable(typeof(DedupeConfig));
            _ORM.InitializeTable(typeof(DedupeObject));
            _ORM.InitializeTable(typeof(DedupeChunk));
            _ORM.InitializeTable(typeof(DedupeObjectMap));
        }

        #endregion

        #region Public-Methods

        #region General-APIs

        /// <summary>
        /// Check if the database is initialized.  With internal Sqlite databases, this will always return true, because the constructor initializes the database.
        /// </summary>
        /// <returns>True.</returns>
        public override bool IsInitialized()
        {
            DbExpression e1 = new DbExpression(
                _ORM.GetColumnName<DedupeConfig>(nameof(DedupeConfig.Key)),
                DbOperators.Equals,
                "min_chunk_size");

            DbExpression e2 = new DbExpression(
                _ORM.GetColumnName<DedupeConfig>(nameof(DedupeConfig.Key)),
                DbOperators.Equals,
                "max_chunk_size");

            DbExpression e3 = new DbExpression(
                _ORM.GetColumnName<DedupeConfig>(nameof(DedupeConfig.Key)),
                DbOperators.Equals,
                "shift_count");

            DbExpression e4 = new DbExpression(
                _ORM.GetColumnName<DedupeConfig>(nameof(DedupeConfig.Key)),
                DbOperators.Equals,
                "boundary_check_bytes");

            lock (_ConfigLock)
            {
                DedupeConfig dc1 = _ORM.SelectFirst<DedupeConfig>(e1);
                DedupeConfig dc2 = _ORM.SelectFirst<DedupeConfig>(e2);
                DedupeConfig dc3 = _ORM.SelectFirst<DedupeConfig>(e3);
                DedupeConfig dc4 = _ORM.SelectFirst<DedupeConfig>(e4);

                if (dc1 != null && dc2 != null && dc3 != null && dc4 != null) return true;
            }

            return false;
        }

        /// <summary>
        /// Add a configuration key-value pair.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="val">The value.</param>
        public override void AddConfigValue(string key, string val)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (String.IsNullOrEmpty(val)) throw new ArgumentNullException(nameof(val));

            key = DedupeCommon.SanitizeString(key);
            val = DedupeCommon.SanitizeString(val);

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<DedupeConfig>(nameof(DedupeConfig.Key)),
                DbOperators.Equals,
                key);

            lock (_ConfigLock)
            {
                DedupeConfig config = _ORM.SelectFirst<DedupeConfig>(e);
                if (config != null) _ORM.Delete<DedupeConfig>(config); 
                config = new DedupeConfig(key, val);
                config = _ORM.Insert<DedupeConfig>(config);
            }
        }

        /// <summary>
        /// Retrieve a configuration value.
        /// </summary>
        /// <param name="key">The key.</param> 
        /// <returns>Value.</returns>
        public override string GetConfigValue(string key)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            key = DedupeCommon.SanitizeString(key);

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<DedupeConfig>(nameof(DedupeConfig.Key)),
                DbOperators.Equals,
                key);

            lock (_ConfigLock)
            {
                DedupeConfig config = _ORM.SelectFirst<DedupeConfig>(e);
                if (config == null) return null;
                return config.Value;
            }
        }

        /// <summary>
        /// Retrieve index statistics.
        /// </summary> 
        /// <returns>Index statistics.</returns>
        public override IndexStatistics GetStatistics()
        {
            IndexStatistics ret = new IndexStatistics();
             
            DbExpression eObjects = new DbExpression(
                _ORM.GetColumnName<DedupeObject>(nameof(DedupeObject.Id)),
                DbOperators.GreaterThan,
                0);

            ret.Objects = _ORM.Count<DedupeObject>(eObjects);

            DbExpression eChunks = new DbExpression(
                _ORM.GetColumnName<DedupeChunk>(nameof(DedupeChunk.Id)),
                DbOperators.GreaterThan,
                0);

            ret.Chunks = _ORM.Count<DedupeChunk>(eChunks);

            DbExpression eLogicalBytes = new DbExpression(
                _ORM.GetColumnName<DedupeObject>(nameof(DedupeObject.Id)),
                DbOperators.GreaterThan,
                0);

            decimal logicalBytes = _ORM.Sum<DedupeObject>(_ORM.GetColumnName<DedupeObject>(nameof(DedupeObject.Length)), eLogicalBytes);
            ret.LogicalBytes = Convert.ToInt64(logicalBytes);

            DbExpression ePhysicalBytes = new DbExpression(
                _ORM.GetColumnName<DedupeChunk>(nameof(DedupeChunk.Id)),
                DbOperators.GreaterThan,
                0);

            decimal physicalBytes = _ORM.Sum<DedupeChunk>(_ORM.GetColumnName<DedupeChunk>(nameof(DedupeChunk.Length)), ePhysicalBytes);
            ret.PhysicalBytes = Convert.ToInt64(physicalBytes);

            return ret; 
        }

        #endregion

        #region Enumeration-APIs

        /// <summary>
        /// List the objects stored in the index.
        /// </summary>
        /// <param name="prefix">Prefix upon which to match object keys.</param>
        /// <param name="indexStart">The index (DedupeObject.Id) from which to begin the enumeration.</param>
        /// <param name="maxResults">Maximum number of results to retrieve.</param>
        /// <return>Enumeration result.</return>
        public override EnumerationResult ListObjects(string prefix, int indexStart, int maxResults)
        {
            if (indexStart < 0) throw new ArgumentException("Starting index must be zero or greater.");
            if (maxResults < 1 || maxResults > 100) throw new ArgumentException("Max results must be greater than zero and less than or equal to 100.");

            EnumerationResult ret = new EnumerationResult(prefix, indexStart, indexStart, maxResults, new List<DedupeObject>());
             
            DbExpression e = new DbExpression(
                _ORM.GetColumnName<DedupeObject>(nameof(DedupeObject.Id)),
                DbOperators.GreaterThan,
                indexStart);

            if (!String.IsNullOrEmpty(prefix))
            {
                e.PrependAnd(
                    _ORM.GetColumnName<DedupeObject>(nameof(DedupeObject.Key)),
                    DbOperators.StartsWith,
                    prefix);
            } 

            List<DedupeObject> objects = _ORM.SelectMany<DedupeObject>(null, maxResults, e);

            if (objects != null && objects.Count > 0)
            {
                foreach (DedupeObject obj in objects)
                {
                    obj.Chunks = GetChunks(obj.Key);
                    obj.ObjectMap = GetObjectMap(obj.Key);

                    if (obj.ObjectMap != null && obj.ObjectMap.Count > 0)
                    {
                        obj.ObjectMap = obj.ObjectMap.OrderBy(o => o.ChunkAddress).ToList();
                    }

                    ret.Objects.Add(obj);
                }
            }

            if (objects != null && objects.Count == maxResults)
            {
                ret.NextIndexStart = objects[(objects.Count - 1)].Id;
            }

            return ret;
        }

        #endregion

        #region Exists-APIs

        /// <summary>
        /// Determine if an object exists in the index.
        /// </summary>
        /// <param name="key">Object key.</param>
        /// <returns>True if the object exists.</returns>
        public override bool Exists(string key)
        {
            if (String.IsNullOrEmpty(key)) return false;

            key = DedupeCommon.SanitizeString(key);

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<DedupeObject>(nameof(DedupeObject.Key)),
                DbOperators.Equals,
                key);
             
            return _ORM.Exists<DedupeObject>(e); 
        }
         
        #endregion

        #region Get-APIs

        /// <summary>
        /// Retrieve metadata for a given object.
        /// DedupeObjectMap objects returned should be ordered in ascending order based on the chunk's position or address.
        /// </summary>
        /// <param name="key">Object key.</param>
        /// <returns>Object metadata.</returns>
        public override DedupeObject GetObjectMetadata(string key)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            key = DedupeCommon.SanitizeString(key);

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<DedupeObject>(nameof(DedupeObject.Key)),
                DbOperators.Equals,
                key);

            DedupeObject ret = _ORM.SelectFirst<DedupeObject>(e);

            if (ret != null)
            {
                ret.Chunks = GetChunks(key);
                ret.ObjectMap = GetObjectMap(key);

                if (ret.ObjectMap != null && ret.ObjectMap.Count > 0) 
                    ret.ObjectMap = ret.ObjectMap.OrderBy(o => o.ChunkAddress).ToList();
            }

            return ret;
        }
         
        /// <summary>
        /// Retrieve metadata for a given chunk by its key.
        /// </summary>
        /// <param name="chunkKey">Chunk key.</param>
        /// <returns>Chunk metadata.</returns> 
        public override DedupeChunk GetChunkMetadata(string chunkKey)
        {
            if (String.IsNullOrEmpty(chunkKey)) throw new ArgumentNullException(nameof(chunkKey));

            chunkKey = DedupeCommon.SanitizeString(chunkKey);

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<DedupeChunk>(nameof(DedupeChunk.Key)),
                DbOperators.Equals,
                chunkKey);

            DedupeChunk ret = _ORM.SelectFirst<DedupeChunk>(e);

            return ret;
        }
         
        /// <summary>
        /// Retrieve chunk metadata for a given object.
        /// </summary>
        /// <param name="key">Object key.</param>
        /// <returns>Chunks associated with the object.</returns>
        public override List<DedupeChunk> GetChunks(string key)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            List<DedupeChunk> ret = new List<DedupeChunk>();

            key = DedupeCommon.SanitizeString(key);
            List<DedupeObjectMap> maps = GetObjectMap(key);
            if (maps == null || maps.Count < 1) return ret;

            List<string> chunkKeys = maps.Select(m => m.ChunkKey).ToList();
            if (chunkKeys == null || chunkKeys.Count < 1) return ret;

            chunkKeys = chunkKeys.Distinct().ToList();

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<DedupeChunk>(nameof(DedupeChunk.Key)),
                DbOperators.In,
                chunkKeys);
             
            ret = _ORM.SelectMany<DedupeChunk>(e);
            return ret;
        }

        /// <summary>
        /// Retrieve the object map containing the metadata for a given address within the original object.
        /// </summary> 
        /// <param name="key">Object key.</param>
        /// <param name="position">Starting byte position.</param>
        /// <returns>Dedupe object map.</returns>
        public override DedupeObjectMap GetObjectMapForPosition(string key, long position)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (position < 0) throw new ArgumentOutOfRangeException("Start of range must be zero or greater.");

            key = DedupeCommon.SanitizeString(key);

            DedupeObject obj = GetObjectMetadata(key);
            if (obj == null) return null;
            
            string objMapTable = _ORM.GetTableName(typeof(DedupeObjectMap));
            string objKeyCol = _ORM.GetColumnName<DedupeObjectMap>(nameof(DedupeObjectMap.ObjectKey));
            string chunkAddrCol = _ORM.GetColumnName<DedupeObjectMap>(nameof(DedupeObjectMap.ChunkAddress));
            string chunkLenCol = _ORM.GetColumnName<DedupeObjectMap>(nameof(DedupeObjectMap.ChunkLength));

            string query =
                "SELECT * FROM " + objMapTable + " " +
                "WHERE " +
                "  " + objKeyCol + " = '" + obj.Key + "' " +
                "  AND " + chunkAddrCol + " <= " + position + " AND " + chunkAddrCol + " + " + chunkLenCol + " > " + position + " ";

            DedupeObjectMap map = null;
            DataTable result = _ORM.Query(query);

            if (result != null && result.Rows.Count > 0)
            {
                map = _ORM.DataRowToObject<DedupeObjectMap>(result.Rows[0]);
            }

            return map;
        }

        /// <summary>
        /// Retrieve the object map for a given object by key.
        /// </summary>
        /// <param name="key">Object key.</param>
        /// <returns>Object map entries.</returns>
        public override List<DedupeObjectMap> GetObjectMap(string key)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            List<DedupeObjectMap> ret = new List<DedupeObjectMap>();
             
            DbExpression e = new DbExpression(
                _ORM.GetColumnName<DedupeObjectMap>(nameof(DedupeObjectMap.ObjectKey)),
                DbOperators.Equals,
                key);

            ret = _ORM.SelectMany<DedupeObjectMap>(e);

            return ret;
        }

        #endregion

        #region Add-APIs

        /// <summary>
        /// Add a new object to the index.
        /// </summary>
        /// <param name="key">Object key.</param>
        /// <param name="length">The total length of the object.</param> 
        public override void AddObject(string key, long length)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (length < 1) throw new ArgumentException("Length must be greater than zero."); 

            key = DedupeCommon.SanitizeString(key);
            if (Exists(key)) throw new ArgumentException("An object with key '" + key + "' already exists.");

            DedupeObject obj = _ORM.Insert<DedupeObject>(new DedupeObject(key, length)); 
        }
         
        /// <summary>
        /// Add an object map to an existing object.
        /// </summary>
        /// <param name="key">Object key.</param>
        /// <param name="chunkKey">Chunk key.</param>
        /// <param name="chunkLength">Chunk length.</param>
        /// <param name="chunkPosition">Ordinal position of the chunk, i.e. 1, 2, ..., n.</param>
        /// <param name="chunkAddress">Byte address of the chunk within the original object.</param>
        public override void AddObjectMap(string key, string chunkKey, int chunkLength, int chunkPosition, long chunkAddress)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (String.IsNullOrEmpty(chunkKey)) throw new ArgumentNullException(nameof(chunkKey));
            if (chunkLength < 1) throw new ArgumentException("Chunk length must be greater than zero.");
            if (chunkPosition < 0) throw new ArgumentException("Position must be zero or greater.");
            if (chunkAddress < 0) throw new ArgumentException("Address must be zero or greater.");

            DedupeObjectMap map = new DedupeObjectMap(key, chunkKey, chunkLength, chunkPosition, chunkAddress);
            _ORM.Insert<DedupeObjectMap>(map);
        }

        /// <summary>
        /// Increment reference count for a chunk by its key.  If the chunk does not exist, it is created.
        /// </summary>
        /// <param name="chunkKey">Chunk key.</param>
        /// <param name="length">The chunk length, used when creating the chunk.</param>
        public override void IncrementChunkRefcount(string chunkKey, int length)
        {
            if (String.IsNullOrEmpty(chunkKey)) throw new ArgumentNullException(nameof(chunkKey));

            chunkKey = DedupeCommon.SanitizeString(chunkKey);

            lock (_ChunkLock)
            {
                DedupeChunk chunk = GetChunkMetadata(chunkKey);

                if (chunk != null)
                {
                    chunk.RefCount = chunk.RefCount + 1;
                    _ORM.Update<DedupeChunk>(chunk);
                }
                else
                {
                    chunk = new DedupeChunk(chunkKey, length, 1);
                    _ORM.Insert<DedupeChunk>(chunk);
                }
            }
        }
         
        #endregion

        #region Delete-APIs
         
        /// <summary>
        /// Delete an object and dereference the associated chunks.
        /// </summary>
        /// <param name="key">Object key.</param>
        /// <returns>List of chunk keys that should be garbage collected.</returns>
        public override List<string> Delete(string key)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            List<string> ret = new List<string>();

            key = DedupeCommon.SanitizeString(key);

            DedupeObject obj = GetObjectMetadata(key);
            if (obj == null) throw new KeyNotFoundException("Key '" + key + "' not found.");

            List<DedupeObjectMap> maps = GetObjectMap(key);
            if (maps != null && maps.Count > 0)
            {
                foreach (DedupeObjectMap map in maps)
                {
                    if (DecrementChunkRefcount(map.ChunkKey))
                    {
                        ret.Add(map.ChunkKey);
                    }
                }
            }

            DbExpression e = new DbExpression(
                _ORM.GetColumnName<DedupeObjectMap>(nameof(DedupeObjectMap.ObjectKey)),
                DbOperators.Equals,
                key);

            _ORM.DeleteMany<DedupeObjectMap>(e);
            _ORM.Delete<DedupeObject>(obj);
            // chunks to GC
            return ret;
        }
         
        /// <summary>
        /// Decrement the reference count of a chunk by its key.  If the reference count reaches zero, the chunk is deleted.
        /// </summary>
        /// <param name="chunkKey">Chunk key.</param>
        /// <returns>Boolean indicating if the chunk should be garbage collected.</returns>
        public override bool DecrementChunkRefcount(string chunkKey)
        {
            if (String.IsNullOrEmpty(chunkKey)) throw new ArgumentNullException(nameof(chunkKey));
             
            chunkKey = DedupeCommon.SanitizeString(chunkKey);

            lock (_ChunkLock)
            {
                DedupeChunk chunk = GetChunkMetadata(chunkKey);
                if (chunk == null) return false;

                chunk.RefCount = chunk.RefCount - 1;
                if (chunk.RefCount < 1)
                {
                    _ORM.Delete<DedupeChunk>(chunk);
                    return true;
                }
                else
                {
                    _ORM.Update<DedupeChunk>(chunk);
                    return false;
                }
            }
        }

        #endregion

        #endregion

        #region Private-Methods
         
        #endregion
    }
}
