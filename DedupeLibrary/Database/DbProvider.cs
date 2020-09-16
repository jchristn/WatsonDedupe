using System;
using System.Collections.Generic;
using System.Text;

namespace WatsonDedupe.Database
{
    /// <summary>
    /// External database provider.
    /// </summary>
    public abstract class DbProvider
    {
        #region General-APIs

        /// <summary>
        /// Method to indicate whether or not the database has been initialized.
        /// </summary>
        /// <returns>True if initialized.</returns>
        public abstract bool IsInitialized();

        /// <summary> 
        /// Add configuration-related data by key for deduplication operations.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="val">Value.</param>
        public abstract void AddConfigValue(string key, string val);

        /// <summary>
        /// Retrieve configuration-related data by key for deduplication operations.
        /// </summary>
        /// <param name="key">Key.</param> 
        /// <returns>Value.</returns>
        public abstract string GetConfigValue(string key);

        /// <summary>
        /// Retrieve statistics for the index.
        /// </summary> 
        /// <returns>Index statistics.</returns>
        public abstract IndexStatistics GetStatistics();

        #endregion

        #region Enumeration-APIs

        /// <summary>
        /// List the objects stored in the index.
        /// </summary>
        /// <param name="prefix">Prefix upon which to match object keys.</param>
        /// <param name="indexStart">The index (DedupeObject.Id) from which to begin the enumeration.</param>
        /// <param name="maxResults">Maximum number of results to retrieve.</param>
        /// <return>Enumeration result.</return>
        public abstract EnumerationResult ListObjects(string prefix, int indexStart, int maxResults);

        #endregion

        #region Exists-APIs

        /// <summary>
        /// Check if an object exists.
        /// </summary>
        /// <param name="key">Object key.</param>
        /// <returns>True if exists.</returns>
        public abstract bool Exists(string key);
         
        #endregion

        #region Get-APIs

        /// <summary>
        /// Retrieve metadata for an object by its key.
        /// DedupeObjectMap objects returned should be ordered in ascending order based on the chunk's position or address.
        /// </summary>
        /// <param name="key">Object key.</param>
        /// <returns>Object metadata.</returns>
        public abstract DedupeObject GetObjectMetadata(string key);

        /// <summary>
        /// Retrieve metadata for a given chunk by its key.
        /// </summary>
        /// <param name="chunkKey">Chunk key.</param>
        /// <returns>Chunk metadata.</returns>
        public abstract DedupeChunk GetChunkMetadata(string chunkKey);

        /// <summary>
        /// Retrieve chunks associated with an object.
        /// </summary>
        /// <param name="key">Object key.</param>
        /// <returns>Chunks.</returns>
        public abstract List<DedupeChunk> GetChunks(string key);
         
        /// <summary>
        /// Retrieve the object map containing the metadata for a given address within the original object.
        /// </summary> 
        /// <param name="key">Object key.</param>
        /// <param name="position">Starting byte position.</param>
        /// <returns>Dedupe object map.</returns>
        public abstract DedupeObjectMap GetObjectMapForPosition(string key, long position);

        /// <summary>
        /// Retrieve the object map for a given object by key.
        /// </summary>
        /// <param name="key">Object key.</param>
        /// <returns>Object map entries.</returns>
        public abstract List<DedupeObjectMap> GetObjectMap(string key);

        #endregion

        #region Add-APIs

        /// <summary>
        /// Add a new object to the index.
        /// </summary>
        /// <param name="key">Object key.</param>
        /// <param name="length">The total length of the object.</param>
        public abstract void AddObject(string key, long length);
         
        /// <summary>
        /// Add an object map to an existing object.
        /// </summary>
        /// <param name="key">Object key.</param>
        /// <param name="chunkKey">Chunk key.</param>
        /// <param name="chunkLength">Chunk length.</param>
        /// <param name="chunkPosition">Ordinal position of the chunk, i.e. 1, 2, ..., n.</param>
        /// <param name="chunkAddress">Byte address of the chunk within the original object.</param>
        public abstract void AddObjectMap(string key, string chunkKey, int chunkLength, int chunkPosition, long chunkAddress);

        /// <summary>
        /// Increment reference count for a chunk by its key.  If the chunk does not exist, it is created.
        /// </summary>
        /// <param name="chunkKey">Chunk key.</param>
        /// <param name="length">The chunk length, used when creating the chunk.</param>
        public abstract void IncrementChunkRefcount(string chunkKey, int length);
         
        #endregion

        #region Delete-APIs

        /// <summary>
        /// Delete an object and dereference the associated chunks.
        /// </summary>
        /// <param name="key">Object key.</param>
        /// <returns>List of chunk keys that should be garbage collected.</returns>
        public abstract List<string> Delete(string key);

        /// <summary>
        /// Decrement the reference count of a chunk by its key.  If the reference count reaches zero, the chunk is deleted.
        /// </summary>
        /// <param name="chunkKey">The chunk GUID.</param>
        /// <returns>Boolean indicating if the chunk should be garbage collected.</returns>
        public abstract bool DecrementChunkRefcount(string chunkKey);

        #endregion 
    }
}
