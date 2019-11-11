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
        /// <summary>
        /// Method to determine if the database has been initialized, generally by checking for the existence of rows in the dedupe configuration table.
        /// </summary>
        /// <returns>True if initialized.</returns>
        public abstract bool IsInitialized();

        /// <summary> 
        /// Add configuration-related data by key for deduplication operations.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="val">Value.</param>
        public abstract void AddConfigData(string key, string val);

        /// <summary>
        /// Retrieve configuration-related data by key for deduplication operations.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="val">Value.</param>
        /// <returns>True if successful.</returns>
        public abstract bool GetConfigData(string key, out string val);

        /// <summary>
        /// Check if a chunk exists.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <returns>True if exists.</returns>
        public abstract bool ChunkExists(string key);

        /// <summary>
        /// Check if an object exists.
        /// </summary>
        /// <param name="name">Object name.</param>
        /// <returns>True if exists.</returns>
        public abstract bool ObjectExists(string name);

        /// <summary>
        /// List objects stored in the database.
        /// </summary>
        /// <param name="names">List of object names.</param>
        public abstract void ListObjects(out List<string> names);

        /// <summary>
        /// Add a chunk for an object.
        /// </summary>
        /// <param name="name">Object name.</param>
        /// <param name="totalLen">Total length of the object.</param>
        /// <param name="chunk">Chunk.</param>
        /// <returns>True if successful.</returns>
        public abstract bool AddObjectChunk(string name, long totalLen, Chunk chunk);

        /// <summary>
        /// Add multiple chunks for an object.
        /// </summary>
        /// <param name="name">Object name.</param>
        /// <param name="totalLen">Total length of the object.</param>
        /// <param name="chunks">Chunks.</param>
        /// <returns>True if successful.</returns>
        public abstract bool AddObjectChunks(string name, long totalLen, List<Chunk> chunks);

        /// <summary>
        /// Retrieve metadata for an object.
        /// </summary>
        /// <param name="name">Object name.</param>
        /// <param name="metadata">Object metadata.</param>
        /// <returns>True if successful.</returns>
        public abstract bool GetObjectMetadata(string name, out ObjectMetadata metadata);

        /// <summary>
        /// Retrieve chunks associated with an object.
        /// </summary>
        /// <param name="name">Object name.</param>
        /// <param name="chunks">Chunks.</param>
        /// <returns>True if successful.</returns>
        public abstract bool GetObjectChunks(string name, out List<Chunk> chunks);

        /// <summary>
        /// Retrieve chunks containing data within a range of bytes from the original object.
        /// </summary>
        /// <param name="name">Object name.</param>
        /// <param name="start">Starting range.</param>
        /// <param name="end">Ending range.</param>
        /// <param name="chunks">Chunks.</param>
        /// <returns>True if successful.</returns>
        public abstract bool GetChunksForRange(string name, long start, long end, out List<Chunk> chunks);

        /// <summary>
        /// Retrieve the chunk containing data for a given address within the original object.
        /// </summary>
        /// <param name="name">Object name.</param>
        /// <param name="start">Starting range.</param>
        /// <param name="chunk">Chunk.</param>
        /// <returns>True if successful.</returns>
        public abstract bool GetChunkForPosition(string name, long start, out Chunk chunk);

        /// <summary>
        /// Delete an object and dereference the associated chunks.
        /// </summary>
        /// <param name="name">The name of the object.</param>
        /// <param name="garbageCollectChunks">List of chunk keys that should be garbage collected.</param>
        public abstract void DeleteObjectChunks(string name, out List<string> garbageCollectChunks);

        /// <summary>
        /// Increment reference count for a chunk.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="len">Length of the chunk.</param>
        /// <returns>True if successful.</returns>
        public abstract bool IncrementChunkRefcount(string key, long len);

        /// <summary>
        /// Decrement reference count for a chunk.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="garbageCollect">True if the chunk data can be garbage collected.</param>
        /// <returns>True if successful.</returns>
        public abstract bool DecrementChunkRefcount(string key, out bool garbageCollect);

        /// <summary>
        /// Retrieve statistics for the index.
        /// </summary>
        /// <param name="numObjects">The number of objects stored in the index.</param>
        /// <param name="numChunks">The number of chunks stored in the index.</param>
        /// <param name="logicalBytes">The amount of data stored in the index, i.e. the full size of the original data.</param>
        /// <param name="physicalBytes">The number of bytes consumed by chunks of data, i.e. the deduplication set size.</param>
        /// <param name="dedupeRatioX">Deduplication ratio represented as a multiplier.</param>
        /// <param name="dedupeRatioPercent">Deduplication ratio represented as a percentage.</param>
        /// <returns>True if successful.</returns>
        public abstract bool IndexStats(out int numObjects, out int numChunks, out long logicalBytes, out long physicalBytes, out decimal dedupeRatioX, out decimal dedupeRatioPercent);

        /// <summary>
        /// Backup the deduplication database, not including chunk data, to another file.
        /// </summary>
        /// <param name="filename">The destination file.</param>
        /// <returns>True if successful.</returns>
        public abstract bool BackupDatabase(string filename);
    }
}
