using System;
using System.Collections.Generic;
using System.Text;
using Watson.ORM.Core;

namespace WatsonDedupe
{
    /// <summary>
    /// Chunk identified in one or many deduplicated object.
    /// </summary>
    [Table("dedupechunk")]
    public class DedupeChunk
    {
        /// <summary>
        /// Database ID.
        /// </summary>
        [Column("id", true, DataTypes.Int, false)]
        public int Id { get; set; }

        /// <summary>
        /// Chunk key.
        /// </summary>
        [Column("chunkkey", false, DataTypes.Varchar, 128, false)]
        public string Key { get; set; }

        /// <summary>
        /// Length of the chunk.
        /// </summary>
        [Column("length", false, DataTypes.Int, false)]
        public int Length { get; set; }

        /// <summary>
        /// Reference count for the chunk.
        /// </summary>
        [Column("refcount", false, DataTypes.Int, false)]
        public int RefCount { get; set; }

        /// <summary>
        /// Chunk data.
        /// </summary>
        public byte[] Data = null;

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        public DedupeChunk()
        {

        }

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        /// <param name="key">Chunk key.</param>
        /// <param name="length">Length of the chunk.</param>
        /// <param name="refCount">Reference count for the chunk.</param>
        public DedupeChunk(string key, int length, int refCount)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (length < 1) throw new ArgumentException("Length must be greater than zero.");
            if (refCount < 1) throw new ArgumentException("Reference count must be greater than zero.");

            Key = key;
            Length = length;
            RefCount = refCount;
        }

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        /// <param name="key">Chunk key.</param>
        /// <param name="length">Length of the chunk.</param>
        /// <param name="refCount">Reference count for the chunk.</param>
        /// <param name="data">Byte data.</param>
        public DedupeChunk(string key, int length, int refCount, byte[] data)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (length < 1) throw new ArgumentException("Length must be greater than zero.");
            if (refCount < 1) throw new ArgumentException("Reference count must be greater than zero.");
            if (data == null || data.Length < 1) throw new ArgumentNullException(nameof(data));
            if (length != data.Length) throw new ArgumentException("Supplied length and data length do not match.");

            Key = key;
            Length = length;
            RefCount = refCount;
            Data = data;
        }
    }
}
