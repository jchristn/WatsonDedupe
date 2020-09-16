using System;
using System.Collections.Generic;
using System.Text;
using Watson.ORM.Core;

namespace WatsonDedupe
{
    /// <summary>
    /// Object that maps a dedupe chunk to a range within a deduplicated object.
    /// </summary>
    [Table("dedupeobjmap")]
    public class DedupeObjectMap
    {
        /// <summary>
        /// Database ID.
        /// </summary>
        [Column("id", true, DataTypes.Int, false)]
        public int Id { get; set; }

        /// <summary>
        /// Object key.
        /// </summary>
        [Column("objectkey", false, DataTypes.Varchar, 1024, false)]
        public string ObjectKey { get; set; }

        /// <summary>
        /// Chunk key.
        /// </summary>
        [Column("chunkkey", false, DataTypes.Varchar, 64, false)]
        public string ChunkKey { get; set; }

        /// <summary>
        /// Chunk length.
        /// </summary>
        [Column("length", false, DataTypes.Int, false)]
        public int ChunkLength { get; set; }

        /// <summary>
        /// The ordinal position of the chunk.
        /// </summary>
        [Column("position", false, DataTypes.Int, false)]
        public int ChunkPosition { get; set; }

        /// <summary>
        /// The byte position of the chunk within the original object.
        /// </summary>
        [Column("address", false, DataTypes.Long, false)]
        public long ChunkAddress { get; set; }
         
        /// <summary>
        /// Instantiate the object.
        /// </summary>
        public DedupeObjectMap()
        {

        }

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        /// <param name="objKey">Object key.</param>
        /// <param name="chunkKey">Chunk key.</param>
        /// <param name="chunkLength">Chunk length.</param>
        /// <param name="chunkPosition">The ordinal position of the chunk.</param>
        /// <param name="chunkAddress">The byte position of the chunk within the original object.</param>
        public DedupeObjectMap(string objKey, string chunkKey, int chunkLength, int chunkPosition, long chunkAddress)
        {
            if (String.IsNullOrEmpty(objKey)) throw new ArgumentNullException(nameof(objKey));
            if (String.IsNullOrEmpty(chunkKey)) throw new ArgumentNullException(nameof(chunkKey));
            if (chunkLength < 1) throw new ArgumentException("Chunk length must be greater than zero.");
            if (chunkPosition < 0) throw new ArgumentException("Chunk position must be zero or greater.");
            if (chunkAddress < 0) throw new ArgumentException("Chunk address must be zero or greater.");

            ObjectKey = objKey;
            ChunkKey = chunkKey;
            ChunkLength = chunkLength;
            ChunkPosition = chunkPosition;
            ChunkAddress = chunkAddress;
        }
    }
}
