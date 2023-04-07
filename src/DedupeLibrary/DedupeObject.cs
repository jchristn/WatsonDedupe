using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Watson.ORM.Core;

namespace WatsonDedupe
{
    /// <summary>
    /// Deduplicated object stored in the index.
    /// </summary>
    [Table("dedupeobject")]
    public class DedupeObject
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
        public string Key { get; set; }

        /// <summary>
        /// Length of the object.
        /// </summary>
        [Column("originallength", false, DataTypes.Long, false)]
        public long OriginalLength { get; set; }

        /// <summary>
        /// Compressed length of the object.
        /// </summary>
        [Column("complength", false, DataTypes.Long, false)]
        public long CompressedLength { get; set; }

        /// <summary>
        /// Number of chunks.
        /// </summary>
        [Column("chunkcount", false, DataTypes.Long, false)]
        public long ChunkCount { get; set; }

        /// <summary>
        /// Creation timestamp in UTC time.
        /// </summary>
        [Column("createdutc", false, DataTypes.DateTime, false)]
        public DateTime CreatedUtc { get; set; }

        /// <summary>
        /// List of chunks that comprise the object.
        /// </summary>
        public List<DedupeChunk> Chunks = new List<DedupeChunk>();

        /// <summary>
        /// Object map indicating chunk positions.
        /// </summary>
        public List<DedupeObjectMap> ObjectMap = new List<DedupeObjectMap>();

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        public DedupeObject()
        { 
            CreatedUtc = DateTime.Now.ToUniversalTime();
        }

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        /// <param name="key">Object key.</param>
        /// <param name="originalLength">Original length.</param>
        /// <param name="compressedLength">Compressed length.</param>
        /// <param name="chunkCount">Chunk count.</param>
        public DedupeObject(string key, long originalLength, long compressedLength, long chunkCount)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));
            if (originalLength < 1) throw new ArgumentException("Length must be greater than zero.");

            Key = key;
            OriginalLength = originalLength;
            CompressedLength = compressedLength;
            ChunkCount = chunkCount;
            CreatedUtc = DateTime.Now.ToUniversalTime();
        }

        /// <summary>
        /// Data from the object.  Using this property will fully read the stream.
        /// </summary>
        public byte[] Data
        {
            get
            {
                if (_Data == null && _DataStream != null && OriginalLength > 0)
                {
                    _Data = DedupeCommon.StreamToBytes(_DataStream);
                }

                return _Data;
            }
        }

        /// <summary>
        /// Stream containing data from the object.
        /// </summary>
        public Stream DataStream
        {
            get
            {
                return _DataStream;
            }
            internal set
            {
                if (value == null) throw new ArgumentNullException(nameof(DataStream));
                if (!value.CanRead) throw new ArgumentException("Cannot read from supplied stream.");
                _DataStream = value;
            }
        }

        /// <summary>
        /// Human-readable string version of the object.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            string ret =
                "--- DedupeObject ---" + Environment.NewLine +
                "    Key               : " + Key + Environment.NewLine +
                "    Original Length   : " + OriginalLength + Environment.NewLine +
                "    Compressed Length : " + OriginalLength + Environment.NewLine +
                "    Chunk Count       : " + ChunkCount+ Environment.NewLine +
                "    CreatedUtc        : " + CreatedUtc.ToString() + Environment.NewLine +
                // "    Chunks            : " + Chunks.Count + Environment.NewLine +
                "    ObjectMap         : " + ObjectMap.Count;

            return ret;
        }

        private byte[] _Data = null;
        private Stream _DataStream = null;
    }
}
