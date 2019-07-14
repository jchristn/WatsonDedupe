using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WatsonDedupe
{
    /// <summary>
    /// Represents a chunk of data identified during deduplication.
    /// </summary>
    public class Chunk
    {
        #region Public-Members

        /// <summary>
        /// The key of the chunk.
        /// </summary>
        public string Key { get; set; }

        /// <summary>
        /// The length of the chunk data.
        /// </summary>
        public long Length { get; set; }

        /// <summary>
        /// The order of the chunk within the current object.
        /// </summary>
        public long Position { get; set; }

        /// <summary>
        /// The address of the chunk within the current object.
        /// </summary>
        public long Address { get; set; }

        /// <summary>
        /// The byte data of the chunk.
        /// </summary>
        public byte[] Value { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        public Chunk()
        {

        }

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        /// <param name="key">The key of the chunk.</param>
        /// <param name="len">The length of the chunk data.</param>
        /// <param name="pos">The order of the chunk within the object.</param>
        /// <param name="address">The address of the chunk within the current object.</param>
        public Chunk(string key, long len, long pos, long address)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(key);
            if (len < 1) throw new ArgumentOutOfRangeException(nameof(len));
            if (pos < 0) throw new ArgumentOutOfRangeException(nameof(pos));
            if (address < 0) throw new ArgumentOutOfRangeException(nameof(address));

            Key = DedupeCommon.SanitizeString(key);
            Length = len;
            Position = pos;
            Address = address;
        }

        /// <summary>
        /// Instantiates the object.
        /// </summary>
        /// <param name="key">The key of the chunk.</param>
        /// <param name="len">The length of the chunk data.</param>
        /// <param name="pos">The order of the chunk within the object.</param>
        /// <param name="address">The address of the chunk within the current object.</param>
        /// <param name="value">The byte data of the chunk.</param>
        public Chunk(string key, long len, long pos, long address, byte[] value)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(key);
            if (len < 1) throw new ArgumentOutOfRangeException(nameof(len));
            if (pos < 0) throw new ArgumentOutOfRangeException(nameof(pos));
            if (address < 0) throw new ArgumentOutOfRangeException(nameof(Address));
            if (value == null || value.Length < 1) throw new ArgumentNullException(nameof(value));

            Key = DedupeCommon.SanitizeString(key);
            Length = len;
            Position = pos;
            Address = address;
            Value = value;
        }

        /// <summary>
        /// Converts a DataRow to a Chunk.
        /// </summary>
        /// <param name="row">The DataRow.</param>
        /// <returns>A populated Chunk.</returns>
        public static Chunk FromDataRow(DataRow row)
        {
            if (row == null) throw new ArgumentNullException(nameof(row));

            Chunk c = new Chunk(
                row["ChunkKey"].ToString(),
                Convert.ToInt64(row["ChunkLength"]),
                Convert.ToInt64(row["ChunkPosition"]),
                Convert.ToInt64(row["ChunkAddress"])
                );

            return c;
        }

        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        #endregion
    }
}
