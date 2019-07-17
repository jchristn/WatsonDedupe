using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace WatsonDedupe
{
    /// <summary>
    /// Metadata about a stored object.
    /// </summary>
    public class ObjectMetadata
    {
        #region Public-Members

        /// <summary>
        /// Object map ID.
        /// </summary>
        public int ObjectMapId { get; set; }
         
        /// <summary>
        /// The name of the object.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The length of the object.
        /// </summary>
        public long ContentLength { get; set; }

        /// <summary>
        /// The number of chunks associated with the object.
        /// </summary>
        public long ChunkCount { get; set; }

        /// <summary>
        /// The length of all chunks.
        /// </summary>
        public long ChunkLength { get; set; }

        /// <summary>
        /// Chunks associated with the object.
        /// </summary>
        public List<Chunk> Chunks { get; set; }

        #endregion

        #region Private-Members

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        public ObjectMetadata()
        {
            ChunkCount = 0;
            ChunkLength = 0;
            Chunks = new List<Chunk>(); 
        }

        /// <summary>
        /// Retrieve object metadata from a DataRow.  This will not populate chunk-related fields.
        /// </summary>
        /// <param name="row">DataRow.</param>
        /// <returns>Object metadata.</returns>
        public static ObjectMetadata FromDataRow(DataRow row)
        {
            if (row == null) throw new ArgumentNullException(nameof(row));

            ObjectMetadata ret = new ObjectMetadata();

            if (row.Table.Columns.Contains("ObjectMapId") && row["ObjectMapId"] != null && row["ObjectMapId"] != DBNull.Value)
                ret.ObjectMapId = Convert.ToInt32(row["ObjectMapId"]);
             
            if (row.Table.Columns.Contains("Name") && row["Name"] != null && row["Name"] != DBNull.Value)
                ret.Name = row["Name"].ToString();

            if (row.Table.Columns.Contains("ContentLength") && row["ContentLength"] != null && row["ContentLength"] != DBNull.Value)
                ret.ContentLength = Convert.ToInt64(row["ContentLength"]);

            ret.ChunkCount = 0;
            ret.ChunkLength = 0;
            ret.Chunks = new List<Chunk>();

            return ret;
        }

        /// <summary>
        /// Retrieve object metadata from a DataTable. 
        /// </summary>
        /// <param name="table">DataTable.</param>
        /// <returns>Object metadata.</returns>
        public static ObjectMetadata FromDataTable(DataTable table)
        {
            if (table == null) throw new ArgumentNullException(nameof(table));
            if (table.Rows == null || table.Rows.Count < 1) throw new ArgumentException("Supplied DataTable contains fewer than one row.");

            ObjectMetadata ret = ObjectMetadata.FromDataRow(table.Rows[0]);

            foreach (DataRow row in table.Rows)
            {
                Chunk currChunk = Chunk.FromDataRow(row);
                ret.ChunkCount++;
                ret.ChunkLength += currChunk.Length;
                ret.Chunks.Add(currChunk);
            }

            return ret;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Display a human-readable string.
        /// </summary>
        /// <returns>String.</returns>
        public override string ToString()
        {
            string ret =
                "----" + Environment.NewLine +
                "  ObjectMapId    : " + ObjectMapId + Environment.NewLine + 
                "  Name           : " + Name + Environment.NewLine +
                "  ContentLength  : " + ContentLength + Environment.NewLine +
                "  ChunkCount     : " + ChunkCount + Environment.NewLine +
                "  ChunkLength    : " + ChunkLength + Environment.NewLine;

            if (Chunks != null)
            {
                ret += "  Chunks         : " + Environment.NewLine;
                foreach (Chunk curr in Chunks)
                {
                    ret += "    " + curr.Position + ": " + curr.Key + " [" + curr.Length + "B address " + curr.Address + "]" + Environment.NewLine;
                }
            }
            else
            {
                ret += "  Chunks         : [null]";
            }

            return ret;
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
