using System;
using System.Collections.Generic;
using System.Text;
using Watson.ORM.Core;

namespace WatsonDedupe
{
    /// <summary>
    /// Dedupe configuration setting
    /// </summary>
    [Table("dedupeconfig")]
    public class DedupeConfig
    {
        /// <summary>
        /// Database ID.
        /// </summary>
        [Column("id", true, DataTypes.Int, false)]
        public int Id { get; set; }

        /// <summary>
        /// Chunk key.
        /// </summary>
        [Column("key", false, DataTypes.Varchar, 128, false)]
        public string Key { get; set; }

        /// <summary>
        /// Length of the chunk.
        /// </summary>
        [Column("val", false, DataTypes.Varchar, 1024, false)]
        public string Value { get; set; }

        /// <summary>
        /// Config key-value pair GUID.
        /// </summary>
        [Column("guid", false, DataTypes.Varchar, 64, false)]
        public string GUID { get; set; }

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        public DedupeConfig()
        {
            GUID = Guid.NewGuid().ToString();
        }

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="val">Value.</param>
        public DedupeConfig(string key, string val)
        {
            if (String.IsNullOrEmpty(key)) throw new ArgumentNullException(nameof(key));

            Key = key;
            Value = val;
            GUID = Guid.NewGuid().ToString();
        }
    }
}
