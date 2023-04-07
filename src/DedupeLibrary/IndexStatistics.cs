using System;
using System.Collections.Generic;
using System.Text;

namespace WatsonDedupe
{
    /// <summary>
    /// Deduplication index statistics.
    /// </summary>
    public class IndexStatistics
    {
        #region Public-Members

        /// <summary>
        /// The number of objects stored in the index.
        /// </summary>
        public long Objects
        {
            get
            {
                return _Objects;
            }
            set
            {
                if (value < 0) throw new ArgumentOutOfRangeException("Objects must be greater than or equal to zero.");
                _Objects = value;
            }
        }

        /// <summary>
        /// The number of chunks stored in the index.
        /// </summary>
        public long Chunks
        {
            get
            {
                return _Chunks;
            }
            set
            {
                if (value < 0) throw new ArgumentOutOfRangeException("Chunks must be greater than or equal to zero.");
                _Chunks = value;
            }
        }

        /// <summary>
        /// The amount of data stored in the index, i.e. a sum of the content length of all objects.
        /// </summary>
        public long LogicalBytes
        {
            get
            {
                return _LogicalBytes;
            }
            set
            {
                if (value < 0) throw new ArgumentOutOfRangeException("Logical bytes must be greater than or equal to zero.");
                _LogicalBytes = value;
            }
        }

        /// <summary>
        /// The physical data stored as chunks in support of index objects, i.e. the amount of deduplicated data.
        /// </summary>
        public long PhysicalBytes
        {
            get
            {
                return _PhysicalBytes;
            }
            set
            {
                if (value < 0) throw new ArgumentOutOfRangeException("Physical bytes must be greater than or equal to zero.");
                _PhysicalBytes = value;
            }
        }

        /// <summary>
        /// The deduplication ratio represented as a multiplier, i.e. 3X deduplication means 3MB of logical data (objects) has been reduced to 1MB of physical data (chunks).
        /// </summary>
        public decimal RatioX
        {
            get
            {
                if (_PhysicalBytes > 0 && _LogicalBytes > 0)
                {
                    return (decimal)_LogicalBytes / (decimal)_PhysicalBytes; 
                }

                return 0m;
            }
        }

        /// <summary>
        /// The deduplication ratio represented as a percentage, i.e. 50% deduplication means 10MB of logical data (objects) has been reduced to 5MB of physical data (chunks).
        /// </summary>
        public decimal RatioPercent
        {
            get
            {
                if (_PhysicalBytes > 0 && _LogicalBytes > 0)
                {
                    return (100 * (1 - ((decimal)_PhysicalBytes / (decimal)_LogicalBytes)));
                }

                return 0m;
            }
        }

        #endregion

        #region Private-Members

        private long _Objects = 0;
        private long _Chunks = 0;
        private long _LogicalBytes = 0;
        private long _PhysicalBytes = 0; 

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        public IndexStatistics()
        {

        }
         
        #endregion

        #region Public-Methods

        /// <summary>
        /// Human-readable version of the object.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            string ret =
                "--- Index Statistics ---" + Environment.NewLine +
                "    Objects       : " + Objects + Environment.NewLine +
                "    Chunks        : " + Chunks + Environment.NewLine +
                "    LogicalBytes  : " + LogicalBytes + Environment.NewLine +
                "    PhysicalBytes : " + PhysicalBytes + Environment.NewLine +
                "    RatioX        : " + RatioX + "X" + Environment.NewLine +
                "    RatioPercent  : " + RatioPercent + "%";

            return ret;
        }

        #endregion

        #region Private-Methods

        #endregion
    }
}
