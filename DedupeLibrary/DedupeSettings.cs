using System;
using System.Collections.Generic;
using System.Text;

namespace WatsonDedupe
{
    /// <summary>
    /// Settings that dictate how deduplication operates internally.
    /// </summary>
    public class DedupeSettings
    {
        #region Public-Members

        /// <summary>
        /// The minimum amount of data that can be considered an independent chunk of data upon which to identify duplicates.
        /// With smaller values, the probability of finding redundancy increases, but comes at the cost of processing time and computation.
        /// With larger values, processing times are faster, but less redundancy is identified.
        /// Use smaller values when attempting to deduplicate within a file and larger values when deduplicating across files.
        /// This value must be a positive integer evenly divisible by 64, and must be at least 256.
        /// Default is 1024.
        /// </summary>
        public int MinChunkSize
        {
            get
            {
                return _MinChunkSize;
            }
            set
            {
                if (value < 256 || value % 64 != 0) throw new ArgumentException("Minimum chunk size must be a positive integer evenly divisible by 64 and greater than or equal to 256.");
                _MinChunkSize = value;
            }
        }

        /// <summary>
        /// The maximum amount of data that can be considered an independent chunk of data upon which to identify duplicates.
        /// With smaller values, the probability of finding redundancy increases, but comes at the cost of processing time and computation.
        /// With larger values, processing times are faster, but less redundancy is identified.
        /// Use smaller values when attempting to deduplicate within a file and larger values when deduplicating across files.
        /// This value must be greater than the minimum chunk size and no larger than 8 times the minimum chunk size.
        /// This value must be a positive integer evenly divisible by 64, and must be at least 2048.
        /// Default is 32768.
        /// </summary>
        public int MaxChunkSize
        {
            get
            {
                return _MaxChunkSize;
            }
            set
            {
                if (value < 2048 || value % 64 != 0) throw new ArgumentException("Maximum chunk size must be a positive integer evenly divisible by 64 and greater than or equal to 2048.");
                if (value <= _MinChunkSize || value < (8 * _MinChunkSize)) throw new ArgumentOutOfRangeException("Maximum chunk size must be at least 8 times larger than minimum chunk size.");
                _MaxChunkSize = value;
            }
        }

        /// <summary>
        /// The number of bytes to shift to mathematically re-evaluate content in the sliding window for a chunk boundary.
        /// With smaller values, more chunk boundaries will be detected at the cost of processing time and computation.
        /// This value must be less than the minimum chunk size.
        /// It is recommended that this value be approximately 1/32 the size of the minimum chunk size.
        /// Default is 32.
        /// </summary>
        public int ShiftCount
        {
            get
            {
                return _ShiftCount;
            }
            set
            {
                if (value <= 1) throw new ArgumentOutOfRangeException("Shift count must be greater than zero.");
                if (value > _MinChunkSize) throw new ArgumentOutOfRangeException("Shift count must be less than or equal to the minimum chunk size.");
                _ShiftCount = value;
            }
        }

        /// <summary>
        /// The number of bytes to evaluate at the end of the sliding window to determine if a chunk boundary has been identified.
        /// With smaller values, it is more likely that chunks will be identified.
        /// With larger values, it is less likely that chunks will be identified.
        /// Use smaller values (i.e. 1) when working with documents, and larger values (i.e. 3 or 4) when working with large blocks of data.
        /// This value must be a positive integer between 1 and 4.
        /// Default is 2.
        /// </summary>
        public int BoundaryCheckBytes
        {
            get
            {
                return _BoundaryCheckBytes;
            }
            set
            {
                if (value < 1 || value > 4) throw new ArgumentOutOfRangeException("Boundary check bytes must be in the range of 1 through 4.");
                _BoundaryCheckBytes = value;
            }
        }

        #endregion

        #region Private-Members

        private int _MinChunkSize = 1024;
        private int _MaxChunkSize = 32768;
        private int _ShiftCount = 32;
        private int _BoundaryCheckBytes = 2;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        public DedupeSettings()
        {

        }

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        /// <param name="minChunkSize"></param>
        /// <param name="maxChunkSize"></param>
        /// <param name="shiftCount"></param>
        /// <param name="boundaryCheckBytes"></param>
        public DedupeSettings(int minChunkSize, int maxChunkSize, int shiftCount, int boundaryCheckBytes)
        {
            if (minChunkSize < 256 || minChunkSize % 64 != 0) throw new ArgumentException("Minimum chunk size must be a positive integer evenly divisible by 64 and greater than or equal to 256.");
            if (maxChunkSize < 2048 || maxChunkSize % 64 != 0) throw new ArgumentException("Maximum chunk size must be a positive integer evenly divisible by 64 and greater than or equal to 2048.");
            if (maxChunkSize <= minChunkSize || maxChunkSize < (8 * minChunkSize)) throw new ArgumentOutOfRangeException("Maximum chunk size must be at least 8 times larger than minimum chunk size.");
            if (shiftCount <= 1) throw new ArgumentOutOfRangeException("Shift count must be greater than zero."); 
            if (shiftCount > minChunkSize) throw new ArgumentOutOfRangeException("Shift count must be less than or equal to the minimum chunk size.");
            if (boundaryCheckBytes < 1 || boundaryCheckBytes > 4) throw new ArgumentOutOfRangeException("Boundary check bytes must be in the range of 1 through 4.");

            _MinChunkSize = minChunkSize;
            _MaxChunkSize = maxChunkSize;
            _ShiftCount = shiftCount;
            _BoundaryCheckBytes = boundaryCheckBytes;
        }
         
        #endregion

        #region Public-Methods

        #endregion

        #region Private-Methods

        #endregion
    }
}
