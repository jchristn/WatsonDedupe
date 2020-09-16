using System;
using System.Collections.Generic;
using System.Text;

namespace WatsonDedupe
{
    /// <summary>
    /// Callback methods used to to read, write, or delete a chunk.
    /// </summary>
    public class DedupeCallbacks
    {
        /// <summary>
        /// Write a chunk.  Your software should persist the chunk to storage.
        /// </summary>
        public Action<DedupeChunk> WriteChunk
        {
            get
            {
                return _WriteChunk;
            }
            set
            {
                if (value == null) throw new ArgumentNullException(nameof(WriteChunk));
                _WriteChunk = value;
            }
        }

        /// <summary>
        /// Read a chunk.  Your software should retrieve the chunk data as a byte array using the string key supplied.
        /// </summary>
        public Func<string, byte[]> ReadChunk
        {
            get
            {
                return _ReadChunk;
            }
            set
            {
                if (value == null) throw new ArgumentNullException(nameof(WriteChunk));
                _ReadChunk = value;
            }
        } 

        /// <summary>
        /// Delete a chunk.  Your software should delete the chunk associated with the supplied string key.
        /// </summary>
        public Action<string> DeleteChunk
        {
            get
            {
                return _DeleteChunk;
            }
            set
            {
                if (value == null) throw new ArgumentNullException(nameof(WriteChunk));
                _DeleteChunk = value;
            }
        }

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        public DedupeCallbacks()
        {

        }

        /// <summary>
        /// Instantiate the object.
        /// </summary>
        /// <param name="writeChunk">Function with no return value used to write a chunk.</param>
        /// <param name="readChunk">Function returning a byte[] used to read a chunk's data.</param>
        /// <param name="deleteChunk">Function with no return value used to delete a chunk.</param>
        public DedupeCallbacks(Action<DedupeChunk> writeChunk, Func<string, byte[]> readChunk, Action<string> deleteChunk)
        {
            if (writeChunk == null) throw new ArgumentNullException(nameof(writeChunk));
            if (readChunk == null) throw new ArgumentNullException(nameof(readChunk));
            if (deleteChunk == null) throw new ArgumentNullException(nameof(deleteChunk));

            WriteChunk = writeChunk;
            ReadChunk = readChunk;
            DeleteChunk = deleteChunk;
        }

        private Action<DedupeChunk> _WriteChunk = null;
        private Func<string, byte[]> _ReadChunk = null;
        private Action<string> _DeleteChunk = null;
    }
}
