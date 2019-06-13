using System;
using System.Collections.Generic;
using System.Text;

namespace WatsonDedupe
{
    /// <summary>
    /// Callback methods used to to read, write, or delete a chunk.
    /// </summary>
    public class CallbackMethods
    {
        /// <summary>
        /// Write a chunk.  Passes the Chunk object; you must return true. 
        /// </summary>
        public Func<Chunk, bool> WriteChunk
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
        /// Read a chunk.  Passes the the chunk's key as a string; you must return the byte array data of the chunk. 
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
        /// Delete a chunk.  Passes the chunk's key; you must return true;
        /// </summary>
        public Func<string, bool> DeleteChunk
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


        private Func<Chunk, bool> _WriteChunk = null;
        private Func<string, byte[]> _ReadChunk = null;
        private Func<string, bool> _DeleteChunk = null;
    }
}
