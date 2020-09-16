using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using WatsonDedupe.Database;

namespace WatsonDedupe
{
    /// <summary>
    /// A read-only stream over a deduplicated object.
    /// </summary>
    public class DedupeStream : Stream
    {
        private DedupeObject _Metadata = null;
        private DbProvider _Database = null;
        private DedupeCallbacks _Callbacks = null;
        private long _Position = 0;

        internal DedupeStream(DedupeObject md, DbProvider db, DedupeCallbacks callbacks)
        {
            if (md == null) throw new ArgumentNullException(nameof(md));
            if (db == null) throw new ArgumentNullException(nameof(db));
            if (callbacks == null) throw new ArgumentNullException(nameof(callbacks));

            _Metadata = md;
            _Database = db;
            _Callbacks = callbacks;
        }

        /// <summary>
        /// Indicates if the stream allows read operations.
        /// </summary>
        public override bool CanRead => true;

        /// <summary>
        /// Indicates if the stream allows seek operations.
        /// </summary>
        public override bool CanSeek => true;

        /// <summary>
        /// Indicates if the stream allows write operations.
        /// </summary>
        public override bool CanWrite => false;

        /// <summary>
        /// Indicates the length of the content contained within the stream.
        /// </summary>
        public override long Length => _Metadata.Length;

        /// <summary>
        /// Indicates the current position within the stream.
        /// </summary>
        public override long Position
        {
            get
            {
                return _Position;
            }
            set
            {
                if (value < 0) throw new ArgumentOutOfRangeException("Position must be zero or greater.");
                if (value > _Metadata.Length) throw new ArgumentOutOfRangeException("Position must be less than or equal to the content length.");
                _Position = value;
            }
        }

        /// <summary>
        /// Not supported.  This method will throw a NotSupportedException.
        /// </summary>
        public override void Flush()
        {
            throw new NotSupportedException("Stream is read-only.");
        }

        /// <summary>
        /// Read data from the stream into the specified buffer and increment the stream position.
        /// </summary>
        /// <param name="buffer">Byte array to use as a buffer.</param>
        /// <param name="offset">The offset within the buffer indicating where to copy the read data.</param>
        /// <param name="count">The number of bytes to populate within the buffer.</param>
        /// <returns>An integer representing the number of bytes read.</returns>
        public override int Read(byte[] buffer, int offset, int count)
        {
            if (buffer == null) throw new ArgumentNullException(nameof(buffer));
            if (offset < 0) throw new ArgumentOutOfRangeException("Offset must be zero or greater.");
            if (count < 0) throw new ArgumentOutOfRangeException("Count must be zero or greater.");
            if ((offset + count) > buffer.Length) throw new ArgumentOutOfRangeException("Offset and count combined must not exceed buffer length.");
            if (offset >= Length) return 0;

            DedupeObjectMap map = _Database.GetObjectMapForPosition(_Metadata.Key, _Position);
            if (map == null) return 0;

            if (map.ChunkAddress > Position) throw new IOException("Data error while reading chunks from object.");

            byte[] chunkData = _Callbacks.ReadChunk(map.ChunkKey); 
            
            int chunkDataReadStart = 0;            
            if (map.ChunkAddress < Position) chunkDataReadStart += (int)(Position - map.ChunkAddress);

            int bytesAvailInChunk = (int)(map.ChunkLength - chunkDataReadStart);

            if (count >= bytesAvailInChunk)
            {
                Buffer.BlockCopy(chunkData, chunkDataReadStart, buffer, offset, bytesAvailInChunk);
                _Position += bytesAvailInChunk;
                return bytesAvailInChunk;
            }
            else
            {
                Buffer.BlockCopy(chunkData, chunkDataReadStart, buffer, offset, count);
                _Position += count;
                return count;
            }
        }

        /// <summary>
        /// Seek to the specified position within the stream.
        /// </summary>
        /// <param name="offset">Offset.</param>
        /// <param name="origin">SeekOrigin.</param>
        /// <returns>New position.</returns>
        public override long Seek(long offset, SeekOrigin origin)
        {
            if (offset >= Length) throw new ArgumentOutOfRangeException("Cannot seek past end of stream.");

            long newPosition = 0;

            switch (origin)
            {
                case SeekOrigin.Begin:
                    newPosition = 0;
                    newPosition += offset;
                    break;
                case SeekOrigin.Current:
                    newPosition = _Position;
                    newPosition += offset;
                    break;
                case SeekOrigin.End:
                    newPosition = Length;
                    newPosition += offset;
                    break;
                default:
                    throw new ArgumentException("Invalid SeekOrigin supplied.");
            }

            if (newPosition < 0) throw new ArgumentOutOfRangeException("Cannot seek to before the beginning of the stream.");
            if (newPosition > Length) throw new ArgumentOutOfRangeException("Cannot seek past the end of the stream.");

            _Position = newPosition;
            return _Position;
        }

        /// <summary>
        /// Not supported.  This method will throw a NotSupportedException.
        /// Set the length of the stream.
        /// </summary>
        /// <param name="value">Length.</param>
        public override void SetLength(long value)
        {
            throw new NotSupportedException("Stream is read-only.");
        }

        /// <summary>
        /// Not supported.  This method will throw a NotSupportedException.
        /// </summary>
        /// <param name="buffer">Byte array.</param>
        /// <param name="offset">Offset.</param>
        /// <param name="count">Number of bytes.</param>
        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException("Stream is read-only.");
        }
    }
}
