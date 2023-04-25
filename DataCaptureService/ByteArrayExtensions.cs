namespace DataCaptureService
{
    public static class ByteArrayExtensions
    {
        public static ArraySegment<byte>[] SplitIntoChunks(this byte[] data, int chunkSize)
        {
            int numChunks = (int)Math.Ceiling((double)data.Length / chunkSize);
            ArraySegment<byte>[] chunks = new ArraySegment<byte>[numChunks];

            for (int i = 0; i < numChunks; i++)
            {
                int offset = i * chunkSize;
                int size = Math.Min(chunkSize, data.Length - offset);
                chunks[i] = new ArraySegment<byte>(data, offset, size);
            }

            return chunks;
        }
    }
}
