namespace DataCaptureService
{
    public static class ArraySegmentExtensions
    {
        public static KafkaMessageModel[] ConvertToMessageChunks(this ArraySegment<byte>[] chunks)
        {
            KafkaMessageModel[] kafkaMessageModels = new KafkaMessageModel[chunks.Length];
            var sequence = Guid.NewGuid();
            var chunksCount = chunks.Length;
            for (int i = 0; i < chunks.Length; i++)
            {
                kafkaMessageModels[i] = new KafkaMessageModel
                {
                    Sequence = sequence,
                    Position = i + 1,
                    IsLastMessage = i == chunksCount - 1,
                    ImageData = chunks[i].ToArray(),
                    ChunksCount = chunksCount
                };
            }
            return kafkaMessageModels;
        }
    }
}
