namespace DataCaptureService
{
    public static class ArraySegmentExtensions
    {
        public static KafkaMessageModel[] ConvertToMessageChunks(this ArraySegment<byte>[] chunks)
        {
            KafkaMessageModel[] kafkaMessageModels = new KafkaMessageModel[chunks.Length];
            var sequence = Guid.NewGuid();
            for (int i = 0; i < chunks.Length; i++)
            {
                kafkaMessageModels[i] = new KafkaMessageModel
                {
                    Sequence = sequence,
                    Position = i,
                    IsLastMessage = i == chunks.Length - 1,
                    Message = chunks[i].ToArray()
                };
            }
            return kafkaMessageModels;
        }
    }
}
