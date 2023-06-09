﻿namespace DataCaptureService
{
    public class KafkaMessageModel
    {
        public Guid Sequence { get; set; }
        public int Position { get; set; }
        public int ChunksCount { get; set; }
        public bool IsLastMessage { get; set; }
        public byte[]? ImageData { get; set; }
    }
}
