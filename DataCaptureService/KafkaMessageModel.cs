namespace DataCaptureService
{
    public class KafkaMessageModel
    {
        public Guid Sequence { get; set; }
        public int Position { get; set; }
        public bool IsLastMessage { get; set; }
        public byte[]? Message { get; set; }
    }
}
