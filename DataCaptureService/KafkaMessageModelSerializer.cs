using Confluent.Kafka;
using Newtonsoft.Json;
using System.Text;

namespace DataCaptureService
{
    public class KafkaMessageModelSerializer : ISerializer<KafkaMessageModel>
    {
        public byte[] Serialize(KafkaMessageModel data, SerializationContext context)
        {
            var json = JsonConvert.SerializeObject(data);
            return Encoding.UTF8.GetBytes(json);
        }
    }
}
