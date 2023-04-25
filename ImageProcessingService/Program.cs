using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

namespace ImageProcessingService
{
    class Program
    {
        private static IConfiguration Configuration;

        static void Main(string[] args)
        {
            SetupConfiguration();
            TestKafkaConsumer();
        }

        private static void TestKafkaConsumer()
        {
            var topic = Configuration.GetValue<string>("topicName");

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; 
                cts.Cancel();
            };
            IEnumerable<KeyValuePair<string, string>> kvps = Configuration.GetSection("kafka").GetChildren().ToDictionary(x => x.Key, x => x.Value);

            using (var consumer = new ConsumerBuilder<string, string>(kvps).Build())
            {
                consumer.Subscribe(topic);
                try
                {
                    while (true)
                    {
                        var cr = consumer.Consume(cts.Token);
                        Console.WriteLine($"Consumed event from topic {topic} with key {cr.Message.Key, -10} and value {cr.Message.Value}");
                    }
                }
                catch(OperationCanceledException)
                {

                }
                finally
                {
                    consumer.Close();
                }
            }
        }

        private static void SetupConfiguration()
        {
            Configuration = new ConfigurationBuilder()
                    .AddJsonFile("appsettings.json")
                    .Build();
        }
    }
}