using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

namespace DataCaptureService;

class Program
{
    private static IConfiguration Configuration;
    private static Dictionary<string, bool> ProcessedImages = new Dictionary<string, bool>();
    private static int Iteration = 0;
    static async Task Main(string[] args)
    {
        SetupConfiguration();
        await ProcessAndSendImages();
    }

    private static FileInfo[] GetAllImagesInfo(string path)
    {
        DirectoryInfo directoryInfo = new DirectoryInfo(path);
        FileInfo[] jpgImages = directoryInfo.GetFiles("*.jpg");
        Iteration += 1;

        Console.WriteLine("Inside " + Iteration + " iteration.");

        return jpgImages;
    }

    private static async Task ProcessAndSendImages()
    {
        string topic = Configuration.GetValue<string>("topicName");
        IEnumerable<KeyValuePair<string, string>> kvps = Configuration.GetSection("kafka").GetChildren().ToDictionary(x => x.Key, x => x.Value);

        //var producer = new ProducerBuilder<string, KafkaMessageModel>(kvps.AsEnumerable());
        //producer1.SetValueSerializer(new KafkaMessageModelSerializer());

        using (var producer = new ProducerBuilder<string, KafkaMessageModel>(kvps.AsEnumerable()).SetValueSerializer(new KafkaMessageModelSerializer()).Build())
        {
            var numberOfMessages = 0;
            while (true)
            {
                var path = Configuration.GetValue<string>("pathToCapture");
                var allJpgImages = GetAllImagesInfo(path);
                foreach (var image in allJpgImages)
                {
                    if (!ProcessedImages.ContainsKey(image.Name))
                    {
                        ProcessedImages[image.Name] = true;
                        byte[] imageData = File.ReadAllBytes(image.FullName);

                        ArraySegment<byte>[] chunks = imageData.SplitIntoChunks(307000);
                        KafkaMessageModel[] messages = chunks.ConvertToMessageChunks();

                        producer.InitTransactions(new TimeSpan(0,0,5));
                        try
                        {
                            producer.BeginTransaction();
                            for (int i = 0; i < messages.Length; i++)
                            {
                                producer.Produce(topic, new Message<string, KafkaMessageModel> { Key = image.Name, Value = messages[i] },
                                    (deliveryReport) =>
                                    {
                                        if (deliveryReport.Error.Code != ErrorCode.NoError)
                                        {
                                            Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                                        }
                                        else
                                        {
                                            Console.WriteLine($"Produced event to topic {topic}: key = {image.Name} and byte array length is {imageData.Length}.");
                                        }
                                    });
                            }
                            producer.CommitTransaction();
                            Console.WriteLine("Transaction got commited."); 
                        }
                        catch(ProduceException<string, string> e)
                        {
                            producer.AbortTransaction();
                            Console.WriteLine($"Error sending message: {e.Message}");
                        }

                        producer.Flush();

                    }
                }
                await Task.Delay(2000);
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