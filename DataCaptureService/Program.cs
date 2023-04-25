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
        using (var producer = new ProducerBuilder<string, byte[]>(kvps.AsEnumerable()).Build())
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

                        producer.Produce(topic, new Message<string, byte[]> { Key = image.Name, Value = imageData },
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