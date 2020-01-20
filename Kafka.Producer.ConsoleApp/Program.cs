using Confluent.Kafka;
using System;
using System.Threading.Tasks;

namespace Kafka.Producer.ConsoleApp
{
    class Program
    {
        static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }

        public static async Task MainAsync(string[] args)
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
            string command = "";

            while (command.ToLower() != "exit")
            {
                Console.Write("Mensagem: ");

                command = Console.ReadLine();

                using (var producer = new ProducerBuilder<Null, string>(config).Build())
                {
                    try
                    {
                        var dr = await producer.ProduceAsync("chat-topic", new Message<Null, string> { Value = command });

                        Console.WriteLine($"Mensagem '{dr.Value}' entregue para '{dr.TopicPartitionOffset}'");
                    }
                    catch (ProduceException<Null, string> exception)
                    {
                        Console.WriteLine($"Falha na entrega! {exception.Error.Reason}");
                    }
                }
            }
        }
    }
}
