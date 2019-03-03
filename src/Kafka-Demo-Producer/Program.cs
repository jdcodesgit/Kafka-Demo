using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Kafka_Demo
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Starting Producer...");

            ProducerTask().GetAwaiter().GetResult();

            Console.ReadKey();
        }

        public async static Task ProducerTask()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };

            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                var hourMessage = string.Empty;
                var minuteMessage = string.Empty;
                               
                while (true)
                {
                    if(string.IsNullOrEmpty(hourMessage) || hourMessage != DateTime.UtcNow.Hour.ToString())
                    {
                        await WriteMessage(producer, "hour", DateTime.UtcNow.Hour.ToString());
                        hourMessage = DateTime.UtcNow.Hour.ToString();
                    }

                    if (string.IsNullOrEmpty(minuteMessage) || minuteMessage != DateTime.UtcNow.Minute.ToString())
                    {
                        await WriteMessage(producer, "minute", DateTime.UtcNow.Minute.ToString());
                        minuteMessage = DateTime.UtcNow.Minute.ToString();
                    }

                    await WriteMessage(producer, "second", DateTime.UtcNow.Second.ToString());
                    Thread.Sleep(1000);
                }
            }
        }

        public static async Task WriteMessage(Producer<Null, string> producer, string topic, string message)
        {
            try
            {
                var msg = await producer.ProduceAsync(topic, new Message<Null, string> { Value = message });
                Console.WriteLine($"Delivered '{msg.Value}' to '{msg.TopicPartitionOffset}'");
            }
            catch (ProduceException<Null, string> ex)
            {
                Console.WriteLine($"Delivery failed: {ex.Error.Reason}");
            }
        }
    }
}
