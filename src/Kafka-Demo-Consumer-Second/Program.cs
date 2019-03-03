using Confluent.Kafka;
using Kafka_Demo_Consumer;
using System;
using System.Threading;
using System.Threading.Tasks;
using System.Drawing;

namespace Kafka_Demo_Consumer_Minute
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Starting Second Consumer...");

            ConsumerTask();

            Console.ReadKey();
        }

        public static void ConsumerTask()
        {
            var secondConsumer = new KafkaConsumer("second-consumer", "localhost:9092");
            secondConsumer.ConstructConsumer("second");
            
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            try
            {
                while (true)
                {
                    try
                    {
                        var secondMessage = secondConsumer.Consumer.Consume(cts.Token);
                        
                        Console.Clear();
                        Colorful.Console.WriteAscii(secondMessage.Value, Color.Yellow);
                    }
                    catch (ConsumeException ex)
                    {
                        Console.WriteLine($"Error ocurrect: {ex.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                secondConsumer.Consumer.Close();
            }
        }

    }
}
