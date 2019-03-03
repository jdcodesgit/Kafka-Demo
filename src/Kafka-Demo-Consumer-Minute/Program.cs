using Confluent.Kafka;
using Kafka_Demo_Consumer;
using System;
using System.Threading;
using System.Threading.Tasks;
using System.Drawing;

namespace Kafka_Demo_Consumer_Second
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Starting Minute Consumer...");

            ConsumerTask();

            Console.ReadKey();
        }

        public static void ConsumerTask()
        {
            var minuteConsumer = new KafkaConsumer("minute-consumer", "localhost:9092");
            minuteConsumer.ConstructConsumer("minute");

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
                        var minuteMessage = minuteConsumer.Consumer.Consume(cts.Token);

                        Console.Clear();
                        Colorful.Console.WriteAscii(minuteMessage.Value, Color.Red);
                    }
                    catch (ConsumeException ex)
                    {
                        Console.WriteLine($"Error ocurrect: {ex.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                minuteConsumer.Consumer.Close();
            }
        }

    }
}
