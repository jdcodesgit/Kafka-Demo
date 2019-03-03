using Confluent.Kafka;
using System.Text;

namespace Kafka_Demo_Consumer
{
    public class KafkaConsumer
    {
        private ConsumerConfig Config { get; }

        public Consumer<Ignore, string> Consumer { get; set; }

        public KafkaConsumer(string groupId, string bootstrapServers)
        {
            Config = new ConsumerConfig
            {
                GroupId = groupId,
                BootstrapServers = bootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Latest
            };
        }

        public void ConstructConsumer(string topicName)
        {
            Consumer = new ConsumerBuilder<Ignore, string>(Config)
                        .Build();
            Consumer.Subscribe(topicName);
        }
    }
}
