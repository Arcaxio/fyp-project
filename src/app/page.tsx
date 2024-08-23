interface Message {
  offset: number;
  value: Buffer; // Assuming message.value is a Buffer
}

const { Kafka, Partitioners } = require('kafkajs')
const fs = require('fs')

const tenantId = '6ecfd23a-e229-4ce1-bf6d-5ebb92e5b8d8';
const deviceId = '24a00c96-7650-48d8-b8e9-8334e427b757';
const commandTopic = `hono.command.${tenantId}`;
const eventTopic = `hono.event.${tenantId}`;

// Init Kafka
const kafka = new Kafka({
  clientId: 'hono-client',
  brokers: ['146.190.203.23:9094'],
  ssl: {
    rejectUnauthorized: false,
    ca: [fs.readFileSync('src/app/truststore.pem', 'utf-8')],
  },
  sasl: {
    mechanism: 'plain', // scram-sha-256 or scram-sha-512
    username: 'hono',
    password: 'hono-secret'
  },
})

// Define producer and consumer
const producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner })
const consumer = kafka.consumer({ groupId: 'test-group' })

// Function to handle the message logic
const handleMessage = async (topic: string, messageValue: string) => {
  if (topic === eventTopic) {
    const parsedValue = JSON.parse(messageValue);

    if (parsedValue.lights === 'OFF') {
      await producer.send({
        topic: commandTopic,
        messages: [
          { 
            key: deviceId,
            value: 'OFF',
            headers: {
              device_id: deviceId,
              subject: 'setLight',
            },
          }
        ],
      });
    } else if (parsedValue.lights === 'ON') {
      await producer.send({
        topic: commandTopic,
        messages: [
          { 
            key: deviceId,
            value: 'ON',
            headers: {
              device_id: deviceId,
              subject: 'setLight',
            },
          }
        ],
      });
    }
  }
}

// Runs
const run = async () => {
  // Producing an initial message
  await producer.connect()
  await producer.send({
    topic: commandTopic,
    messages: [
      { 
        key: deviceId,
        value: 'OFF',
        headers: {
          device_id: deviceId,
          subject: 'setLight',
        },
      }
    ],
  })

  // Consuming messages
  await consumer.connect()
  await consumer.subscribe({ topics: [commandTopic, eventTopic] })

  await consumer.run({
    eachMessage: async ({ topic, partition, message } : { topic: string, partition: string, message: Message }) => {
      const messageValue = message.value.toString();
      console.log({
        topic,
        partition,
        offset: message.offset,
        value: messageValue,
      });

      await handleMessage(topic, messageValue);
    },
  })
}

run().catch(console.error)

export default function Home() {
  return (
    <main className="flex min-h-screen flex-col items-center justify-between p-24">
      <div className="w-full max-w-5xl text-2xl lg:flex justify-center">
        <p className="">TEST</p>
      </div>
    </main>
  );
}
