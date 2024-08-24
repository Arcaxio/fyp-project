import { OnCommandComponent, OffCommandComponent } from "./ButtonComponent";

interface Message {
  offset: number;
  value: Buffer; // Assuming message.value is a Buffer
}

const { Kafka, Partitioners } = require('kafkajs')
const fs = require('fs')

const tenantId = '6ecfd23a-e229-4ce1-bf6d-5ebb92e5b8d8';
const deviceId = '24a00c96-7650-48d8-b8e9-8334e427b757';
const device2Id = '05642c84-06e0-4e69-84bc-a6439797a3ed';
const commandTopic = `hono.command.${tenantId}`;
const eventTopic = `hono.event.${tenantId}`;
const logMessage = {};

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

// Runs
const run = async () => {
  // Producing an initial message
  await producer.connect()
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
  })
  // await producer.send({
  //   topic: eventTopic,
  //   messages: [
  //     { 
  //       key: device2Id,
  //       value: '{"lights": "ON"}',
  //       // headers: {
  //       //   device_id: deviceId,
  //       //   subject: 'setLight',
  //       // },
  //     }
  //   ],
  // })
  
  // Consuming messages
  await consumer.connect()
  await consumer.subscribe({ topics: [commandTopic, eventTopic] })
  
  await consumer.run({
    eachMessage: async ({ topic, partition, message } : { topic: string, partition: string, message: Message }) => {
      const messageValue = message.value ? message.value.toString() : null;
      const temp = {topic, partition, value: messageValue}
      Object.assign(temp, logMessage)
      
      console.log({
        topic,
        partition,
        offset: message.offset,
        value: messageValue,
      });
  
      if (messageValue) {
        await handleMessage(topic, messageValue);
      } else {
        console.warn(`Received a message with null value at offset ${message.offset}`);
      }
    },
  })
}

run().catch(console.error)

// Function to handle the message logic
const handleMessage = async (topic: string, messageValue: string) => {
  if (topic === eventTopic) {
    let parsedValue;

    try {
      parsedValue = JSON.parse(messageValue);
    } catch (error) {
      console.error(`Failed to parse JSON:`);
      return; // Exit the function if parsing fails
    }

    if (parsedValue && parsedValue.lights === 'OFF') {
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
    } else if (parsedValue && parsedValue.lights === 'ON') {
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

async function buttonOnCommand() {
  'use server'
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
  })
}

async function buttonOffCommand() {
  'use server'
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
}

export default function Home() {
  return (
    <main className="flex min-h-screen flex-col items-center justify-between p-12">
      <div className="w-full max-w-5xl text-2xl lg:flex justify-center">
        <OnCommandComponent commandOnAction={buttonOnCommand} />
        <OffCommandComponent commandOffAction={buttonOffCommand} />
      </div>
    </main>
  );
}
