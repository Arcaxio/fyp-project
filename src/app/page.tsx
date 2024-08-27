import { OnCommandComponent, OffCommandComponent } from "./ButtonComponent";

interface Message {
  offset: number;
  value: Buffer; // Assuming message.value is a Buffer
}

const { Kafka, Partitioners } = require('kafkajs')
const fs = require('fs')

const tenantIdMQTT = 'MQTT_ENV';
const tenantIdHTTP = 'HTTP_ENV';

const deviceId_MQTT1 = 'MQTTDev1';
const deviceId_MQTT3 = 'MQTTDev3';
const deviceId_MQTT5 = 'MQTTDev5';

const commandTopicMQTT = `hono.command.${tenantIdMQTT}`;
const eventTopicMQTT = `hono.event.${tenantIdMQTT}`;
const telemetryTopicMQTT = `hono.telemetry.${tenantIdMQTT}`;

const commandTopicHTTP = `hono.command.${tenantIdHTTP}`;
const eventTopicHTTP = `hono.event.${tenantIdHTTP}`;

// Init Kafka
const kafka = new Kafka({
  clientId: 'hono-server',
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
const consumer = kafka.consumer({ groupId: 'test-group-alpha' })

// Runs
const run = async () => {
  // Producing an initial message
  await producer.connect()
  await producer.send({
    topic: commandTopicMQTT,
    messages: [
      { 
        key: deviceId_MQTT5,
        value: 'ON',
        headers: {
          device_id: deviceId_MQTT5,
          subject: 'setLight',
        },
      }
    ],
  })
  
  // Consuming messages
  await consumer.connect()
  await consumer.subscribe({ topics: [commandTopicMQTT, eventTopicMQTT, telemetryTopicMQTT, commandTopicHTTP, eventTopicHTTP], fromBeginning: true })
  
  await consumer.run({
    eachMessage: async ({ topic, partition, message } : { topic: string, partition: string, message: Message }) => {
      const messageValue = message.value ? message.value.toString() : null;
      
      console.log({
        topic,
        partition,
        offset: message.offset,
        value: messageValue,
      });
  
      if (messageValue) {
        // await HTTP1_to_MQTT5(topic, messageValue);
        await MQTT2_to_MQTT13(topic, messageValue);
        await HTTP1_to_MQTT_All(topic, messageValue);
      } else {
        console.warn(`Received a message with null value at offset ${message.offset}`);
      }
    },
  })
}

run().catch(console.error)

// Function to handle the message logic
const HTTP1_to_MQTT5 = async (topic: string, messageValue: string) => {
  if (topic === eventTopicHTTP) {
    let parsedValue;

    try {
      parsedValue = JSON.parse(messageValue);
    } catch (error) {
      console.error(`Failed to parse JSON 1:`);
      return; // Exit the function if parsing fails
    }

    if (parsedValue && parsedValue.lights === 'OFF') {
      await producer.send({
        topic: commandTopicMQTT,
        messages: [
          { 
            key: deviceId_MQTT5,
            value: 'OFF',
            headers: {
              device_id: deviceId_MQTT5,
              subject: 'setLight',
            },
          }
        ],
      });
    } else if (parsedValue && parsedValue.lights === 'ON') {
      await producer.send({
        topic: commandTopicMQTT,
        messages: [
          { 
            key: deviceId_MQTT5,
            value: 'ON',
            headers: {
              device_id: deviceId_MQTT5,
              subject: 'setLight',
            },
          }
        ],
      });
    }
  }
}

const MQTT2_to_MQTT13 = async (topic: string, messageValue: string) => {
  if (topic === telemetryTopicMQTT || eventTopicMQTT) {
    let parsedValue;

    try {
      parsedValue = JSON.parse(messageValue);
    } catch (error) {
      console.error(`Failed to parse JSON 2:`);
      return; // Exit the function if parsing fails
    }

    if (parsedValue && parsedValue.lights === 'OFF') {
      await producer.send({
        topic: commandTopicMQTT,
        messages: [
          { 
            key: deviceId_MQTT1,
            value: 'OFF',
            headers: {
              device_id: deviceId_MQTT1,
              subject: 'setLight',
            },
          }
        ],
      });
      await producer.send({
        topic: 'hono.command.MQTT_ENV',
        messages: [
          { 
            key: deviceId_MQTT3,
            value: 'OFF',
            headers: {
              device_id: deviceId_MQTT3,
              subject: 'setLight',
            },
          }
        ],
      });
    } else if (parsedValue && parsedValue.lights === 'ON') {
      await producer.send({
        topic: 'hono.command.MQTT_ENV',
        messages: [
          { 
            key: deviceId_MQTT1,
            value: 'ON',
            headers: {
              device_id: deviceId_MQTT1,
              subject: 'setLight',
            },
          }
        ],
      });
      await producer.send({
        topic: 'hono.command.MQTT_ENV',
        messages: [
          { 
            key: deviceId_MQTT3,
            value: 'ON',
            headers: {
              device_id: deviceId_MQTT3,
              subject: 'setLight',
            },
          }
        ],
      });
    }
  }
}

const HTTP1_to_MQTT_All = async (topic: string, messageValue: string) => {
  if (topic === telemetryTopicMQTT || eventTopicMQTT) {
    let parsedValue;

    try {
      parsedValue = JSON.parse(messageValue);
    } catch (error) {
      console.error(`Failed to parse JSON 2:`);
      return; // Exit the function if parsing fails
    }

    if (parsedValue && parsedValue.lights === 'OFF') {
      await producer.send({
        topic: commandTopicMQTT,
        messages: [
          { 
            key: deviceId_MQTT1,
            value: 'OFF',
            headers: {
              device_id: deviceId_MQTT1,
              subject: 'setLight',
            },
          }
        ],
      });
      await producer.send({
        topic: 'hono.command.MQTT_ENV',
        messages: [
          { 
            key: deviceId_MQTT3,
            value: 'OFF',
            headers: {
              device_id: deviceId_MQTT3,
              subject: 'setLight',
            },
          }
        ],
      });
      await producer.send({
        topic: 'hono.command.MQTT_ENV',
        messages: [
          { 
            key: deviceId_MQTT5,
            value: 'OFF',
            headers: {
              device_id: deviceId_MQTT5,
              subject: 'setLight',
            },
          }
        ],
      });
    } else if (parsedValue && parsedValue.lights === 'ON') {
      await producer.send({
        topic: 'hono.command.MQTT_ENV',
        messages: [
          { 
            key: deviceId_MQTT1,
            value: 'ON',
            headers: {
              device_id: deviceId_MQTT1,
              subject: 'setLight',
            },
          }
        ],
      });
      await producer.send({
        topic: 'hono.command.MQTT_ENV',
        messages: [
          { 
            key: deviceId_MQTT3,
            value: 'ON',
            headers: {
              device_id: deviceId_MQTT3,
              subject: 'setLight',
            },
          }
        ],
      });
      await producer.send({
        topic: 'hono.command.MQTT_ENV',
        messages: [
          { 
            key: deviceId_MQTT5,
            value: 'ON',
            headers: {
              device_id: deviceId_MQTT5,
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
    topic: commandTopicMQTT,
    messages: [
      { 
        key: deviceId_MQTT5,
        value: 'ON',
        headers: {
          device_id: deviceId_MQTT5,
          subject: 'setLight',
        },
      }
    ],
  })
}

async function buttonOffCommand() {
  'use server'
  await producer.send({
    topic: commandTopicMQTT,
    messages: [
      { 
        key: deviceId_MQTT5,
        value: 'OFF',
        headers: {
          device_id: deviceId_MQTT5,
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
