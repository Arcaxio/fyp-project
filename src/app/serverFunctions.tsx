'use server'

export async function sendCommandOn(producer:any, commandTopic: any, deviceId: any) {
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

export async function sendCommandOff() {
   
}