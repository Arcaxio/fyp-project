"use client"

import { sendCommandOn, sendCommandOff } from "./serverFunctions"

export default function LogComponent({props}: any) {
    console.log(props)
    const sendCommandOnAction = sendCommandOn.bind(props.producer, props.commandTopic, props.deviceId)
    return (
        <div>
            <form action={sendCommandOnAction}>
            <button className="rounded-full bg-sky-500 px-4 py-2" onClick={props}>send</button>
            </form>
            <form action={sendCommandOff}>
            <button className="rounded-full bg-sky-500 px-4 py-2" onClick={props}>send</button>
            </form>
        </div>
    )
}