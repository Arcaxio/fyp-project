"use client"

export function OnCommandComponent({
    commandOnAction, 
} : {
    commandOnAction: (formData: FormData) => void
}) {

    return (
        <div>
            <form action={commandOnAction}>
                <button className="rounded-full bg-sky-500 px-4 py-2 mx-2">Turn On</button>
            </form>
        </div>
    )
}

export function OffCommandComponent({
    commandOffAction, 
} : {
    commandOffAction: (formData: FormData) => void
}) {

    return (
        <div>
            <form action={commandOffAction}>
                <button className="rounded-full bg-sky-500 px-4 py-2 mx-2">Turn Off</button>
            </form>
        </div>
    )
}