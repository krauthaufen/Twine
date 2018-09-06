namespace Twine

open System.Threading

[<AutoOpen>]
module internal Helpers =

    let startThread (name : string) (action : unit -> unit) =
        let thread = Thread(ThreadStart(action), IsBackground = true, Name = name)
        thread.Start()
        thread

