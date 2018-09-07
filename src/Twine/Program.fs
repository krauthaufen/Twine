open System
open Twine
open System.Threading
open System.IO
open System.Diagnostics

[<EntryPoint;STAThread>]
let main argv = 
    

    let sem = new SemaphoreSlim(0)

    let sw = Stopwatch()
    let test =
        cont {
            printfn "waiting: %d" Thread.CurrentThread.ManagedThreadId
            let! r = Cont.AwaitWaitHandle sem.AvailableWaitHandle
            sw.Stop()
            printfn "done: %d" Thread.CurrentThread.ManagedThreadId
            printfn "took: %.3fs" sw.Elapsed.TotalSeconds
            ()
        }

    let t = Cont.StartAsTwine test
    
    Thread.Sleep 100
    printfn "signal: %d" Thread.CurrentThread.ManagedThreadId
    sw.Start()
    sem.Release() |> ignore

    t.Wait()

    for i in 1 .. 100 do
        let sem = new Semaphore(0, 1)

        let sw = Stopwatch()
        let test =
            cont {
                //sem.WaitOne() |> ignore
                let! r = Cont.AwaitWaitHandle sem
                sw.Stop()
                printfn "took: %.3fus" (1000.0 * sw.Elapsed.TotalMilliseconds)
                ()
            }

        let t = Cont.StartAsTwine test
    
        sw.Start()
        sem.Release() |> ignore

        t.Wait()

  
    0
