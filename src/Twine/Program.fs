open System
open Twine
open System.Threading

[<EntryPoint;STAThread>]
let main argv = 

    use pool = new TwineThreadPool(4)

    let twine = TwinyBuilder()

    let a = 
        pool.Start(fun () ->
            Thread.Sleep(100)
            10
        )


    let test =
        twine {
            let! a = a
            return 10 * a
        }

    let b = pool.Start test
        //a.ContinueWith(fun tw ->
        //    let a = tw.Result
        //    2 * a
        //)

    printfn "%A" b.Result
  
    0
