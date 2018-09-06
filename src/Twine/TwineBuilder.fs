namespace Twine

open System

type twine<'a> = { runTwine : TwineThreadPool * ('a -> unit) * (OperationCanceledException -> unit) * (exn -> unit) -> unit }

[<AutoOpen>]
module Extensions = 
    type TwineThreadPool with
        member x.Start(t : twine<'a>) =
            x.FromContinuations(fun (a,b,c) -> t.runTwine(x,a,b,c))

module Twine =
    
    let ofTask (pool : TwineThreadPool) (t : System.Threading.Tasks.Task<'a>) =
        let twine = Twine<'a>(fun () -> t.Result)
        let cont (t : System.Threading.Tasks.Task<'a>) = pool.Post twine
        t.ContinueWith(cont,System.Threading.Tasks.TaskContinuationOptions.ExecuteSynchronously) |> ignore
        twine


type TwinyBuilder() =
    member x.Return (v : 'a) =
        { runTwine = fun (_,ok,_,_) -> ok v }

    member x.Bind(t : System.Threading.Tasks.Task<'a>, f : 'a -> twine<'b>) =
        let taskTwine = 
            { runTwine = fun (pool, ok, cancel, err) ->
                let t = Twine.ofTask pool t
                t.ContinueWith (fun t ->
                    try ok t.Result
                    with
                    | :? OperationCanceledException as e -> cancel e
                    | e -> err e
                ) |> ignore
            }
        x.Bind(taskTwine, f)


    member x.Bind(m : Twine<'a>, f : 'a -> twine<'b>) =
        let mtwine = 
            { runTwine = fun (pool,ok,cancel,err) ->
                m.ContinueWith (fun m ->
                    try ok m.Result
                    with
                    | :? OperationCanceledException as e -> cancel e
                    | e -> err e
                ) |> ignore
            }
        x.Bind(mtwine, f)

    member x.Bind(m : twine<'a>, f : 'a -> twine<'b>) =
        { runTwine = fun (pool, ok, cancel, err) ->
            let oka (a : 'a) = 
                try 
                    let b = f a
                    b.runTwine (pool, ok, cancel, err)
                with
                | :? OperationCanceledException as e -> cancel e
                | e -> err e

            m.runTwine (pool, oka, cancel, err)
        }

    member x.Zero() = x.Return ()

    member x.While(guard : unit -> bool, body : twine<unit>) =
        let mutable self = Unchecked.defaultof<twine<unit>>
        self <- 
            { runTwine = fun a ->
                if guard() then
                    x.Bind(body, fun () -> self).runTwine a
                else
                    x.Return().runTwine a
            }
        self

    member x.Delay (f : unit -> twine<'a>) =
        { runTwine = fun c ->
            f().runTwine c
        }

    member x.Combine(l : twine<unit>, r : twine<'a>) =
        x.Bind(l, fun () -> r)

type TwineBuilder(pool : TwineThreadPool) =
    
    member x.Return(v : 'a) = pool.FromResult v


    member x.Bind(m : Twine<'a>, f : 'a -> Twine<'b>) =
        pool.FromContinuations(fun (ok, cancel, error) ->
            m.ContinueWith(fun t ->
                try
                    let r = f t.Result
                    r.ContinueWith(fun b ->
                        try
                            ok b.Result
                        with 
                            | :? OperationCanceledException as e -> cancel e
                            | e -> error e
                    ) |> ignore
                with 
                    | :? OperationCanceledException as e -> cancel e
                    | e -> error e
            ) |> ignore
        )

    member x.While(guard : unit -> bool, body : unit -> Twine<unit>) =
        if guard() then
            x.Bind(body(), fun () -> if guard() then x.While(guard, body) else x.Return ())
        else
            x.Return()
            
     member x.Delay(f : unit -> Twine<'a>) = f
     member x.Run(f : unit -> Twine<'a>) = f()

     member x.For(seq : seq<'a>, action : 'a -> Twine<unit>) =
        let e = seq.GetEnumerator()
        let guard() = 
            if e.MoveNext() then true
            else
                e.Dispose()
                false
        x.While(guard, fun () -> action e.Current)


    member x.Zero() = pool.FromResult ()
    member x.Combine(l : Twine<unit>, r : unit -> Twine<'a>) =
        x.Bind(l, r)

