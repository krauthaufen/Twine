namespace Twine

open System
open System.Threading

type IContContext =
    abstract member CancellationToken : CancellationToken
    abstract member Post<'a> : cont<'a> -> unit
    abstract member Start : (unit -> 'a) -> unit

and cont<'a> = 
    { runCont : IContContext * ('a -> unit) * (OperationCanceledException -> unit) * (exn -> unit) -> unit }
        member inline x.run (ctx : IContContext, ok, cancel, error) =
            let ok (value : 'a) =
                if ctx.CancellationToken.IsCancellationRequested then
                    cancel (OperationCanceledException())
                else
                    ok value
            x.runCont(ctx, ok, cancel, error)


module private Context =
    type private TwineContext(p : TwineThreadPool) =
        interface IContContext with
            member x.Post<'a>(c : cont<'a>) =
                p.FromContinuations<'a> (fun (ok,cancel,error) -> c.runCont(x :> IContContext, ok, cancel, error)) |> ignore
            
            member x.Start(action : unit -> 'a) =
                p.Start(action) |> ignore
    

            member x.CancellationToken = Unchecked.defaultof<CancellationToken>

    type private TPLContext() =
        interface IContContext with
            member x.Post<'a>(c : cont<'a>) =
                Async.FromContinuations(fun (ok,error,cancel) -> c.runCont(x :> IContContext,ok,cancel, error)) 
                    |> Async.StartAsTask |> ignore

            member x.Start(action : unit -> 'a) =
                System.Threading.Tasks.Task.Run(action) |> ignore
    
            member x.CancellationToken = Unchecked.defaultof<CancellationToken>

    let tpl = TPLContext() :> IContContext
    let twineDefault = TwineContext(TwineThreadPool.Default) :> IContContext


[<AbstractClass>]
type Cont private() =

    static let once (ok, cancel, error) =
        let mutable cnt = 0
        let ok v = if Interlocked.Exchange(&cnt, 1) = 0 then ok v
        let cancel v = if Interlocked.Exchange(&cnt, 1) = 0 then cancel v
        let error v = if Interlocked.Exchange(&cnt, 1) = 0 then error v
            
        ok, cancel, error

    static member FromContinuations (action : IContContext * ('a -> unit) * (OperationCanceledException -> unit)*(exn -> unit) -> unit) =
        { runCont = fun (pool,ok,cancel,error) ->
            let mutable cnt = 0
            let ok v = if Interlocked.Exchange(&cnt, 1) = 0 then ok v
            let cancel v = if Interlocked.Exchange(&cnt, 1) = 0 then cancel v
            let error v = if Interlocked.Exchange(&cnt, 1) = 0 then error v
            
            action(pool,ok, cancel, error)
        }
        
    static member FromContinuations (action : ('a -> unit) * (OperationCanceledException -> unit)*(exn -> unit) -> unit) =
        Cont.FromContinuations(fun (_,ok,cancel,error) -> action (ok,cancel, error))
        
    static member AwaitEvent(e : IEvent<'del, 'a>, ?cancelAction : unit -> unit) =
        let cancelAction = defaultArg cancelAction id
        { runCont = fun (pool : IContContext,ok,cancel,error) ->
            
            let mutable sub = { new IDisposable with member x.Dispose() = () }
            let mutable reg = sub
            let finish() = 
                sub.Dispose()
                reg.Dispose()

            let ok v = pool.Start (fun () -> finish(); ok v)
            let cancel v = pool.Start (fun () -> finish(); cancelAction(); cancel v)
            let error v = pool.Start (fun () -> finish(); error v)
            let (ok,cancel,error) = once (ok,cancel,error)

            sub <- e.Subscribe(fun a -> ok a)
            reg <- pool.CancellationToken.Register(fun () -> cancel (OperationCanceledException()))
        }
          
    static member AwaitWaitHandle(w : WaitHandle, ?millisecondsTimeout : int) =
        let millisecondsTimeout = defaultArg millisecondsTimeout -1
        { runCont = fun (pool : IContContext,ok,cancel,error) ->
            if w.WaitOne(0) then
                pool.Start (fun () ->
                    ok true
                )
            else
                let mutable sub = { new IDisposable with member x.Dispose() = () }
                let mutable reg = Unchecked.defaultof<RegisteredWaitHandle>
                let finish() = 
                    sub.Dispose()
                    reg.Unregister(w) |> ignore

                let ok (v : bool) =
                    pool.Start (fun () ->
                        finish()
                        ok v
                    )

                let cancel e =
                    pool.Start (fun () ->
                        finish()
                        cancel e
                    )

                let error e =
                    pool.Start (fun () ->
                        finish()
                        error e
                    )

                let (ok,cancel,error) = once (ok,cancel,error)

                let cont (o : obj) (isTimeout : bool) =
                    ok (not isTimeout)

                sub <- pool.CancellationToken.Register(fun () -> cancel (OperationCanceledException()))
                reg <- ThreadPool.RegisterWaitForSingleObject(w, WaitOrTimerCallback(cont), null, millisecondsTimeout, true)
        }

    static member AwaitIAsyncResult(e : IAsyncResult, ?millisecondsTimeout : int) =
        let millisecondsTimeout = defaultArg millisecondsTimeout -1
        Cont.AwaitWaitHandle(e.AsyncWaitHandle, millisecondsTimeout)
    
    static member Catch (c : cont<'a>) =
        { runCont = fun (p,ok,cancel,error) ->
            c.run(p, Choice1Of2 >> ok, cancel, Choice2Of2 >> ok)
        }
         
    static member FromBeginEnd (beginFun : AsyncCallback * obj -> IAsyncResult, endFun : IAsyncResult -> 'a, ?cancelAction : unit -> unit) =
        Cont.FromContinuations(fun (pool : IContContext,ok,cancel,error) ->
            if pool.CancellationToken.IsCancellationRequested then
                cancel (OperationCanceledException())
            else
                let mutable sub = { new IDisposable with member x.Dispose() = () }
                let finish() = sub.Dispose()
            
                let cancel e =
                    pool.Start (fun () ->
                        finish()
                        cancel e
                    )

                let callback (r : IAsyncResult) =
                    pool.Start (fun () ->
                        finish()
                        try ok (endFun r)
                        with
                        | :? OperationCanceledException as e -> cancel e
                        | e -> error e
                    )

                let error e =
                    pool.Start (fun () ->
                        finish()
                        error e
                    )

                let (callback, cancel, error) = once (callback, cancel, error)

                try 
                    sub <- pool.CancellationToken.Register(fun () -> cancel (OperationCanceledException()))
                    beginFun(AsyncCallback callback, null) |> ignore
                with
                | :? OperationCanceledException as e -> cancel e
                | e -> error e

        )

    static member FromBeginEnd (x : 'x, beginAction : 'x * AsyncCallback * obj -> IAsyncResult, endAction : IAsyncResult -> 'a, ?cancelAction : unit -> unit) =
        Cont.FromBeginEnd((fun (iar,state) -> beginAction(x,iar,state)), endAction, ?cancelAction=cancelAction)

    static member FromBeginEnd (x : 'x, y : 'y, beginAction : 'x * 'y * AsyncCallback * obj -> IAsyncResult, endAction : IAsyncResult -> 'a, ?cancelAction : unit -> unit) =
        Cont.FromBeginEnd((fun (iar,state) -> beginAction(x,y,iar,state)), endAction, ?cancelAction=cancelAction)
        
    static member FromBeginEnd (x : 'x, y : 'y, z : 'z, beginAction : 'x * 'y * 'z * AsyncCallback * obj -> IAsyncResult, endAction : IAsyncResult -> 'a, ?cancelAction : unit -> unit) =
        Cont.FromBeginEnd((fun (iar,state) -> beginAction(x,y,z,iar,state)), endAction, ?cancelAction=cancelAction)

    static member Ignore(m : cont<'a>) =
        { runCont = fun (p,ok,cancel,error) ->
            m.run (p, ignore >> ok, cancel, error)
        }

    static member OnCancel (action : unit -> unit) =
        { runCont = fun (p : IContContext, ok, cancel, error) ->
            let reg = p.CancellationToken.Register (Action (fun () -> p.Start action))
            ok reg
        }

    static member Parallel(seq : seq<cont<'a>>) : cont<'a[]> =
        let elems = Seq.toArray seq
        Cont.FromContinuations (fun (p : IContContext,ok,cancel, error) ->
            let results = Array.zeroCreate elems.Length
            let mutable remaining = elems.Length

            let cts = new CancellationTokenSource()
            let innerPool =
                { new IContContext with
                    member x.Start a = p.Start a
                    member x.Post a = p.Post a
                    member x.CancellationToken = cts.Token
                }

            let okelem (i : int) (v : 'a) =
                results.[i] <- v
                if Interlocked.Decrement(&remaining) = 0 then 
                    ok results
                    cts.Dispose()
                
            let cancelelem (o : OperationCanceledException) =
                cts.Cancel()
                cancel o
                cts.Dispose()
                
            let errorelem (o : exn) =
                cts.Cancel()
                error o
                cts.Dispose()

            for i in 0 .. elems.Length do
                elems.[i].run(p, okelem i, cancelelem, errorelem)
        )

    static member StartAsTwine (c : cont<'a>) =
        let twine = Twine<'a>(Unchecked.defaultof<_>)
        
        let p = TwineThreadPool.Default
        p.Start (fun () ->
            let ok v = twine.SetResult(p, v)
            let cancel _ = twine.SetCanceled(p)
            let error e = twine.SetError(p, e)
            c.run(Context.twineDefault, ok, cancel, error)
        ) |> ignore

        twine

    static member Start (c : cont<'a>) =
        let p = TwineThreadPool.Default
        p.Start (fun () ->
            c.run(Context.twineDefault, ignore, ignore, ignore)
        ) |> ignore
        

module Cont =

    //let asBeginEnd (f : 'a -> cont<'b>) : ('a * AsyncCallback * obj -> IAsyncResult) * (IAsyncResult -> 'b) * (IAsyncResult -> unit) =
        

    let onCancel (action : unit -> unit) =
        { runCont = fun (p, ok, cancel, error) ->
            let reg = p.CancellationToken.Register (Action action)
            ok reg
        }

    let cancellationToken =
        { runCont = fun (p,ok,_,_) -> ok p.CancellationToken }

    let unit =
        { runCont = fun (_,ok,_,_) -> ok () }

    let value (v : 'a) =
        { runCont = fun (_,ok,_,_) -> ok v }

    let error (e : exn) =
        { runCont = fun (_,_,_,error) -> error e }

    let cancel (e : OperationCanceledException) =
        { runCont = fun (_,_,cancel,_) -> cancel e }


    let map (mapping : 'a -> 'b) (m : cont<'a>) =
        { runCont = fun (p,ok,cancel,error) ->
            let ok (a : 'a) =
                try 
                    let res = mapping a
                    ok res
                with
                | :? OperationCanceledException as e -> cancel e
                | e -> error e

            m.run (p, ok, cancel, error)
        }

    let bind (mapping : 'a -> cont<'b>) (m : cont<'a>) =
        { runCont = fun (p,ok,cancel,error) ->
            let ok (a : 'a) =
                try 
                    let res = mapping a
                    res.run (p, ok, cancel, error)
                with
                | :? OperationCanceledException as e -> cancel e
                | e -> error e

            m.run (p, ok, cancel, error)
        }

    let tryWith (m : cont<'a>) (handler : exn -> cont<'a>) =
        { runCont = fun (p,ok,cancel,error) ->
            let error (e : exn) =
                handler(e).run(p, ok, cancel, error)
            m.run(p, ok, cancel, error)
        }

    let tryFinally (m : cont<'a>) (fin : unit -> unit) =
        { runCont = fun (p,ok,cancel,error) ->
            let error (e : exn) =
                error e
                fin()

            let cancel (e : OperationCanceledException) =
                cancel e
                fin()

            let ok (e : 'a) =
                ok e
                fin()

            m.run(p, ok, cancel, error)
        }

type ContBuilder() =
    member x.Return (v : 'a) = Cont.value v
    member x.Bind(m : cont<'a>, f : 'a -> cont<'b>) = Cont.bind f m
    member x.Zero() = Cont.unit
    member x.Delay(f : unit -> cont<'a>) = { runCont = fun a -> f().runCont a }
    member x.Combine(l : cont<unit>, r : cont<'a>) = Cont.bind (fun () -> r) l
    member x.TryWith(l : cont<'a>, r : exn -> cont<'a>) = Cont.tryWith l r
    member x.TryFinally(l : cont<'a>, fin : unit -> unit) = Cont.tryFinally l fin

    member x.Using<'a, 'b when 'a :> IDisposable>(v : 'a, action : 'a -> cont<'b>) =
        x.TryFinally(action v, v.Dispose)

    member x.ReturnFrom(c : cont<'a>) = c

    member x.ReturnFrom(t : Twine<'a>) =
        { runCont = fun (pool,ok,cancel,error) ->
            let cont (t : Twine<'a>) =
                pool.Start(fun () ->
                    try ok t.Result
                    with
                    | :? OperationCanceledException as e -> cancel e
                    | e -> error e
                )
            t.ContinueWith(cont) |> ignore
        }
 
    member x.Bind(t : Twine<'a>, f : 'a -> cont<'b>) =
        x.Bind(x.ReturnFrom t, f)

    member x.While(guard : unit -> bool, body : cont<unit>) =
        let mutable self = Unchecked.defaultof<cont<unit>>
        self <- 
            x.Delay (fun () ->
                if guard() then
                    x.Combine(body, self)
                else
                    x.Zero()
            )
        self

    member x.For(seq : seq<'a>, action : 'a -> cont<unit>) =
        x.Delay(fun () ->
            let e = seq.GetEnumerator()
            x.TryFinally(
                x.While(
                    (fun () -> e.MoveNext()),
                    x.Delay(fun () -> action e.Current)
                ),
                e.Dispose
            )
        )


[<AutoOpen>]
module Extensions =
    let cont = ContBuilder()

