namespace rec Twine

open System.Threading
open System
open System.Collections.Concurrent
open System.Collections.Generic

type internal ITwine =
    abstract member Enqueued : TwineThreadPool -> unit
    abstract member RunInternal : unit -> list<ITwine>
    
type TwineStatus =
    | Created = -1
    | Waiting = 0
    | Running = 2
    | Finished = 3
    | Canceled = 4
    | Failed = 5

type Twine<'a>(action : unit -> 'a) =
    let mutable action = action
    let mutable status = TwineStatus.Created
    let mutable result = Unchecked.defaultof<'a>
    let mutable error = Unchecked.defaultof<exn>
    let mutable pool = Unchecked.defaultof<TwineThreadPool>
    let mutable continuations : list<ITwine> = []

    member inline private x.UpdateStatus(mapping : unit -> TwineStatus) =
        lock x (fun () ->
            status <- mapping()
            Monitor.PulseAll x
        )
        
    member inline private x.UpdateStatusAndGetCont(mapping : unit -> TwineStatus) =
        lock x (fun () ->
            status <- mapping()
            Monitor.PulseAll x
            let c = continuations
            continuations <- []
            c
        )

    member internal x.SetResult(p : TwineThreadPool, value : 'a) =
        let cont =
            x.UpdateStatusAndGetCont(fun () ->
                pool <- p
                result <- value
                TwineStatus.Finished
            )
        cont |> List.iter pool.Post
        
    member internal x.SetCanceled(p : TwineThreadPool) =
        let cont =
            x.UpdateStatusAndGetCont(fun () ->
                pool <- p
                TwineStatus.Canceled
            )
        cont |> List.iter pool.Post

    member internal x.SetError(p : TwineThreadPool, err : exn) =
        let cont =
            x.UpdateStatusAndGetCont(fun () ->
                pool <- p
                error <- err
                TwineStatus.Failed
            )
        cont |> List.iter pool.Post

    interface ITwine with
        member x.Enqueued p = 
            x.UpdateStatus (fun _ ->
                pool <- p
                TwineStatus.Waiting
            )

        member x.RunInternal() =
            x.UpdateStatus (fun () -> TwineStatus.Running)
            let a = action
            action <- Unchecked.defaultof<_>

            try
                let res = a()
                x.UpdateStatusAndGetCont(fun () -> 
                    result <- res
                    TwineStatus.Finished
                )
            with
                | :? OperationCanceledException -> 
                    x.UpdateStatusAndGetCont(fun () -> TwineStatus.Canceled)
                | e ->
                    x.UpdateStatusAndGetCont(fun () -> 
                        error <- e
                        TwineStatus.Failed
                    )

    member x.Status = status

    member x.Wait() =
        if status < TwineStatus.Finished then
            lock x (fun () ->
                while status < TwineStatus.Finished do
                    Monitor.Wait x |> ignore
            )

        match status with
            | TwineStatus.Finished -> ()
            | TwineStatus.Canceled -> raise <| OperationCanceledException()
            | TwineStatus.Failed -> raise error
            | _ -> failwith "[Twine] unexpected state"

    member x.Result =
        x.Wait()
        result

    member x.ContinueWith(action : Twine<'a> -> 'b) =
        let t = Twine<'b>(fun () -> action x)
        if status >= TwineStatus.Finished then
            let worker = TwineWorker.TwineWorker
            if not (isNull worker) then
                let rec run (t : ITwine) =
                    t.Enqueued pool
                    match t.RunInternal() with
                        | [] -> ()
                        | h :: rest ->
                            rest |> List.iter pool.Post
                            run h
                run t
            else
                pool.Post t
        else
            lock x (fun () ->
                if status >= TwineStatus.Finished then
                    pool.Post t
                else
                    let a = t :> ITwine
                    continuations <- a :: continuations
            )
        t

type MVar<'a>() =
    
    let mutable isSet = false
    let mutable content = Unchecked.defaultof<'a>

    member x.Put(v : 'a) =
        lock x (fun () ->
            while isSet do
                Monitor.Wait x |> ignore

            content <- v
            isSet <- true
            Monitor.PulseAll x
        )

    member x.Take() =
        lock x (fun () ->
            while not isSet do
                Monitor.Wait x |> ignore
            let v = content
            content <- Unchecked.defaultof<'a>
            isSet <- false
            v
        )

    
    member x.Take(ct : CancellationToken) =
        let reg = ct.Register(fun () -> lock x (fun () -> Monitor.PulseAll x))
        lock x (fun () ->
            while not isSet && not ct.IsCancellationRequested do
                Monitor.Wait x |> ignore

            reg.Dispose()
            if ct.IsCancellationRequested then
                raise <| OperationCanceledException()
            else
                let v = content
                content <- Unchecked.defaultof<'a>
                isSet <- false
                v
        )

[<AllowNullLiteral>]
type internal TwineWorker(parent : TwineThreadPool, index : int) as this =
    
    [<ThreadStatic; DefaultValue>]
    static val mutable private WorkerValue : TwineWorker
    
    let job = MVar<ITwine>()
    let name = sprintf "TwineWorker%d" index
    let cancel = new CancellationTokenSource()

    let thread =
        startThread name (fun () ->
            TwineWorker.WorkerValue <- this
            try
                while not cancel.IsCancellationRequested do
                    let t = job.Take(cancel.Token)
                    match t.RunInternal() with
                        | [] -> 
                            parent.AddIdleWorker this
                        | h :: rest ->
                            job.Put h
                            rest |> List.iter parent.Post
            with :? OperationCanceledException -> 
                ()
        )

    static member TwineWorker : TwineWorker = TwineWorker.WorkerValue

    member x.Post(t : ITwine) =
        job.Put t

    member x.Cancel() =
        cancel.Cancel()
        thread.Join()

type TwineThreadPool (initial : int) as this =
    static let def = new TwineThreadPool(Environment.ProcessorCount)
    let mutable canceled = false

    let mutable workerIndex = -1
    let workers = ConcurrentBag<TwineWorker>()
    let allWorkers = HashSet<TwineWorker>()

    let createWorker() =
        let id = Interlocked.Increment(&workerIndex)
        let worker = TwineWorker(this, id)
        allWorkers.Add worker |> ignore
        worker
        
    let spawnWorker () =
        let worker = createWorker()
        workers.Add worker

    do for i in 1 .. initial do spawnWorker()

    let pending = new BlockingCollection<ITwine>()
    let poster =
        startThread "TwinePoster" (fun () ->
            for t in pending.GetConsumingEnumerable() do
                match workers.TryTake() with
                    | (true, w) -> 
                        w.Post t
                    | _ ->
                        let worker = createWorker()
                        worker.Post t
        )

    member internal x.AddIdleWorker(w : TwineWorker) =
        workers.Add w
        
    member internal x.Post<'a>(t : ITwine) : unit =
        if canceled then raise <| ObjectDisposedException("TwineThreadPool")
        match workers.TryTake() with
            | (true, w) -> w.Post t
            | _ -> pending.Add t

    static member Default = def

    member x.Post<'a>(t : Twine<'a>) : unit =
        if t.Status > TwineStatus.Created then failwith "[Twine] cannot post a started twine"
        x.Post(t :> ITwine)

    member x.Dispose() =
        let mutable dummy = Unchecked.defaultof<ITwine>
        while pending.TryTake(&dummy) do ()
        pending.CompleteAdding()
        poster.Join()
        for w in allWorkers do w.Cancel()

        workers.Clear()
        allWorkers.Clear()
        pending.Dispose()
        
    member x.Start(action : unit -> 'a) =
        let worker = TwineWorker.TwineWorker
        let twine = Twine<'a>(action)

        if isNull worker then
            x.Post twine
            twine
        else       
            let rec run (t : ITwine) =
                t.Enqueued x
                match t.RunInternal() with
                    | [] -> ()
                    | h :: rest ->
                        rest |> List.iter x.Post
                        run h
            run twine
            twine

    member x.FromWaitHandle(handle : WaitHandle) =
        let mutable reg = Unchecked.defaultof<RegisteredWaitHandle>
        let twine = Twine<unit>(Unchecked.defaultof<_>)
        let cont (o : obj) (b : bool) =
            twine.SetResult(x, ())
            reg.Unregister(handle) |> ignore
            
        reg <- ThreadPool.RegisterWaitForSingleObject(handle, WaitOrTimerCallback(cont), null, -1, true)
        twine

    member x.FromWaitHandle(handle : WaitHandle, ct : CancellationToken) =
        let mutable cancelReg = { new IDisposable with member x.Dispose() = () }
        let mutable waitReg = Unchecked.defaultof<RegisteredWaitHandle>

        let mutable cnt = 0


        let cleanup() =
            waitReg.Unregister handle |> ignore
            cancelReg.Dispose()
            waitReg <- Unchecked.defaultof<_>
            cancelReg <- Unchecked.defaultof<_>

        let twine = Twine<unit>(Unchecked.defaultof<_>)
        let cont (o : obj) (isTimeout : bool) =
            if Interlocked.Exchange(&cnt, 1) = 0 then
                twine.SetResult(x, ())
                cleanup()
            
        waitReg <- 
            ThreadPool.RegisterWaitForSingleObject(handle, WaitOrTimerCallback(cont), null, -1, true)

        cancelReg <- 
            ct.Register(fun () ->
                if Interlocked.Exchange(&cnt, 1) = 0 then
                    twine.SetCanceled(x)
                    cleanup()
            )

        twine
        
    member x.FromWaitHandle(handle : WaitHandle, timeout : int, ct : CancellationToken) =
        let mutable cancelReg = { new IDisposable with member x.Dispose() = () }
        let mutable waitReg = Unchecked.defaultof<RegisteredWaitHandle>

        let mutable cnt = 0


        let cleanup() =
            waitReg.Unregister handle |> ignore
            cancelReg.Dispose()
            waitReg <- Unchecked.defaultof<_>
            cancelReg <- Unchecked.defaultof<_>

        let twine = Twine<bool>(Unchecked.defaultof<_>)
        let cont (o : obj) (isTimeout : bool) =
            
            if Interlocked.Exchange(&cnt, 1) = 0 then
                if isTimeout then
                    twine.SetResult(x, false)
                else
                    twine.SetResult(x, true)
                    cleanup()
            
        waitReg <- 
            ThreadPool.RegisterWaitForSingleObject(handle, WaitOrTimerCallback(cont), null, timeout, true)

        cancelReg <- 
            ct.Register(fun () ->
                if Interlocked.Exchange(&cnt, 1) = 0 then
                    twine.SetCanceled(x)
                    cleanup()
            )

        twine

    member x.FromBeginEnd(beginFun : (IAsyncResult * obj -> unit) * obj -> IAsyncResult, endFun : IAsyncResult -> 'a) =
        let mutable res = Unchecked.defaultof<IAsyncResult>
        let twine = Twine<'a> (fun () -> endFun res)

        let callback (r, o) =
            res <- r
            x.Post twine
            
        beginFun(callback, null) |> ignore

        twine

    member x.FromResult(value : 'a) =
        let twine = Twine<'a>(Unchecked.defaultof<_>)
        twine.SetResult(x, value)
        twine

    member x.FromContinuations (cont : ('a -> unit) * (OperationCanceledException -> unit) * (exn -> unit) -> unit) =
        let twine = Twine<'a>(Unchecked.defaultof<_>)
        let ok v = twine.SetResult(x, v)
        let cancel _ = twine.SetCanceled(x)
        let error e = twine.SetError(x,e)
        cont (ok, cancel, error)
        twine



    interface IDisposable with
        member x.Dispose() = x.Dispose()

[<AbstractClass>]
type Twine =
    
    static member inline FromResult(value : 'a) = TwineThreadPool.Default.FromResult value
    static member inline Run(action : unit -> 'a) = TwineThreadPool.Default.Start action
    static member inline FromBeginEnd(b,e) = TwineThreadPool.Default.FromBeginEnd(b,e)
    static member inline FromWaitHandle(w) = TwineThreadPool.Default.FromWaitHandle(w)
    static member inline FromWaitHandle(w,ct) = TwineThreadPool.Default.FromWaitHandle(w, ct)
    static member inline FromWaitHandle(w,ct,timeout) = TwineThreadPool.Default.FromWaitHandle(w, ct, timeout)



