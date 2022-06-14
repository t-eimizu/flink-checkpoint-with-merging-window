# flink-checkpoint-with-merging-window

## The purpose of this code
Proof of concept code for the exception in creating checkpoint with a session windows

## Summary
In Flink 1.15.0, the combination of following functions always occur ClassCastException.

- Session Window
- Checkpoint
- Keyed State

This repository provides minimal source code that can combine these features to reproduce the exception.

## Description
### How the Exception Occurred

In the process window function of the session window, we must use `context.globalState()`
instead of `context.windowState()`. If you use `context.windowState()` in this situation, Flink throws `UnsupportedOperationException`.

So we have to do following:
```Java
stPreviousValue = context.globalState().getState(desc4PreviousValue);
```

Then stPreviousValue will have the following fields:

|Field Name          |Value                 |
|--------------------|----------------------|
|currentNamespace    |VoidNamespace         |
|namespaceSerializer |TimeWindow$Serializer |

As a result, when flink create checkpoint on this job, ClassCastException occurs.
```
2022-06-14 11:04:57,212 INFO  org.apache.flink.streaming.runtime.tasks.AsyncCheckpointRunnable [] - ProcessingData -> Sink: PrintData (1/1)#0 - asynchronous part of checkpoint 1 could not be completed.
java.util.concurrent.ExecutionException: java.lang.ClassCastException: class org.apache.flink.runtime.state.VoidNamespace cannot be cast to class org.apache.flink.streaming.api.windowing.windows.TimeWindow (org.apache.flink.runtime.state.VoidNamespace and org.apache.flink.streaming.api.windowing.windows.TimeWindow are in unnamed module of loader 'app')
	at java.util.concurrent.FutureTask.report(FutureTask.java:122) ~[?:?]
	at java.util.concurrent.FutureTask.get(FutureTask.java:191) ~[?:?]
	at org.apache.flink.util.concurrent.FutureUtils.runIfNotDoneAndGet(FutureUtils.java:645) ~[flink-core-1.15.0.jar:1.15.0]
	at org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer.<init>(OperatorSnapshotFinalizer.java:54) ~[flink-streaming-java-1.15.0.jar:1.15.0]
	at org.apache.flink.streaming.runtime.tasks.AsyncCheckpointRunnable.finalizeNonFinishedSnapshots(AsyncCheckpointRunnable.java:191) ~[flink-streaming-java-1.15.0.jar:1.15.0]
	at org.apache.flink.streaming.runtime.tasks.AsyncCheckpointRunnable.run(AsyncCheckpointRunnable.java:124) [flink-streaming-java-1.15.0.jar:1.15.0]
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128) [?:?]
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628) [?:?]
	at java.lang.Thread.run(Thread.java:834) [?:?]
Caused by: java.lang.ClassCastException: class org.apache.flink.runtime.state.VoidNamespace cannot be cast to class org.apache.flink.streaming.api.windowing.windows.TimeWindow (org.apache.flink.runtime.state.VoidNamespace and org.apache.flink.streaming.api.windowing.windows.TimeWindow are in unnamed module of loader 'app')
	at org.apache.flink.streaming.api.windowing.windows.TimeWindow$Serializer.serialize(TimeWindow.java:130) ~[flink-streaming-java-1.15.0.jar:1.15.0]
	at org.apache.flink.runtime.state.heap.CopyOnWriteStateMapSnapshot.writeState(CopyOnWriteStateMapSnapshot.java:145) ~[flink-runtime-1.15.0.jar:1.15.0]
	at org.apache.flink.runtime.state.heap.AbstractStateTableSnapshot.writeStateInKeyGroup(AbstractStateTableSnapshot.java:116) ~[flink-runtime-1.15.0.jar:1.15.0]
	at org.apache.flink.runtime.state.heap.CopyOnWriteStateTableSnapshot.writeStateInKeyGroup(CopyOnWriteStateTableSnapshot.java:38) ~[flink-runtime-1.15.0.jar:1.15.0]
	at org.apache.flink.runtime.state.heap.HeapSnapshotStrategy.lambda$asyncSnapshot$3(HeapSnapshotStrategy.java:172) ~[flink-runtime-1.15.0.jar:1.15.0]
	at org.apache.flink.runtime.state.SnapshotStrategyRunner$1.callInternal(SnapshotStrategyRunner.java:91) ~[flink-runtime-1.15.0.jar:1.15.0]
	at org.apache.flink.runtime.state.SnapshotStrategyRunner$1.callInternal(SnapshotStrategyRunner.java:88) ~[flink-runtime-1.15.0.jar:1.15.0]
	at org.apache.flink.runtime.state.AsyncSnapshotCallable.call(AsyncSnapshotCallable.java:78) ~[flink-runtime-1.15.0.jar:1.15.0]
	at java.util.concurrent.FutureTask.run(FutureTask.java:264) ~[?:?]
	at org.apache.flink.util.concurrent.FutureUtils.runIfNotDoneAndGet(FutureUtils.java:642) ~[flink-core-1.15.0.jar:1.15.0]
	... 6 more
```

## workaround
Turn off the checkpoint function.
