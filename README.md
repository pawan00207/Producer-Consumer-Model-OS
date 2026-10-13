# Producer-Consumer Synchronization Lab

An educational Operating Systems simulator with the original producer-consumer dashboard and additive synchronization, race-condition, CPU-scheduling, analytics, guided-experiment, and presentation labs.

## Setup

The project uses HTML, CSS, browser JavaScript modules, and Node.js only for tests. No package installation is needed. Because browser ES modules need an HTTP origin, serve the project directory with either:

```powershell
python -m http.server 5500
```

or, from the project directory:

```powershell
npx http-server -p 5500
```

Open `http://localhost:5500/` (or the server URL printed in the terminal). To run the test suite, install Node.js 20 or newer and run:

```powershell
npm test
```

## Architecture

- `index.html` holds the original dashboard unchanged, followed by separate advanced lab tabs.
- `style.css` retains the original dashboard rules and adds styles scoped to `.lab-*` lab elements.
- `advanced-ui.js` connects the original controls and the additional screens to a single authoritative simulation instance. It handles UI rendering, timing, event display, exports, and cleanup of auto-run timers.
- `os-lab.js` contains the deterministic producer-consumer state machine, semaphore and mutex models, process transitions and histories, metrics, race-condition trace, CPU scheduling algorithms, scenarios, glossary, viva questions, and CSV serialization.
- `test/os-lab.test.js` uses Node's built-in test runner; no third-party testing dependency is required.
- `server.py` is empty and no web backend is implemented. `requirements.txt` is legacy dependency metadata and is not used by this application.

The simulation ordering follows the implementation: producer `wait(sem_empty) -> lock(mutex) -> enqueue -> unlock(mutex) -> signal(sem_full)`; consumer `wait(sem_full) -> lock(mutex) -> dequeue -> unlock(mutex) -> signal(sem_empty)`. Waiting on the resource semaphore before taking the mutex avoids holding the critical-section lock while a process is blocked for a buffer resource.

## Features

- Original buffer dashboard, controls, statistics, and event log, driven by the same simulation state as the advanced labs.
- New, Ready, Running, Waiting/Blocked, and Terminated process states; state histories, blocking reasons, ready/blocked queues, and next-step/previous-step replay.
- Modeled empty/full semaphores and mutex with explicit wait/signal/lock/unlock events, owner and waiter inspection, and buffer/semaphore bounds checks.
- Separate deterministic lost-update demonstration with synchronization off/on; it never changes the main buffer.
- FCFS, non-preemptive SJF, and Round Robin scheduler with arrival/burst input, deterministic ties, idle periods, Gantt chart, ready-queue evolution, and completion/turnaround/waiting/response metrics.
- Live event-derived operation counts, throughput, sampled buffer utilization, blocked duration, activity charts, resettable experiment baseline, and CSV download.
- Eight selectable guided scenarios, presentation controls, full-screen presentation panel, architecture ordering, 15-term glossary, 20 viva questions, and summary/event CSV.

## Model limitations

This is a deterministic educational model executed by one JavaScript event loop. Modeled processes, semaphores, mutexes, and scheduling are not actual OS threads, kernel synchronization primitives, or kernel scheduling. The race-condition lab uses an explicit deterministic interleaving of read/write operations to demonstrate a lost update; it does not claim a genuine concurrent race. Charts and throughput use virtual simulation time and model events, not wall-clock benchmark results. Buffer utilization is a current/sample-based measure. No Python or Flask server is present or required.

## Tests

```powershell
npm test
```

The tests cover buffer and semaphore bounds, mutex ownership, full/empty blocking, stop/reset and replay, race-condition traces, FCFS/SJF/Round Robin scheduling, scheduling input validation, event-derived summaries, and CSV escaping. There is no configured lint or production build command; the app is native browser JavaScript.

## Two-minute classroom walkthrough

1. In Guided Experiments, select **Buffer-full blocking** and apply it. Start the simulation and watch producer states, the blocked queue, and `sem_empty`.
2. Switch to Process & Sync, enable step-through, and advance across semaphore wait, mutex lock, enqueue, unlock, and signal. Use Previous step to replay the state.
3. Select **Buffer-empty blocking** and observe consumers block on `sem_full`.
4. Open Race Condition, run with synchronization disabled, then enable mutex protection and compare the final shared counter.
5. Open CPU Scheduling, compare FCFS with Round Robin, and explain the Gantt chart plus waiting/turnaround values.
6. Open Presentation for the architecture ordering, glossary, and viva questions. Download the experiment CSV from Analytics or Presentation.
