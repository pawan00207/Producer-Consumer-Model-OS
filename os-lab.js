export const PROCESS_STATES = Object.freeze({
	NEW: 'New',
	READY: 'Ready',
	RUNNING: 'Running',
	BLOCKED: 'Waiting/Blocked',
	TERMINATED: 'Terminated'
});

const clone = (value) => JSON.parse(JSON.stringify(value));

export class ProducerConsumerSimulation {
	constructor({ capacity = 6, producers = 2, consumers = 2, tickMs = 100 } = {}) {
		if (!Number.isInteger(capacity) || capacity < 1 || capacity > 12) {
			throw new RangeError('Buffer capacity must be an integer from 1 to 12.');
		}
		if (!Number.isInteger(producers) || producers < 1 || producers > 4 ||
			!Number.isInteger(consumers) || consumers < 1 || consumers > 4) {
			throw new RangeError('Configure between 1 and 4 producers and consumers.');
		}
		this.capacity = capacity;
		this.tickMs = tickMs;
		this.clock = 0;
		this.buffer = Array(capacity).fill(null);
		this.head = 0;
		this.tail = 0;
		this.nextItem = 1;
		this.produced = 0;
		this.consumed = 0;
		this.semaphores = {
			empty: { value: capacity, waiters: [] },
			full: { value: 0, waiters: [] }
		};
		this.inFlightEmpty = 0;
		this.inFlightFull = 0;
		this.mutex = { locked: false, owner: null, waiters: [] };
		this.readyQueue = [];
		this.processes = [];
		this.currentId = null;
		this.events = [];
		this.undoStack = [];
		this.stopped = false;
		let id = 1;
		for (let index = 1; index <= producers; index += 1) {
			this.processes.push(this._newProcess(id++, 'producer', index));
		}
		for (let index = 1; index <= consumers; index += 1) {
			this.processes.push(this._newProcess(id++, 'consumer', index));
		}
		this._assertInvariants();
	}

	_newProcess(id, kind, ordinal) {
		return {
			id: `P${id}`,
			kind,
			name: `${kind === 'producer' ? 'Producer' : 'Consumer'} ${ordinal}`,
			state: PROCESS_STATES.NEW,
			reason: '',
			history: [{ time: 0, state: PROCESS_STATES.NEW, reason: 'Created' }],
			blockedMs: 0,
			blockedAt: null,
			operations: 0,
			phase: kind === 'producer' ? 'wait-empty' : 'wait-full',
			item: null
		};
	}

	_snapshot() {
		const keys = [
			'clock', 'buffer', 'head', 'tail', 'nextItem', 'produced', 'consumed',
			'semaphores', 'inFlightEmpty', 'inFlightFull', 'mutex', 'readyQueue', 'processes', 'currentId', 'events', 'stopped'
		];
		return Object.fromEntries(keys.map((key) => [key, clone(this[key])]));
	}

	_restore(snapshot) {
		Object.assign(this, clone(snapshot));
	}

	_event(type, process, message, details = {}) {
		const event = {
			sequence: this.events.length + 1,
			time: this.clock,
			type,
			process: process?.name ?? 'System',
			processId: process?.id ?? null,
			message,
			...details
		};
		this.events.push(event);
		return event;
	}

	_transition(process, nextState, reason = '') {
		if (process.state === nextState && process.reason === reason) return;
		const previous = process.state;
		if (previous === PROCESS_STATES.BLOCKED && process.blockedAt !== null) {
			process.blockedMs += this.clock - process.blockedAt;
			process.blockedAt = null;
		}
		process.state = nextState;
		process.reason = reason;
		if (nextState === PROCESS_STATES.BLOCKED) process.blockedAt = this.clock;
		process.history.push({ time: this.clock, state: nextState, reason });
		this._event('state', process, `${previous} -> ${nextState}${reason ? ` (${reason})` : ''}`, {
			from: previous,
			to: nextState,
			reason
		});
	}

	_admitNewProcess() {
		const process = this.processes.find((entry) => entry.state === PROCESS_STATES.NEW);
		if (!process) return false;
		this.readyQueue.push(process.id);
		this._transition(process, PROCESS_STATES.READY);
		this._event('queue', process, 'Entered the ready queue.', { queue: [...this.readyQueue] });
		return true;
	}

	_takeReadyProcess() {
		if (this.currentId !== null || this.readyQueue.length === 0) return false;
		const id = this.readyQueue.shift();
		const process = this._process(id);
		if (!process || process.state !== PROCESS_STATES.READY) return false;
		this.currentId = id;
		this._transition(process, PROCESS_STATES.RUNNING);
		this._event('schedule', process, 'Selected from the ready queue.');
		return true;
	}

	_process(id) {
		return this.processes.find((process) => process.id === id);
	}

	_block(process, resource, queue, reason) {
		if (!queue.includes(process.id)) queue.push(process.id);
		this.currentId = null;
		this._transition(process, PROCESS_STATES.BLOCKED, reason);
		this._event('wait', process, `wait(${resource}) blocked: ${reason}.`, {
			resource,
			waiting: [...queue]
		});
	}

	_waitSemaphore(process, name) {
		const semaphore = this.semaphores[name];
		if (semaphore.value === 0) {
			this._block(
				process,
				`sem_${name}`,
				semaphore.waiters,
				name === 'empty' ? 'Buffer is full' : 'Buffer is empty'
			);
			return false;
		}
		semaphore.value -= 1;
		if (name === 'empty') this.inFlightEmpty += 1;
		else this.inFlightFull += 1;
		this._event('wait', process, `wait(sem_${name}) acquired one permit.`, {
			resource: `sem_${name}`,
			value: semaphore.value,
			explanation: name === 'empty' ? 'Reserved one free buffer slot.' : 'Reserved one available item.'
		});
		return true;
	}

	_signalSemaphore(name, process) {
		const semaphore = this.semaphores[name];
		if (name === 'full') this.inFlightEmpty -= 1;
		else this.inFlightFull -= 1;
		const waitingId = semaphore.waiters.shift();
		if (waitingId) {
			const waiting = this._process(waitingId);
			if (name === 'full') this.inFlightFull += 1;
			else this.inFlightEmpty += 1;
			waiting.phase = 'wait-mutex';
			this.readyQueue.push(waiting.id);
			this._transition(waiting, PROCESS_STATES.READY, 'Semaphore permit transferred');
			this._event('signal', process, `signal(sem_${name}) transferred a permit to ${waiting.name}.`, {
				resource: `sem_${name}`,
				value: semaphore.value,
				woken: waiting.id,
				explanation: 'A waiting process receives the released permit directly.'
			});
		} else {
			semaphore.value += 1;
			this._event('signal', process, `signal(sem_${name}) released one permit.`, {
				resource: `sem_${name}`,
				value: semaphore.value,
				explanation: 'One resource permit is now available.'
			});
		}
	}

	_lockMutex(process) {
		if (this.mutex.locked) {
			this._block(process, 'mutex', this.mutex.waiters, `Mutex held by ${this._process(this.mutex.owner)?.name ?? 'another process'}`);
			return false;
		}
		this.mutex.locked = true;
		this.mutex.owner = process.id;
		this._event('lock', process, 'Acquired the mutex; entering the critical section.', {
			owner: process.id,
			explanation: 'Only this process may modify the shared buffer now.'
		});
		return true;
	}

	_unlockMutex(process) {
		if (this.mutex.owner !== process.id) throw new Error('Only the mutex owner may unlock it.');
		const waitingId = this.mutex.waiters.shift();
		if (waitingId) {
			const waiting = this._process(waitingId);
			this.mutex.owner = waiting.id;
			waiting.phase = 'mutate';
			this.readyQueue.push(waiting.id);
			this._transition(waiting, PROCESS_STATES.READY, 'Mutex ownership transferred');
			this._event('unlock', process, `Released mutex; ownership transferred to ${waiting.name}.`, {
				owner: waiting.id,
				woken: waiting.id,
				explanation: 'The next mutex waiter is granted ownership before it runs.'
			});
		} else {
			this.mutex.locked = false;
			this.mutex.owner = null;
			this._event('unlock', process, 'Released the mutex.', { owner: null });
		}
	}

	_finishOperation(process) {
		process.operations += 1;
		process.phase = process.kind === 'producer' ? 'wait-empty' : 'wait-full';
		process.item = null;
		this.currentId = null;
		this._transition(process, PROCESS_STATES.READY);
		this.readyQueue.push(process.id);
		this._event('queue', process, 'Returned to the ready queue.', { queue: [...this.readyQueue] });
	}

	step() {
		if (this.stopped) return { advanced: false, reason: 'Simulation is stopped.' };
		this.undoStack.push(this._snapshot());
		if (this.undoStack.length > 1000) this.undoStack.shift();
		this.clock += this.tickMs;
		if (this.processes.some((process) => process.state === PROCESS_STATES.NEW)) {
			this._admitNewProcess();
			this._assertInvariants();
			return { advanced: true, event: this.events.at(-1) };
		}
		if (this.currentId === null && !this._takeReadyProcess()) {
			const result = { advanced: false, reason: 'No ready processes.' };
			this.undoStack.pop();
			return result;
		}

		const process = this._process(this.currentId);
		if (process.phase === 'wait-empty' || process.phase === 'wait-full') {
			const semaphore = process.phase === 'wait-empty' ? 'empty' : 'full';
			if (this._waitSemaphore(process, semaphore)) process.phase = 'wait-mutex';
		} else if (process.phase === 'wait-mutex') {
			if (this._lockMutex(process)) process.phase = 'mutate';
		} else if (process.phase === 'mutate') {
			if (process.kind === 'producer') {
				if (this.buffer[this.tail] !== null) throw new Error('Producer would overwrite an occupied slot.');
				process.item = this.nextItem++;
				this.buffer[this.tail] = process.item;
				this.tail = (this.tail + 1) % this.capacity;
				this.produced += 1;
				this._event('produce', process, `Enqueued item ${process.item}.`, { item: process.item, index: (this.tail + this.capacity - 1) % this.capacity });
			} else {
				if (this.buffer[this.head] === null) throw new Error('Consumer would read an empty slot.');
				const item = this.buffer[this.head];
				const index = this.head;
				this.buffer[this.head] = null;
				this.head = (this.head + 1) % this.capacity;
				this.consumed += 1;
				process.item = item;
				this._event('consume', process, `Dequeued item ${item}.`, { item, index });
			}
			process.phase = 'unlock';
		} else if (process.phase === 'unlock') {
			this._unlockMutex(process);
			process.phase = 'signal';
		} else if (process.phase === 'signal') {
			this._signalSemaphore(process.kind === 'producer' ? 'full' : 'empty', process);
			this._finishOperation(process);
		}
		this._assertInvariants();
		return { advanced: true, event: this.events.at(-1) };
	}

	back() {
		const snapshot = this.undoStack.pop();
		if (!snapshot) return false;
		this._restore(snapshot);
		return true;
	}

	stop() {
		if (this.stopped) return;
		this.stopped = true;
		if (this.currentId) {
			const current = this._process(this.currentId);
			this.mutex.locked = false;
			this.mutex.owner = null;
			if (current) this._transition(current, PROCESS_STATES.TERMINATED, 'Simulation stopped');
		}
		for (const process of this.processes) {
			if (process.state !== PROCESS_STATES.TERMINATED) {
				this._transition(process, PROCESS_STATES.TERMINATED, 'Simulation stopped');
			}
		}
		this.currentId = null;
		this.readyQueue = [];
		this.semaphores.empty.waiters = [];
		this.semaphores.full.waiters = [];
		this.mutex.waiters = [];
		this._event('system', null, 'All processes terminated.');
	}

	reset(options = {}) {
		return new ProducerConsumerSimulation({
			capacity: options.capacity ?? this.capacity,
			producers: options.producers ?? this.processes.filter((process) => process.kind === 'producer').length,
			consumers: options.consumers ?? this.processes.filter((process) => process.kind === 'consumer').length,
			tickMs: options.tickMs ?? this.tickMs
		});
	}

	snapshot() {
		return {
			capacity: this.capacity,
			clock: this.clock,
			buffer: [...this.buffer],
			head: this.head,
			tail: this.tail,
			produced: this.produced,
			consumed: this.consumed,
			semaphores: clone(this.semaphores),
			inFlightEmpty: this.inFlightEmpty,
			inFlightFull: this.inFlightFull,
			mutex: clone(this.mutex),
			readyQueue: [...this.readyQueue],
			blockedQueue: this.processes.filter((process) => process.state === PROCESS_STATES.BLOCKED).map((process) => process.id),
			processes: clone(this.processes),
			currentId: this.currentId,
			events: clone(this.events),
			stopped: this.stopped
		};
	}

	metrics() {
		const elapsedMs = this.clock;
		const occupied = this.buffer.filter((item) => item !== null).length;
		const processes = this.processes.map((process) => ({
			...process,
			blockedMs: process.blockedMs + (process.state === PROCESS_STATES.BLOCKED ? this.clock - process.blockedAt : 0)
		}));
		const completedOps = processes.reduce((total, process) => total + process.operations, 0);
		const completed = processes.filter((process) => process.operations > 0);
		const averageBlockedMs = completed.length
			? completed.reduce((total, process) => total + process.blockedMs / process.operations, 0) / completed.length
			: 0;
		return {
			elapsedMs,
			produced: this.produced,
			consumed: this.consumed,
			throughputPerSecond: elapsedMs ? (this.produced + this.consumed) / (elapsedMs / 1000) : 0,
			bufferUtilization: this.capacity ? occupied / this.capacity : 0,
			averageBlockedMs,
			completedOperations: completedOps,
			producerOperations: processes.filter((process) => process.kind === 'producer').reduce((total, process) => total + process.operations, 0),
			consumerOperations: processes.filter((process) => process.kind === 'consumer').reduce((total, process) => total + process.operations, 0),
			processes
		};
	}

	_assertInvariants() {
		const occupied = this.buffer.filter((item) => item !== null).length;
		if (occupied < 0 || occupied > this.capacity) throw new Error('Buffer occupancy invariant violated.');
		if (this.semaphores.empty.value < 0 || this.semaphores.empty.value > this.capacity ||
			this.semaphores.full.value < 0 || this.semaphores.full.value > this.capacity) {
			throw new Error('Semaphore count invariant violated.');
		}
		if (this.inFlightEmpty < 0 || this.inFlightFull < 0 ||
			this.semaphores.empty.value + this.semaphores.full.value + this.inFlightEmpty + this.inFlightFull !== this.capacity) {
			throw new Error('Semaphore permit conservation invariant violated.');
		}
		if (this.mutex.locked !== (this.mutex.owner !== null)) throw new Error('Mutex ownership invariant violated.');
		if (this.mutex.owner && !this._process(this.mutex.owner)) throw new Error('Mutex owner does not exist.');
		for (const id of this.readyQueue) {
			if (this._process(id)?.state !== PROCESS_STATES.READY) throw new Error('Ready queue contains a non-ready process.');
		}
		for (const process of this.processes) {
			if (process.state === PROCESS_STATES.BLOCKED && !process.reason) throw new Error('Blocked process has no reason.');
		}
		return true;
	}
}

export function runRaceConditionDemo({ synchronized = false } = {}) {
	const initial = 0;
	const trace = [];
	let shared = initial;
	if (synchronized) {
		trace.push({ actor: 'Process A', operation: 'lock(mutex)', observed: shared });
		const aRead = shared;
		trace.push({ actor: 'Process A', operation: 'read shared', observed: aRead });
		shared = aRead + 1;
		trace.push({ actor: 'Process A', operation: 'write shared = 1', observed: shared });
		trace.push({ actor: 'Process A', operation: 'unlock(mutex)', observed: shared });
		trace.push({ actor: 'Process B', operation: 'lock(mutex)', observed: shared });
		const bRead = shared;
		trace.push({ actor: 'Process B', operation: 'read shared', observed: bRead });
		shared = bRead + 1;
		trace.push({ actor: 'Process B', operation: `write shared = ${shared}`, observed: shared });
		trace.push({ actor: 'Process B', operation: 'unlock(mutex)', observed: shared });
	} else {
		const aRead = shared;
		trace.push({ actor: 'Process A', operation: 'read shared', observed: aRead });
		const bRead = shared;
		trace.push({ actor: 'Process B', operation: 'read shared', observed: bRead });
		shared = aRead + 1;
		trace.push({ actor: 'Process A', operation: `write shared = ${shared}`, observed: shared });
		shared = bRead + 1;
		trace.push({ actor: 'Process B', operation: `write shared = ${shared}`, observed: shared });
	}
	return { synchronized, initial, expected: 2, actual: shared, lostUpdates: 2 - shared, trace };
}

export function scheduleProcesses(algorithm, input, quantum = 2) {
	if (!['FCFS', 'SJF', 'RR'].includes(algorithm)) throw new RangeError('Choose FCFS, SJF, or RR.');
	if (!Array.isArray(input) || input.length === 0) throw new TypeError('Add at least one process.');
	if (algorithm === 'RR' && (!Number.isInteger(quantum) || quantum < 1 || quantum > 1000)) {
		throw new RangeError('Time quantum must be an integer from 1 to 1000.');
	}
	const seen = new Set();
	const processes = input.map((process, index) => {
		const id = String(process.id ?? '').trim();
		if (!id || seen.has(id)) throw new TypeError('Process IDs must be non-empty and unique.');
		seen.add(id);
		if (!Number.isInteger(process.arrival) || process.arrival < 0 ||
			!Number.isInteger(process.burst) || process.burst < 1) {
			throw new RangeError(`Process ${id} needs an arrival >= 0 and burst >= 1 (whole numbers).`);
		}
		return { id, arrival: process.arrival, burst: process.burst, remaining: process.burst, order: index, firstStart: null, completion: null };
	});
	const gantt = [];
	const readyEvolution = [];
	const completed = [];
	let time = 0;
	let cursor = 0;
	const arrivalOrder = [...processes].sort((a, b) => a.arrival - b.arrival || a.order - b.order);
	const ready = [];
	const enqueueArrivals = () => {
		while (cursor < arrivalOrder.length && arrivalOrder[cursor].arrival <= time) {
			ready.push(arrivalOrder[cursor++]);
		}
	};
	while (completed.length < processes.length) {
		enqueueArrivals();
		if (ready.length === 0) {
			const nextTime = arrivalOrder[cursor].arrival;
			if (nextTime > time) gantt.push({ process: 'Idle', start: time, end: nextTime });
			time = nextTime;
			enqueueArrivals();
		}
		if (algorithm === 'SJF') ready.sort((a, b) => a.burst - b.burst || a.arrival - b.arrival || a.order - b.order);
		const chosen = ready.shift();
		readyEvolution.push({ time, selected: chosen.id, queue: ready.map((process) => process.id) });
		const duration = algorithm === 'RR' ? Math.min(quantum, chosen.remaining) : chosen.remaining;
		if (chosen.firstStart === null) chosen.firstStart = time;
		const start = time;
		time += duration;
		chosen.remaining -= duration;
		gantt.push({ process: chosen.id, start, end: time });
		enqueueArrivals();
		if (chosen.remaining > 0) ready.push(chosen);
		else {
			chosen.completion = time;
			completed.push(chosen);
		}
	}
	const results = processes.map((process) => {
		const turnaround = process.completion - process.arrival;
		return {
			id: process.id,
			arrival: process.arrival,
			burst: process.burst,
			completion: process.completion,
			turnaround,
			waiting: turnaround - process.burst,
			response: process.firstStart - process.arrival
		};
	});
	return { algorithm, quantum: algorithm === 'RR' ? quantum : null, gantt, readyEvolution, results, finishTime: time };
}

export function experimentSummary(snapshot, metrics) {
	const events = snapshot.events;
	return {
		capacity: snapshot.capacity,
		elapsedMs: metrics.elapsedMs,
		produced: snapshot.produced,
		consumed: snapshot.consumed,
		bufferUtilization: metrics.bufferUtilization,
		throughputPerSecond: metrics.throughputPerSecond,
		averageBlockedMs: metrics.averageBlockedMs,
		waitOperations: events.filter((event) => event.type === 'wait').length,
		signalOperations: events.filter((event) => event.type === 'signal').length,
		mutexLocks: events.filter((event) => event.type === 'lock').length,
		mutexUnlocks: events.filter((event) => event.type === 'unlock').length
		};
}

export function toCsv(rows) {
	if (!Array.isArray(rows) || rows.length === 0) return '';
	const columns = [...new Set(rows.flatMap((row) => Object.keys(row)))];
	const cell = (value) => {
		const text = Array.isArray(value) ? value.join(' | ') : String(value ?? '');
		return /[",\r\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
	};
	return [columns.map(cell).join(','), ...rows.map((row) => columns.map((key) => cell(row[key])).join(','))].join('\r\n');
}

export const GUIDED_SCENARIOS = Object.freeze([
	{ id: 'balanced', name: 'Balanced production and consumption', capacity: 6, producers: 2, consumers: 2, producerSpeed: 800, consumerSpeed: 800, expected: 'Both sides progress at similar rates; the buffer usually stays partially filled.' },
	{ id: 'producer-fast', name: 'Producer faster than consumer', capacity: 6, producers: 2, consumers: 1, producerSpeed: 350, consumerSpeed: 1400, expected: 'The buffer fills and producers block on sem_empty.' },
	{ id: 'consumer-fast', name: 'Consumer faster than producer', capacity: 6, producers: 1, consumers: 2, producerSpeed: 1400, consumerSpeed: 350, expected: 'The buffer empties and consumers block on sem_full.' },
	{ id: 'size-one', name: 'Buffer size of one', capacity: 1, producers: 2, consumers: 2, producerSpeed: 500, consumerSpeed: 700, expected: 'At most one item is present; producers and consumers alternate through the single slot.' },
	{ id: 'full', name: 'Buffer-full blocking', capacity: 2, producers: 4, consumers: 1, producerSpeed: 250, consumerSpeed: 1600, expected: 'All free slots are consumed; one or more producers wait for sem_empty.' },
	{ id: 'empty', name: 'Buffer-empty blocking', capacity: 4, producers: 1, consumers: 4, producerSpeed: 1600, consumerSpeed: 250, expected: 'Consumers wait while sem_full has no item permits.' },
	{ id: 'multiple', name: 'Multiple producers and consumers', capacity: 8, producers: 4, consumers: 4, producerSpeed: 650, consumerSpeed: 850, expected: 'Several modeled processes compete fairly for serialized buffer access.' },
	{ id: 'race', name: 'Unsynchronized race demonstration', capacity: 6, producers: 2, consumers: 2, producerSpeed: 800, consumerSpeed: 1000, expected: 'The separate race lab deterministically demonstrates a lost update; the main buffer remains synchronized.' }
]);

export const OS_GLOSSARY = Object.freeze([
	['Semaphore', 'A counter used to coordinate access to a finite number of resources. This lab models it in JavaScript.'],
	['Mutex', 'A mutual-exclusion lock that gives one process ownership of a critical section at a time.'],
	['Critical section', 'Code that accesses shared data and must be protected from concurrent modification.'],
	['Bounded buffer', 'A fixed-capacity queue shared by producers and consumers.'],
	['Producer', 'A process that creates an item and inserts it into the shared buffer.'],
	['Consumer', 'A process that removes an item from the shared buffer.'],
	['Ready queue', 'Processes able to run but waiting for the modeled CPU scheduler.'],
	['Blocked process', 'A process unable to continue until a resource or event becomes available.'],
	['Context switch', 'A scheduler changing which process is modeled as running.'],
	['Race condition', 'A result that depends on the timing/interleaving of unsynchronized operations.'],
	['Deadlock', 'A state in which processes wait forever for resources held by one another.'],
	['Throughput', 'Completed work per unit of elapsed simulation time.'],
	['Turnaround time', 'Completion time minus arrival time for a scheduled process.'],
	['Response time', 'First CPU start time minus arrival time.'],
	['Round Robin', 'A preemptive scheduler that gives each ready process up to one time quantum.']
]);

export const VIVA_QUESTIONS = Object.freeze([
	['Why does a producer wait on sem_empty?', 'It must reserve a free slot before entering the critical section; otherwise it could overflow the buffer.'],
	['Why does a consumer wait on sem_full?', 'It must reserve an available item before reading; otherwise it could read an empty slot.'],
	['Why acquire the semaphore before the mutex?', 'A process should not hold the mutex while blocked waiting for a resource, which can prevent the process that releases it from entering.'],
	['What does the mutex protect?', 'The shared buffer and its head/tail updates from simultaneous modification.'],
	['Can a mutex be released by a non-owner?', 'No. The owner must release it; otherwise mutual exclusion is not guaranteed.'],
	['What is a race condition?', 'A timing-dependent outcome caused by unsynchronized access to shared state.'],
	['What is a lost update?', 'Two operations read the same old value and overwrite each other, losing one increment.'],
	['Are these JavaScript workers real OS threads?', 'No. They are deterministic educational process models executed by one JavaScript event loop.'],
	['What happens when the bounded buffer is full?', 'Producers block until a consumer removes an item and signals sem_empty.'],
	['What happens when the buffer is empty?', 'Consumers block until a producer inserts an item and signals sem_full.'],
	['What is the ready queue?', 'The FIFO list of modeled processes eligible to be selected by the scheduler.'],
	['How does FCFS choose a process?', 'It selects the ready process that arrived first; ties use input order.'],
	['How does non-preemptive SJF choose?', 'It selects the ready process with the shortest burst; ties use arrival then input order.'],
	['How does Round Robin work?', 'It runs each ready process for at most one quantum, then requeues unfinished processes.'],
	['Define turnaround time.', 'Completion time minus arrival time.'],
	['Define waiting time.', 'Turnaround time minus CPU burst time for these single-burst scheduling examples.'],
	['Define response time.', 'The time from arrival until the process first receives CPU time.'],
	['What is deadlock?', 'A permanent wait cycle where processes hold resources needed by one another.'],
	['Why use a buffer of size one?', 'It makes producer/consumer handoffs and full/empty blocking especially easy to observe.'],
	['What does utilization measure here?', 'The sampled fraction of buffer slots occupied in the current model state.']
]);