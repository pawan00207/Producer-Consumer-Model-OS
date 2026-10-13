import test from 'node:test';
import assert from 'node:assert/strict';
import {
	ProducerConsumerSimulation,
	PROCESS_STATES,
	experimentSummary,
	runRaceConditionDemo,
	scheduleProcesses,
	toCsv
} from '../os-lab.js';

function runSteps(simulation, count) {
	for (let index = 0; index < count; index += 1) simulation.step();
}

test('producer and consumer simulation preserves buffer and mutex invariants', () => {
	const simulation = new ProducerConsumerSimulation({ capacity: 3, producers: 3, consumers: 3 });
	runSteps(simulation, 500);
	const snapshot = simulation.snapshot();
	const occupied = snapshot.buffer.filter((item) => item !== null).length;
	assert.ok(occupied >= 0 && occupied <= snapshot.capacity);
	assert.equal(new Set(snapshot.buffer.filter((item) => item !== null)).size, occupied);
	assert.ok(snapshot.semaphores.empty.value >= 0);
	assert.ok(snapshot.semaphores.full.value >= 0);
	assert.equal(snapshot.mutex.locked, snapshot.mutex.owner !== null);
	assert.ok(snapshot.events.some((event) => event.type === 'produce'));
	assert.ok(snapshot.events.some((event) => event.type === 'consume'));
	assert.equal(simulation._assertInvariants(), true);
});

test('processes enter observable blocked states for full and empty buffers', () => {
	const simulation = new ProducerConsumerSimulation({ capacity: 1, producers: 2, consumers: 2 });
	runSteps(simulation, 240);
	const events = simulation.snapshot().events;
	assert.ok(events.some((event) => event.to === PROCESS_STATES.BLOCKED && event.reason === 'Buffer is full'));
	assert.ok(events.some((event) => event.to === PROCESS_STATES.BLOCKED && event.reason === 'Buffer is empty'));
	assert.ok(simulation.snapshot().processes.every((process) => process.history.length > 1));
});

test('stop terminates processes and reset creates a clean model', () => {
	const simulation = new ProducerConsumerSimulation();
	runSteps(simulation, 10);
	simulation.stop();
	assert.ok(simulation.snapshot().processes.every((process) => process.state === PROCESS_STATES.TERMINATED));
	assert.equal(simulation.step().advanced, false);
	const reset = simulation.reset({ capacity: 2, producers: 1, consumers: 1 });
	assert.equal(reset.snapshot().capacity, 2);
	assert.equal(reset.snapshot().events.length, 0);
});

test('step-back restores the previous simulation snapshot', () => {
	const simulation = new ProducerConsumerSimulation({ producers: 1, consumers: 1 });
	simulation.step();
	const before = simulation.snapshot();
	simulation.step();
	assert.equal(simulation.back(), true);
	assert.deepEqual(simulation.snapshot(), before);
	assert.equal(simulation.back(), true);
	assert.equal(simulation.back(), false);
});

test('race lab reproduces a lost update and mutex serializes increments', () => {
	const unsafe = runRaceConditionDemo();
	const safe = runRaceConditionDemo({ synchronized: true });
	assert.equal(unsafe.actual, 1);
	assert.equal(unsafe.lostUpdates, 1);
	assert.equal(safe.actual, 2);
	assert.equal(safe.lostUpdates, 0);
	assert.ok(unsafe.trace[0].operation.startsWith('read'));
	assert.ok(safe.trace[0].operation.startsWith('lock'));
});

test('FCFS, SJF and round robin report deterministic metrics', () => {
	const processes = [
		{ id: 'A', arrival: 0, burst: 5 },
		{ id: 'B', arrival: 1, burst: 2 },
		{ id: 'C', arrival: 2, burst: 1 }
	];
	assert.deepEqual(scheduleProcesses('FCFS', processes).gantt.map((entry) => entry.process), ['A', 'B', 'C']);
	assert.deepEqual(scheduleProcesses('SJF', processes).gantt.map((entry) => entry.process), ['A', 'C', 'B']);
	assert.deepEqual(scheduleProcesses('RR', processes, 2).gantt.map((entry) => entry.process), ['A', 'B', 'C', 'A', 'A']);
	const result = scheduleProcesses('FCFS', [{ id: 'idle', arrival: 4, burst: 2 }]);
	assert.deepEqual(result.gantt[0], { process: 'Idle', start: 0, end: 4 });
	assert.deepEqual(result.results[0], { id: 'idle', arrival: 4, burst: 2, completion: 6, turnaround: 2, waiting: 0, response: 0 });
	assert.throws(() => scheduleProcesses('RR', processes, 0), /quantum/i);
	assert.throws(() => scheduleProcesses('SJF', [{ id: 'x', arrival: -1, burst: 1 }]), /arrival/i);
});

test('experiment metrics come from recorded events and CSV escapes values', () => {
	const simulation = new ProducerConsumerSimulation({ capacity: 2, producers: 1, consumers: 1 });
	runSteps(simulation, 20);
	const snapshot = simulation.snapshot();
	const summary = experimentSummary(snapshot, simulation.metrics());
	assert.equal(summary.produced, snapshot.produced);
	assert.equal(summary.waitOperations, snapshot.events.filter((event) => event.type === 'wait').length);
	assert.match(toCsv([{ label: 'one,two', value: 'say "hi"' }]), /"one,two"/);
	assert.match(toCsv([{ label: 'one,two', value: 'say "hi"' }]), /"say ""hi"""/);
	assert.equal(toCsv([]), '');
});