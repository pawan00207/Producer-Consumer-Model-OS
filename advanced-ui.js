import {
	GUIDED_SCENARIOS,
	OS_GLOSSARY,
	PROCESS_STATES,
	ProducerConsumerSimulation,
	experimentSummary,
	runRaceConditionDemo,
	scheduleProcesses,
	toCsv,
	VIVA_QUESTIONS
} from './os-lab.js';

const byId = (id) => document.getElementById(id);
const state = {
	simulation: null,
	running: false,
	stepMode: false,
	timer: null,
	lastDashboardEvent: 0,
	chartSamples: [],
	baseline: { produced: 0, consumed: 0, time: 0, eventCount: 0 },
	rows: [
		{ id: 'A', arrival: 0, burst: 5 },
		{ id: 'B', arrival: 1, burst: 2 },
		{ id: 'C', arrival: 2, burst: 1 }
	]
};

const esc = (value) => String(value).replace(/[&<>"']/g, (char) => ({
	'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
}[char]));

function configFromControls() {
	return {
		capacity: Number(byId('buf-size').value),
		producers: Number(byId('prod-count').textContent),
		consumers: Number(byId('cons-count').textContent)
	};
}

function makeSimulation() {
	return new ProducerConsumerSimulation(configFromControls());
}

function currentProcess() {
	return state.simulation?.processes.find((process) => process.id === state.simulation.currentId) ?? null;
}

function eventTime(ms) {
	return `${(ms / 1000).toFixed(1)}s`;
}

function appendDashboardLogs() {
	const container = byId('log-scroll');
	const events = state.simulation.events;
	if (state.lastDashboardEvent > events.length) state.lastDashboardEvent = 0;
	if (state.lastDashboardEvent === 0) container.replaceChildren();
	for (const event of events.slice(state.lastDashboardEvent)) {
		const entry = document.createElement('div');
		const time = document.createElement('span');
		const tag = document.createElement('span');
		const message = document.createElement('span');
		const type = event.type === 'produce' ? 'produce' : event.type === 'consume' ? 'consume' :
			['wait', 'signal'].includes(event.type) ? 'wait' : event.type === 'lock' || event.type === 'unlock' ? 'mutex' : 'system';
		entry.className = 'log-entry';
		time.className = 'log-time';
		time.textContent = eventTime(event.time);
		tag.className = `log-tag tag-${type}`;
		tag.textContent = event.type.toUpperCase();
		message.className = 'log-msg';
		message.textContent = `${event.process}: ${event.message}`;
		entry.append(time, tag, message);
		container.append(entry);
	}
	state.lastDashboardEvent = events.length;
	while (container.children.length > 120) container.firstElementChild.remove();
	container.scrollTop = container.scrollHeight;
}

function renderOriginalDashboard() {
	const snapshot = state.simulation.snapshot();
	const metrics = state.simulation.metrics();
	const track = byId('buffer-track');
	track.replaceChildren();
	snapshot.buffer.forEach((item, index) => {
		const slot = document.createElement('div');
		const label = document.createElement('div');
		const value = document.createElement('div');
		slot.className = `buf-slot ${item === null ? 'empty' : 'filled'}`;
		label.className = 'slot-idx';
		label.textContent = String(index);
		value.className = 'slot-val';
		value.textContent = item === null ? 'empty' : String(item);
		slot.append(label, value);
		track.append(slot);
	});
	const occupied = snapshot.buffer.filter((item) => item !== null).length;
	byId('buf-meta').textContent = `${occupied} / ${snapshot.capacity} slots used`;
	byId('ptr-head').textContent = String(snapshot.head);
	byId('ptr-tail').textContent = String(snapshot.tail);
	byId('stat-produced').textContent = String(snapshot.produced);
	byId('stat-consumed').textContent = String(snapshot.consumed);
	byId('stat-inbuf').textContent = String(occupied);
	byId('stat-waiting').textContent = String(snapshot.blockedQueue.length);
	byId('sem-empty-val').textContent = String(snapshot.semaphores.empty.value);
	byId('sem-full-val').textContent = String(snapshot.semaphores.full.value);
	byId('sem-empty-bar').style.width = `${snapshot.semaphores.empty.value / snapshot.capacity * 100}%`;
	byId('sem-full-bar').style.width = `${snapshot.semaphores.full.value / snapshot.capacity * 100}%`;
	byId('mutex-val').textContent = snapshot.mutex.locked ? '0' : '1';
	byId('mutex-bar').style.width = snapshot.mutex.locked ? '0%' : '100%';
	byId('lock-icon').textContent = snapshot.mutex.locked ? '🔒' : '🔓';
	byId('lock-status').textContent = snapshot.mutex.locked ? 'Locked' : 'Unlocked';
	byId('lock-status').style.color = snapshot.mutex.locked ? 'var(--amber)' : 'var(--green)';
	byId('lock-holder').textContent = snapshot.mutex.owner
		? `Owned by ${snapshot.processes.find((process) => process.id === snapshot.mutex.owner)?.name ?? snapshot.mutex.owner}`
		: 'Available for any process';
	byId('elapsed-pill').textContent = `${(snapshot.clock / 1000).toFixed(1)}s`;
	const statusText = state.running ? 'Running' : snapshot.stopped ? 'Stopped' : snapshot.clock ? 'Paused' : 'Ready';
	byId('status-pill').textContent = statusText;
	byId('status-pill').className = `status-pill ${state.running ? 'running' : snapshot.stopped ? 'stopped' : 'ready'}`;
	const activeCount = snapshot.processes.filter((process) => process.state === PROCESS_STATES.RUNNING).length;
	byId('overview-status').textContent = state.running ? 'Running' : snapshot.stopped ? 'Stopped' : snapshot.clock ? 'Paused' : 'Ready';
	byId('overview-runtime').textContent = `${(snapshot.clock / 1000).toFixed(1)} seconds elapsed`;
	byId('overview-produced').textContent = String(snapshot.produced);
	byId('overview-consumed').textContent = String(snapshot.consumed);
	byId('overview-utilization').textContent = `${(metrics.bufferUtilization * 100).toFixed(0)}%`;
	byId('overview-buffer-count').textContent = `${occupied} of ${snapshot.capacity} slots occupied`;
	byId('overview-active').textContent = String(activeCount);
	byId('overview-worker-count').textContent = `${snapshot.processes.length} configured processes`;
	byId('overview-blocked').textContent = String(snapshot.blockedQueue.length);
	byId('overview-feedback').textContent = state.running ? 'Simulation is running.' : snapshot.stopped ? 'Simulation stopped. Start again to create a fresh run.' : snapshot.clock ? 'Simulation paused. Resume to continue.' : 'Simulation is ready.';
	byId('overview-start').disabled = state.running;
	byId('overview-pause').disabled = !state.running;
	byId('overview-resume').disabled = state.running || snapshot.stopped || snapshot.clock === 0;
	byId('status-dot').classList.toggle('paused', !state.running);
	byId('btn-start').disabled = state.running;
	byId('btn-stop').disabled = !state.running;
	byId('buf-size').disabled = state.running;
	document.querySelectorAll('.counter-btn').forEach((button) => { button.disabled = state.running; });

	const grid = byId('threads-grid');
	grid.replaceChildren();
	for (const process of snapshot.processes) {
		const card = document.createElement('div');
		card.className = `thread-card ${process.kind}${process.state === PROCESS_STATES.BLOCKED ? ' waiting' : ''}${process.state === PROCESS_STATES.RUNNING ? ` active-${process.kind}` : ''}`;
		const header = document.createElement('div');
		header.className = 'thread-header';
		const dot = document.createElement('span');
		dot.className = 'thread-dot';
		const name = document.createElement('span');
		name.className = 'thread-name';
		name.textContent = process.name;
		const status = document.createElement('span');
		status.className = `thread-state ${process.state === PROCESS_STATES.RUNNING ? 'state-running' : process.state === PROCESS_STATES.BLOCKED ? 'state-waiting' : 'state-sleeping'}`;
		status.textContent = process.state;
		header.append(dot, name, status);
		const progress = document.createElement('div');
		progress.className = 'thread-progress';
		const fill = document.createElement('div');
		fill.className = 'thread-progress-fill';
		fill.style.width = process.state === PROCESS_STATES.RUNNING ? '100%' : '0%';
		progress.append(fill);
		const count = document.createElement('div');
		count.className = 'thread-count';
		const reason = process.reason || (process.state === PROCESS_STATES.NEW ? 'Awaiting scheduler admission' : 'No blocking reason');
		count.textContent = `${reason} · Operations: ${process.operations}`;
		card.append(header, progress, count);
		grid.append(card);
	}
	appendDashboardLogs();
	const recent = byId('recent-activity');
	recent.replaceChildren();
	const recentEvents = snapshot.events.slice(-5).reverse();
	if (recentEvents.length === 0) {
		const item = document.createElement('li');
		item.textContent = 'Simulator initialized. Start a run to record events.';
		recent.append(item);
	} else {
		for (const event of recentEvents) {
			const item = document.createElement('li');
			item.textContent = `${eventTime(event.time)} · ${event.process}: ${event.message}`;
			recent.append(item);
		}
	}
	renderProcessLab(snapshot);
	renderAnalytics(snapshot, metrics);
}

function renderProcessLab(snapshot) {
	byId('lab-clock').textContent = `${snapshot.clock} ms`;
	byId('ready-queue').textContent = snapshot.readyQueue.length
		? snapshot.readyQueue.map((id) => snapshot.processes.find((process) => process.id === id)?.name ?? id).join(' → ')
		: 'Empty';
	byId('blocked-queue').textContent = snapshot.blockedQueue.length
		? snapshot.blockedQueue.map((id) => snapshot.processes.find((process) => process.id === id)?.name ?? id).join(' → ')
		: 'Empty';
	const list = byId('process-list');
	list.replaceChildren();
	for (const process of snapshot.processes) {
		const item = document.createElement('button');
		item.type = 'button';
		item.className = `process-row${byId('process-select').value === process.id ? ' selected' : ''}`;
		item.dataset.process = process.id;
		item.setAttribute('aria-pressed', String(byId('process-select').value === process.id));
		const reason = process.reason || (process.state === PROCESS_STATES.NEW ? 'Awaiting scheduler admission' : 'No blocking reason');
		item.innerHTML = `<strong>${esc(process.name)}</strong><span class="process-state">${esc(process.state)}</span><span class="process-reason">${esc(reason)}</span>`;
		list.append(item);
	}
	const select = byId('process-select');
	const selectedBefore = select.value;
	select.replaceChildren(...snapshot.processes.map((process) => {
		const option = document.createElement('option');
		option.value = process.id;
		option.textContent = process.name;
		return option;
	}));
	if (snapshot.processes.some((process) => process.id === selectedBefore)) select.value = selectedBefore;
	else if (snapshot.processes.length) select.value = snapshot.processes[0].id;
	const selected = snapshot.processes.find((process) => process.id === select.value);
	const history = byId('process-history');
	history.replaceChildren();
	for (const transition of selected?.history ?? []) {
		const row = document.createElement('li');
		row.textContent = `${transition.time} ms · ${transition.state}${transition.reason ? ` · ${transition.reason}` : ''}`;
		history.append(row);
	}
	const sem = snapshot.semaphores;
	const sync = byId('sync-values');
	sync.innerHTML = [
		['sem_empty', sem.empty.value, `Waiters: ${sem.empty.waiters.length}`],
		['sem_full', sem.full.value, `Waiters: ${sem.full.waiters.length}`],
		['mutex', snapshot.mutex.locked ? 'Locked' : 'Available', snapshot.mutex.owner ? `Owner: ${snapshot.processes.find((process) => process.id === snapshot.mutex.owner)?.name ?? snapshot.mutex.owner}` : `Waiters: ${snapshot.mutex.waiters.length}`]
	].map(([label, value, detail]) => `<div class="inspector-item"><strong>${esc(label)}</strong><span>${esc(value)}</span><small>${esc(detail)}</small></div>`).join('') +
		`<p class="permit-note">Permits in progress: ${snapshot.inFlightEmpty} reserved empty slot(s), ${snapshot.inFlightFull} reserved item(s).</p>`;
	const timeline = byId('operation-timeline');
	timeline.replaceChildren();
	for (const event of snapshot.events.slice(-30).reverse()) {
		const row = document.createElement('li');
		row.innerHTML = `<time>${event.time} ms</time><strong>${esc(event.type)}</strong><span>${esc(event.process)}: ${esc(event.message)}</span>${event.explanation ? `<small>${esc(event.explanation)}</small>` : ''}`;
		timeline.append(row);
	}
	byId('lab-back').disabled = state.simulation.undoStack.length === 0 || state.running;
	byId('lab-pause').disabled = !state.running;
	byId('lab-play').disabled = state.running;
}

function renderAnalytics(snapshot, metrics) {
	const counts = {
		produced: snapshot.produced - state.baseline.produced,
		consumed: snapshot.consumed - state.baseline.consumed,
		time: snapshot.clock - state.baseline.time
	};
	const rows = [
		['Produced', counts.produced],
		['Consumed', counts.consumed],
		['Throughput', counts.time > 0 ? `${((counts.produced + counts.consumed) / counts.time * 1000).toFixed(2)} ops/s` : '0.00 ops/s'],
		['Buffer utilization', `${(metrics.bufferUtilization * 100).toFixed(1)}%`],
		['Average blocked duration', `${blockedDurationSinceBaseline(snapshot).averageMs.toFixed(0)} ms/episode`],
		['Producer / consumer operations', `${counts.produced} / ${counts.consumed}`]
	];
	byId('analytics-metrics').innerHTML = rows.map(([label, value]) => `<div class="analytics-metric"><span>${esc(label)}</span><strong>${esc(value)}</strong></div>`).join('');
	if (state.chartSamples.at(-1)?.time !== snapshot.clock) {
		state.chartSamples.push({ time: snapshot.clock, produced: counts.produced, consumed: counts.consumed, utilization: metrics.bufferUtilization, blocked: snapshot.blockedQueue.length });
		if (state.chartSamples.length > 60) state.chartSamples.shift();
	}
	drawChart(byId('activity-chart'), [
		{ label: 'Produced', key: 'produced', color: '#3ddc84' },
		{ label: 'Consumed', key: 'consumed', color: '#ff5b5b' }
	], state.chartSamples);
	drawChart(byId('utilization-chart'), [
		{ label: 'Buffer %', key: 'utilization', color: '#5b9cf6', percent: true },
		{ label: 'Blocked', key: 'blocked', color: '#fca63c' }
	], state.chartSamples);
	const events = snapshot.events.slice(state.baseline.eventCount);
	const blocked = blockedDurationSinceBaseline(snapshot);
	const summary = experimentSummary({ ...snapshot, events, produced: counts.produced, consumed: counts.consumed }, {
		...metrics,
		produced: counts.produced,
		consumed: counts.consumed,
		elapsedMs: Math.max(0, counts.time),
		averageBlockedMs: blocked.averageMs,
		producerOperations: counts.produced,
		consumerOperations: counts.consumed
	});
	byId('experiment-summary').innerHTML = Object.entries(summary)
		.map(([key, value]) => `<tr><th>${esc(key)}</th><td>${esc(typeof value === 'number' && key === 'bufferUtilization' ? `${(value * 100).toFixed(1)}%` : value)}</td></tr>`).join('');
}

function blockedDurationSinceBaseline(snapshot) {
	const starts = new Map(Object.entries(state.baseline.blockedSince ?? {}));
	let totalMs = 0;
	let episodes = 0;
	for (const event of snapshot.events.slice(state.baseline.eventCount)) {
		if (!event.processId || event.type !== 'state') continue;
		if (event.to === PROCESS_STATES.BLOCKED) {
			starts.set(event.processId, Math.max(event.time, state.baseline.time));
		} else if (event.from === PROCESS_STATES.BLOCKED && starts.has(event.processId)) {
			totalMs += Math.max(0, event.time - starts.get(event.processId));
			episodes += 1;
			starts.delete(event.processId);
		}
	}
	for (const [id, start] of starts) {
		const process = snapshot.processes.find((item) => item.id === id);
		if (process?.state === PROCESS_STATES.BLOCKED) {
			totalMs += Math.max(0, snapshot.clock - Number(start));
			episodes += 1;
		}
	}
	return { totalMs, episodes, averageMs: episodes ? totalMs / episodes : 0 };
}

function drawChart(canvas, series, samples) {
	const ratio = window.devicePixelRatio || 1;
	const width = Math.max(canvas.clientWidth, 280);
	const height = Number(canvas.getAttribute('height')) || 220;
	canvas.width = width * ratio;
	canvas.height = height * ratio;
	const context = canvas.getContext('2d');
	if (!context) return;
	context.setTransform(ratio, 0, 0, ratio, 0, 0);
	context.clearRect(0, 0, width, height);
	const margin = { left: 34, right: 12, top: 18, bottom: 30 };
	const chartWidth = width - margin.left - margin.right;
	const chartHeight = height - margin.top - margin.bottom;
	context.font = '11px Consolas, monospace';
	context.strokeStyle = 'rgba(255,255,255,.12)';
	context.fillStyle = '#8b90a8';
	for (let line = 0; line <= 4; line += 1) {
		const y = margin.top + chartHeight * line / 4;
		context.beginPath();
		context.moveTo(margin.left, y);
		context.lineTo(width - margin.right, y);
		context.stroke();
		context.fillText(series.some((item) => item.percent) ? `${100 - line * 25}%` : String(line), 2, y + 4);
	}
	for (let index = 0; index < series.length; index += 1) {
		const item = series[index];
		context.strokeStyle = item.color;
		context.lineWidth = 2;
		context.beginPath();
		const max = item.percent ? 1 : Math.max(1, ...samples.map((sample) => sample[item.key]));
		samples.forEach((sample, sampleIndex) => {
			const x = margin.left + (samples.length < 2 ? 0 : chartWidth * sampleIndex / (samples.length - 1));
			const normalized = Math.min(1, sample[item.key] / max);
			const y = margin.top + chartHeight * (1 - normalized);
			if (sampleIndex === 0) context.moveTo(x, y);
			else context.lineTo(x, y);
		});
		context.stroke();
		const legendX = margin.left + index * 110;
		context.fillStyle = item.color;
		context.fillRect(legendX, height - 15, 10, 3);
		context.fillText(item.label, legendX + 15, height - 11);
	}
}

function setRunning(running) {
	state.running = running;
	if (state.timer) window.clearTimeout(state.timer);
	state.timer = null;
	if (running && !state.stepMode) scheduleAutoStep();
	renderOriginalDashboard();
}

function scheduleAutoStep() {
	if (!state.running || state.stepMode) return;
	const process = currentProcess();
	const speedId = process?.kind === 'consumer' ? 'cons-speed' : 'prod-speed';
	const delay = Math.max(35, Number(byId(speedId).value) / 6);
	state.timer = window.setTimeout(() => {
		advanceOne();
		if (state.running) scheduleAutoStep();
	}, delay);
}

function advanceOne() {
	if (!state.simulation || state.simulation.stopped) return;
	const result = state.simulation.step();
	if (!result.advanced && result.reason) {
		byId('presentation-detail').textContent = result.reason;
		return;
	}
	renderOriginalDashboard();
	const event = result.event;
	if (event) {
		byId('presentation-current').textContent = `${event.process} · ${event.type}`;
		byId('presentation-detail').textContent = event.explanation || event.message;
	}
}

function resetSimulation() {
	if (state.timer) window.clearTimeout(state.timer);
	state.timer = null;
	state.running = false;
	state.simulation = makeSimulation();
	state.lastDashboardEvent = 0;
	state.chartSamples = [];
	state.baseline = { produced: 0, consumed: 0, time: 0, eventCount: 0 };
	renderOriginalDashboard();
}

function startSimulation() {
	if (state.running) return;
	if (state.simulation.stopped) resetSimulation();
	setRunning(true);
}

function stopSimulation() {
	if (state.timer) window.clearTimeout(state.timer);
	state.timer = null;
	state.running = false;
	state.simulation.stop();
	renderOriginalDashboard();
}

function useTab(name) {
	document.querySelectorAll('.lab-tab').forEach((tab) => {
		const selected = tab.dataset.lab === name;
		tab.classList.toggle('active', selected);
		tab.setAttribute('aria-selected', String(selected));
		tab.tabIndex = selected ? 0 : -1;
	});
	document.querySelectorAll('.lab-view').forEach((view) => {
		const selected = view.id === `lab-${name}`;
		view.classList.toggle('active', selected);
		view.hidden = !selected;
	});
	if (name === 'analytics') renderOriginalDashboard();
}

const ROUTES = {
	dashboard: { title: 'Overview', kind: 'dashboard' },
	simulator: { title: 'Producer–Consumer Simulator', kind: 'simulator' },
	buffer: { title: 'Shared Circular Buffer', kind: 'simulator', target: '#buffer-track' },
	settings: { title: 'Simulation Configuration', kind: 'simulator', target: '#ctrl-panel' },
	process: { title: 'Process State Management', kind: 'lab', tab: 'process' },
	semaphore: { title: 'Semaphore Inspector', kind: 'lab', tab: 'process', target: '#sync-values' },
	mutex: { title: 'Mutex / Critical Section Lab', kind: 'lab', tab: 'process', target: '#sync-values' },
	race: { title: 'Race Condition Lab', kind: 'lab', tab: 'race' },
	flow: { title: 'Synchronization Flow', kind: 'simulator', target: '#flow-graphic' },
	logs: { title: 'Live Event Logs', kind: 'simulator', tab: 'logs', target: '#log-scroll' },
	analytics: { title: 'Performance Analytics', kind: 'lab', tab: 'analytics' },
	guided: { title: 'Guided Experiments', kind: 'lab', tab: 'guided' },
	scheduling: { title: 'CPU Scheduling Lab', kind: 'lab', tab: 'scheduling' },
	queue: { title: 'Process Queue Viewer', kind: 'lab', tab: 'process', target: '#ready-queue' },
	docs: { title: 'Documentation & OS Concepts', kind: 'simulator', tab: 'docs', target: '#tab-docs' },
	viva: { title: 'Viva Practice', kind: 'lab', tab: 'presentation', target: '#viva-list' },
	presentation: { title: 'Presentation Mode', kind: 'lab', tab: 'presentation' },
	about: { title: 'About 404 Invalid Team', kind: 'about' }
};

function navigateToCurrentHash() {
	const requested = location.hash.replace(/^#\/?/, '') || 'dashboard';
	const route = ROUTES[requested] ? requested : 'dashboard';
	if (route !== requested) history.replaceState(null, '', '#/dashboard');
	const definition = ROUTES[route];
	const dashboard = definition.kind === 'dashboard';
	byId('dashboard-view').hidden = !dashboard;
	byId('workspace-view').hidden = dashboard;
	byId('route-breadcrumb').textContent = definition.title;
	byId('workspace-title').textContent = definition.title;
	document.title = `${definition.title} · OS Synchronization Lab`;
	document.querySelectorAll('.sidebar-link[data-route]').forEach((link) => {
		const selected = link.dataset.route === route;
		link.classList.toggle('active', selected);
		if (selected) link.setAttribute('aria-current', 'page');
		else link.removeAttribute('aria-current');
	});
	const layout = document.querySelector('#workspace-view .layout');
	const suite = document.querySelector('#workspace-view .lab-suite');
	const about = byId('about-view');
	layout.hidden = dashboard || definition.kind !== 'simulator';
	suite.hidden = dashboard || definition.kind !== 'lab';
	about.hidden = dashboard || definition.kind !== 'about';
	if (definition.tab) {
		if (definition.kind === 'lab') useTab(definition.tab);
		else {
			const button = document.querySelector(`.tabs-bar .tab-btn[onclick*="${definition.tab}"]`);
			if (button) window.switchTab(definition.tab, button);
		}
	}
	if (route === 'viva') byId('viva-list').open = true;
	if (definition.target && !dashboard) {
		window.requestAnimationFrame(() => byId('workspace-view').querySelector(definition.target)?.scrollIntoView({ block: 'center', behavior: 'smooth' }));
	} else window.scrollTo({ top: 0, behavior: 'smooth' });
}

function runRaceLab() {
	const result = runRaceConditionDemo({ synchronized: byId('race-mutex').checked });
	byId('race-result').textContent = result.lostUpdates
		? `Expected ${result.expected}; actual ${result.actual}. Lost updates: ${result.lostUpdates}.`
		: `Expected ${result.expected}; actual ${result.actual}. Mutex prevented lost updates.`;
	byId('race-trace').replaceChildren(...result.trace.map((operation) => {
		const row = document.createElement('li');
		row.textContent = `${operation.actor}: ${operation.operation} (shared = ${operation.observed})`;
		return row;
	}));
}

function renderScheduleInputs() {
	const body = byId('sched-inputs');
	body.replaceChildren(...state.rows.map((process, index) => {
		const row = document.createElement('tr');
		row.innerHTML = `<td><label class="sr-only" for="sched-id-${index}">Process ID ${index + 1}</label><input id="sched-id-${index}" data-row="${index}" data-field="id" value="${esc(process.id)}" maxlength="12"></td><td><label class="sr-only" for="sched-arrival-${index}">Arrival time for ${esc(process.id)}</label><input id="sched-arrival-${index}" data-row="${index}" data-field="arrival" type="number" min="0" step="1" value="${process.arrival}"></td><td><label class="sr-only" for="sched-burst-${index}">Burst time for ${esc(process.id)}</label><input id="sched-burst-${index}" data-row="${index}" data-field="burst" type="number" min="1" step="1" value="${process.burst}"></td><td><button class="lab-action sched-remove" data-row="${index}" aria-label="Remove ${esc(process.id)}">Remove</button></td>`;
		return row;
	}));
}

function runScheduler() {
	const error = byId('sched-error');
	error.textContent = '';
	try {
		const algorithm = byId('sched-algorithm').value;
		const result = scheduleProcesses(algorithm, state.rows, Number(byId('sched-quantum').value));
		const gantt = byId('gantt-chart');
		gantt.replaceChildren(...result.gantt.map((block) => {
			const segment = document.createElement('div');
			segment.className = `gantt-segment${block.process === 'Idle' ? ' idle' : ''}`;
			segment.style.flexGrow = String(block.end - block.start);
			segment.setAttribute('aria-label', `${block.process}, ${block.start} to ${block.end}`);
			segment.innerHTML = `<strong>${esc(block.process)}</strong><span>${block.start}–${block.end}</span>`;
			return segment;
		}));
		byId('sched-results').innerHTML = result.results.map((row) => `<tr><th>${esc(row.id)}</th><td>${row.completion}</td><td>${row.turnaround}</td><td>${row.waiting}</td><td>${row.response}</td></tr>`).join('');
		byId('sched-ready').replaceChildren(...result.readyEvolution.map((entry) => {
			const li = document.createElement('li');
			li.textContent = `t=${entry.time}: selected ${entry.selected}; ready queue [${entry.queue.join(', ') || 'empty'}]`;
			return li;
		}));
		byId('sched-explanation').textContent = algorithm === 'FCFS'
			? 'FCFS selects the earliest arrival; equal arrival times preserve input order.'
			: algorithm === 'SJF'
				? 'Non-preemptive SJF selects the shortest burst among arrived processes; ties use arrival time then input order.'
				: `Round Robin gives each ready process up to ${result.quantum} time units; unfinished processes return to the queue after arrivals during the slice.`;
	} catch (exception) {
		error.textContent = exception.message;
	}
}

function downloadCsv(filename, contents) {
	const blob = new Blob([contents], { type: 'text/csv;charset=utf-8' });
	const url = URL.createObjectURL(blob);
	const anchor = document.createElement('a');
	anchor.href = url;
	anchor.download = filename;
	document.body.append(anchor);
	anchor.click();
	anchor.remove();
	window.setTimeout(() => URL.revokeObjectURL(url), 0);
}

function exportExperiment() {
	const snapshot = state.simulation.snapshot();
	const metrics = state.simulation.metrics();
	const events = snapshot.events.slice(state.baseline.eventCount);
	const produced = snapshot.produced - state.baseline.produced;
	const consumed = snapshot.consumed - state.baseline.consumed;
	const elapsedMs = Math.max(0, snapshot.clock - state.baseline.time);
	const blocked = blockedDurationSinceBaseline(snapshot);
	const rows = [
		{ record: 'summary', ...experimentSummary({ ...snapshot, events, produced, consumed }, {
			...metrics,
			produced,
			consumed,
			elapsedMs,
			averageBlockedMs: blocked.averageMs,
			producerOperations: produced,
			consumerOperations: consumed
		}) },
		...events.map((event) => ({ record: 'event', timeMs: event.time, type: event.type, process: event.process, message: event.message, resource: event.resource ?? '', value: event.value ?? '' }))
	];
	downloadCsv('os-synchronization-experiment.csv', toCsv(rows));
}

function initializeAdditions() {
	document.querySelectorAll('.lab-tab').forEach((tab) => {
		tab.addEventListener('click', () => useTab(tab.dataset.lab));
	});
	document.querySelector('.lab-tabs').addEventListener('keydown', (event) => {
		if (!['ArrowRight', 'ArrowLeft', 'Home', 'End'].includes(event.key)) return;
		const tabs = [...document.querySelectorAll('.lab-tab')];
		const current = tabs.indexOf(document.activeElement);
		const next = event.key === 'Home' ? 0 : event.key === 'End' ? tabs.length - 1 :
			(current + (event.key === 'ArrowRight' ? 1 : -1) + tabs.length) % tabs.length;
		event.preventDefault();
		tabs[next].focus();
		tabs[next].click();
	});
	byId('lab-play').addEventListener('click', startSimulation);
	byId('lab-pause').addEventListener('click', () => setRunning(false));
	byId('lab-step').addEventListener('click', advanceOne);
	byId('lab-back').addEventListener('click', () => {
		if (state.simulation.back()) renderOriginalDashboard();
	});
	byId('lab-reset').addEventListener('click', resetSimulation);
	byId('overview-start').addEventListener('click', startSimulation);
	byId('overview-pause').addEventListener('click', () => setRunning(false));
	byId('overview-resume').addEventListener('click', startSimulation);
	byId('overview-reset').addEventListener('click', resetSimulation);
	byId('sidebar-toggle').addEventListener('click', () => {
		const shell = document.querySelector('.app-shell');
		const expanded = byId('sidebar-toggle').getAttribute('aria-expanded') === 'true';
		shell.classList.toggle('sidebar-collapsed', expanded);
		byId('sidebar-toggle').setAttribute('aria-expanded', String(!expanded));
		byId('sidebar-toggle').setAttribute('aria-label', expanded ? 'Expand navigation' : 'Collapse navigation');
		byId('sidebar-toggle').title = expanded ? 'Expand navigation' : 'Collapse navigation';
	});
	byId('lab-step-mode').addEventListener('change', (event) => {
		state.stepMode = event.target.checked;
		if (state.running) setRunning(true);
	});
	byId('process-select').addEventListener('change', () => renderProcessLab(state.simulation.snapshot()));
	byId('process-list').addEventListener('click', (event) => {
		const button = event.target.closest('[data-process]');
		if (button) {
			byId('process-select').value = button.dataset.process;
			renderProcessLab(state.simulation.snapshot());
		}
	});
	byId('race-run').addEventListener('click', runRaceLab);
	byId('sched-add').addEventListener('click', () => {
		const next = String.fromCharCode(65 + state.rows.length);
		state.rows.push({ id: state.rows.some((row) => row.id === next) ? `P${state.rows.length + 1}` : next, arrival: 0, burst: 1 });
		renderScheduleInputs();
	});
	byId('sched-inputs').addEventListener('input', (event) => {
		const input = event.target.closest('[data-row][data-field]');
		if (!input) return;
		const row = state.rows[Number(input.dataset.row)];
		row[input.dataset.field] = input.dataset.field === 'id' ? input.value : input.value === '' ? '' : Number(input.value);
	});
	byId('sched-inputs').addEventListener('click', (event) => {
		const button = event.target.closest('.sched-remove');
		if (!button || state.rows.length === 1) return;
		state.rows.splice(Number(button.dataset.row), 1);
		renderScheduleInputs();
	});
	byId('sched-run').addEventListener('click', runScheduler);
	byId('analytics-reset').addEventListener('click', () => {
		const snapshot = state.simulation.snapshot();
		state.baseline = {
			produced: snapshot.produced,
			consumed: snapshot.consumed,
			time: snapshot.clock,
			eventCount: snapshot.events.length,
			blockedSince: Object.fromEntries(snapshot.processes
				.filter((process) => process.state === PROCESS_STATES.BLOCKED)
				.map((process) => [process.id, snapshot.clock]))
		};
		state.chartSamples = [];
		renderOriginalDashboard();
	});
	byId('analytics-export').addEventListener('click', exportExperiment);
	byId('scenario-select').replaceChildren(...GUIDED_SCENARIOS.map((scenario) => {
		const option = document.createElement('option');
		option.value = scenario.id;
		option.textContent = scenario.name;
		return option;
	}));
	const updateScenarioDescription = () => {
		const scenario = GUIDED_SCENARIOS.find((item) => item.id === byId('scenario-select').value);
		byId('scenario-outcome').textContent = scenario?.expected ?? '';
	};
	byId('scenario-select').addEventListener('change', updateScenarioDescription);
	byId('scenario-apply').addEventListener('click', () => {
		const scenario = GUIDED_SCENARIOS.find((item) => item.id === byId('scenario-select').value);
		if (!scenario) return;
		byId('buf-size').value = String(scenario.capacity);
		byId('buf-size-val').textContent = String(scenario.capacity);
		byId('prod-count').textContent = String(scenario.producers);
		byId('cons-count').textContent = String(scenario.consumers);
		byId('prod-speed').value = String(scenario.producerSpeed);
		byId('cons-speed').value = String(scenario.consumerSpeed);
		byId('prod-speed-val').textContent = `${(scenario.producerSpeed / 1000).toFixed(1)}s`;
		byId('cons-speed-val').textContent = `${(scenario.consumerSpeed / 1000).toFixed(1)}s`;
		resetSimulation();
		if (scenario.id === 'race') {
			byId('race-mutex').checked = false;
			runRaceLab();
			useTab('race');
		} else useTab('process');
	});
	byId('presentation-fullscreen').addEventListener('click', async () => {
		try {
			if (!document.fullscreenElement) await byId('lab-presentation').requestFullscreen();
			else await document.exitFullscreen();
		} catch {
			byId('presentation-detail').textContent = 'Full-screen mode is unavailable in this browser context.';
		}
	});
	byId('presentation-start').addEventListener('click', startSimulation);
	byId('presentation-pause').addEventListener('click', () => setRunning(false));
	byId('presentation-reset').addEventListener('click', resetSimulation);
	byId('presentation-next').addEventListener('click', advanceOne);
	byId('presentation-previous').addEventListener('click', () => {
		if (state.simulation.back()) renderOriginalDashboard();
	});
	byId('presentation-export').addEventListener('click', exportExperiment);
	byId('viva-list').querySelector('dl').innerHTML = VIVA_QUESTIONS.map(([question, answer]) => `<dt>${esc(question)}</dt><dd>${esc(answer)}</dd>`).join('');
	byId('glossary-list').innerHTML = OS_GLOSSARY.map(([term, explanation]) => `<dt>${esc(term)}</dt><dd>${esc(explanation)}</dd>`).join('');
	renderScheduleInputs();
	updateScenarioDescription();
}

window.startSim = startSimulation;
window.stopSim = stopSimulation;
window.resetSim = resetSimulation;
window.adjustCount = (kind, change) => {
	if (state.running) return;
	const id = kind === 'prod' ? 'prod-count' : 'cons-count';
	byId(id).textContent = String(Math.max(1, Math.min(4, Number(byId(id).textContent) + change)));
	resetSimulation();
};
window.switchTab = (tab, button) => {
	document.querySelectorAll('.tabs-bar .tab-btn').forEach((element) => element.classList.remove('active'));
	document.querySelectorAll('.panel .tab-content').forEach((element) => element.classList.remove('active'));
	button.classList.add('active');
	byId(`tab-${tab}`).classList.add('active');
};

byId('conn-pill').textContent = 'Educational model';
byId('conn-pill').style.color = 'var(--amber)';
byId('buf-size').addEventListener('input', () => {
	byId('buf-size-val').textContent = byId('buf-size').value;
	if (!state.running) resetSimulation();
});
byId('prod-speed').addEventListener('input', () => {
	byId('prod-speed-val').textContent = `${(Number(byId('prod-speed').value) / 1000).toFixed(1)}s`;
});
byId('cons-speed').addEventListener('input', () => {
	byId('cons-speed-val').textContent = `${(Number(byId('cons-speed').value) / 1000).toFixed(1)}s`;
});

initializeAdditions();
state.simulation = makeSimulation();
renderOriginalDashboard();
window.addEventListener('hashchange', navigateToCurrentHash);
if (!location.hash) history.replaceState(null, '', '#/dashboard');
navigateToCurrentHash();