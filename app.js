(() => {
	const byId = (id) => document.getElementById(id);
	const state = {
		running: false,
		buffer: [],
		size: 6,
		head: 0,
		tail: 0,
		nextItem: 1,
		produced: 0,
		consumed: 0,
		startedAt: 0,
		elapsedBeforeStart: 0,
		workers: [],
		logCount: 0
	};

	function counts() {
		return {
			producers: Number(byId('prod-count').textContent),
			consumers: Number(byId('cons-count').textContent)
		};
	}

	function addLog(type, thread, message) {
		const container = byId('log-scroll');
		const elapsed = state.running
			? state.elapsedBeforeStart + (Date.now() - state.startedAt) / 1000
			: state.elapsedBeforeStart;
		const entry = document.createElement('div');
		const time = document.createElement('span');
		const tag = document.createElement('span');
		const text = document.createElement('span');
		const minutes = Math.floor(elapsed / 60).toString().padStart(2, '0');
		const seconds = (elapsed % 60).toFixed(1).padStart(4, '0');

		entry.className = 'log-entry';
		time.className = 'log-time';
		time.textContent = `${minutes}:${seconds}`;
		tag.className = `log-tag tag-${type}`;
		tag.textContent = type === 'system' ? 'SYSTEM' : type.toUpperCase();
		text.className = 'log-msg';
		text.textContent = thread ? `${thread}: ${message}` : message;
		entry.append(time, tag, text);
		container.append(entry);
		state.logCount += 1;
		while (container.children.length > 120) container.firstElementChild.remove();
		container.scrollTop = container.scrollHeight;
	}

	function renderBuffer() {
		const track = byId('buffer-track');
		track.replaceChildren();
		for (let index = 0; index < state.size; index += 1) {
			const slot = document.createElement('div');
			const label = document.createElement('div');
			const value = document.createElement('div');
			const item = state.buffer[index];

			slot.className = `buf-slot ${item === null ? 'empty' : 'filled'}`;
			label.className = 'slot-idx';
			label.textContent = String(index);
			value.className = 'slot-val';
			value.textContent = item === null ? 'empty' : String(item);
			slot.append(label, value);
			track.append(slot);
		}
		const used = state.buffer.filter((item) => item !== null).length;
		byId('buf-meta').textContent = `${used} / ${state.size} slots used`;
		byId('ptr-head').textContent = String(state.head);
		byId('ptr-tail').textContent = String(state.tail);
		byId('stat-inbuf').textContent = String(used);
		byId('sem-empty-val').textContent = String(state.size - used);
		byId('sem-full-val').textContent = String(used);
		byId('sem-empty-bar').style.width = `${((state.size - used) / state.size) * 100}%`;
		byId('sem-full-bar').style.width = `${(used / state.size) * 100}%`;
	}

	function renderWorkers() {
		const grid = byId('threads-grid');
		grid.replaceChildren();
		for (const worker of state.workers) {
			const card = document.createElement('div');
			const header = document.createElement('div');
			const dot = document.createElement('span');
			const name = document.createElement('span');
			const status = document.createElement('span');
			const progress = document.createElement('div');
			const fill = document.createElement('div');
			const count = document.createElement('div');
			const kind = worker.kind === 'producer' ? 'producer' : 'consumer';

			card.className = `thread-card ${kind}${worker.status === 'waiting' ? ' waiting' : ''}`;
			header.className = 'thread-header';
			dot.className = 'thread-dot';
			name.className = 'thread-name';
			name.textContent = worker.name;
			status.className = `thread-state state-${worker.status}`;
			status.textContent = worker.status[0].toUpperCase() + worker.status.slice(1);
			header.append(dot, name, status);
			progress.className = 'thread-progress';
			fill.className = 'thread-progress-fill';
			fill.style.width = `${worker.progress}%`;
			progress.append(fill);
			count.className = 'thread-count';
			count.textContent = `Operations: ${worker.operations}`;
			card.append(header, progress, count);
			grid.append(card);
		}
		byId('stat-waiting').textContent = String(
			state.workers.filter((worker) => worker.status === 'waiting').length
		);
	}

	function renderStatus() {
		byId('stat-produced').textContent = String(state.produced);
		byId('stat-consumed').textContent = String(state.consumed);
		byId('elapsed-pill').textContent = `${currentElapsed().toFixed(1)}s`;
		byId('status-pill').textContent = state.running ? 'Running' : 'Stopped';
		byId('status-pill').className = `status-pill ${state.running ? 'running' : 'stopped'}`;
		byId('status-dot').classList.toggle('paused', !state.running);
		byId('btn-start').disabled = state.running;
		byId('btn-stop').disabled = !state.running;
	}

	function currentElapsed() {
		return state.elapsedBeforeStart + (state.running ? (Date.now() - state.startedAt) / 1000 : 0);
	}

	function render() {
		renderBuffer();
		renderWorkers();
		renderStatus();
	}

	function makeWorkers() {
		const configured = counts();
		state.workers = [];
		for (let index = 1; index <= configured.producers; index += 1) {
			state.workers.push({
				kind: 'producer', name: `Producer ${index}`, status: 'sleeping',
				progress: 0, operations: 0, timer: null, startedAt: 0, duration: 0
			});
		}
		for (let index = 1; index <= configured.consumers; index += 1) {
			state.workers.push({
				kind: 'consumer', name: `Consumer ${index}`, status: 'sleeping',
				progress: 0, operations: 0, timer: null, startedAt: 0, duration: 0
			});
		}
	}

	function scheduleWorker(worker, delay = 0) {
		if (!state.running) return;
		worker.timer = window.setTimeout(() => runWorker(worker), delay);
	}

	function runWorker(worker) {
		if (!state.running) return;
		const used = state.buffer.filter((item) => item !== null).length;
		const blocked = worker.kind === 'producer' ? used >= state.size : used === 0;
		if (blocked) {
			worker.status = 'waiting';
			worker.progress = 0;
			renderWorkers();
			scheduleWorker(worker, 160);
			return;
		}

		worker.status = 'running';
		worker.progress = 0;
		worker.startedAt = Date.now();
		worker.duration = Number(byId(worker.kind === 'producer' ? 'prod-speed' : 'cons-speed').value);
		worker.timer = window.setTimeout(() => completeOperation(worker), worker.duration);
	}

	function completeOperation(worker) {
		if (!state.running) return;
		const used = state.buffer.filter((item) => item !== null).length;
		if (worker.kind === 'producer' && used < state.size) {
			const item = state.nextItem++;
			state.buffer[state.tail] = item;
			state.tail = (state.tail + 1) % state.size;
			state.produced += 1;
			worker.operations += 1;
			addLog('mutex', worker.name, 'acquired mutex; enqueued item');
			addLog('produce', worker.name, `produced item ${item}`);
		} else if (worker.kind === 'consumer' && used > 0) {
			const item = state.buffer[state.head];
			state.buffer[state.head] = null;
			state.head = (state.head + 1) % state.size;
			state.consumed += 1;
			worker.operations += 1;
			addLog('mutex', worker.name, 'acquired mutex; dequeued item');
			addLog('consume', worker.name, `consumed item ${item}`);
		} else {
			worker.status = 'waiting';
			worker.progress = 0;
			render();
			scheduleWorker(worker, 160);
			return;
		}

		worker.status = 'sleeping';
		worker.progress = 0;
		render();
		scheduleWorker(worker);
	}

	window.startSim = () => {
		if (state.running) return;
		state.size = Number(byId('buf-size').value);
		state.buffer = Array(state.size).fill(null);
		state.head = 0;
		state.tail = 0;
		state.nextItem = 1;
		state.produced = 0;
		state.consumed = 0;
		state.elapsedBeforeStart = 0;
		state.startedAt = Date.now();
		state.running = true;
		makeWorkers();
		byId('log-scroll').replaceChildren();
		addLog('system', '', 'Simulation started in local JavaScript mode.');
		render();
		state.workers.forEach((worker) => scheduleWorker(worker));
	};

	window.stopSim = () => {
		if (!state.running) return;
		state.elapsedBeforeStart = currentElapsed();
		state.running = false;
		for (const worker of state.workers) {
			window.clearTimeout(worker.timer);
			worker.status = 'sleeping';
			worker.progress = 0;
		}
		addLog('system', '', 'Simulation stopped.');
		render();
	};

	window.resetSim = () => {
		if (state.running) window.stopSim();
		state.buffer = Array(Number(byId('buf-size').value)).fill(null);
		state.size = state.buffer.length;
		state.head = 0;
		state.tail = 0;
		state.nextItem = 1;
		state.produced = 0;
		state.consumed = 0;
		state.elapsedBeforeStart = 0;
		makeWorkers();
		byId('log-scroll').replaceChildren();
		addLog('system', '', 'Simulator reset and ready.');
		render();
	};

	window.adjustCount = (kind, change) => {
		if (state.running) return;
		const id = kind === 'prod' ? 'prod-count' : 'cons-count';
		const value = Math.max(1, Math.min(4, Number(byId(id).textContent) + change));
		byId(id).textContent = String(value);
		makeWorkers();
		renderWorkers();
	};

	window.switchTab = (tab, button) => {
		document.querySelectorAll('.tab-btn').forEach((element) => element.classList.remove('active'));
		document.querySelectorAll('.tab-content').forEach((element) => element.classList.remove('active'));
		button.classList.add('active');
		byId(`tab-${tab}`).classList.add('active');
	};

	byId('conn-pill').textContent = 'Local JS mode';
	byId('conn-pill').style.color = 'var(--green)';
	byId('buf-size').addEventListener('input', () => {
		if (!state.running) window.resetSim();
	});
	makeWorkers();
	state.buffer = Array(state.size).fill(null);
	render();
	window.setInterval(() => {
		if (!state.running) return;
		const now = Date.now();
		for (const worker of state.workers) {
			if (worker.status === 'running') {
				worker.progress = Math.min(100, ((now - worker.startedAt) / worker.duration) * 100);
			}
		}
		renderStatus();
		renderWorkers();
	}, 100);
})();
