(function () {
    "use strict";

    const MAX_HISTORY_PER_CONSUMER = 20;
    const TRAVEL_LONG_MS = 1900;
    const TRAVEL_SHORT_MS = 1300;

    const state = {
        partitions: 0,
        consumers: new Map(), // id -> {partitions: [], history: [{partition, offset}]}
        laneColors: [],
    };

    const PALETTE = ["#38bdf8", "#f97316", "#a78bfa", "#34d399", "#fbbf24", "#f472b6", "#60a5fa", "#4ade80"];

    const el = {
        connDot: document.getElementById("conn-dot"),
        connLabel: document.getElementById("conn-label"),
        producerToggle: document.getElementById("btn-producer-toggle"),
        interval: document.getElementById("input-interval"),
        applyInterval: document.getElementById("btn-apply-interval"),
        errorRate: document.getElementById("input-error-rate"),
        errorRateLabel: document.getElementById("label-error-rate"),
        triggerError: document.getElementById("btn-trigger-error"),
        partitionsLabel: document.getElementById("label-partitions"),
        consumerCountLabel: document.getElementById("label-consumer-count"),
        consumerMinus: document.getElementById("btn-consumer-minus"),
        consumerPlus: document.getElementById("btn-consumer-plus"),
        topicName: document.getElementById("topic-name"),
        lanes: document.getElementById("lanes"),
        consumers: document.getElementById("consumers"),
        producerBox: document.getElementById("producer-box"),
        dltBox: document.getElementById("dlt-box"),
        stage: document.getElementById("stage"),
        log: document.getElementById("log"),
    };

    let producerRunning = false;

    // ---------- API helpers ----------

    function api(method, url, body) {
        return fetch(url, {
            method,
            headers: { "Content-Type": "application/json" },
            body: body !== undefined ? JSON.stringify(body) : undefined,
        }).then((r) => (r.ok ? r.json().catch(() => ({})) : r.text().then((t) => Promise.reject(t))));
    }

    // ---------- Rendering ----------

    function renderLanes(count) {
        state.partitions = count;
        el.partitionsLabel.textContent = count;
        el.lanes.innerHTML = "";
        state.laneColors = [];
        for (let i = 0; i < count; i++) {
            const color = PALETTE[i % PALETTE.length];
            state.laneColors.push(color);

            const lane = document.createElement("div");
            lane.className = "lane";
            lane.id = "lane-" + i;
            lane.style.setProperty("--lane-color", color);

            const label = document.createElement("div");
            label.className = "lane-label";
            label.innerHTML =
                "Partition " + i + '<span class="lane-offset" id="lane-offset-' + i + '">offset producteur : -</span>';

            const track = document.createElement("div");
            track.className = "lane-track";

            lane.appendChild(label);
            lane.appendChild(track);
            el.lanes.appendChild(lane);
        }
    }

    function partitionColor(partition) {
        return state.laneColors[partition % state.laneColors.length] || "#475569";
    }

    function consumerChipDomId(consumerId, partition, offset) {
        return "cchip-" + consumerId + "-p" + partition + "-o" + offset;
    }

    /**
     * Ajoute un wagon stationnaire dans la pile de messages traites par ce consumer.
     * Les MAX_HISTORY_PER_CONSUMER derniers messages sont conserves ; au-dela, le
     * plus ancien est retire (avec une petite animation de sortie) pour laisser la
     * place au nouveau : c'est la pile visible de ce qui a ete consomme recemment.
     */
    function addConsumedChip(consumerId, partition, offset, id, isError) {
        const data = state.consumers.get(consumerId);
        const queue = document.getElementById("queue-" + consumerId);
        if (!data || !queue) {
            return;
        }

        const chip = document.createElement("div");
        chip.id = consumerChipDomId(consumerId, partition, offset);
        chip.className = "chip" + (isError ? " error" : "");
        chip.style.setProperty("--lane-color", partitionColor(partition));
        chip.title = "Message #" + id + " — partition " + partition + ", offset " + offset;
        chip.textContent = "p" + partition + ":" + offset;
        queue.appendChild(chip);

        data.history.push({ partition, offset });
        while (data.history.length > MAX_HISTORY_PER_CONSUMER) {
            const old = data.history.shift();
            const oldChip = document.getElementById(consumerChipDomId(consumerId, old.partition, old.offset));
            if (oldChip) {
                oldChip.classList.add("leaving");
                setTimeout(() => oldChip.remove(), 400);
            }
        }
    }

    function offsetSpanId(consumerId, partition) {
        return "offset-" + consumerId + "-p" + partition;
    }

    function partBlockId(consumerId, partition) {
        return "partblock-" + consumerId + "-p" + partition;
    }

    /**
     * Met a jour l'affichage de l'offset courant d'un consumer sur une partition donnee.
     * @param {boolean} [flash] true pour declencher une petite animation (utilise lors d'un seek manuel)
     */
    function updateOffsetDisplay(consumerId, partition, flash) {
        const span = document.getElementById(offsetSpanId(consumerId, partition));
        const data = state.consumers.get(consumerId);
        if (span && data) {
            const value = data.offsets ? data.offsets[partition] : undefined;
            span.textContent = "offset : " + (value !== undefined ? value : "-");
        }
        if (flash) {
            const block = document.getElementById(partBlockId(consumerId, partition));
            if (block) {
                block.classList.remove("rewind-flash");
                // force reflow pour pouvoir rejouer l'animation si elle est deja en cours
                void block.offsetWidth;
                block.classList.add("rewind-flash");
            }
        }
    }

    function renderConsumers() {
        el.consumers.innerHTML = "";
        el.consumerCountLabel.textContent = state.consumers.size;
        for (const [id, data] of state.consumers.entries()) {
            const card = document.createElement("div");
            card.className = "consumer-card";
            card.id = "consumer-" + id;
            const partsHtml = (data.partitions || [])
                .map((p) => {
                    const offsetValue = data.offsets ? data.offsets[p] : undefined;
                    return (
                        '<div class="part-block" id="' + partBlockId(id, p) + '">' +
                        '<span class="part-tag">p' + p + "</span>" +
                        '<span class="lane-offset" id="' + offsetSpanId(id, p) + '">offset : ' +
                        (offsetValue !== undefined ? offsetValue : "-") + "</span>" +
                        '<div class="seek-controls">' +
                        '<button class="btn-mini" data-action="beginning" data-consumer="' + id + '" data-partition="' + p + '" title="Revenir au debut">⏮</button>' +
                        '<button class="btn-mini" data-action="end" data-consumer="' + id + '" data-partition="' + p + '" title="Aller a la fin">⏩</button>' +
                        '<input type="number" min="0" class="offset-input" id="offset-input-' + id + '-p' + p + '" placeholder="offset">' +
                        '<button class="btn-mini" data-action="manual" data-consumer="' + id + '" data-partition="' + p + '" title="Aller a cet offset">Go</button>' +
                        "</div>" +
                        "</div>"
                    );
                })
                .join("");
            card.innerHTML =
                '<div class="cid">👤 ' + id + "</div>" +
                '<div class="parts">' + (partsHtml || '<span class="part-tag">en attente…</span>') + "</div>" +
                '<div class="queue-label">Derniers messages traités</div>' +
                '<div class="queue" id="queue-' + id + '"></div>';
            el.consumers.appendChild(card);

            // Re-affiche l'historique conserve en memoire (ex: apres un re-render suite a un rebalance)
            (data.history || []).forEach((h) => {
                const queue = card.querySelector(".queue");
                const chip = document.createElement("div");
                chip.id = consumerChipDomId(id, h.partition, h.offset);
                chip.className = "chip";
                chip.style.setProperty("--lane-color", partitionColor(h.partition));
                chip.textContent = "p" + h.partition + ":" + h.offset;
                queue.appendChild(chip);
            });
        }
    }

    // Delegation d'evenements : les cartes consumer sont recreees a chaque rebalance,
    // donc on ecoute une seule fois sur le conteneur parent plutot que de re-brancher
    // un listener par bouton a chaque renderConsumers().
    function bindConsumerActions() {
        el.consumers.addEventListener("click", (evt) => {
            const btn = evt.target.closest(".btn-mini");
            if (!btn) {
                return;
            }
            const consumerId = btn.dataset.consumer;
            const partition = Number(btn.dataset.partition);
            const action = btn.dataset.action;

            if (action === "beginning") {
                api("POST", "/api/consumers/" + consumerId + "/seek-beginning/" + partition);
            } else if (action === "end") {
                api("POST", "/api/consumers/" + consumerId + "/seek-end/" + partition);
            } else if (action === "manual") {
                const input = document.getElementById("offset-input-" + consumerId + "-p" + partition);
                const offset = Number(input ? input.value : 0) || 0;
                api("POST", "/api/consumers/" + consumerId + "/seek/" + partition, { offset });
            }
        });
    }

    function logLine(text) {
        const line = document.createElement("div");
        const time = new Date().toLocaleTimeString();
        line.textContent = "[" + time + "] " + text;
        el.log.prepend(line);
        while (el.log.childElementCount > 200) {
            el.log.removeChild(el.log.lastChild);
        }
    }

    // ---------- Wagon animation ----------

    function stageRect() {
        return el.stage.getBoundingClientRect();
    }

    function centerOf(elem) {
        const r = elem.getBoundingClientRect();
        const s = stageRect();
        return { x: r.left - s.left + r.width / 2 - 17, y: r.top - s.top + r.height / 2 - 17 };
    }

    /**
     * Fait voyager un wagon visuel de fromElem vers toElem.
     * @param {boolean} short     true = trajet topic -> consumer/DLT (plus court),
     *                            false = trajet producteur -> topic (plus long)
     * @param {Function} [onArrive]  callback execute a l'arrivee, avant le fade
     */
    function spawnWagon(fromElem, toElem, label, isError, short, onArrive) {
        if (!fromElem || !toElem) {
            if (onArrive) {
                onArrive();
            }
            return;
        }
        const duration = short ? TRAVEL_SHORT_MS : TRAVEL_LONG_MS;
        const wagon = document.createElement("div");
        wagon.className = "wagon" + (isError ? " error" : "") + (short ? " short" : "");
        wagon.textContent = label;
        const from = centerOf(fromElem);
        el.stage.appendChild(wagon);
        wagon.style.left = from.x + "px";
        wagon.style.top = from.y + "px";

        requestAnimationFrame(() => {
            const to = centerOf(toElem);
            wagon.style.transform = "translate(" + (to.x - from.x) + "px," + (to.y - from.y) + "px)";
            wagon.classList.add("fade");
        });

        setTimeout(() => {
            if (onArrive) {
                onArrive();
            }
        }, duration);

        setTimeout(() => wagon.remove(), duration + 500);
    }

    // ---------- WebSocket / STOMP ----------

    function connect() {
        const socket = new SockJS("/ws");
        const client = Stomp.over(socket);
        client.debug = null;

        client.connect(
            {},
            () => {
                el.connDot.className = "dot dot-on";
                el.connLabel.textContent = "connecté";
                client.subscribe("/topic/events", (msg) => {
                    const event = JSON.parse(msg.body);
                    handleEvent(event.type, event.payload);
                });
            },
            () => {
                el.connDot.className = "dot dot-off";
                el.connLabel.textContent = "déconnecté — reconnexion…";
                setTimeout(connect, 2000);
            }
        );
    }

    function handleEvent(type, payload) {
        switch (type) {
            case "PRODUCER_CONFIG":
                producerRunning = payload.running;
                el.producerToggle.textContent = producerRunning ? "Arrêter" : "Démarrer";
                el.interval.value = payload.intervalMs;
                el.errorRate.value = payload.errorRatePercent;
                el.errorRateLabel.textContent = payload.errorRatePercent + "%";
                break;

            case "PRODUCER_MESSAGE": {
                const lane = document.getElementById("lane-" + payload.partition);
                const offsetBadge = document.getElementById("lane-offset-" + payload.partition);
                if (offsetBadge) {
                    offsetBadge.textContent = "offset producteur : " + payload.offset;
                }
                // La partition reste un simple point de passage : le wagon traverse le rail
                // lentement puis disparait, sans y rester stocke.
                spawnWagon(el.producerBox, lane, "#" + payload.id, payload.error, false);
                logLine("Producer → partition " + payload.partition + " (offset " + payload.offset + ", clé " + payload.key + ")");
                break;
            }

            case "PRODUCER_ERROR":
                logLine("⚠️ Erreur d'envoi producer : " + payload.reason);
                break;

            case "CONSUMER_MESSAGE": {
                const lane = document.getElementById("lane-" + payload.partition);
                const consumerCard = document.getElementById("consumer-" + payload.consumerId);
                const data = state.consumers.get(payload.consumerId);
                if (data) {
                    data.offsets = data.offsets || {};
                    data.offsets[payload.partition] = payload.offset + 1;
                }
                // Le message traverse la partition puis vient s'empiler dans la pile du consumer :
                // c'est la que doit se voir la persistance des 20 derniers messages traites.
                spawnWagon(lane, consumerCard, payload.consumerId, false, true, () => {
                    addConsumedChip(payload.consumerId, payload.partition, payload.offset, payload.id, false);
                    updateOffsetDisplay(payload.consumerId, payload.partition, false);
                });
                logLine(
                    "Partition " + payload.partition + " → " + payload.consumerId +
                    " (offset " + payload.offset + ", lag " + payload.lag + ")"
                );
                break;
            }

            case "DLT_MESSAGE": {
                const lane = document.getElementById("lane-" + payload.partition);
                spawnWagon(lane, el.dltBox, "#" + payload.originalId, true, true);
                logLine("☠️ Message #" + payload.originalId + " routé vers le Dead Letter Topic (" + payload.reason + ")");
                break;
            }

            case "CONSUMER_SEEK": {
                const data = state.consumers.get(payload.consumerId);
                if (data) {
                    data.offsets = data.offsets || {};
                    data.offsets[payload.partition] = payload.newOffset;
                }
                updateOffsetDisplay(payload.consumerId, payload.partition, true);
                const modeLabel = payload.mode === "beginning" ? "début" : payload.mode === "end" ? "fin" : "offset manuel";
                logLine(
                    "⏪ " + payload.consumerId + " repositionné sur partition " + payload.partition +
                    " → " + modeLabel + " (nouvel offset " + payload.newOffset + ")"
                );
                break;
            }

            case "CONSUMER_ADDED":
                state.consumers.set(payload.consumerId, { partitions: [], history: [], offsets: {} });
                renderConsumers();
                logLine("➕ Consumer " + payload.consumerId + " démarré");
                break;

            case "CONSUMER_REMOVED":
                state.consumers.delete(payload.consumerId);
                renderConsumers();
                logLine("➖ Consumer " + payload.consumerId + " arrêté");
                break;

            case "CONSUMER_ASSIGNED":
                if (state.consumers.has(payload.consumerId)) {
                    state.consumers.get(payload.consumerId).partitions = payload.partitions;
                } else {
                    state.consumers.set(payload.consumerId, { partitions: payload.partitions, history: [], offsets: {} });
                }
                renderConsumers();
                logLine("🔀 Rebalance : " + payload.consumerId + " ← partitions [" + payload.partitions.join(", ") + "]");
                break;

            default:
                break;
        }
    }

    // ---------- Initial state ----------

    function loadInitialState() {
        api("GET", "/api/state").then((data) => {
            el.topicName.textContent = data.topic.topic;
            renderLanes(data.topic.partitions);

            producerRunning = data.producer.running;
            el.producerToggle.textContent = producerRunning ? "Arrêter" : "Démarrer";
            el.interval.value = data.producer.intervalMs;
            el.errorRate.value = data.producer.errorRatePercent;
            el.errorRateLabel.textContent = data.producer.errorRatePercent + "%";

            state.consumers.clear();
            (data.consumers || []).forEach((c) => state.consumers.set(c.consumerId, { partitions: c.partitions, history: [], offsets: {} }));
            renderConsumers();
        });
    }

    // ---------- UI wiring ----------

    el.producerToggle.addEventListener("click", () => {
        api("POST", producerRunning ? "/api/producer/stop" : "/api/producer/start");
    });

    el.applyInterval.addEventListener("click", () => {
        api("PUT", "/api/producer/interval", { ms: Number(el.interval.value) });
    });

    el.errorRate.addEventListener("input", () => {
        el.errorRateLabel.textContent = el.errorRate.value + "%";
    });
    el.errorRate.addEventListener("change", () => {
        api("PUT", "/api/producer/error-rate", { percent: Number(el.errorRate.value) });
    });

    el.triggerError.addEventListener("click", () => {
        api("POST", "/api/producer/trigger-error", { count: 1 });
    });

    el.consumerPlus.addEventListener("click", () => {
        const target = state.consumers.size + 1;
        api("PUT", "/api/consumers/count", { count: target });
    });

    el.consumerMinus.addEventListener("click", () => {
        const target = Math.max(0, state.consumers.size - 1);
        api("PUT", "/api/consumers/count", { count: target });
    });

    // ---------- Boot ----------

    bindConsumerActions();
    loadInitialState();
    connect();
})();
