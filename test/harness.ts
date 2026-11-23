/**
 * Drives the extension against a real store with stubbed host APIs: two
 * instances in one process, so claiming, delivery, waiting and spooling are
 * exercised without a model. Run it against a scratch store:
 *
 *   OMP_PEERS_DIR=/tmp/peers-test bun run test/harness.ts
 */
import peers from "../src/peers.ts";

/** Stands in for the schema builder: the stub host never validates parameters. */
const schemaStub: unknown = new Proxy(() => undefined, {
	get: () => schemaStub,
	apply: () => schemaStub,
});

interface ToolResult {
	content: { type: string; text: string }[];
	isError?: boolean;
}

interface ToolStub {
	name: string;
	execute: (
		toolCallId: string,
		params: unknown,
		signal: AbortSignal | undefined,
		onUpdate: undefined,
		ctx: unknown,
	) => Promise<ToolResult>;
}

type Handler = (event: unknown, ctx: unknown) => Promise<void>;
type CommandHandler = (args: string, ctx: unknown) => Promise<void>;

interface Instance {
	tools: Map<string, ToolStub>;
	commands: Map<string, { handler: CommandHandler }>;
	handlers: Map<string, Handler>;
	injected: string[];
	entries: { type: string; customType: string; data: unknown }[];
	notices: string[];
	timers: Timer[];
	ctx: unknown;
}

function instance(sessionName: string, sessionId: string): Instance {
	const self: Instance = {
		tools: new Map(),
		commands: new Map(),
		handlers: new Map(),
		injected: [],
		entries: [],
		notices: [],
		timers: [],
		ctx: undefined,
	};
	self.ctx = {
		agent: { kind: "main", id: "Main", name: "main", depth: 0 },
		cwd: `/tmp/${sessionName}`,
		isIdle: () => true,
		sessionManager: {
			getSessionId: () => sessionId,
			getSessionName: () => sessionName,
			getBranch: () => self.entries,
		},
		ui: { notify: (message: string) => self.notices.push(message) },
		setInterval: (callback: () => void, ms: number) => {
			const timer = setInterval(callback, ms);
			self.timers.push(timer);
			return timer;
		},
	};
	const host = {
		zod: schemaStub,
		logger: { debug: () => {}, warn: (m: string, d: unknown) => console.log(`  [warn] ${m}`, d) },
		registerTool: (tool: ToolStub) => self.tools.set(tool.name, tool),
		registerCommand: (name: string, options: { handler: CommandHandler }) => self.commands.set(name, options),
		on: (event: string, handler: Handler) => self.handlers.set(event, handler),
		sendUserMessage: (text: string) => self.injected.push(text),
		appendEntry: (customType: string, data: unknown) => self.entries.push({ type: "custom", customType, data }),
	};
	// The host contract is ExtensionAPI; this stub implements the slice the
	// extension touches, which the compiler cannot check against a partial.
	peers(host as unknown as Parameters<typeof peers>[0]);
	return self;
}

const sleep = (ms: number) => Bun.sleep(ms);
let failures = 0;

function check(label: string, ok: boolean, detail?: unknown): void {
	console.log(`${ok ? "PASS" : "FAIL"}  ${label}`);
	if (ok) return;
	failures++;
	if (detail !== undefined) console.log("      got:", detail);
}

async function call(inst: Instance, tool: string, params: unknown): Promise<ToolResult> {
	const definition = inst.tools.get(tool);
	if (!definition) throw new Error(`no tool ${tool}`);
	return await definition.execute("call-1", params, undefined, undefined, inst.ctx);
}

const textOf = (result: ToolResult) => result.content.map(block => block.text).join("\n");
const start = (inst: Instance) => inst.handlers.get("session_start")?.({}, inst.ctx);
const stop = async (inst: Instance) => {
	for (const timer of inst.timers) clearInterval(timer);
	await inst.handlers.get("session_shutdown")?.({}, inst.ctx);
};

const alpha = instance("alpha", "sess-alpha");
const beta = instance("Beta Session", "sess-beta");

await start(alpha);
await start(beta);

const list = textOf(await call(alpha, "peer_list", {}));
check("claims a name and lists both peers", list.includes('"alpha"') && list.includes("beta-session"), list);

const sent = await call(alpha, "peer_send", { to: "beta-session", body: "ping from alpha" });
check("send to an online peer reports delivered", textOf(sent).includes("Delivered"), textOf(sent));

await sleep(1400);
check(
	"recipient is woken with the body, quoted",
	beta.injected.length === 1 && beta.injected[0]!.includes("ping from alpha") && beta.injected[0]!.includes("----8<----"),
	beta.injected,
);
check("message is not delivered twice", beta.injected.length === 1, beta.injected.length);

// A pending wait takes the message instead of it being injected.
const waiting = call(alpha, "peer_wait", { from: "beta-session", timeout_sec: 10 });
await sleep(50);
await call(beta, "peer_send", { to: "alpha", body: "pong from beta" });
const waited = textOf(await waiting);
check("peer_wait returns the reply", waited.includes("pong from beta"), waited);
check("a waited message is not also injected", alpha.injected.length === 0, alpha.injected);

await call(beta, "peer_send", { to: "alpha", body: "second" });
const immediate = textOf(await call(alpha, "peer_wait", { timeout_sec: 10 }));
check("peer_wait drains an already-spooled message", immediate.includes("second"), immediate);

const timedOut = await call(alpha, "peer_wait", { timeout_sec: 1 });
check("peer_wait times out cleanly", textOf(timedOut).includes("No message"), textOf(timedOut));

const unknown = await call(alpha, "peer_send", { to: "nobody", body: "x" });
check(
	"unknown name is an error that lists the roster",
	unknown.isError === true && textOf(unknown).includes("alpha"),
	textOf(unknown),
);

const tooBig = await call(alpha, "peer_send", { to: "beta-session", body: "x".repeat(9000) });
check("oversized body is refused", tooBig.isError === true, textOf(tooBig));

const toSelf = await call(alpha, "peer_send", { to: "alpha", body: "x" });
check("sending to yourself is refused", toSelf.isError === true, textOf(toSelf));

// A second session may not steal a live name.
const impostor = instance("alpha", "sess-impostor");
await start(impostor);
await call(beta, "peer_send", { to: "alpha", body: "for the real alpha" });
await sleep(1400);
check("a live name cannot be stolen", impostor.injected.length === 0, impostor.injected);
check("mail still reaches the real holder", alpha.injected.some(m => m.includes("for the real alpha")), alpha.injected);

// Offline peer: beta signs off, so a send to it spools instead of failing.
await stop(beta);
const spooled = await call(alpha, "peer_send", { to: "beta-session", body: "while you were out" });
check("send to a departed peer spools", spooled.isError !== true && textOf(spooled).includes("spooled"), textOf(spooled));

// Restarting beta picks the spool up, oldest first.
const beta2 = instance("Beta Session", "sess-beta-2");
await start(beta2);
await call(alpha, "peer_send", { to: "beta-session", body: "welcome back" });
await sleep(1400);
check(
	"a restarted peer drains its spool, in order",
	beta2.injected.length === 2 &&
		beta2.injected[0]!.includes("while you were out") &&
		beta2.injected[1]!.includes("welcome back"),
	beta2.injected,
);

await beta2.commands.get("peer-name")?.handler("Release Notes", beta2.ctx);
check(
	"/peer-name claims a new handle and persists it",
	beta2.notices.some(n => n.includes("release-notes")) && beta2.entries.some(e => e.customType === "peer-name"),
	{ notices: beta2.notices, entries: beta2.entries },
);

const afterRename = textOf(await call(alpha, "peer_list", {}));
check("the old handle is dropped on rename", !afterRename.includes("beta-session"), afterRename);

await beta2.commands.get("peer-name")?.handler("alpha", beta2.ctx);
check(
	"/peer-name refuses a name in use",
	beta2.notices.some(n => n.includes("is held by a running session")),
	beta2.notices,
);

for (const inst of [alpha, impostor, beta2]) await stop(inst);

console.log(failures === 0 ? "\nall checks passed" : `\n${failures} check(s) failed`);
process.exit(failures === 0 ? 0 : 1);
