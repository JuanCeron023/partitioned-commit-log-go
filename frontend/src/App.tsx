import { useEffect, useState } from "react";
import type { Message, Stats } from "./types";

const TOPIC = "order.events";
const GROUP = "analytics";
const EVENTS = ["order.created", "order.paid", "order.shipped", "order.refunded"];

async function jpost(url: string, body: unknown) {
  return fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) }).then(
    (r) => r.json()
  );
}

export default function App() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [partition, setPartition] = useState(0);
  const [out, setOut] = useState<string>("");
  const [key, setKey] = useState("");
  const [val, setVal] = useState("");

  useEffect(() => {
    const refresh = async () => {
      try {
        setStats(await fetch("/api/stats").then((r) => r.json() as Promise<Stats>));
      } catch {
        /* ignore */
      }
    };
    refresh();
    const id = setInterval(refresh, 800);
    return () => clearInterval(id);
  }, []);

  const topic = stats?.topics.find((t) => t.name === TOPIC);

  const poll = async () => {
    const msgs = (await fetch(`/api/consume?group=${GROUP}&topic=${TOPIC}&partition=${partition}&max=20`).then((r) =>
      r.json()
    )) as Message[];
    setOut(msgs.map((m) => `#${m.offset}  ${atob(m.value)}`).join("\n") || "(no new messages)");
  };

  const bar = (committed: number, end: number) => {
    const pct = end > 0 ? Math.round((100 * committed) / end) : 0;
    return (
      <div className="w-full h-2 rounded-full bg-slate-700 overflow-hidden">
        <div className="h-full bg-emerald-400" style={{ width: `${pct}%` }} />
      </div>
    );
  };

  return (
    <div className="max-w-6xl mx-auto px-6 py-8">
      <header className="flex items-center gap-3 mb-8">
        <div className="h-10 w-10 rounded-xl bg-gradient-to-br from-amber-300 to-orange-500 grid place-items-center font-bold text-slate-900">
          SH
        </div>
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">StreamHive</h1>
          <p className="text-sm text-slate-300/70">
            Partitioned, ordered, durable event log · consumer groups · lag tracking
          </p>
        </div>
      </header>

      <section className="grid md:grid-cols-2 gap-6 mb-8">
        <div className="bg-slate-900/50 border border-slate-800 rounded-2xl p-5">
          <h2 className="text-sm uppercase tracking-widest text-slate-400 mb-4">Produce</h2>
          <div className="space-y-3">
            <input
              value={key}
              onChange={(e) => setKey(e.target.value)}
              placeholder="key (e.g. order-42) — same key ⇒ same partition"
              className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm"
            />
            <input
              value={val}
              onChange={(e) => setVal(e.target.value)}
              placeholder='value (e.g. {"event":"order.created"})'
              className="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm"
            />
            <div className="flex gap-2">
              <button
                onClick={() => jpost("/api/produce", { topic: TOPIC, key, value: val || "{}" })}
                className="bg-amber-400 hover:bg-amber-300 text-slate-900 font-medium rounded-lg px-4 py-2 text-sm"
              >
                Publish
              </button>
              <button
                onClick={async () => {
                  for (let i = 0; i < 30; i++)
                    await jpost("/api/produce", {
                      topic: TOPIC,
                      key: "order-" + Math.floor(Math.random() * 12),
                      value: JSON.stringify({ event: EVENTS[i % 4], n: i }),
                    });
                }}
                className="bg-orange-500 hover:bg-orange-400 text-slate-900 font-medium rounded-lg px-4 py-2 text-sm"
              >
                Publish 30 random
              </button>
            </div>
          </div>
        </div>

        <div className="bg-slate-900/50 border border-slate-800 rounded-2xl p-5">
          <h2 className="text-sm uppercase tracking-widest text-slate-400 mb-4">
            Consume (group: <span className="text-amber-300">analytics</span>)
          </h2>
          <div className="flex gap-2 mb-3">
            <select
              value={partition}
              onChange={(e) => setPartition(Number(e.target.value))}
              className="bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm"
            >
              {(topic?.partitions ?? []).map((p) => (
                <option key={p.partition} value={p.partition}>
                  partition {p.partition}
                </option>
              ))}
            </select>
            <button onClick={poll} className="bg-cyan-500 hover:bg-cyan-400 text-slate-900 font-medium rounded-lg px-4 py-2 text-sm">
              Poll partition
            </button>
          </div>
          <pre className="bg-slate-950/60 rounded-lg p-3 text-xs h-40 overflow-auto text-cyan-200">{out}</pre>
        </div>
      </section>

      <section className="grid md:grid-cols-2 gap-6">
        <div>
          <h2 className="text-sm uppercase tracking-widest text-slate-400 mb-3">Topic: order.events</h2>
          <div className="space-y-2">
            {(topic?.partitions ?? []).map((p) => (
              <div
                key={p.partition}
                className="bg-slate-900/60 border border-slate-800 rounded-xl p-3 flex items-center justify-between"
              >
                <span className="font-mono text-amber-200">partition {p.partition}</span>
                <span className="font-mono text-slate-300">{p.end_offset} msgs</span>
              </div>
            ))}
          </div>
        </div>
        <div>
          <h2 className="text-sm uppercase tracking-widest text-slate-400 mb-3">Consumer Lag</h2>
          <div className="space-y-2">
            {(stats?.lags ?? []).length === 0 ? (
              <div className="text-slate-500 text-sm">Poll a partition to create a consumer group.</div>
            ) : (
              stats?.lags.map((l) => (
                <div key={`${l.group}-${l.partition}`} className="bg-slate-900/60 border border-slate-800 rounded-xl p-3">
                  <div className="flex items-center justify-between text-sm mb-1">
                    <span className="font-mono">
                      {l.group} · p{l.partition}
                    </span>
                    <span className={`font-mono ${l.lag > 0 ? "text-orange-300" : "text-emerald-300"}`}>lag {l.lag}</span>
                  </div>
                  {bar(l.committed, l.end_offset)}
                </div>
              ))
            )}
          </div>
        </div>
      </section>
    </div>
  );
}
