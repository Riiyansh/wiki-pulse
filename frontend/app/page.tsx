"use client";
import { useEffect, useState, useCallback } from "react";
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  BarChart, Bar, PieChart, Pie, Cell, Legend,
} from "recharts";

const API = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

// ── Types ──────────────────────────────────────────────────────────────
interface Edit {
  event_time: string;
  title: string;
  wiki: string;
  language: string;
  user_name: string;
  is_bot: boolean;
  is_new_page: boolean;
  delta_bytes: number;
  comment: string;
}

interface StatPoint { window_start: string; total_edits: number; bot_edits: number; human_edits: number; }
interface Article   { title: string; wiki: string; total_edits: number; is_spike: boolean; }
interface BotHuman  { bot: number; human: number; new_pages: number; total: number; }
interface Spike     { detected_at: string; title: string; wiki: string; edits_in_window: number; is_active: boolean; }
interface LangStat  { language: string; edit_count: number; }

const COLORS = ["#4fc3f7","#6bcb77","#ffd93d","#ff6b6b","#c084fc","#fb923c","#34d399","#f472b6"];

function fmt(iso: string) {
  return new Date(iso).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

// ── Hook ───────────────────────────────────────────────────────────────
function useWikiData() {
  const [feed,     setFeed]     = useState<Edit[]>([]);
  const [stats,    setStats]    = useState<StatPoint[]>([]);
  const [articles, setArticles] = useState<Article[]>([]);
  const [botHuman, setBotHuman] = useState<BotHuman>({ bot: 0, human: 0, new_pages: 0, total: 0 });
  const [spikes,   setSpikes]   = useState<Spike[]>([]);
  const [langs,    setLangs]    = useState<LangStat[]>([]);
  const [loading,  setLoading]  = useState(true);

  const fetchAll = useCallback(async () => {
    try {
      const [f, s, a, b, sp, l] = await Promise.all([
        fetch(`${API}/api/live-feed?limit=30`).then(r => r.json()),
        fetch(`${API}/api/stats?minutes=30`).then(r => r.json()),
        fetch(`${API}/api/top-articles?minutes=15`).then(r => r.json()),
        fetch(`${API}/api/bot-vs-human?minutes=30`).then(r => r.json()),
        fetch(`${API}/api/spikes`).then(r => r.json()),
        fetch(`${API}/api/languages?minutes=30`).then(r => r.json()),
      ]);
      setFeed(f.edits || []);
      setStats(s.stats || []);
      setArticles(a.articles || []);
      setBotHuman(b);
      setSpikes(sp.spikes || []);
      setLangs(l.languages || []);
      setLoading(false);
    } catch (e) {
      console.error(e);
    }
  }, []);

  useEffect(() => {
    fetchAll();
    const t = setInterval(fetchAll, 5000);
    return () => clearInterval(t);
  }, [fetchAll]);

  return { feed, stats, articles, botHuman, spikes, langs, loading };
}

// ── Components ─────────────────────────────────────────────────────────
function Card({ title, children, className = "" }: { title: string; children: React.ReactNode; className?: string }) {
  return (
    <div className={`bg-gray-900 border border-gray-800 rounded-xl p-4 ${className}`}>
      <h2 className="text-xs font-bold tracking-widest text-gray-500 uppercase mb-3">{title}</h2>
      {children}
    </div>
  );
}

// ── Main page ──────────────────────────────────────────────────────────
export default function Home() {
  const { feed, stats, articles, botHuman, spikes, langs, loading } = useWikiData();

  const botPct   = botHuman.total ? Math.round((botHuman.bot / botHuman.total) * 100) : 0;
  const pieData  = [
    { name: "Human", value: botHuman.human },
    { name: "Bot",   value: botHuman.bot },
  ];

  return (
    <div className="min-h-screen bg-gray-950 text-gray-100 font-mono">

      {/* Header */}
      <header className="border-b border-gray-800 px-6 py-4 flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold tracking-widest text-white">
            WIKI<span className="text-blue-400">PULSE</span>
          </h1>
          <p className="text-xs text-gray-500 tracking-wider mt-0.5">REAL-TIME WIKIPEDIA EDIT ANALYTICS</p>
        </div>
        <div className="flex items-center gap-6">
          <div className="text-center">
            <div className="text-2xl font-bold text-white tabular-nums">{botHuman.total.toLocaleString()}</div>
            <div className="text-xs text-gray-500 tracking-wider">EDITS (30M)</div>
          </div>
          <div className="text-center">
            <div className="text-2xl font-bold text-green-400 tabular-nums">{botHuman.human.toLocaleString()}</div>
            <div className="text-xs text-gray-500 tracking-wider">HUMAN</div>
          </div>
          <div className="text-center">
            <div className="text-2xl font-bold text-purple-400 tabular-nums">{botHuman.bot.toLocaleString()}</div>
            <div className="text-xs text-gray-500 tracking-wider">BOT</div>
          </div>
          <div className="text-center">
            <div className="text-2xl font-bold text-yellow-400 tabular-nums">{botHuman.new_pages.toLocaleString()}</div>
            <div className="text-xs text-gray-500 tracking-wider">NEW PAGES</div>
          </div>
          <div className="flex items-center gap-2">
            <div className={`w-2 h-2 rounded-full ${loading ? "bg-yellow-400 animate-pulse" : "bg-green-400"}`}
              style={{ boxShadow: loading ? "0 0 6px #facc15" : "0 0 6px #4ade80" }} />
            <span className="text-xs text-gray-500">{loading ? "CONNECTING" : "LIVE"}</span>
          </div>
        </div>
      </header>

      {/* Breaking news banner */}
      {spikes.length > 0 && (
        <div className="bg-red-950 border-b border-red-800 px-6 py-2 flex items-center gap-3 overflow-hidden">
          <span className="text-red-400 text-xs font-bold tracking-widest animate-pulse flex-shrink-0">🔥 SPIKE</span>
          <div className="flex gap-6 overflow-x-auto">
            {spikes.slice(0, 5).map((s, i) => (
              <span key={i} className="text-red-300 text-xs whitespace-nowrap">
                <strong>{s.title}</strong> — {s.edits_in_window} edits
              </span>
            ))}
          </div>
        </div>
      )}

      <div className="p-4 grid grid-cols-12 gap-4">

        {/* Edit velocity chart */}
        <Card title="Edit Velocity (30 min)" className="col-span-8">
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={stats}>
              <XAxis dataKey="window_start" tickFormatter={fmt} tick={{ fill: "#6b7280", fontSize: 10 }} />
              <YAxis tick={{ fill: "#6b7280", fontSize: 10 }} />
              <Tooltip
                contentStyle={{ background: "#111827", border: "1px solid #374151", borderRadius: 8, fontSize: 11 }}
                labelFormatter={(v: any) => fmt(String(v))}
              />
              <Line type="monotone" dataKey="total_edits" stroke="#4fc3f7" strokeWidth={2} dot={false} name="Total" />
              <Line type="monotone" dataKey="human_edits" stroke="#6bcb77" strokeWidth={1.5} dot={false} name="Human" />
              <Line type="monotone" dataKey="bot_edits"   stroke="#c084fc" strokeWidth={1.5} dot={false} name="Bot" />
            </LineChart>
          </ResponsiveContainer>
        </Card>

        {/* Bot vs Human pie */}
        <Card title="Bot vs Human" className="col-span-4">
          <div className="flex items-center justify-center">
            <PieChart width={160} height={160}>
              <Pie data={pieData} dataKey="value" cx="50%" cy="50%" outerRadius={65} innerRadius={35}>
                <Cell fill="#6bcb77" />
                <Cell fill="#c084fc" />
              </Pie>
              <Legend iconSize={8} wrapperStyle={{ fontSize: 11 }} />
            </PieChart>
          </div>
          <div className="text-center text-xs text-gray-500 mt-1">{botPct}% automated</div>
        </Card>

        {/* Top articles */}
        <Card title="Top Articles (15 min)" className="col-span-5">
          <div className="space-y-1 max-h-64 overflow-y-auto">
            {articles.slice(0, 15).map((a, i) => (
              <div key={i} className={`flex items-center justify-between py-1 px-2 rounded ${a.is_spike ? "bg-red-950 border border-red-800" : "hover:bg-gray-800"}`}>
                <div className="flex items-center gap-2 min-w-0">
                  {a.is_spike && <span className="text-red-400 text-xs flex-shrink-0">🔥</span>}
                  <span className="text-xs text-gray-300 truncate">{a.title}</span>
                  <span className="text-xs text-gray-600 flex-shrink-0">{a.wiki}</span>
                </div>
                <span className="text-xs font-bold text-blue-400 ml-2 flex-shrink-0">{a.total_edits}</span>
              </div>
            ))}
            {articles.length === 0 && <p className="text-xs text-gray-600 text-center py-4">Waiting for data…</p>}
          </div>
        </Card>

        {/* Language breakdown */}
        <Card title="By Language (30 min)" className="col-span-3">
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={langs.slice(0, 10)} layout="vertical" margin={{ left: 0, right: 20 }}>
              <XAxis type="number" hide />
              <YAxis type="category" dataKey="language" tick={{ fill: "#6b7280", fontSize: 10 }} width={24} />
              <Tooltip contentStyle={{ background: "#111827", border: "1px solid #374151", borderRadius: 8, fontSize: 11 }} />
              <Bar dataKey="edit_count" radius={[0, 4, 4, 0]}>
                {langs.slice(0, 10).map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </Card>

        {/* Live feed */}
        <Card title="Live Edit Feed" className="col-span-4">
          <div className="space-y-1 max-h-64 overflow-y-auto">
            {feed.map((e, i) => (
              <div key={i} className="flex items-start gap-2 py-1 border-b border-gray-800 text-xs">
                <span className="text-gray-600 flex-shrink-0 tabular-nums">{fmt(e.event_time)}</span>
                <span className={`flex-shrink-0 ${e.is_bot ? "text-purple-400" : "text-green-400"}`}>
                  {e.is_bot ? "🤖" : "👤"}
                </span>
                <div className="min-w-0">
                  <span className="text-blue-300 font-medium truncate block">{e.title}</span>
                  <span className="text-gray-600">{e.user_name}</span>
                  {e.delta_bytes !== 0 && (
                    <span className={`ml-1 ${e.delta_bytes > 0 ? "text-green-500" : "text-red-500"}`}>
                      {e.delta_bytes > 0 ? "+" : ""}{e.delta_bytes}B
                    </span>
                  )}
                </div>
                {e.is_new_page && <span className="text-yellow-400 flex-shrink-0 text-xs">NEW</span>}
              </div>
            ))}
            {feed.length === 0 && <p className="text-xs text-gray-600 text-center py-4">Waiting for edits…</p>}
          </div>
        </Card>

      </div>
    </div>
  );
}
