const C = { blue: "#3b82f6", green: "#22c55e", amber: "#f59e0b", pink: "#ec4899" };

function fmt(n) {
    if (n >= 1e6) return (n / 1e6).toFixed(1) + "M";
    if (n >= 1e3) return (n / 1e3).toFixed(1) + "K";
    return String(n);
}
function timeAgo(us) {
    const s = (Date.now() * 1000 - us) / 1e6;
    if (s < 60) return Math.round(s) + "s";
    if (s < 3600) return Math.round(s / 60) + "m";
    if (s < 86400) return Math.round(s / 3600) + "h";
    return Math.round(s / 86400) + "d";
}
function esc(s) { const d = document.createElement("div"); d.textContent = s || ""; return d.innerHTML; }
function badge(d) {
    if (!d.has_embed) return "";
    return `<span class="badge">${(d.embed_type || "").replace("app.bsky.embed.", "") || "embed"}</span>`;
}

const baseOpts = {
    responsive: true, maintainAspectRatio: false,
    interaction: { mode: "index", intersect: false },
    scales: {
        x: { ticks: { color: "#555", font: { family: "'JetBrains Mono'", size: 9 }, maxTicksLimit: 8 }, grid: { color: "#1a1a1a" }, border: { color: "#1a1a1a" } },
        y: { ticks: { color: "#555", font: { family: "'JetBrains Mono'", size: 9 } }, grid: { color: "#1a1a1a" }, border: { color: "#1a1a1a" } },
    },
    plugins: {
        legend: { labels: { color: "#666", font: { family: "'Inter'", size: 10 }, boxWidth: 8, boxHeight: 8, padding: 12 } },
        tooltip: { backgroundColor: "#111", borderColor: "#333", borderWidth: 1, titleFont: { family: "'JetBrains Mono'", size: 10 }, bodyFont: { family: "'Inter'", size: 11 }, padding: 8, cornerRadius: 4 },
    },
};

const rateChart = new Chart(document.getElementById("rateChart"), {
    type: "line",
    data: { labels: [], datasets: [
        { label: "Posts", data: [], borderColor: C.blue, borderWidth: 1.5, pointRadius: 0, tension: 0.3 },
        { label: "Likes", data: [], borderColor: C.green, borderWidth: 1, pointRadius: 0, tension: 0.3 },
        { label: "Reposts", data: [], borderColor: C.pink, borderWidth: 1, pointRadius: 0, tension: 0.3 },
    ] },
    options: baseOpts,
});

const tpChart = new Chart(document.getElementById("throughputChart"), {
    type: "bar",
    data: { labels: [], datasets: [
        { label: "Posts", data: [], backgroundColor: C.blue, borderRadius: 2 },
        { label: "Likes", data: [], backgroundColor: "rgba(34,197,94,0.4)", borderRadius: 2 },
    ] },
    options: { ...baseOpts, plugins: { ...baseOpts.plugins, legend: { display: false } }, scales: { ...baseOpts.scales, x: { ...baseOpts.scales.x, stacked: true }, y: { ...baseOpts.scales.y, stacked: true } } },
});

async function api(url) { return (await fetch(url)).json(); }

async function refreshStatus() {
    try {
        const d = await api("/api/status");
        document.getElementById("s-likes").textContent = fmt(d.counts.likes || 0);
        document.getElementById("s-reposts").textContent = fmt(d.counts.reposts || 0);
    } catch (e) {}
}

async function refreshHealth() {
    try {
        const d = await api("/api/health");
        if (!d.totals) return;
        document.getElementById("s-roots").textContent = fmt(d.totals.root_posts);
        document.getElementById("s-db").textContent = d.db_size_mb < 1024 ? d.db_size_mb + "MB" : (d.db_size_mb / 1024).toFixed(1) + "GB";

        const tiers = d.repost_tiers || {};
        const keys = Object.keys(tiers).map(Number).sort((a, b) => a - b);
        const max = Math.max(...keys.map(k => tiers[String(k)]), 1);
        const tierColors = { 1: "#555", 5: "#666", 10: C.blue, 25: C.blue, 50: C.amber, 100: C.pink, 250: C.pink, 500: C.green };
        document.getElementById("tier-bars").innerHTML = keys.map(k => {
            const c = tiers[String(k)];
            const pct = Math.max((c / max) * 100, c > 0 ? 1 : 0);
            const color = tierColors[k] || "#fff";
            return `<div class="tier"><span class="tier-label">${k}+</span><div class="tier-track"><div class="tier-fill" style="width:${pct}%;background:${color}"></div></div><span class="tier-count">${fmt(c)}</span></div>`;
        }).join("");

        if (d.hourly_throughput && d.hourly_throughput.length) {
            tpChart.data.labels = d.hourly_throughput.map(h => h.hour.slice(11, 16));
            tpChart.data.datasets[0].data = d.hourly_throughput.map(h => h.posts);
            tpChart.data.datasets[1].data = d.hourly_throughput.map(h => h.likes);
            tpChart.update("none");
        }
    } catch (e) {}
}

async function refreshRate() {
    try {
        const data = await api("/api/rate?hours=6");
        if (!data.length) return;
        rateChart.data.labels = data.map(d => d.ts.slice(11, 16));
        rateChart.data.datasets[0].data = data.map(d => d.posts);
        rateChart.data.datasets[1].data = data.map(d => d.likes);
        rateChart.data.datasets[2].data = data.map(d => d.reposts);
        rateChart.update("none");
    } catch (e) {}
}

function renderPosts(posts, tbodyId, cardsId) {
    const tbody = document.getElementById(tbodyId);
    const cards = document.getElementById(cardsId);

    if (!posts.length) {
        tbody.innerHTML = `<tr><td colspan="5" class="empty">No data yet</td></tr>`;
        cards.innerHTML = `<div class="empty">No data yet</div>`;
        return;
    }

    tbody.innerHTML = posts.map(p => {
        return `<tr>` +
            `<td class="text-col">${esc(p.text)}</td>` +
            `<td>${badge(p)}</td>` +
            `<td class="mono r">${fmt(p.likes)}</td>` +
            `<td class="mono r">${fmt(p.reposts)}</td>` +
            `<td class="mono r muted">${timeAgo(p.time_us)}</td></tr>`;
    }).join("");

    cards.innerHTML = posts.map(p => {
        return `<div class="post-card">` +
            `<div class="post-card-text">${esc(p.text)}</div>` +
            `<div class="post-card-meta">` +
            `<span class="post-card-stat" style="color:${C.green}">${fmt(p.likes)} <span>likes</span></span>` +
            `<span class="post-card-stat" style="color:${C.pink}">${fmt(p.reposts)} <span>rp</span></span>` +
            `<span class="post-card-stat">${timeAgo(p.time_us)}</span>` +
            (p.has_embed ? ` ${badge(p)}` : "") +
            `</div></div>`;
    }).join("");
}

async function refreshViral() {
    try {
        const d = await api("/api/viral");
        document.getElementById("s-viral").textContent = d.viral || 0;
        const posts = await api("/api/viral/posts");
        renderPosts(posts, "viral-body", "viral-cards");
    } catch (e) {}
}

async function refreshRecent() {
    try {
        const data = await api("/api/recent");
        renderPosts(data, "recent-body", "recent-cards");
    } catch (e) {}
}

async function refreshAll() {
    await Promise.all([refreshStatus(), refreshHealth(), refreshRate(), refreshViral(), refreshRecent()]);
}
refreshAll();
setInterval(refreshAll, 30000);
