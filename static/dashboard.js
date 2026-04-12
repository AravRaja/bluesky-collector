// Minimal dashboard — OLED dark, white accents

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
    const t = (d.embed_type || "").replace("app.bsky.embed.", "");
    return `<span class="badge">${t || "embed"}</span>`;
}

// Chart defaults
const chartOpts = {
    responsive: true, maintainAspectRatio: false,
    interaction: { mode: "index", intersect: false },
    scales: {
        x: { ticks: { color: "#555", font: { family: "'JetBrains Mono'", size: 9 }, maxTicksLimit: 8 }, grid: { color: "#1a1a1a" }, border: { color: "#1a1a1a" } },
        y: { ticks: { color: "#555", font: { family: "'JetBrains Mono'", size: 9 } }, grid: { color: "#1a1a1a" }, border: { color: "#1a1a1a" } },
    },
    plugins: {
        legend: { labels: { color: "#555", font: { family: "'Inter'", size: 10 }, boxWidth: 8, boxHeight: 8, padding: 12 } },
        tooltip: { backgroundColor: "#111", borderColor: "#333", borderWidth: 1, titleFont: { family: "'JetBrains Mono'", size: 10 }, bodyFont: { family: "'Inter'", size: 11 }, padding: 8, cornerRadius: 4 },
    },
};

// Rate chart
const rateChart = new Chart(document.getElementById("rateChart"), {
    type: "line",
    data: { labels: [], datasets: [
        { label: "Posts", data: [], borderColor: "#fff", borderWidth: 1, pointRadius: 0, tension: 0.3 },
        { label: "Likes", data: [], borderColor: "#666", borderWidth: 1, pointRadius: 0, tension: 0.3 },
        { label: "Reposts", data: [], borderColor: "#444", borderWidth: 1, pointRadius: 0, tension: 0.3 },
    ] },
    options: chartOpts,
});

// Throughput chart
const tpChart = new Chart(document.getElementById("throughputChart"), {
    type: "bar",
    data: { labels: [], datasets: [
        { label: "Posts", data: [], backgroundColor: "#fff", borderRadius: 2 },
        { label: "Likes", data: [], backgroundColor: "#444", borderRadius: 2 },
    ] },
    options: { ...chartOpts, plugins: { ...chartOpts.plugins, legend: { display: false } }, scales: { ...chartOpts.scales, x: { ...chartOpts.scales.x, stacked: true }, y: { ...chartOpts.scales.y, stacked: true } } },
});

async function api(url) { return (await fetch(url)).json(); }

// Active detail charts (cleanup on close)
let activeDetailChart = null;

async function refreshStatus() {
    try {
        const d = await api("/api/status");
        document.getElementById("s-likes").textContent = fmt(d.counts.likes);
        document.getElementById("s-reposts").textContent = fmt(d.counts.reposts);
    } catch (e) {}
}

async function refreshHealth() {
    try {
        const d = await api("/api/health");
        document.getElementById("s-roots").textContent = fmt(d.totals.root_posts);
        document.getElementById("s-db").textContent = d.db_size_mb + "MB";

        // Tiers
        const tiers = d.repost_tiers;
        const keys = Object.keys(tiers).map(Number).sort((a, b) => a - b);
        const max = Math.max(...keys.map(k => tiers[String(k)]), 1);
        document.getElementById("tier-bars").innerHTML = keys.map(k => {
            const c = tiers[String(k)];
            const pct = Math.max((c / max) * 100, c > 0 ? 1 : 0);
            return `<div class="tier"><span class="tier-label">${k}+</span><div class="tier-track"><div class="tier-fill" style="width:${pct}%"></div></div><span class="tier-count">${fmt(c)}</span></div>`;
        }).join("");

        // Throughput
        if (d.hourly_throughput.length) {
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
        rateChart.data.labels = data.map(d => d.ts.slice(11, 16));
        rateChart.data.datasets[0].data = data.map(d => d.posts);
        rateChart.data.datasets[1].data = data.map(d => d.likes);
        rateChart.data.datasets[2].data = data.map(d => d.reposts);
        rateChart.update("none");
    } catch (e) {}
}

async function refreshViral() {
    try {
        const d = await api("/api/viral?threshold=100");
        document.getElementById("s-viral").textContent = d.viral;

        const posts = await api("/api/viral/posts?n=50");
        const tbody = document.getElementById("viral-body");
        if (!posts.length) {
            tbody.innerHTML = '<tr><td colspan="5" class="empty">No posts with 100+ reposts yet</td></tr>';
            return;
        }
        tbody.innerHTML = posts.map((p, i) =>
            `<tr class="clickable" data-uri="${esc(p.uri)}" data-idx="${i}" onclick="toggleDetail(this)">` +
            `<td class="text-col">${esc(p.text)}</td>` +
            `<td>${badge(p)}</td>` +
            `<td class="mono r">${fmt(p.likes)}</td>` +
            `<td class="mono r">${fmt(p.reposts)}</td>` +
            `<td class="mono r muted">${timeAgo(p.time_us)}</td>` +
            `</tr>` +
            `<tr class="detail" id="detail-${i}"><td colspan="5"><div class="detail-inner">` +
            `<div class="detail-stats" id="detail-stats-${i}"></div>` +
            `<div class="detail-chart"><canvas id="detail-chart-${i}"></canvas></div>` +
            `<div class="detail-note">Cumulative engagement in the first 30 minutes</div>` +
            `</div></td></tr>`
        ).join("");
    } catch (e) {}
}

async function toggleDetail(row) {
    const idx = row.dataset.idx;
    const uri = row.dataset.uri;
    const detailRow = document.getElementById("detail-" + idx);

    // Close if open
    if (detailRow.classList.contains("open")) {
        detailRow.classList.remove("open");
        if (activeDetailChart) { activeDetailChart.destroy(); activeDetailChart = null; }
        return;
    }

    // Close any other open detail
    document.querySelectorAll(".detail.open").forEach(el => el.classList.remove("open"));
    if (activeDetailChart) { activeDetailChart.destroy(); activeDetailChart = null; }

    detailRow.classList.add("open");

    // Fetch timeline
    try {
        const d = await api("/api/post/timeline?uri=" + encodeURIComponent(uri));
        const b = d.buckets;
        const last = b[b.length - 1];

        // Stats
        document.getElementById("detail-stats-" + idx).innerHTML =
            `<div class="detail-stat"><div class="detail-stat-value">${last.likes}</div><div class="detail-stat-label">Likes</div></div>` +
            `<div class="detail-stat"><div class="detail-stat-value">${last.reposts}</div><div class="detail-stat-label">Reposts</div></div>` +
            `<div class="detail-stat"><div class="detail-stat-value">${last.replies}</div><div class="detail-stat-label">Replies</div></div>`;

        // Stacked area chart
        const canvas = document.getElementById("detail-chart-" + idx);
        activeDetailChart = new Chart(canvas, {
            type: "line",
            data: {
                labels: b.map(x => x.minute + "m"),
                datasets: [
                    { label: "Likes", data: b.map(x => x.likes), borderColor: "#fff", backgroundColor: "rgba(255,255,255,0.08)", borderWidth: 1, pointRadius: 0, fill: true, tension: 0.3 },
                    { label: "Reposts", data: b.map(x => x.reposts), borderColor: "#666", backgroundColor: "rgba(102,102,102,0.08)", borderWidth: 1, pointRadius: 0, fill: true, tension: 0.3 },
                    { label: "Replies", data: b.map(x => x.replies), borderColor: "#333", backgroundColor: "rgba(51,51,51,0.08)", borderWidth: 1, pointRadius: 0, fill: true, tension: 0.3 },
                ],
            },
            options: {
                ...chartOpts,
                scales: {
                    ...chartOpts.scales,
                    y: { ...chartOpts.scales.y, stacked: true },
                },
            },
        });
    } catch (e) {
        document.getElementById("detail-stats-" + idx).innerHTML = '<div class="empty">Failed to load timeline</div>';
    }
}

async function refreshRecent() {
    try {
        const data = await api("/api/recent?n=30");
        const tbody = document.getElementById("recent-body");
        if (!data.length) { tbody.innerHTML = '<tr><td colspan="5" class="empty">No posts yet</td></tr>'; return; }
        tbody.innerHTML = data.map(d =>
            `<tr>` +
            `<td class="text-col">${esc(d.text)}</td>` +
            `<td>${badge(d)}</td>` +
            `<td class="mono r">${d.likes}</td>` +
            `<td class="mono r">${d.reposts}</td>` +
            `<td class="mono r muted">${timeAgo(d.time_us)}</td>` +
            `</tr>`
        ).join("");
    } catch (e) {}
}

async function refreshAll() {
    await Promise.all([refreshStatus(), refreshHealth(), refreshRate(), refreshViral(), refreshRecent()]);
}

refreshAll();
setInterval(refreshAll, 30000);
