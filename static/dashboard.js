// Dashboard auto-refresh logic

function fmt(n) {
    if (n >= 1e6) return (n / 1e6).toFixed(1) + "M";
    if (n >= 1e3) return (n / 1e3).toFixed(1) + "K";
    return String(n);
}

function timeAgo(timeUs) {
    const sec = (Date.now() * 1000 - timeUs) / 1e6;
    if (sec < 60) return Math.round(sec) + "s";
    if (sec < 3600) return Math.round(sec / 60) + "m";
    if (sec < 86400) return Math.round(sec / 3600) + "h";
    return Math.round(sec / 86400) + "d";
}

function escHtml(s) {
    const d = document.createElement("div");
    d.textContent = s || "";
    return d.innerHTML;
}

function embedBadge(d) {
    if (!d.has_embed) return "";
    const t = (d.embed_type || "").replace("app.bsky.embed.", "");
    const cls = {
        images: "embed-images",
        video: "embed-video",
        external: "embed-external",
        record: "embed-record",
        recordWithMedia: "embed-recordWithMedia",
    }[t] || "embed-other";
    return `<span class="embed-badge ${cls}">${t || "embed"}</span>`;
}

// ── Charts setup ──
const chartDefaults = {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: "index", intersect: false },
    scales: {
        x: {
            ticks: { color: "#64748b", font: { family: "'Fira Code', monospace", size: 10 }, maxTicksLimit: 8 },
            grid: { color: "rgba(30,41,59,0.5)" },
            border: { color: "#1e293b" },
        },
        y: {
            ticks: { color: "#64748b", font: { family: "'Fira Code', monospace", size: 10 } },
            grid: { color: "rgba(30,41,59,0.5)" },
            border: { color: "#1e293b" },
        },
    },
    plugins: {
        legend: {
            labels: {
                color: "#94a3b8",
                font: { family: "'Fira Sans', sans-serif", size: 11 },
                boxWidth: 10, boxHeight: 10,
                useBorderRadius: true, borderRadius: 2, padding: 16,
            },
        },
        tooltip: {
            backgroundColor: "#111827", borderColor: "#1e293b", borderWidth: 1,
            titleFont: { family: "'Fira Code', monospace", size: 11 },
            bodyFont: { family: "'Fira Sans', sans-serif", size: 12 },
            padding: 10, cornerRadius: 8,
        },
    },
};

// Event rate chart
const rateChart = new Chart(document.getElementById("rateChart").getContext("2d"), {
    type: "line",
    data: {
        labels: [],
        datasets: [
            { label: "Posts",   data: [], borderColor: "#3b82f6", borderWidth: 1.5, pointRadius: 0, fill: false, tension: 0.3 },
            { label: "Likes",   data: [], borderColor: "#22c55e", borderWidth: 1.5, pointRadius: 0, fill: false, tension: 0.3 },
            { label: "Reposts", data: [], borderColor: "#ec4899", borderWidth: 1.5, pointRadius: 0, fill: false, tension: 0.3 },
            { label: "Follows", data: [], borderColor: "#f59e0b", borderWidth: 1.5, pointRadius: 0, fill: false, tension: 0.3 },
        ],
    },
    options: chartDefaults,
});

// Hourly throughput chart
const throughputChart = new Chart(document.getElementById("throughputChart").getContext("2d"), {
    type: "bar",
    data: {
        labels: [],
        datasets: [
            { label: "Posts",   data: [], backgroundColor: "rgba(59,130,246,0.6)", borderRadius: 3 },
            { label: "Likes",   data: [], backgroundColor: "rgba(34,197,94,0.6)", borderRadius: 3 },
            { label: "Reposts", data: [], backgroundColor: "rgba(236,72,153,0.6)", borderRadius: 3 },
        ],
    },
    options: {
        ...chartDefaults,
        plugins: {
            ...chartDefaults.plugins,
            legend: { display: false },
        },
        scales: {
            ...chartDefaults.scales,
            x: { ...chartDefaults.scales.x, stacked: true },
            y: { ...chartDefaults.scales.y, stacked: true },
        },
    },
});

// ── API fetcher ──
async function fetchJSON(url) {
    const r = await fetch(url);
    return r.json();
}

// ── Refresh functions ──
async function refreshStatus() {
    try {
        const d = await fetchJSON("/api/status");
        document.getElementById("s-posts").textContent = fmt(d.counts.posts);
        document.getElementById("s-likes").textContent = fmt(d.counts.likes);
        document.getElementById("s-reposts").textContent = fmt(d.counts.reposts);
        document.getElementById("s-follows").textContent = fmt(d.counts.follows);
        document.getElementById("s-rate").textContent = d.events_per_sec;
    } catch (e) { /* ignore */ }
}

async function refreshRate() {
    try {
        const data = await fetchJSON("/api/rate?hours=6");
        rateChart.data.labels = data.map(d => d.ts.slice(11, 16));
        rateChart.data.datasets[0].data = data.map(d => d.posts);
        rateChart.data.datasets[1].data = data.map(d => d.likes);
        rateChart.data.datasets[2].data = data.map(d => d.reposts);
        rateChart.data.datasets[3].data = data.map(d => d.follows);
        rateChart.update("none");
    } catch (e) { /* ignore */ }
}

async function refreshTop() {
    try {
        const data = await fetchJSON("/api/top?n=20&window=60");
        const tbody = document.getElementById("top-body");
        if (!data.length) {
            tbody.innerHTML = '<tr><td colspan="4" class="empty-state">No posts in the last 60 minutes</td></tr>';
            return;
        }
        tbody.innerHTML = data.map(d =>
            `<tr>` +
            `<td class="text-col">${escHtml(d.text)}</td>` +
            `<td>${embedBadge(d)}</td>` +
            `<td class="num-col">${d.likes}</td>` +
            `<td class="num-col">${d.reposts}</td>` +
            `</tr>`
        ).join("");
    } catch (e) { /* ignore */ }
}

async function refreshViral() {
    try {
        const d = await fetchJSON("/api/viral?threshold=100");
        document.getElementById("viral-pct").textContent = d.pct + "%";
        document.getElementById("viral-detail").textContent =
            `${fmt(d.viral)} with 100+ reposts / ${fmt(d.total)} root posts`;
    } catch (e) { /* ignore */ }
}

async function refreshRecent() {
    try {
        const data = await fetchJSON("/api/recent?n=50");
        const tbody = document.getElementById("recent-body");
        if (!data.length) {
            tbody.innerHTML = '<tr><td colspan="5" class="empty-state">No posts yet</td></tr>';
            return;
        }
        tbody.innerHTML = data.map(d =>
            `<tr>` +
            `<td class="text-col">${escHtml(d.text)}</td>` +
            `<td>${embedBadge(d)}</td>` +
            `<td class="num-col">${d.likes}</td>` +
            `<td class="num-col">${d.reposts}</td>` +
            `<td class="time-col">${timeAgo(d.time_us)}</td>` +
            `</tr>`
        ).join("");
    } catch (e) { /* ignore */ }
}

async function refreshHealth() {
    try {
        const d = await fetchJSON("/api/health");

        // Summary stats
        document.getElementById("h-since").textContent = d.collection_started || "not started";
        document.getElementById("h-dbsize").textContent = d.db_size_mb + " MB";
        document.getElementById("h-roots").textContent = fmt(d.totals.root_posts);
        document.getElementById("h-authors").textContent = fmt(d.totals.unique_authors);

        const pctLikes = d.engagement_coverage.pct_with_likes;
        const pctReposts = d.engagement_coverage.pct_with_reposts;
        const elLikes = document.getElementById("h-pct-likes");
        const elReposts = document.getElementById("h-pct-reposts");
        elLikes.textContent = pctLikes + "%";
        elReposts.textContent = pctReposts + "%";
        elLikes.className = "health-value " + (pctLikes > 20 ? "good" : pctLikes > 5 ? "warn" : "bad");
        elReposts.className = "health-value " + (pctReposts > 5 ? "good" : pctReposts > 1 ? "warn" : "bad");

        // Repost tier bars
        const tiers = d.repost_tiers;
        const tierKeys = Object.keys(tiers).map(Number).sort((a, b) => a - b);
        const maxTier = Math.max(...tierKeys.map(k => tiers[String(k)]), 1);
        const tierContainer = document.getElementById("tier-bars");
        tierContainer.innerHTML = tierKeys.map(k => {
            const count = tiers[String(k)];
            const pct = Math.max((count / maxTier) * 100, count > 0 ? 2 : 0);
            const color = k >= 100 ? "#ec4899" : k >= 25 ? "#f59e0b" : "#3b82f6";
            return `<div class="tier-bar">` +
                `<span class="tier-label">${k}+</span>` +
                `<div class="tier-track"><div class="tier-fill" style="width:${pct}%;background:${color}"></div></div>` +
                `<span class="tier-count">${fmt(count)}</span>` +
                `</div>`;
        }).join("");

        // All-time most reposted
        const tbody = document.getElementById("alltime-body");
        if (d.top_reposted.length) {
            tbody.innerHTML = d.top_reposted.map(p =>
                `<tr>` +
                `<td class="text-col">${escHtml(p.text)}</td>` +
                `<td class="num-col">${p.reposts}</td>` +
                `<td class="time-col">${timeAgo(p.time_us)}</td>` +
                `</tr>`
            ).join("");
        }

        // Hourly throughput chart
        if (d.hourly_throughput.length) {
            throughputChart.data.labels = d.hourly_throughput.map(h => h.hour.slice(11, 16));
            throughputChart.data.datasets[0].data = d.hourly_throughput.map(h => h.posts);
            throughputChart.data.datasets[1].data = d.hourly_throughput.map(h => h.likes);
            throughputChart.data.datasets[2].data = d.hourly_throughput.map(h => h.reposts);
            throughputChart.update("none");
        }
    } catch (e) { /* ignore */ }
}

// ── Refresh all ──
async function refreshAll() {
    await Promise.all([
        refreshStatus(),
        refreshRate(),
        refreshTop(),
        refreshViral(),
        refreshRecent(),
        refreshHealth(),
    ]);
}

refreshAll();
setInterval(refreshAll, 30000);
