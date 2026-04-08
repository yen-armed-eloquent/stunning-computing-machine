const fs = require('fs');
const { execSync } = require('child_process');
const axios = require('axios');
const parquet = require('parquetjs-lite');

// --- ⚙️ SETTINGS ---
const PARQUET_FILE = 'ig_master_data.parquet';
const REPORT_FILE = 'scraping_report.txt';
const REMOTE_PATH = 'vfx:/IG_Scraping/';
const WORKER_COUNT = 7; 
const MAX_COMMENTS = 50000;

// --- 🛡️ LATEST COOKIES INTEGRATED ---
const rawCookiesJson = [
    { "name": "ps_n", "value": "1" },
    { "name": "datr", "value": "wcfOafbBYFvBuRyHBRcxEO1A" },
    { "name": "ds_user_id", "value": "34851865843" },
    { "name": "csrftoken", "value": "CoBFT1SDnGcdRS0i2lov3sCyoxWcKN5g" },
    { "name": "ig_did", "value": "76B0D61F-6867-4A75-88C8-FC1A15058137" },
    { "name": "mid", "value": "ac7HwQALAAGvToJWin-i6VFVHDzB" },
    { "name": "sessionid", "value": "34851865843%3ACnYkCa6qjGPFcW%3A7%3AAYivzB7OCnajrFpozaKAsmZIfa7FxGAxwvVy2nxQHQ" },
    { "name": "rur", "value": "\"LDC\\05434851865843\\0541807213088:01fee9ad5b5a5231484594c7f17f0d96f544ea40b76ea26f58ffdda71d43fe08d4d80f98\"" }
];
const cookieString = rawCookiesJson.map(c => `${c.name}=${c.value}`).join('; ');

const STEALTH_HEADERS = {
    'x-ig-app-id': '936619743392459',
    'x-csrftoken': "CoBFT1SDnGcdRS0i2lov3sCyoxWcKN5g",
    'Cookie': cookieString,
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Referer': 'https://www.instagram.com/'
};

// --- 📊 STATS & PARQUET SCHEMA ---
let stats = { success: 0, failed: 0, total: 0, errors: [] };
const schema = new parquet.ParquetSchema({
    shortcode: { type: 'UTF8' },
    scrapedAt: { type: 'UTF8' },
    postRawData: { type: 'UTF8' },    // Full Rich Dump
    commentsRawData: { type: 'UTF8' } // Full Rich Dump
});

// --- ☁️ REAL-TIME SYNC & REPORTING ---
function syncToMega() {
    try {
        execSync(`rclone copy ${PARQUET_FILE} ${REMOTE_PATH} -q`);
        execSync(`rclone copy ${REPORT_FILE} ${REMOTE_PATH} -q`);
    } catch (e) { console.log("   ⚠️ [Sync] Cloud busy or connection glitch."); }
}

function updateReport() {
    const report = `=========================================\n🚀 ENTERPRISE REPORT\nGenerated: ${new Date().toISOString()}\n=========================================\n✅ Success: ${stats.success}\n❌ Failed: ${stats.failed}\n📋 Total: ${stats.total}\n\nERRORS:\n${stats.errors.map(e => `[${e.sc}] -> ${e.msg}`).join('\n')}\n=========================================`;
    fs.writeFileSync(REPORT_FILE, report);
}

// --- 🔍 ROBUST FETCH ENGINE ---
async function igFetch(url) {
    return await axios.get(url, { 
        headers: STEALTH_HEADERS, 
        maxRedirects: 0, 
        validateStatus: (s) => s >= 200 && s < 303 
    });
}

async function fetchAllRawComments(mediaId, workerId) {
    let allComments = [];
    let minId = '';
    let hasNext = true;
    while (allComments.length < MAX_COMMENTS && hasNext) {
        let url = `https://www.instagram.com/api/v1/media/${mediaId}/comments/?can_support_threading=true${minId ? `&min_id=${minId}` : ''}`;
        try {
            const res = await igFetch(url);
            if (!res.data.comments) break;
            allComments.push(...res.data.comments);
            console.log(`   💬 [Worker ${workerId}] Total Scraped: ${allComments.length}...`);
            if (!res.data.next_min_id) hasNext = false;
            else { minId = res.data.next_min_id; await new Promise(r => setTimeout(r, 1500)); }
        } catch (e) { break; }
    }
    return allComments;
}

async function scrapeAndAppend(shortcode, writer, workerId, taskNum, total) {
    console.log(`🚀 [Worker ${workerId}] Started Post: ${shortcode} (${taskNum}/${total})`);
    stats.total++;
    try {
        const docId = '8845758582119845';
        const vars = JSON.stringify({ shortcode });
        const url = `https://www.instagram.com/graphql/query/?doc_id=${docId}&variables=${encodeURIComponent(vars)}`;
        
        const res = await igFetch(url);
        const rawData = res.data.data?.xdt_shortcode_v2 || res.data.data?.xdt_shortcode_media;

        // 🔥 FIX: Check for Null Data before accessing .id
        if (!rawData) throw new Error("POST_NOT_FOUND_OR_BLOCKED");

        const comments = await fetchAllRawComments(rawData.id, workerId);

        await writer.appendRow({
            shortcode: shortcode,
            scrapedAt: new Date().toISOString(),
            postRawData: JSON.stringify(rawData),
            commentsRawData: JSON.stringify(comments)
        });

        stats.success++;
        console.log(`✅ [Worker ${workerId}] Done Post: ${shortcode}`);
    } catch (err) {
        stats.failed++;
        let msg = err.response?.status === 302 ? "SESSION_EXPIRED" : err.message;
        stats.errors.push({ sc: shortcode, msg: msg });
        console.log(`❌ [Worker ${workerId}] Error: ${msg}`);
        if (msg === "SESSION_EXPIRED") { console.log("🛑 Cookies are dead!"); process.exit(1); }
    }
    updateReport();
    syncToMega(); // REAL-TIME SYNC
}

(async () => {
    let links = fs.readFileSync('links.txt', 'utf-8').split(/[\n\s,]+/).filter(Boolean);
    const writer = await parquet.ParquetWriter.openFile(schema, PARQUET_FILE);
    let currentIndex = 0;

    async function worker(id) {
        while (currentIndex < links.length) {
            const i = currentIndex++;
            await scrapeAndAppend(links[i], writer, id, i + 1, links.length);
            await new Promise(r => setTimeout(r, 2000));
        }
    }

    await Promise.all(Array.from({ length: WORKER_COUNT }, (_, i) => worker(i + 1)));
    await writer.close();
    console.log("\n🎉 Extraction Finished. All data and report are on MEGA.");
})();
