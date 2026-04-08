const fs = require('fs');
const { execSync } = require('child_process');
const axios = require('axios');
const parquet = require('parquetjs-lite');

// --- 🛠️ CONFIGURATION ---
const PARQUET_FILE = 'ig_master_data.parquet';
const REPORT_FILE = 'scraping_report.txt';
const REMOTE_PATH = 'vfx:/IG_Scraping/';
const WORKER_COUNT = 5; 
const MAX_COMMENTS_PER_POST = 50000;

// --- 🛡️ UPDATED COOKIES (INTEGRATED) ---
const rawCookiesJson = [
    { "name": "ps_n", "value": "1" },
    { "name": "datr", "value": "wcfOafbBYFvBuRyHBRcxEO1A" },
    { "name": "ds_user_id", "value": "34851865843" },
    { "name": "csrftoken", "value": "CoBFT1SDnGcdRS0i2lov3sCyoxWcKN5g" },
    { "name": "ig_did", "value": "76B0D61F-6867-4A75-88C8-FC1A15058137" },
    { "name": "mid", "value": "ac7HwQALAAGvToJWin-i6VFVHDzB" },
    { "name": "sessionid", "value": "34851865843%3ACnYkCa6qjGPFcW%3A7%3AAYivzB7OCnajrFpozaKAsmZIfa7FxGAxwvVy2nxQHQ" }
];
const cookieString = rawCookiesJson.map(c => `${c.name}=${c.value}`).join('; ');
const csrfToken = "CoBFT1SDnGcdRS0i2lov3sCyoxWcKN5g";

const STEALTH_HEADERS = {
    'x-ig-app-id': '936619743392459',
    'x-csrftoken': csrfToken,
    'Cookie': cookieString,
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'Referer': 'https://www.instagram.com/'
};

// --- 📊 STATS & SCHEMA ---
let stats = { success: 0, failed: 0, total: 0, errors: [] };
const schema = new parquet.ParquetSchema({
    shortcode: { type: 'UTF8' },
    scrapedAt: { type: 'UTF8' },
    postRawData: { type: 'UTF8' },
    commentsRawData: { type: 'UTF8' }
});

// --- ☁️ FUNCTIONS ---
function syncToMega() {
    try {
        execSync(`rclone copy ${PARQUET_FILE} ${REMOTE_PATH} -q`);
        execSync(`rclone copy ${REPORT_FILE} ${REMOTE_PATH} -q`);
    } catch (e) { console.log("   ⚠️ [Sync] Cloud busy, retrying next post..."); }
}

function updateReport() {
    const report = `=========================================
🚀 ENTERPRISE SCRAPING REPORT
Timestamp: ${new Date().toISOString()}
=========================================
✅ Success: ${stats.success}
❌ Failed: ${stats.failed}
📋 Processed: ${stats.total}

FAILED LINKS:
${stats.errors.map(e => `[${e.sc}] -> ${e.msg}`).join('\n')}
=========================================`;
    fs.writeFileSync(REPORT_FILE, report);
}

async function safeFetch(url) {
    return await axios.get(url, { 
        headers: STEALTH_HEADERS,
        maxRedirects: 0,
        validateStatus: (status) => status >= 200 && status < 303 
    });
}

async function fetchAllRawComments(mediaId, workerId) {
    let allRaw = [];
    let minId = '';
    let hasNext = true;
    while (allRaw.length < MAX_COMMENTS_PER_POST && hasNext) {
        let url = `https://www.instagram.com/api/v1/media/${mediaId}/comments/?can_support_threading=true${minId ? `&min_id=${minId}` : ''}`;
        try {
            const res = await safeFetch(url);
            if (!res.data.comments) break;
            allRaw.push(...res.data.comments);
            console.log(`   💬 [Worker ${workerId}] Extracted ${allRaw.length} comments...`);
            if (!res.data.next_min_id) hasNext = false;
            else {
                minId = res.data.next_min_id;
                await new Promise(r => setTimeout(r, 1500));
            }
        } catch (e) { break; }
    }
    return allRaw;
}

async function scrapeAndAppend(shortcode, writer, workerId, taskNum, total) {
    console.log(`🚀 [Worker ${workerId}] Started: ${shortcode} (${taskNum}/${total})`);
    stats.total++;
    try {
        const docId = '8845758582119845';
        const vars = JSON.stringify({ shortcode });
        const url = `https://www.instagram.com/graphql/query/?doc_id=${docId}&variables=${encodeURIComponent(vars)}`;
        
        const res = await safeFetch(url);
        const rawPost = res.data.data.xdt_shortcode_v2 || res.data.data.xdt_shortcode_media;

        const rawComments = await fetchAllRawComments(rawPost.id, workerId);

        await writer.appendRow({
            shortcode: shortcode,
            scrapedAt: new Date().toISOString(),
            postRawData: JSON.stringify(rawPost),
            commentsRawData: JSON.stringify(rawComments)
        });

        stats.success++;
        console.log(`✅ [Worker ${workerId}] Finished: ${shortcode}`);
    } catch (err) {
        stats.failed++;
        let msg = err.response && (err.response.status === 302) ? "COOKIE_EXPIRED" : err.message;
        stats.errors.push({ sc: shortcode, msg: msg });
        console.log(`❌ [Worker ${workerId}] Error: ${msg}`);
        if (msg === "COOKIE_EXPIRED") process.exit(1);
    }
    updateReport();
    syncToMega();
}

(async () => {
    const links = fs.readFileSync('links.txt', 'utf-8').split(/[\n\s,]+/).filter(Boolean);
    const writer = await parquet.ParquetWriter.openFile(schema, PARQUET_FILE);
    let currentIndex = 0;

    async function worker(id) {
        while (currentIndex < links.length) {
            const i = currentIndex++;
            await scrapeAndAppend(links[i], writer, id, i + 1, links.length);
            await new Promise(r => setTimeout(r, 2500));
        }
    }

    await Promise.all(Array.from({ length: WORKER_COUNT }, (_, i) => worker(i + 1)));
    await writer.close();
    console.log("\n🎉 Process Finished! Data saved to Parquet and Sync to MEGA completed.");
})();
