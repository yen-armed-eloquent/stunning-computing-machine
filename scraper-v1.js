const fs = require('fs');
const { execSync } = require('child_process');
const axios = require('axios');
const parquet = require('parquetjs-lite');

// --- 🛠️ CONFIGURATION ---
const PARQUET_FILE = 'ig_master_data.parquet';
const REPORT_FILE = 'scraping_report.txt';
const REMOTE_PATH = 'vfx:/IG_Scraping/';
const WORKER_COUNT = 7; 
const MAX_COMMENTS_PER_POST = 50000;

// --- 🛡️ COOKIES & HEADERS (Placeholder - apni asli cookies yahan dalein) ---
const rawCookiesJson = [
    { "name": "sessionid", "value": "34851865843%3ACnYkCa6qjGPFcW%3A7%3AAYgj4BL5ueSBe4ETd3YXD4PE6C9EHH-nxShNa4XINA" },
    { "name": "csrftoken", "value": "CoBFT1SDnGcdRS0i2lov3sCyoxWcKN5g" }
];
const cookieString = rawCookiesJson.map(c => `${c.name}=${c.value}`).join('; ');
const STEALTH_HEADERS = {
    'x-ig-app-id': '936619743392459',
    'Cookie': cookieString,
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Referer': 'https://www.instagram.com/'
};

// --- 📊 ENTERPRISE STATS ---
let stats = { success: 0, failed: 0, total: 0, errors: [] };

// --- 📁 PARQUET SCHEMA (Full Rich Dump) ---
const schema = new parquet.ParquetSchema({
    shortcode: { type: 'UTF8' },
    scrapedAt: { type: 'UTF8' },
    postRawData: { type: 'UTF8' },    // Full Post JSON as string
    commentsRawData: { type: 'UTF8' } // Full Comments JSON as string
});

// --- ☁️ REAL-TIME SYNC TO MEGA ---
function syncToMega() {
    try {
        // -q flag for quiet mode, only shows errors
        execSync(`rclone copy ${PARQUET_FILE} ${REMOTE_PATH} -q`);
        execSync(`rclone copy ${REPORT_FILE} ${REMOTE_PATH} -q`);
    } catch (e) {
        console.log("   ⚠️ [Sync] Cloud path busy or temporary network glitch.");
    }
}

// --- 📝 ENTERPRISE REPORT GENERATOR ---
function updateReport() {
    const report = `
=========================================
🚀 ENTERPRISE SCRAPING REPORT
Generated: ${new Date().toISOString()}
=========================================
✅ Success: ${stats.success}
❌ Failed: ${stats.failed}
📋 Processed: ${stats.total}

FAILED LINKS LOG:
${stats.errors.length ? stats.errors.map(e => `[${e.sc}] Error: ${e.msg}`).join('\n') : 'None'}
=========================================`;
    fs.writeFileSync(REPORT_FILE, report);
}

// --- 💬 COMMENT SCRAPER (Full Rich Dump) ---
async function fetchAllRawComments(mediaId, workerId) {
    let allRaw = [];
    let minId = '';
    let hasNext = true;
    while (allRaw.length < MAX_COMMENTS_PER_POST && hasNext) {
        let url = `https://www.instagram.com/api/v1/media/${mediaId}/comments/?can_support_threading=true${minId ? `&min_id=${minId}` : ''}`;
        try {
            const res = await axios.get(url, { headers: STEALTH_HEADERS });
            if (!res.data.comments) break;
            allRaw.push(...res.data.comments);
            console.log(`   💬 [Worker ${workerId}] Extracted ${allRaw.length} comments...`);
            if (!res.data.next_min_id) hasNext = false;
            else {
                minId = res.data.next_min_id;
                await new Promise(r => setTimeout(r, 1200));
            }
        } catch (e) { break; }
    }
    return allRaw;
}

// --- 🚀 MAIN WORKER FUNCTION ---
async function scrapeAndAppend(shortcode, writer, workerId, taskNum, totalTasks) {
    console.log(`🚀 [Worker ${workerId}] Started Post: ${shortcode} (${taskNum}/${totalTasks})`);
    stats.total++;
    try {
        const docId = '8845758582119845';
        const vars = JSON.stringify({ shortcode: shortcode });
        const url = `https://www.instagram.com/graphql/query/?doc_id=${docId}&variables=${encodeURIComponent(vars)}`;
        
        const res = await axios.get(url, { headers: STEALTH_HEADERS });
        const rawPost = res.data.data.xdt_shortcode_v2 || res.data.data.xdt_shortcode_media;

        console.log(`📡 Post found: ${shortcode}. Extracting FULL RAW comments...`);
        const rawComments = await fetchAllRawComments(rawPost.id, workerId);

        // Append directly to Parquet (Streaming)
        await writer.appendRow({
            shortcode: shortcode,
            scrapedAt: new Date().toISOString(),
            postRawData: JSON.stringify(rawPost),
            commentsRawData: JSON.stringify(rawComments)
        });

        stats.success++;
        console.log(`✅ [Worker ${workerId}] Done Post: ${shortcode}`);
    } catch (err) {
        stats.failed++;
        stats.errors.push({ sc: shortcode, msg: err.message });
        console.log(`❌ [Worker ${workerId}] Error on ${shortcode}: ${err.message}`);
    }

    // Real-time Backup & Reporting after every post
    updateReport();
    syncToMega();
}

(async () => {
    console.log("🛠️ IG MEGA ENTERPRISE Engine Initialized...");
    let links = fs.readFileSync('links.txt', 'utf-8').split(/[\n\s,]+/).filter(Boolean);
    links = [...new Set(links)];

    const writer = await parquet.ParquetWriter.openFile(schema, PARQUET_FILE);
    let currentIndex = 0;

    async function worker(id) {
        while (currentIndex < links.length) {
            const taskIndex = currentIndex++;
            await scrapeAndAppend(links[taskIndex], writer, id, taskIndex + 1, links.length);
            await new Promise(r => setTimeout(r, 2000));
        }
    }

    const workers = Array.from({ length: WORKER_COUNT }, (_, i) => worker(i + 1));
    await Promise.all(workers);

    await writer.close();
    console.log("\n🎉 Extraction Complete! Master Parquet dataset and Enterprise Report are live on MEGA.");
})();
