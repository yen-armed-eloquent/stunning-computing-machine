const fs = require('fs');
const axios = require('axios');
const parquet = require('parquetjs-lite');
const { execSync } = require('child_process');

// 1. ⚠️ APNI COOKIES YAHAN UPDATE KARO
const rawCookiesJson = [
    { "name": "ps_n", "value": "1" },
    { "name": "datr", "value": "wcfOafbBYFvBuRyHBRcxEO1A" },
    { "name": "ds_user_id", "value": "34851865843" },
    { "name": "csrftoken", "value": "CoBFT1SDnGcdRS0i2lov3sCyoxWcKN5g" },
    { "name": "ig_did", "value": "76B0D61F-6867-4A75-88C8-FC1A15058137" },
    { "name": "ps_l", "value": "1" },
    { "name": "wd", "value": "1517x674" },
    { "name": "mid", "value": "ac7HwQALAAGvToJWin-i6VFVHDzB" },
    { "name": "sessionid", "value": "34851865843%3ACnYkCa6qjGPFcW%3A7%3AAYgj4BL5ueSBe4ETd3YXD4PE6C9EHH-nxShNa4XINA" },
    { "name": "dpr", "value": "0.8999999761581421" },
    { "name": "rur", "value": "\"LLA\\05434851865843\\0541807110100:01fe907d193dcb6519cd530369ab27a8eed5a796b7cfea820a29ddffb2b6026f4f53eb7d\"" }
];
const cookieString = rawCookiesJson.map(c => `${c.name}=${c.value}`).join('; ');
const csrfToken = rawCookiesJson.find(c => c.name === 'csrftoken')?.value || '';

// --- ⚙️ SETTINGS FOR STORAGE & CLOUD ---
const PARQUET_FILE = 'ig_master_data.parquet';
const REPORT_FILE = 'scraping_report.txt';
const REMOTE_PATH = 'vfx:/IG_Scraping/'; // Mega Cloud Path
let stats = { success: 0, failed: 0, total: 0, errors: [] };

// --- 📊 PARQUET SCHEMA ---
const schema = new parquet.ParquetSchema({
    shortcode: { type: 'UTF8' },
    scrapedAt: { type: 'UTF8' },
    postRawData: { type: 'UTF8' },    // Full JSON dump as string
    commentsRawData: { type: 'UTF8' } // Full JSON dump as string
});

// 🛡️ Stealth Headers Setup (IG security bypass)
const STEALTH_HEADERS = {
    'x-ig-app-id': '936619743392459',
    'x-csrftoken': csrfToken,
    'x-requested-with': 'XMLHttpRequest',
    'Cookie': cookieString,
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'Referer': 'https://www.instagram.com/'
};

const wait = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// --- ☁️ REAL-TIME SYNC & REPORTING FUNCTIONS ---
function syncToMega() {
    try {
        execSync(`rclone copy ${PARQUET_FILE} ${REMOTE_PATH} -q`);
        execSync(`rclone copy ${REPORT_FILE} ${REMOTE_PATH} -q`);
    } catch (e) {
        console.log("   ⚠️ [Sync] Cloud busy ya rclone path ka issue aaya.");
    }
}

function updateReport() {
    const report = `=========================================\n🚀 MEGA SCRAPER ENTERPRISE REPORT\nGenerated: ${new Date().toISOString()}\n=========================================\n✅ Success: ${stats.success}\n❌ Failed: ${stats.failed}\n📋 Total Tried: ${stats.total}\n\nERRORS:\n${stats.errors.map(e => `[${e.sc}] -> ${e.msg}`).join('\n')}\n=========================================`;
    fs.writeFileSync(REPORT_FILE, report);
}

// 🌐 Core Fetch with Auto-Retry for 429 Rate Limits
async function igFetch(url) {
    try {
        const response = await axios.get(url, { headers: STEALTH_HEADERS });
        return response.data;
    } catch (error) {
        if (error.response) {
            if (error.response.status === 429) {
                console.log('⏳ [429] Rate limited! 15 seconds wait kar raha hoon...');
                await wait(15000); // 15s Cool-down
                return await igFetch(url); // Auto Retry
            }
            console.error(`❌ HTTP Error: ${error.response.status} for URL: ${url}`);
        } else {
            console.error(`❌ Connection Error: ${error.message}`);
        }
        return null;
    }
}

// 💬 Raw Comments Fetcher
async function fetchAllRawComments(mediaId, maxLimit = 500) {
    let rawCommentsDump = []; 
    let totalFetched = 0;
    let minId = '';
    let hasNext = true;
    
    while (totalFetched < maxLimit && hasNext) {
        let url = `https://www.instagram.com/api/v1/media/${mediaId}/comments/?can_support_threading=true`;
        if (minId) url += `&min_id=${minId}`;
        
        const data = await igFetch(url);
        if (!data || !data.comments || data.comments.length === 0) break;
        
        rawCommentsDump.push(data);
        totalFetched += data.comments.length;
        
        console.log(`   💬 Raw Comments Fetching: ${totalFetched}...`);
        
        if (!data.next_min_id) {
            hasNext = false;
        } else {
            minId = data.next_min_id;
            await wait(1200); 
        }
    }
    return rawCommentsDump;
}

// 📄 Main Post ka Poora Raw GraphQL Data
async function scrapeSinglePostRaw(shortcode, commentLimit) {
    const docId = '8845758582119845';
    const vars = JSON.stringify({ shortcode: shortcode, fetch_tagged_user_count: null, hoisted_comment_id: null, hoisted_reply_id: null });
    const url = `https://www.instagram.com/graphql/query/?doc_id=${docId}&variables=${encodeURIComponent(vars)}`;
    
    const data = await igFetch(url);
    if (!data || !data.data) return null;

    const rawData = data.data.xdt_shortcode_v2 || data.data.xdt_shortcode_media;
    if (!rawData) return null;

    const mediaId = rawData.id;
    console.log(`📡 Post found: ${shortcode}. Extracting FULL RAW comments...`);
    
    const rawComments = await fetchAllRawComments(mediaId, commentLimit);

    return {
        shortcode: shortcode,
        postRawData: rawData,
        commentsRawData: rawComments
    };
}

// 🔗 URL se shortcode nikalna
function extractShortcode(url) {
    const match = url.trim().match(/(?:p|reels?|tv)\/([A-Za-z0-9_\-]+)/);
    return match ? match[1] : url.trim(); 
}

// ⚡ Asli Concurrency Engine (Worker Pool + Parquet Integration)
async function runMegaPipelineQueue(shortcodes, concurrentLimit, commentLimit) {
    const writer = await parquet.ParquetWriter.openFile(schema, PARQUET_FILE);
    let currentIndex = 0;

    async function worker(workerId) {
        while (currentIndex < shortcodes.length) {
            const taskIndex = currentIndex++; 
            const sc = shortcodes[taskIndex];
            stats.total++;
            
            console.log(`\n🚀 [Worker ${workerId}] Started Post: ${sc} (${taskIndex + 1}/${shortcodes.length})`);
            
            try {
                const data = await scrapeSinglePostRaw(sc, commentLimit);
                
                if (data) {
                    console.log(`✅ [Worker ${workerId}] Done Post: ${sc}`);
                    stats.success++;
                    
                    // 💾 Seedha Parquet mein write aur RAM save
                    await writer.appendRow({
                        shortcode: data.shortcode,
                        scrapedAt: new Date().toISOString(),
                        postRawData: JSON.stringify(data.postRawData),
                        commentsRawData: JSON.stringify(data.commentsRawData)
                    });
                } else {
                    // Agar igFetch null de, yani API ne data chupaya hai ya post udi hui hai
                    throw new Error("DATA_NULL_OR_BLOCKED");
                }
            } catch (err) {
                stats.failed++;
                let msg = err.message || "UNKNOWN_ERROR";
                stats.errors.push({ sc: sc, msg: msg });
                console.log(`❌ [Worker ${workerId}] Failed Post: ${sc} - ${msg}`);
            }

            // ☁️ Har link ke baad Live Report aur Mega Sync update karein
            updateReport();
            syncToMega();

            await wait(2000); // Stealth Delay
        }
        console.log(`🛑 [Worker ${workerId}] Koi naya link nahi bacha. Going to sleep.`);
    }

    console.log(`🔥 Starting ${concurrentLimit} Concurrent Workers...`);
    const workers = [];
    for (let i = 1; i <= concurrentLimit; i++) {
        workers.push(worker(i));
    }

    await Promise.all(workers);
    await writer.close(); // Parquet file ko safely close karna zaroori hai
}

// --- EXECUTION START ---
(async () => {
    console.log("🛠️ IG MEGA RAW Scraper Engine Initialized...");
    
    let rawLinks = [];
    try {
        const fileContent = fs.readFileSync('links.txt', 'utf-8');
        rawLinks = fileContent.split(/[\n\s,]+/).filter(Boolean).map(extractShortcode);
        rawLinks = [...new Set(rawLinks)]; 
    } catch (err) {
        console.error("❌ links.txt file nahi mili ya read nahi hui.");
        return;
    }

    if (rawLinks.length === 0) {
        console.log("⚠️ links.txt file khaali hai!");
        return;
    }

    // 🔥 SETTINGS 🔥
    const WORKER_COUNT = 5; 
    const MAX_COMMENTS_PER_POST = 20000; 
    
    console.log(`📋 Total unique links found: ${rawLinks.length}`);
    
    await runMegaPipelineQueue(rawLinks, WORKER_COUNT, MAX_COMMENTS_PER_POST);
    
    console.log(`\n🎉 Hallelujah! Extraction complete.`);
    console.log(`✅ Final Status -> Scraped: ${stats.success} | Failed: ${stats.failed}`);
    console.log(`💾 All data safely stored in ${PARQUET_FILE} and synced to MEGA.`);
})();
