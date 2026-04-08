const fs = require('fs');
const path = require('path');
const axios = require('axios');
const { HttpsProxyAgent } = require('https-proxy-agent');

// ==========================================
// 📂 DATA FOLDER SETUP
// ==========================================
const OUTPUT_DIR = path.join(__dirname, 'scraped_data');
if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR);
    console.log("📁 Naya folder 'scraped_data' create ho gaya hai.");
}

// ==========================================
// 1. 🍪 NAYI COOKIES (Updated)
// ==========================================
const rawCookiesJson = [
    { "name": "ps_n", "value": "1" },
    { "name": "datr", "value": "wcfOafbBYFvBuRyHBRcxEO1A" },
    { "name": "ds_user_id", "value": "34851865843" },
    { "name": "csrftoken", "value": "CoBFT1SDnGcdRS0i2lov3sCyoxWcKN5g" },
    { "name": "ig_did", "value": "76B0D61F-6867-4A75-88C8-FC1A15058137" },
    { "name": "ps_l", "value": "1" },
    { "name": "wd", "value": "1517x674" },
    { "name": "mid", "value": "ac7HwQALAAGvToJWin-i6VFVHDzB" },
    { "name": "sessionid", "value": "34851865843%3ACnYkCa6qjGPFcW%3A7%3AAYhcGLFLa5wBaoSrC9aolbasa489qR6ttbEhTmWVgA" },
    { "name": "dpr", "value": "0.8999999761581421" },
    { "name": "rur", "value": "\"LLA\\05434851865843\\0541807097904:01fee7fcf381dbb699ad3e4eb809c1ede746671d95079656501937f83afe98a3408f8c90\"" }
];
const cookieString = rawCookiesJson.map(c => `${c.name}=${c.value}`).join('; ');
const csrfToken = rawCookiesJson.find(c => c.name === 'csrftoken')?.value || '';

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

// ==========================================
// 🛑 GRACEFUL STOP (Ctrl+C Feature)
// ==========================================
let isForceStopped = false;
process.on('SIGINT', () => {
    console.log("\n\n🛑 [STOP COMMAND] Aapne Ctrl+C dabaya hai!");
    console.log(`💾 Data loss = Zero! Ab tak ka saara data 'scraped_data' folder mein already save hai.`);
    console.log("👋 Script ko safely band kiya ja raha hai...");
    isForceStopped = true;
    process.exit(0);
});

// ==========================================
// 🌐 PROXY SYSTEM
// ==========================================
let proxyList = [];
try {
    const proxyFileContent = fs.readFileSync('Webshare 10 proxies.txt', 'utf-8');
    proxyList = proxyFileContent.split('\n').map(line => line.trim()).filter(Boolean).map(line => {
        const parts = line.split(':');
        if (parts.length === 4) return `http://${parts[2]}:${parts[3]}@${parts[0]}:${parts[1]}`;
        return null;
    }).filter(Boolean);
    console.log(`🛡️ Total Proxies Loaded: ${proxyList.length}`);
} catch (err) {
    console.error("⚠️ Proxy file nahi mili.");
}

function getRandomProxy() {
    if (proxyList.length === 0) return null;
    return proxyList[Math.floor(Math.random() * proxyList.length)];
}

// URL se sirf IP nikalne ka chota sa function (Console mein dikhane ke liye)
function extractIP(proxyUrl) {
    if (!proxyUrl) return 'DIRECT-IP';
    try {
        return proxyUrl.split('@')[1].split(':')[0]; // Returns just the IP part
    } catch (e) {
        return 'UNKNOWN-IP';
    }
}

// ==========================================
// 🚀 FETCH ENGINE WITH AUTO-SKIP & RETRIES
// ==========================================
// Yahan workerId add kiya hai taake pata chale kon sa worker konsi IP use kar raha hai
async function igFetch(url, retries = 3, workerId = 'System') {
    if (retries === 0) {
        console.log(`⏭️ [Worker ${workerId}] 3 dafa try kiya, link issue kar raha hai. Skipping...`);
        return null; 
    }

    let config = { headers: STEALTH_HEADERS, timeout: 15000 };
    const proxyUrl = getRandomProxy();
    const currentIP = extractIP(proxyUrl);
    
    if (proxyUrl) {
        const httpsAgent = new HttpsProxyAgent(proxyUrl);
        config.httpsAgent = httpsAgent;
        config.httpAgent = httpsAgent;
    }

    console.log(`🌐 [Worker ${workerId}] Requesting via Proxy IP: ${currentIP} ...`);

    try {
        const response = await axios.get(url, config);
        return response.data;
    } catch (error) {
        if (error.response && error.response.status === 429) {
            console.log(`⏳ [Worker ${workerId}] [429 Limit Hit!] IP ${currentIP} block hui. Nayi proxy aur 10s wait... (Retries left: ${retries - 1})`);
            await wait(10000);
            return await igFetch(url, retries - 1, workerId);
        }
        console.log(`❌ [Worker ${workerId}] Proxy IP ${currentIP} failed or slow. Retrying...`);
        await wait(3000);
        return await igFetch(url, retries - 1, workerId); 
    }
}

// ==========================================
// 💬 COMMENTS FETCHER
// ==========================================
async function fetchAllRawComments(mediaId, maxLimit = 5000000000, workerId) {
    let rawCommentsDump = []; 
    let totalFetched = 0;
    let minId = '';
    let hasNext = true;
    
    while (totalFetched < maxLimit && hasNext && !isForceStopped) {
        let url = `https://www.instagram.com/api/v1/media/${mediaId}/comments/?can_support_threading=true`;
        if (minId) url += `&min_id=${minId}`;
        
        const data = await igFetch(url, 3, workerId);
        if (!data || !data.comments || data.comments.length === 0) break;
        
        rawCommentsDump.push(data);
        totalFetched += data.comments.length;
        
        console.log(`   💬 [Worker ${workerId}] Extracted ${totalFetched} comments...`);
        
        if (!data.next_min_id) {
            hasNext = false;
        } else {
            minId = data.next_min_id;
            await wait(1500);
        }
    }
    return rawCommentsDump;
}

// ==========================================
// 📄 SINGLE POST SCRAPER & REAL-TIME SAVE
// ==========================================
async function scrapeSinglePostRaw(shortcode, commentLimit, workerId) {
    const docId = '8845758582119845';
    const vars = JSON.stringify({ shortcode: shortcode, fetch_tagged_user_count: null, hoisted_comment_id: null, hoisted_reply_id: null });
    const url = `https://www.instagram.com/graphql/query/?doc_id=${docId}&variables=${encodeURIComponent(vars)}`;
    
    const data = await igFetch(url, 3, workerId);
    if (!data || !data.data) return null;

    const rawData = data.data.xdt_shortcode_v2 || data.data.xdt_shortcode_media;
    if (!rawData) return null;

    console.log(`📡 [Worker ${workerId}] Post ${shortcode} mil gayi. Comments nikal raha hoon...`);
    const rawComments = await fetchAllRawComments(rawData.id, commentLimit, workerId);

    const postFinalData = {
        shortcode: shortcode,
        scrapedAt: new Date().toISOString(),
        totalCommentsScraped: rawComments.reduce((acc, curr) => acc + (curr.comments ? curr.comments.length : 0), 0),
        postRawData: rawData,
        commentsRawData: rawComments
    };

    // 🔥 REAL-TIME SAVE TRIGGER 🔥
    const filePath = path.join(OUTPUT_DIR, `post_${shortcode}_${Date.now()}.json`);
    fs.writeFileSync(filePath, JSON.stringify(postFinalData, null, 2));
    console.log(`💾 [Worker ${workerId}] SAVED -> post_${shortcode}_xxx.json`);

    return true;
}

function extractShortcode(url) {
    const match = url.trim().match(/(?:p|reels?|tv)\/([A-Za-z0-9_\-]+)/);
    return match ? match[1] : url.trim(); 
}

// ==========================================
// ⚡ QUEUE ENGINE (7 WORKERS)
// ==========================================
async function runMegaPipelineQueue(shortcodes, concurrentLimit, commentLimit) {
    let successCount = 0;
    let currentIndex = 0; 

    async function worker(workerId) {
        while (currentIndex < shortcodes.length && !isForceStopped) {
            const taskIndex = currentIndex++; 
            const sc = shortcodes[taskIndex];
            
            console.log(`\n🚀 [Worker ${workerId}] Started Post: ${sc} (Task ${taskIndex + 1}/${shortcodes.length})`);
            
            const isSuccess = await scrapeSinglePostRaw(sc, commentLimit, workerId);
            if (isSuccess) {
                successCount++;
            } else {
                console.log(`⏭️ [Worker ${workerId}] Skipping Post: ${sc} (Failed after retries)`);
            }

            await wait(2500); 
        }
        console.log(`🛑 [Worker ${workerId}] Free ho gaya. Koi naya link nahi bacha.`);
    }

    const workers = [];
    for (let i = 1; i <= concurrentLimit; i++) {
        workers.push(worker(i));
    }

    await Promise.all(workers);
    return { success: successCount, total: shortcodes.length };
}

// ==========================================
// 🏁 EXECUTION START
// ==========================================
(async () => {
    console.log("🛠️ IG MEGA RAW Scraper Engine Initialized...");
    
    let rawLinks = [];
    try {
        const fileContent = fs.readFileSync('links.txt', 'utf-8');
        rawLinks = fileContent.split(/[\n\s,]+/).filter(Boolean).map(extractShortcode);
        rawLinks = [...new Set(rawLinks)]; 
    } catch (err) {
        console.error("❌ links.txt file nahi mili!");
        return;
    }

    if (rawLinks.length === 0) return;

    // 🔥 AAPKI DEMAND: 7 WORKERS 🔥
    const WORKER_COUNT = 7; 
    const MAX_COMMENTS_PER_POST = 20000; 
    
    console.log(`📋 Total unique links found: ${rawLinks.length}`);
    console.log(`🔥 Starting ${WORKER_COUNT} Parallel Workers...`);
    console.log(`💡 [TIP] Agar script rokni ho, toh terminal mein 'Ctrl + C' dabayen.\n`);
    
    const result = await runMegaPipelineQueue(rawLinks, WORKER_COUNT, MAX_COMMENTS_PER_POST);
    
    if (!isForceStopped) {
        console.log(`\n🎉 Extraction Complete! Saara data 'scraped_data' folder mein hai.`);
        console.log(`✅ Successfully Scraped: ${result.success} / ${result.total}`);
    }
})();