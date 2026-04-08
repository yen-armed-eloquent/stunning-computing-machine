const fs = require('fs');
const axios = require('axios');

// 1. ⚠️ APNI COOKIES YAHAN UPDATE KARO (Abhi isme tumhari di hui cookies hain)
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

// 💬 Raw Comments Fetcher (Direct JSON Dump)
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
        
        // Pura response store kar rahe hain bilkul original file jesa
        rawCommentsDump.push(data);
        totalFetched += data.comments.length;
        
        console.log(`   💬 Raw Comments Fetching: ${totalFetched}...`);
        
        if (!data.next_min_id) {
            hasNext = false;
        } else {
            minId = data.next_min_id;
            await wait(1200); // Stealth delay page change par
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

// ⚡ Asli Concurrency Engine (Worker Pool / Task Queue)
async function runMegaPipelineQueue(shortcodes, concurrentLimit, commentLimit) {
    let allRawData = [];
    let successCount = 0;
    let currentIndex = 0; // Queue ka pointer

    // Yeh ek Worker hai jo continuously free hote hi agla link uthayega
    async function worker(workerId) {
        while (currentIndex < shortcodes.length) {
            const taskIndex = currentIndex++; // Apna link uthao aur aagay barho
            const sc = shortcodes[taskIndex];
            
            console.log(`\n🚀 [Worker ${workerId}] Started Post: ${sc} (${taskIndex + 1}/${shortcodes.length})`);
            
            const data = await scrapeSinglePostRaw(sc, commentLimit);
            if (data) {
                console.log(`✅ [Worker ${workerId}] Done Post: ${sc}`);
                successCount++;
                allRawData.push(data);
            } else {
                console.log(`❌ [Worker ${workerId}] Failed Post: ${sc}`);
            }

            // Agla link uthane se pehle saans (Stealth)
            await wait(2000); 
        }
        console.log(`🛑 [Worker ${workerId}] Koi naya link nahi bacha. Going to sleep.`);
    }

    // Workers banayen (Jitni limit di hogi, utne parallel threads banenge)
    const workers = [];
    console.log(`🔥 Starting ${concurrentLimit} Concurrent Workers...`);
    for (let i = 1; i <= concurrentLimit; i++) {
        workers.push(worker(i));
    }

    // Wait jab tak saare workers apna kaam khatam na kar lein
    await Promise.all(workers);
    
    return { rawPosts: allRawData, success: successCount, total: shortcodes.length };
}

// --- EXECUTION START ---
(async () => {
    console.log("🛠️ IG MEGA RAW Scraper Engine Initialized...");
    
    // 1. links.txt se URLs read karna
    let rawLinks = [];
    try {
        const fileContent = fs.readFileSync('links.txt', 'utf-8');
        rawLinks = fileContent.split(/[\n\s,]+/).filter(Boolean).map(extractShortcode);
        rawLinks = [...new Set(rawLinks)]; // Remove duplicates
    } catch (err) {
        console.error("❌ links.txt file nahi mili ya read nahi hui. Pehle links.txt file banao aur usme links daalo.");
        return;
    }

    if (rawLinks.length === 0) {
        console.log("⚠️ links.txt file khaali hai!");
        return;
    }

    // 🔥 SETTINGS 🔥
    const WORKER_COUNT = 5; // Kitne parallel threads chalane hain (Isay 3 ya 5 rakh sakte ho)
    const MAX_COMMENTS_PER_POST = 20000; // Har post ke comments ki max limit
    
    console.log(`📋 Total unique links found: ${rawLinks.length}`);
    
    // Naya Queue System Run Karein
    const result = await runMegaPipelineQueue(rawLinks, WORKER_COUNT, MAX_COMMENTS_PER_POST);
    
    // 2. Data ko JSON file mein save karna
    const timestamp = Date.now();
    const filename = `IG_RawData_Full_NodeJS_${timestamp}.json`;
    
    fs.writeFileSync(filename, JSON.stringify({ rawPosts: result.rawPosts }, null, 2));
    
    console.log(`\n🎉 Hallelujah! Extraction complete.`);
    console.log(`✅ Successfully Scraped: ${result.success} / ${result.total}`);
    console.log(`💾 File saved: ${filename}`);
})();