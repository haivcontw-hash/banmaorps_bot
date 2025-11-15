const sqlite3 = require('sqlite3').verbose();
const https = require('https');
const { normalizeLanguageCode } = require('./i18n.js');

function resolveSupabaseConnectionString() {
    const candidates = [
        'SUPABASE_CONNECTION_STRING',
        'SUPABASE_CONNECTION_URI',
        'SUPABASE_DATABASE_URL',
        'SUPABASE_DB_URL',
        'SUPABASE_URL',
        'DATABASE_URL',
        'POSTGRES_URL'
    ];

    for (const key of candidates) {
        const raw = process.env[key];
        if (typeof raw !== 'string') {
            continue;
        }

        const trimmed = raw.trim();
        if (!trimmed) {
            continue;
        }

        const sanitised = sanitisePostgresConnectionString(trimmed);
        if (sanitised) {
            return sanitised;
        }
    }

    return null;
}

function sanitisePostgresConnectionString(raw) {
    try {
        const parsed = new URL(raw);

        if (!/^postgres(?:ql)?:$/i.test(parsed.protocol)) {
            return null;
        }

        if (parsed.username) {
            parsed.username = decodeURIComponent(parsed.username);
        }

        if (parsed.password) {
            parsed.password = decodeURIComponent(parsed.password);
        }

        // URL sẽ tự động encode lại khi toString()
        const normalisedProtocol = parsed.protocol.toLowerCase() === 'postgres:' ? 'postgresql:' : parsed.protocol;
        parsed.protocol = normalisedProtocol;
        return parsed.toString();
    } catch (error) {
        const match = raw.match(/^(postgres(?:ql)?:\/\/[^:]+:)([^@]+)@(.+)$/i);
        if (match) {
            return `${match[1]}${encodeURIComponent(match[2])}@${match[3]}`;
        }

        console.error('[Supabase] Chuỗi kết nối không hợp lệ:', error.message);
        return null;
    }
}

function resolveSupabaseServiceRoleKey() {
    const candidates = [
        'SUPABASE_SERVICE_ROLE_KEY',
        'SUPABASE_SERVICE_KEY',
        'SUPABASE_SECRET_KEY',
        'SUPABASE_API_KEY',
        'SUPABASE_KEY'
    ];

    for (const key of candidates) {
        const value = process.env[key];
        if (typeof value !== 'string') {
            continue;
        }

        const trimmed = value.trim();
        if (trimmed) {
            return trimmed;
        }
    }

    return null;
}

function normaliseSupabaseRestUrl(raw) {
    try {
        const parsed = new URL(raw);
        parsed.search = '';
        parsed.hash = '';

        const currentPath = parsed.pathname.replace(/\/+$/, '');
        if (!currentPath || currentPath === '' || currentPath === '/') {
            parsed.pathname = '/rest/v1';
        } else if (!/\/rest\/v1$/i.test(currentPath)) {
            parsed.pathname = `${currentPath.replace(/\/+$/, '')}/rest/v1`;
        }

        return parsed.toString().replace(/\/+$/, '');
    } catch (error) {
        return null;
    }
}

function resolveSupabaseRestUrl(connectionString) {
    const candidates = [
        'SUPABASE_REST_URL',
        'SUPABASE_REST_ENDPOINT',
        'SUPABASE_API_URL',
        'SUPABASE_URL'
    ];

    for (const key of candidates) {
        const value = process.env[key];
        if (typeof value !== 'string') {
            continue;
        }

        const trimmed = value.trim();
        if (!trimmed || !/^https?:\/\//i.test(trimmed)) {
            continue;
        }

        const normalised = normaliseSupabaseRestUrl(trimmed);
        if (normalised) {
            return normalised;
        }
    }

    if (!connectionString) {
        return null;
    }

    try {
        const parsed = new URL(connectionString);
        const host = parsed.hostname;
        if (!host) {
            return null;
        }

        const apiHost = host.replace(/^db\./i, '');
        return normaliseSupabaseRestUrl(`https://${apiHost}`);
    } catch (error) {
        return null;
    }
}

function createSupabaseRestClient(baseUrl, apiKey) {
    const base = new URL(baseUrl.endsWith('/') ? baseUrl : `${baseUrl}/`);

    return {
        async request(method, path, options = {}) {
            const { query, body, headers: extraHeaders, prefer } = options;
            const targetPath = path.startsWith('/') ? path.slice(1) : path;
            const url = new URL(targetPath, base);

            if (query) {
                if (query instanceof URLSearchParams) {
                    for (const [key, value] of query.entries()) {
                        url.searchParams.append(key, value);
                    }
                } else if (typeof query === 'object') {
                    for (const [key, value] of Object.entries(query)) {
                        if (value === undefined || value === null) {
                            continue;
                        }

                        if (Array.isArray(value)) {
                            for (const entry of value) {
                                url.searchParams.append(key, entry);
                            }
                        } else {
                            url.searchParams.append(key, value);
                        }
                    }
                }
            }

            const headers = {
                apikey: apiKey,
                Authorization: `Bearer ${apiKey}`,
                Accept: 'application/json',
                ...extraHeaders
            };

            let payload = null;
            if (body !== undefined) {
                payload = JSON.stringify(body);
                headers['Content-Type'] = 'application/json';
            }

            if (prefer) {
                headers['Prefer'] = prefer;
            }

            return await new Promise((resolve, reject) => {
                const req = https.request(url, { method, headers }, (res) => {
                    let data = '';
                    res.on('data', (chunk) => {
                        data += chunk;
                    });
                    res.on('end', () => {
                        let parsed = null;
                        if (data) {
                            try {
                                parsed = JSON.parse(data);
                            } catch (error) {
                                parsed = null;
                            }
                        }

                        const statusCode = res.statusCode || 0;
                        if (statusCode >= 200 && statusCode < 300) {
                            resolve({ ok: true, status: statusCode, data: parsed, headers: res.headers });
                        } else {
                            const message = parsed?.message || parsed?.error || parsed?.hint || data || `HTTP ${statusCode}`;
                            resolve({ ok: false, status: statusCode, error: message, data: parsed, headers: res.headers });
                        }
                    });
                });

                req.on('error', (error) => {
                    reject(error);
                });

                if (payload) {
                    req.write(payload);
                }

                req.end();
            });
        }
    };
}

let Pool = null;
try {
    ({ Pool } = require('pg'));
} catch (error) {
    // Module pg có thể chưa được cài đặt – sẽ cảnh báo khi cố kết nối.
}

const db = new sqlite3.Database('banmao.db', (err) => {
    if (err) {
        console.error("LỖI KHỞI TẠO DB:", err.message);
        process.exit(1);
    }
    console.log("Cơ sở dữ liệu SQLite đã kết nối.");
});
const ethers = require('ethers');

const SUPABASE_CONNECTION_STRING = resolveSupabaseConnectionString();
const SUPABASE_POOL_MAX = Number(process.env.SUPABASE_POOL_MAX || 5);
const SUPABASE_SERVICE_ROLE_KEY = resolveSupabaseServiceRoleKey();
const SUPABASE_REST_URL = resolveSupabaseRestUrl(SUPABASE_CONNECTION_STRING);

let supabasePool = null;
let supabaseRestClient = null;
let supabaseSchemaEnsured = false;

if (SUPABASE_CONNECTION_STRING) {
    if (!Pool) {
        console.warn('[Supabase] Module "pg" chưa được cài đặt. Vui lòng chạy npm install trước khi đồng bộ.');
    } else {
        try {
            supabasePool = new Pool({
                connectionString: SUPABASE_CONNECTION_STRING,
                max: Number.isFinite(SUPABASE_POOL_MAX) ? SUPABASE_POOL_MAX : 5,
                idleTimeoutMillis: 30_000,
                ssl: { require: true, rejectUnauthorized: false }
            });

            supabasePool.on('error', (err) => {
                console.error('[Supabase] Lỗi kết nối không mong muốn:', err);
            });

            supabasePool
                .query('select 1')
                .then(() => {
                    console.log('[Supabase] Đã kết nối tới PostgreSQL.');
                })
                .catch((err) => {
                    console.error('[Supabase] Không thể kiểm tra kết nối:', err.message);
                });
        } catch (error) {
            console.error('[Supabase] Lỗi khởi tạo Pool:', error.message);
        }
    }
} else {
    console.log('[Supabase] Không tìm thấy chuỗi kết nối, bỏ qua đồng bộ Supabase.');
}

if (SUPABASE_REST_URL && SUPABASE_SERVICE_ROLE_KEY) {
    try {
        supabaseRestClient = createSupabaseRestClient(SUPABASE_REST_URL, SUPABASE_SERVICE_ROLE_KEY);
        console.log(`[Supabase] REST API đã sẵn sàng tại ${SUPABASE_REST_URL}.`);
    } catch (error) {
        console.error('[Supabase] Không thể khởi tạo REST client:', error.message);
    }
} else if (SUPABASE_REST_URL && !SUPABASE_SERVICE_ROLE_KEY) {
    console.warn('[Supabase] Thiếu SUPABASE_SERVICE_ROLE_KEY nên không thể dùng REST API.');
} else if (!SUPABASE_REST_URL && SUPABASE_SERVICE_ROLE_KEY) {
    console.warn('[Supabase] Đã có khóa dịch vụ nhưng thiếu URL REST. Vui lòng cấu hình SUPABASE_REST_URL hoặc SUPABASE_URL.');
}

async function ensureSupabaseDailyCheckinSchema() {
    if (supabaseSchemaEnsured) {
        return;
    }

    if (supabasePool) {
        try {
            await supabasePool.query(`
                create table if not exists public.daily_checkins (
                    group_chat_id text not null,
                    user_id text not null,
                    streak integer not null default 0,
                    last_checkin_date text,
                    last_checkin_at bigint,
                    total_checkins integer not null default 0,
                    updated_at bigint,
                    constraint daily_checkins_pk primary key (group_chat_id, user_id)
                )
            `);

            await supabasePool.query(`
                create table if not exists public.daily_checkin_logs (
                    id bigserial primary key,
                    group_chat_id text not null,
                    user_id text not null,
                    checkin_date text not null,
                    checkin_at bigint not null,
                    streak integer not null
                )
            `);

            await supabasePool.query(`
                create index if not exists daily_checkin_logs_group_date_idx
                    on public.daily_checkin_logs (group_chat_id, checkin_date)
            `);

            supabaseSchemaEnsured = true;
            console.log('[Supabase] Đã đảm bảo cấu trúc bảng daily_checkins.');
        } catch (error) {
            console.error('[Supabase] Không thể khởi tạo bảng daily_checkins:', error.message);
        }
        return;
    }

    if (supabaseRestClient) {
        supabaseSchemaEnsured = true;
        console.warn('[Supabase] REST API đang được sử dụng. Vui lòng chạy sql/supabase_checkins.sql trong Supabase để tạo bảng nếu chưa có.');
    }
}

// --- Hàm Helper (Promisify) ---
const dbRun = (sql, params = []) => new Promise((resolve, reject) => {
    db.run(sql, params, function (err) {
        if (err) reject(err);
        else resolve(this); 
    });
});

const dbGet = (sql, params = []) => new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => {
        if (err) reject(err);
        else resolve(row);
    });
});

const dbAll = (sql, params = []) => new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
        if (err) reject(err);
        else resolve(rows);
    });
});

// Hàm khởi tạo database
async function init() {
    console.log("Đang khởi tạo cấu trúc bảng SQLite...");
    await dbRun(`
        CREATE TABLE IF NOT EXISTS users (
            chatId TEXT PRIMARY KEY,
            lang TEXT,
            wallets TEXT,
            lang_source TEXT DEFAULT 'auto'
        );
    `);

    try {
        await dbRun(`ALTER TABLE users ADD COLUMN lang_source TEXT DEFAULT 'auto'`);
    } catch (err) {
        if (!/duplicate column name/i.test(err.message)) {
            throw err;
        }
    }
    await dbRun(`
        CREATE TABLE IF NOT EXISTS pending_tokens (
            token TEXT PRIMARY KEY,
            walletAddress TEXT
        );
    `);
    await dbRun(`
        CREATE TABLE IF NOT EXISTS game_stats (
            walletAddress TEXT,
            result TEXT,
            stake REAL,
            timestamp INTEGER
        );
    `);
    await dbRun(`CREATE INDEX IF NOT EXISTS idx_wallet ON game_stats (walletAddress);`);
    await dbRun(`
        CREATE TABLE IF NOT EXISTS group_subscriptions (
            chatId TEXT PRIMARY KEY,
            lang TEXT,
            minStake REAL,
            messageThreadId TEXT,
            createdAt INTEGER,
            updatedAt INTEGER
        );
    `);
    try {
        await dbRun(`ALTER TABLE group_subscriptions ADD COLUMN messageThreadId TEXT`);
    } catch (err) {
        if (!/duplicate column name/i.test(err.message)) {
            throw err;
        }
    }
    await dbRun(`
        CREATE TABLE IF NOT EXISTS group_member_languages (
            groupChatId TEXT NOT NULL,
            userId TEXT NOT NULL,
            lang TEXT NOT NULL,
            updatedAt INTEGER NOT NULL,
            PRIMARY KEY (groupChatId, userId)
        );
    `);
    await dbRun(`
        CREATE TABLE IF NOT EXISTS daily_checkins (
            groupChatId TEXT NOT NULL,
            userId TEXT NOT NULL,
            streak INTEGER NOT NULL DEFAULT 0,
            lastCheckinDate TEXT,
            lastCheckinAt INTEGER,
            totalCheckins INTEGER NOT NULL DEFAULT 0,
            updatedAt INTEGER,
            PRIMARY KEY (groupChatId, userId)
        );
    `);
    await dbRun(`
        CREATE TABLE IF NOT EXISTS daily_checkin_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            groupChatId TEXT NOT NULL,
            userId TEXT NOT NULL,
            checkinDate TEXT NOT NULL,
            checkinAt INTEGER NOT NULL,
            streak INTEGER NOT NULL
        );
    `);
    await dbRun('CREATE INDEX IF NOT EXISTS idx_daily_checkin_logs_group_date ON daily_checkin_logs (groupChatId, checkinDate);');

    await ensureSupabaseDailyCheckinSchema();
    console.log("Cơ sở dữ liệu đã sẵn sàng.");
}

// --- Hàm xử lý User & Wallet ---

async function addWalletToUser(chatId, lang, walletAddress) {
    const normalizedLangInput = normalizeLanguageCode(lang);
    const normalizedAddr = ethers.getAddress(walletAddress);
    let user = await dbGet('SELECT lang, lang_source, wallets FROM users WHERE chatId = ?', [chatId]);

    if (user) {
        let wallets = [];
        try {
            wallets = JSON.parse(user.wallets) || [];
        } catch (err) {
            console.error(`[DB] Lỗi đọc danh sách ví cho ${chatId}:`, err.message);
            wallets = [];
        }
        if (!wallets.includes(normalizedAddr)) {
            wallets.push(normalizedAddr);
        }

        const hasStoredLang = typeof user.lang === 'string' && user.lang.trim().length > 0;
        const normalizedStored = hasStoredLang ? normalizeLanguageCode(user.lang) : null;
        const source = user.lang_source || 'auto';

        let langToPersist = normalizedStored || normalizedLangInput;
        let nextSource = source;

        if (!normalizedStored) {
            nextSource = 'auto';
        } else if (source !== 'manual' && normalizedStored !== normalizedLangInput) {
            langToPersist = normalizedLangInput;
            nextSource = 'auto';
        }

        await dbRun('UPDATE users SET lang = ?, lang_source = ?, wallets = ? WHERE chatId = ?', [langToPersist, nextSource, JSON.stringify(wallets), chatId]);
    } else {
        await dbRun('INSERT INTO users (chatId, lang, wallets, lang_source) VALUES (?, ?, ?, ?)', [chatId, normalizedLangInput, JSON.stringify([normalizedAddr]), 'auto']);
    }
    console.log(`[DB] Đã thêm/cập nhật ví ${normalizedAddr} cho chatId ${chatId}`);
}

async function removeWalletFromUser(chatId, walletAddress) {
    let user = await dbGet('SELECT * FROM users WHERE chatId = ?', [chatId]);
    if (!user) return false;
    let wallets = JSON.parse(user.wallets);
    const newWallets = wallets.filter(w => w !== walletAddress);
    await dbRun('UPDATE users SET wallets = ? WHERE chatId = ?', [JSON.stringify(newWallets), chatId]);
    console.log(`[DB] Đã xóa ví ${walletAddress} khỏi chatId ${chatId}`);
    return true;
}

async function removeAllWalletsFromUser(chatId) {
    await dbRun('UPDATE users SET wallets = ? WHERE chatId = ?', ['[]', chatId]);
    console.log(`[DB] Đã xóa tất cả ví khỏi chatId ${chatId}`);
    return true;
}

async function getWalletsForUser(chatId) {
    let user = await dbGet('SELECT wallets FROM users WHERE chatId = ?', [chatId]);
    return user ? JSON.parse(user.wallets) : [];
}

async function getUsersForWallet(walletAddress) {
    const normalizedAddr = ethers.getAddress(walletAddress);
    const allUsers = await dbAll('SELECT chatId, lang, wallets FROM users');

    const users = [];
    for (const user of allUsers) {
        // Đảm bảo user.wallets không bị null/undefined
        let wallets = [];
        try {
            if (user.wallets) {
                wallets = JSON.parse(user.wallets);
            }
        } catch (e) {
            console.error(`Lỗi JSON parse wallets cho user ${user.chatId}:`, user.wallets);
        }
        
        if (Array.isArray(wallets) && wallets.includes(normalizedAddr)) {
            const info = await getUserLanguageInfo(user.chatId);
            const normalizedLang = info ? info.lang : normalizeLanguageCode(user.lang);
            users.push({ chatId: user.chatId, lang: normalizedLang });
        }
    }
    return users;
}

async function getUserLanguageInfo(chatId) {
    let user = await dbGet('SELECT lang, lang_source FROM users WHERE chatId = ?', [chatId]);
    if (!user) return null;

    const normalizedLang = normalizeLanguageCode(user.lang);
    const source = user.lang_source || 'auto';

    if (normalizedLang !== user.lang || source !== user.lang_source) {
        try {
            await dbRun('UPDATE users SET lang = ?, lang_source = ? WHERE chatId = ?', [normalizedLang, source, chatId]);
        } catch (err) {
            console.error(`[DB] Không thể đồng bộ lang/lang_source cho ${chatId}:`, err.message);
        }
    }

    return { lang: normalizedLang, source };
}

async function getUserLanguage(chatId) {
    const info = await getUserLanguageInfo(chatId);
    return info ? info.lang : null;
}

async function setUserLanguage(chatId, lang, source = 'manual') {
    const normalizedLang = normalizeLanguageCode(lang);
    const normalizedSource = source === 'manual' ? 'manual' : 'auto';
    let user = await dbGet('SELECT wallets FROM users WHERE chatId = ?', [chatId]);
    if (user) {
        await dbRun('UPDATE users SET lang = ?, lang_source = ? WHERE chatId = ?', [normalizedLang, normalizedSource, chatId]);
    } else {
        await dbRun('INSERT INTO users (chatId, lang, lang_source, wallets) VALUES (?, ?, ?, ?)', [chatId, normalizedLang, normalizedSource, '[]']);
    }
    console.log(`[DB] Đã lưu ngôn ngữ ${normalizedLang} (${normalizedSource}) cho ${chatId}`);
}

async function setLanguage(chatId, lang) {
    await setUserLanguage(chatId, lang, 'manual');
}

async function setLanguageAuto(chatId, lang) {
    await setUserLanguage(chatId, lang, 'auto');
}

// --- Hàm xử lý Pending (Deep Link) ---
async function addPendingToken(token, walletAddress) {
    const normalizedAddr = ethers.getAddress(walletAddress);
    await dbRun('INSERT INTO pending_tokens (token, walletAddress) VALUES (?, ?)', [token, normalizedAddr]);
}

async function getPendingWallet(token) {
    let row = await dbGet('SELECT walletAddress FROM pending_tokens WHERE token = ?', [token]);
    return row ? row.walletAddress : null;
}

async function deletePendingToken(token) {
    await dbRun('DELETE FROM pending_tokens WHERE token = ?', [token]);
}

// --- Hàm xử lý Stats ---
async function writeGameResult(walletAddress, result, stake) {
    const normalizedAddr = ethers.getAddress(walletAddress);
    const timestamp = Math.floor(Date.now() / 1000);
    await dbRun('INSERT INTO game_stats (walletAddress, result, stake, timestamp) VALUES (?, ?, ?, ?)', [normalizedAddr, result, stake, timestamp]);
    console.log(`[DB Stats] Ghi nhận: ${normalizedAddr} ${result} ${stake}`);
}

async function getStats(walletAddress) {
    const normalizedAddr = ethers.getAddress(walletAddress);
    const rows = await dbAll('SELECT result, stake FROM game_stats WHERE walletAddress = ?', [normalizedAddr]);

    let stats = { games: 0, wins: 0, losses: 0, draws: 0, totalWon: 0, totalLost: 0 };
    for (const row of rows) {
        stats.games++;
        if (row.result === 'win') {
            stats.wins++;
            stats.totalWon += row.stake;
        } else if (row.result === 'lose') {
            stats.losses++;
            stats.totalLost += row.stake;
        } else {
            stats.draws++;
        }
    }
    return stats;
}

async function upsertGroupSubscription(chatId, lang, minStake, messageThreadId = null) {
    const now = Math.floor(Date.now() / 1000);
    const normalizedThreadId =
        messageThreadId === undefined || messageThreadId === null
            ? null
            : messageThreadId.toString();
    const existing = await dbGet('SELECT chatId FROM group_subscriptions WHERE chatId = ?', [chatId]);
    if (existing) {
        await dbRun(
            'UPDATE group_subscriptions SET lang = ?, minStake = ?, messageThreadId = ?, updatedAt = ? WHERE chatId = ?',
            [lang, minStake, normalizedThreadId, now, chatId]
        );
    } else {
        await dbRun(
            'INSERT INTO group_subscriptions (chatId, lang, minStake, messageThreadId, createdAt, updatedAt) VALUES (?, ?, ?, ?, ?, ?)',
            [chatId, lang, minStake, normalizedThreadId, now, now]
        );
    }
}

async function removeGroupSubscription(chatId) {
    await dbRun('DELETE FROM group_subscriptions WHERE chatId = ?', [chatId]);
}

async function getGroupSubscription(chatId) {
    const row = await dbGet('SELECT chatId, lang, minStake, messageThreadId FROM group_subscriptions WHERE chatId = ?', [chatId]);
    if (!row) {
        return null;
    }

    return {
        chatId: row.chatId,
        lang: row.lang,
        minStake: row.minStake,
        messageThreadId: row.messageThreadId == null ? null : row.messageThreadId.toString()
    };
}

async function getGroupSubscriptions() {
    const rows = await dbAll('SELECT chatId, lang, minStake, messageThreadId FROM group_subscriptions');
    return rows.map(row => ({
        chatId: row.chatId,
        lang: row.lang,
        minStake: row.minStake,
        messageThreadId: row.messageThreadId == null ? null : row.messageThreadId.toString()
    }));
}

async function getGroupMemberLanguage(groupChatId, userId) {
    const row = await dbGet('SELECT lang FROM group_member_languages WHERE groupChatId = ? AND userId = ?', [groupChatId, userId]);
    if (!row) return null;
    return normalizeLanguageCode(row.lang);
}

async function getGroupMemberLanguages(groupChatId) {
    const rows = await dbAll('SELECT userId, lang FROM group_member_languages WHERE groupChatId = ?', [groupChatId]);
    return rows.map(row => ({
        userId: row.userId,
        lang: normalizeLanguageCode(row.lang)
    }));
}

async function setGroupMemberLanguage(groupChatId, userId, lang) {
    const normalizedLang = normalizeLanguageCode(lang);
    const now = Math.floor(Date.now() / 1000);
    const existing = await dbGet('SELECT userId FROM group_member_languages WHERE groupChatId = ? AND userId = ?', [groupChatId, userId]);
    if (existing) {
        await dbRun('UPDATE group_member_languages SET lang = ?, updatedAt = ? WHERE groupChatId = ? AND userId = ?', [normalizedLang, now, groupChatId, userId]);
    } else {
        await dbRun('INSERT INTO group_member_languages (groupChatId, userId, lang, updatedAt) VALUES (?, ?, ?, ?)', [groupChatId, userId, normalizedLang, now]);
    }
}

async function removeGroupMemberLanguage(groupChatId, userId) {
    await dbRun('DELETE FROM group_member_languages WHERE groupChatId = ? AND userId = ?', [groupChatId, userId]);
}

async function updateGroupSubscriptionLanguage(chatId, lang) {
    const normalizedLang = normalizeLanguageCode(lang);
    const now = Math.floor(Date.now() / 1000);
    await dbRun('UPDATE group_subscriptions SET lang = ?, updatedAt = ? WHERE chatId = ?', [normalizedLang, now, chatId]);
}

async function updateGroupSubscriptionTopic(chatId, messageThreadId) {
    const now = Math.floor(Date.now() / 1000);
    const normalizedThreadId =
        messageThreadId === undefined || messageThreadId === null
            ? null
            : messageThreadId.toString();
    await dbRun(
        'UPDATE group_subscriptions SET messageThreadId = ?, updatedAt = ? WHERE chatId = ?',
        [normalizedThreadId, now, chatId]
    );
}

function toDayIndex(dateKey) {
    if (!dateKey || typeof dateKey !== 'string') {
        return null;
    }

    const parts = dateKey.split('-').map(part => Number(part));
    if (parts.length !== 3 || parts.some(Number.isNaN)) {
        return null;
    }

    const [year, month, day] = parts;
    return Math.floor(Date.UTC(year, month - 1, day) / 86400000);
}

async function fetchLocalDailyCheckinState(groupChatId, userId) {
    const row = await dbGet(
        'SELECT streak, lastCheckinDate, lastCheckinAt, totalCheckins FROM daily_checkins WHERE groupChatId = ? AND userId = ?',
        [groupChatId, userId]
    );

    if (!row) {
        return null;
    }

    return {
        streak: Number(row.streak) || 0,
        lastCheckinDate: row.lastCheckinDate || null,
        lastCheckinAt: row.lastCheckinAt ? Number(row.lastCheckinAt) : null,
        totalCheckins: Number(row.totalCheckins) || 0
    };
}

async function persistLocalDailyCheckinSnapshot(groupChatId, userId, state, updatedAt) {
    if (!state) {
        return;
    }

    const normalizedUpdatedAt = Number(updatedAt) || Math.floor(Date.now() / 1000);
    const existing = await dbGet(
        'SELECT 1 FROM daily_checkins WHERE groupChatId = ? AND userId = ?',
        [groupChatId, userId]
    );

    const params = [
        state.streak || 0,
        state.lastCheckinDate || null,
        state.lastCheckinAt ? Number(state.lastCheckinAt) : null,
        state.totalCheckins || 0,
        normalizedUpdatedAt,
        groupChatId,
        userId
    ];

    if (existing) {
        await dbRun(
            'UPDATE daily_checkins SET streak = ?, lastCheckinDate = ?, lastCheckinAt = ?, totalCheckins = ?, updatedAt = ? WHERE groupChatId = ? AND userId = ?',
            params
        );
    } else {
        await dbRun(
            'INSERT INTO daily_checkins (groupChatId, userId, streak, lastCheckinDate, lastCheckinAt, totalCheckins, updatedAt) VALUES (?, ?, ?, ?, ?, ?, ?)',
            [groupChatId, userId, params[0], params[1], params[2], params[3], params[4]]
        );
    }
}

async function insertLocalDailyCheckinLog(groupChatId, userId, checkinDate, checkinAt, streak) {
    await dbRun(
        'INSERT INTO daily_checkin_logs (groupChatId, userId, checkinDate, checkinAt, streak) VALUES (?, ?, ?, ?, ?)',
        [groupChatId, userId, checkinDate, checkinAt, streak]
    );
}

async function removeLocalDailyCheckinLog(groupChatId, userId, checkinDate) {
    const result = await dbRun(
        'DELETE FROM daily_checkin_logs WHERE groupChatId = ? AND userId = ? AND checkinDate = ?',
        [groupChatId, userId, checkinDate]
    );

    return result?.changes || 0;
}

async function rebuildLocalDailyCheckinSnapshot(groupChatId, userId) {
    const latest = await dbGet(
        'SELECT checkinDate, checkinAt, streak FROM daily_checkin_logs WHERE groupChatId = ? AND userId = ? ORDER BY checkinAt DESC LIMIT 1',
        [groupChatId, userId]
    );

    if (!latest) {
        await dbRun('DELETE FROM daily_checkins WHERE groupChatId = ? AND userId = ?', [groupChatId, userId]);
        return null;
    }

    const totalRow = await dbGet(
        'SELECT COUNT(1) AS total FROM daily_checkin_logs WHERE groupChatId = ? AND userId = ?',
        [groupChatId, userId]
    );

    const normalizedTotal = Number(totalRow?.total) || 0;
    const normalizedState = {
        streak: Number(latest.streak) || 0,
        lastCheckinDate: latest.checkinDate || null,
        lastCheckinAt: latest.checkinAt ? Number(latest.checkinAt) : null,
        totalCheckins: normalizedTotal
    };

    await persistLocalDailyCheckinSnapshot(groupChatId, userId, normalizedState, Math.floor(Date.now() / 1000));
    return normalizedState;
}

async function fetchSupabaseDailyCheckinState(groupChatId, userId) {
    if (supabasePool) {
        try {
            const result = await supabasePool.query(
                'select streak, last_checkin_date, last_checkin_at, total_checkins, updated_at from public.daily_checkins where group_chat_id = $1 and user_id = $2 limit 1',
                [groupChatId, userId]
            );

            if (result?.rows?.length) {
                const row = result.rows[0];
                return {
                    streak: Number(row.streak) || 0,
                    lastCheckinDate: row.last_checkin_date || null,
                    lastCheckinAt: row.last_checkin_at ? Number(row.last_checkin_at) : null,
                    totalCheckins: Number(row.total_checkins) || 0,
                    updatedAt: row.updated_at ? Number(row.updated_at) : null
                };
            }
        } catch (error) {
            console.error('[Supabase] Không thể đọc daily_checkins:', error.message);
            return null;
        }
    }

    if (supabaseRestClient) {
        try {
            const query = new URLSearchParams();
            query.set('select', 'streak,last_checkin_date,last_checkin_at,total_checkins,updated_at');
            query.set('group_chat_id', `eq.${groupChatId}`);
            query.set('user_id', `eq.${userId}`);
            query.set('limit', '1');

            const response = await supabaseRestClient.request('GET', 'daily_checkins', { query });
            if (!response.ok) {
                console.error('[Supabase] Không thể đọc daily_checkins (REST):', response.error);
                return null;
            }

            const rows = Array.isArray(response.data) ? response.data : [];
            if (!rows.length) {
                return null;
            }

            const row = rows[0] || {};
            return {
                streak: Number(row.streak) || 0,
                lastCheckinDate: row.last_checkin_date || null,
                lastCheckinAt: row.last_checkin_at ? Number(row.last_checkin_at) : null,
                totalCheckins: Number(row.total_checkins) || 0,
                updatedAt: row.updated_at ? Number(row.updated_at) : null
            };
        } catch (error) {
            console.error('[Supabase] Lỗi REST daily_checkins:', error.message);
        }
    }

    return null;
}

async function persistSupabaseDailyCheckin(groupChatId, userId, state, updatedAt) {
    if (!state) {
        return;
    }

    const normalizedUpdatedAt = Number(updatedAt) || Math.floor(Date.now() / 1000);

    if (supabasePool) {
        try {
            await supabasePool.query(
                `insert into public.daily_checkins (group_chat_id, user_id, streak, last_checkin_date, last_checkin_at, total_checkins, updated_at)
                 values ($1, $2, $3, $4, $5, $6, $7)
                 on conflict (group_chat_id, user_id) do update
                 set streak = excluded.streak,
                     last_checkin_date = excluded.last_checkin_date,
                     last_checkin_at = excluded.last_checkin_at,
                     total_checkins = excluded.total_checkins,
                     updated_at = excluded.updated_at`,
                [
                    groupChatId,
                    userId,
                    state.streak || 0,
                    state.lastCheckinDate || null,
                    state.lastCheckinAt ? Number(state.lastCheckinAt) : null,
                    state.totalCheckins || 0,
                    normalizedUpdatedAt
                ]
            );
            return;
        } catch (error) {
            console.error('[Supabase] Không thể ghi daily_checkins:', error.message);
        }
    }

    if (supabaseRestClient) {
        try {
            const query = new URLSearchParams();
            query.set('on_conflict', 'group_chat_id,user_id');

            const payload = [{
                group_chat_id: groupChatId,
                user_id: userId,
                streak: state.streak || 0,
                last_checkin_date: state.lastCheckinDate || null,
                last_checkin_at: state.lastCheckinAt ? Number(state.lastCheckinAt) : null,
                total_checkins: state.totalCheckins || 0,
                updated_at: normalizedUpdatedAt
            }];

            const response = await supabaseRestClient.request('POST', 'daily_checkins', {
                query,
                body: payload,
                prefer: 'resolution=merge-duplicates'
            });

            if (!response.ok) {
                console.error('[Supabase] Không thể ghi daily_checkins (REST):', response.error);
            }
        } catch (error) {
            console.error('[Supabase] Lỗi REST khi ghi daily_checkins:', error.message);
        }
    }
}

async function deleteSupabaseDailyCheckin(groupChatId, userId) {
    if (supabasePool) {
        try {
            await supabasePool.query(
                'delete from public.daily_checkins where group_chat_id = $1 and user_id = $2',
                [groupChatId, userId]
            );
            return;
        } catch (error) {
            console.error('[Supabase] Không thể xóa daily_checkins:', error.message);
        }
    }

    if (supabaseRestClient) {
        try {
            const query = new URLSearchParams();
            query.set('group_chat_id', `eq.${groupChatId}`);
            query.set('user_id', `eq.${userId}`);

            const response = await supabaseRestClient.request('DELETE', 'daily_checkins', { query });
            if (!response.ok) {
                console.error('[Supabase] Không thể xóa daily_checkins (REST):', response.error);
            }
        } catch (error) {
            console.error('[Supabase] Lỗi REST khi xóa daily_checkins:', error.message);
        }
    }
}

async function deleteSupabaseDailyCheckinLog(groupChatId, userId, checkinDate) {
    if (supabasePool) {
        try {
            await supabasePool.query(
                'delete from public.daily_checkin_logs where group_chat_id = $1 and user_id = $2 and checkin_date = $3',
                [groupChatId, userId, checkinDate]
            );
            return;
        } catch (error) {
            console.error('[Supabase] Không thể xóa daily_checkin_logs:', error.message);
        }
    }

    if (supabaseRestClient) {
        try {
            const query = new URLSearchParams();
            query.set('group_chat_id', `eq.${groupChatId}`);
            query.set('user_id', `eq.${userId}`);
            query.set('checkin_date', `eq.${checkinDate}`);

            const response = await supabaseRestClient.request('DELETE', 'daily_checkin_logs', { query });
            if (!response.ok) {
                console.error('[Supabase] Không thể xóa daily_checkin_logs (REST):', response.error);
            }
        } catch (error) {
            console.error('[Supabase] Lỗi REST khi xóa daily_checkin_logs:', error.message);
        }
    }
}

async function insertSupabaseDailyCheckinLog(groupChatId, userId, checkinDate, checkinAt, streak) {
    if (supabasePool) {
        try {
            await supabasePool.query(
                'insert into public.daily_checkin_logs (group_chat_id, user_id, checkin_date, checkin_at, streak) values ($1, $2, $3, $4, $5)',
                [groupChatId, userId, checkinDate, Number(checkinAt) || 0, streak || 0]
            );
            return;
        } catch (error) {
            console.error('[Supabase] Không thể ghi daily_checkin_logs:', error.message);
        }
    }

    if (supabaseRestClient) {
        try {
            const payload = [{
                group_chat_id: groupChatId,
                user_id: userId,
                checkin_date: checkinDate,
                checkin_at: Number(checkinAt) || 0,
                streak: streak || 0
            }];

            const response = await supabaseRestClient.request('POST', 'daily_checkin_logs', {
                body: payload,
                prefer: 'return=representation'
            });

            if (!response.ok) {
                console.error('[Supabase] Không thể ghi daily_checkin_logs (REST):', response.error);
            }
        } catch (error) {
            console.error('[Supabase] Lỗi REST khi ghi daily_checkin_logs:', error.message);
        }
    }
}

async function getDailyCheckinState(groupChatId, userId) {
    const supabaseState = await fetchSupabaseDailyCheckinState(groupChatId, userId);

    if (supabaseState) {
        const { updatedAt, ...rest } = supabaseState;
        try {
            await persistLocalDailyCheckinSnapshot(groupChatId, userId, rest, updatedAt);
        } catch (error) {
            console.error('[DB] Không thể đồng bộ daily_checkins từ Supabase:', error.message);
        }
        return rest;
    }

    return await fetchLocalDailyCheckinState(groupChatId, userId);
}

async function recordDailyCheckin(groupChatId, userId, currentDateKey, currentTimestampSec) {
    const existing = await getDailyCheckinState(groupChatId, userId);

    if (existing && existing.lastCheckinDate === currentDateKey) {
        return {
            alreadyCheckedIn: true,
            streak: existing.streak,
            totalCheckins: existing.totalCheckins,
            rewardUnlocked: false,
            streakReset: false
        };
    }

    const nowSeconds = Number(currentTimestampSec) || Math.floor(Date.now() / 1000);
    const todayIndex = toDayIndex(currentDateKey);
    let newStreak = 1;
    let streakReset = false;

    if (existing && existing.lastCheckinDate) {
        const previousIndex = toDayIndex(existing.lastCheckinDate);
        if (previousIndex !== null && todayIndex !== null) {
            const diff = todayIndex - previousIndex;
            if (diff === 1) {
                newStreak = existing.streak + 1;
            } else {
                newStreak = 1;
                streakReset = true;
            }
        } else {
            newStreak = 1;
            streakReset = true;
        }
    }

    const totalCheckins = (existing?.totalCheckins || 0) + 1;
    const rewardUnlocked = newStreak > 0 && newStreak % 7 === 0;
    const updatedAt = nowSeconds;
    const newState = {
        streak: newStreak,
        lastCheckinDate: currentDateKey,
        lastCheckinAt: nowSeconds,
        totalCheckins
    };

    try {
        await persistLocalDailyCheckinSnapshot(groupChatId, userId, newState, updatedAt);
        await insertLocalDailyCheckinLog(groupChatId, userId, currentDateKey, nowSeconds, newStreak);
    } catch (error) {
        console.error('[DB] Không thể lưu daily_checkins nội bộ:', error.message);
    }

    await persistSupabaseDailyCheckin(groupChatId, userId, newState, updatedAt);
    await insertSupabaseDailyCheckinLog(groupChatId, userId, currentDateKey, nowSeconds, newStreak);

    return {
        alreadyCheckedIn: false,
        streak: newStreak,
        totalCheckins,
        rewardUnlocked,
        streakReset
    };
}

async function cancelDailyCheckin(groupChatId, userId, targetDateKey) {
    const existing = await getDailyCheckinState(groupChatId, userId);

    if (!existing || existing.lastCheckinDate !== targetDateKey) {
        return { cancelled: false, reason: 'not_found' };
    }

    try {
        const deleted = await removeLocalDailyCheckinLog(groupChatId, userId, targetDateKey);
        if (!deleted) {
            return { cancelled: false, reason: 'not_found' };
        }

        const newState = await rebuildLocalDailyCheckinSnapshot(groupChatId, userId);

        await deleteSupabaseDailyCheckinLog(groupChatId, userId, targetDateKey);

        if (newState) {
            await persistSupabaseDailyCheckin(groupChatId, userId, newState, Math.floor(Date.now() / 1000));
        } else {
            await deleteSupabaseDailyCheckin(groupChatId, userId);
        }

        return {
            cancelled: true,
            rewardRevoked: existing.streak > 0 && existing.streak % 7 === 0,
            streak: newState ? newState.streak : 0,
            totalCheckins: newState ? newState.totalCheckins : 0
        };
    } catch (error) {
        console.error('[DB] Không thể hủy daily_checkin:', error.message);
        return { cancelled: false, reason: 'error' };
    }
}

module.exports = {
    init,
    addWalletToUser,
    removeWalletFromUser,
    removeAllWalletsFromUser,
    getWalletsForUser,
    getUsersForWallet,
    getUserLanguage,
    getUserLanguageInfo,
    setLanguage,
    setLanguageAuto,
    addPendingToken,
    getPendingWallet,
    deletePendingToken,
    writeGameResult,
    getStats,
    upsertGroupSubscription,
    removeGroupSubscription,
    getGroupSubscription,
    getGroupSubscriptions,
    getGroupMemberLanguage,
    getGroupMemberLanguages,
    setGroupMemberLanguage,
    removeGroupMemberLanguage,
    updateGroupSubscriptionLanguage,
    updateGroupSubscriptionTopic,
    getDailyCheckinState,
    recordDailyCheckin,
    cancelDailyCheckin
};