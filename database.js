const sqlite3 = require('sqlite3').verbose();
const { normalizeLanguageCode } = require('./i18n.js');

const db = new sqlite3.Database('banmao.db', (err) => {
    if (err) {
        console.error("LỖI KHỞI TẠO DB:", err.message);
        process.exit(1);
    }
    console.log("Cơ sở dữ liệu SQLite đã kết nối.");
});
const ethers = require('ethers');

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

async function getDailyCheckinState(groupChatId, userId) {
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

    if (existing) {
        await dbRun(
            'UPDATE daily_checkins SET streak = ?, lastCheckinDate = ?, lastCheckinAt = ?, totalCheckins = ?, updatedAt = ? WHERE groupChatId = ? AND userId = ?',
            [newStreak, currentDateKey, nowSeconds, totalCheckins, updatedAt, groupChatId, userId]
        );
    } else {
        await dbRun(
            'INSERT INTO daily_checkins (groupChatId, userId, streak, lastCheckinDate, lastCheckinAt, totalCheckins, updatedAt) VALUES (?, ?, ?, ?, ?, ?, ?)',
            [groupChatId, userId, newStreak, currentDateKey, nowSeconds, totalCheckins, updatedAt]
        );
    }

    await dbRun(
        'INSERT INTO daily_checkin_logs (groupChatId, userId, checkinDate, checkinAt, streak) VALUES (?, ?, ?, ?, ?)',
        [groupChatId, userId, currentDateKey, nowSeconds, newStreak]
    );

    return {
        alreadyCheckedIn: false,
        streak: newStreak,
        totalCheckins,
        rewardUnlocked,
        streakReset
    };
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
    recordDailyCheckin
};