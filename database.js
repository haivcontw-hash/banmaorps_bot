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
            wallets TEXT
        );
    `);
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
            createdAt INTEGER,
            updatedAt INTEGER
        );
    `);
    console.log("Cơ sở dữ liệu đã sẵn sàng.");
}

// --- Hàm xử lý User & Wallet ---

async function addWalletToUser(chatId, lang, walletAddress) {
    const normalizedLangInput = normalizeLanguageCode(lang);
    const normalizedAddr = ethers.getAddress(walletAddress);
    let user = await dbGet('SELECT * FROM users WHERE chatId = ?', [chatId]);

    if (user) {
        let wallets = JSON.parse(user.wallets);
        if (!wallets.includes(normalizedAddr)) {
            wallets.push(normalizedAddr);
        }
        const hasStoredLang = typeof user.lang === 'string' && user.lang.trim().length > 0;
        const normalizedStored = hasStoredLang ? normalizeLanguageCode(user.lang) : null;
        const langToPersist = normalizedStored || normalizedLangInput;
        await dbRun('UPDATE users SET lang = ?, wallets = ? WHERE chatId = ?', [langToPersist, JSON.stringify(wallets), chatId]);
    } else {
        await dbRun('INSERT INTO users (chatId, lang, wallets) VALUES (?, ?, ?)', [chatId, normalizedLangInput, JSON.stringify([normalizedAddr])]);
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
            const normalizedLang = normalizeLanguageCode(user.lang);
            if (normalizedLang !== user.lang) {
                try {
                    await dbRun('UPDATE users SET lang = ? WHERE chatId = ?', [normalizedLang, user.chatId]);
                } catch (err) {
                    console.error(`[DB] Không thể cập nhật lang cho ${user.chatId}:`, err.message);
                }
            }
            users.push({ chatId: user.chatId, lang: normalizedLang });
        }
    }
    return users;
}

// ===== HÀM MỚI (SỬA LỖI) =====
/**
 * Lấy ngôn ngữ đã lưu của user
 * @param {string} chatId 
 * @returns {string|null} Trả về 'vi', 'en'... hoặc null
 */
async function getUserLanguage(chatId) {
    let user = await dbGet('SELECT lang FROM users WHERE chatId = ?', [chatId]);
    if (!user) return null;

    const normalizedLang = normalizeLanguageCode(user.lang);
    if (normalizedLang !== user.lang) {
        try {
            await dbRun('UPDATE users SET lang = ? WHERE chatId = ?', [normalizedLang, chatId]);
        } catch (err) {
            console.error(`[DB] Không thể đồng bộ lang cho ${chatId}:`, err.message);
        }
    }
    return normalizedLang;
}
// =================================

async function setLanguage(chatId, lang) {
    const normalizedLang = normalizeLanguageCode(lang);
    let user = await dbGet('SELECT * FROM users WHERE chatId = ?', [chatId]);
    if (user) {
        await dbRun('UPDATE users SET lang = ? WHERE chatId = ?', [normalizedLang, chatId]);
    } else {
        await dbRun('INSERT INTO users (chatId, lang, wallets) VALUES (?, ?, ?)', [chatId, normalizedLang, '[]']);
    }
    console.log(`[DB] Đã đổi ngôn ngữ cho ${chatId} thành ${normalizedLang}`);
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

async function upsertGroupSubscription(chatId, lang, minStake) {
    const now = Math.floor(Date.now() / 1000);
    const existing = await dbGet('SELECT chatId FROM group_subscriptions WHERE chatId = ?', [chatId]);
    if (existing) {
        await dbRun('UPDATE group_subscriptions SET lang = ?, minStake = ?, updatedAt = ? WHERE chatId = ?', [lang, minStake, now, chatId]);
    } else {
        await dbRun('INSERT INTO group_subscriptions (chatId, lang, minStake, createdAt, updatedAt) VALUES (?, ?, ?, ?, ?)', [chatId, lang, minStake, now, now]);
    }
}

async function removeGroupSubscription(chatId) {
    await dbRun('DELETE FROM group_subscriptions WHERE chatId = ?', [chatId]);
}

async function getGroupSubscription(chatId) {
    return dbGet('SELECT chatId, lang, minStake FROM group_subscriptions WHERE chatId = ?', [chatId]);
}

async function getGroupSubscriptions() {
    return dbAll('SELECT chatId, lang, minStake FROM group_subscriptions');
}

module.exports = {
    init,
    addWalletToUser,
    removeWalletFromUser,
    removeAllWalletsFromUser,
    getWalletsForUser,
    getUsersForWallet,
    getUserLanguage, // <-- Thêm hàm mới
    setLanguage,
    addPendingToken,
    getPendingWallet,
    deletePendingToken,
    writeGameResult,
    getStats,
    upsertGroupSubscription,
    removeGroupSubscription,
    getGroupSubscription,
    getGroupSubscriptions
};