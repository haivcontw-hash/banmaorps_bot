const sqlite3 = require('sqlite3').verbose();
const crypto = require('crypto');
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

function sanitizeTimeSlot(value) {
    if (typeof value !== 'string') {
        return null;
    }

    const trimmed = value.trim();
    if (!trimmed) {
        return null;
    }

    const match = trimmed.match(/^(\d{1,2}):(\d{2})$/);
    if (!match) {
        return null;
    }

    const hour = Number(match[1]);
    const minute = Number(match[2]);
    if (!Number.isInteger(hour) || !Number.isInteger(minute)) {
        return null;
    }

    if (hour < 0 || hour > 23 || minute < 0 || minute > 59) {
        return null;
    }

    return `${hour.toString().padStart(2, '0')}:${minute.toString().padStart(2, '0')}`;
}

function normalizeAutoMessageTimes(value, fallbackTime = '08:00') {
    let rawList = [];

    if (Array.isArray(value)) {
        rawList = value;
    } else if (typeof value === 'string' && value.trim()) {
        try {
            const parsed = JSON.parse(value);
            if (Array.isArray(parsed)) {
                rawList = parsed;
            } else {
                rawList = value.split(',');
            }
        } catch (error) {
            rawList = value.split(',');
        }
    }

    const seen = new Set();
    const normalized = [];
    for (const entry of rawList) {
        const slot = sanitizeTimeSlot(entry);
        if (!slot || seen.has(slot)) {
            continue;
        }
        seen.add(slot);
        normalized.push(slot);
    }

    if (normalized.length === 0) {
        const fallbackSlot = sanitizeTimeSlot(fallbackTime) || '08:00';
        return fallbackSlot ? [fallbackSlot] : [];
    }

    return normalized.sort();
}

function normalizeSummaryMessageTimes(value) {
    let rawList = [];

    if (Array.isArray(value)) {
        rawList = value;
    } else if (typeof value === 'string' && value.trim()) {
        try {
            const parsed = JSON.parse(value);
            if (Array.isArray(parsed)) {
                rawList = parsed;
            } else {
                rawList = value.split(',');
            }
        } catch (error) {
            rawList = value.split(',');
        }
    }

    const seen = new Set();
    const normalized = [];
    for (const entry of rawList) {
        const slot = sanitizeTimeSlot(entry);
        if (!slot || seen.has(slot)) {
            continue;
        }
        seen.add(slot);
        normalized.push(slot);
    }

    return normalized.sort();
}

const CHECKIN_DEFAULTS = {
    checkinTime: '08:00',
    timezone: 'UTC',
    autoMessageEnabled: 1,
    dailyPoints: 10,
    summaryWindow: 7,
    summaryPeriodStart: null,
    mathWeight: 2,
    physicsWeight: 1,
    chemistryWeight: 1,
    okxWeight: 1,
    web3Weight: 1,
    autoMessageTimes: ['08:00'],
    summaryMessageEnabled: 0,
    summaryMessageTimes: [],
    leaderboardPeriodStart: null
};

function getTodayDateString(timezone = 'UTC') {
    try {
        const formatter = new Intl.DateTimeFormat('en-CA', {
            timeZone: timezone,
            year: 'numeric',
            month: '2-digit',
            day: '2-digit'
        });

        return formatter.format(new Date());
    } catch (error) {
        console.warn(`[Checkin] Không thể format ngày với timezone ${timezone}: ${error.message}`);
    }

    const now = new Date();
    const year = now.getUTCFullYear();
    const month = String(now.getUTCMonth() + 1).padStart(2, '0');
    const day = String(now.getUTCDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
}

function normalizeDateString(value) {
    if (typeof value !== 'string') {
        return null;
    }

    const trimmed = value.trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
        return null;
    }

    return trimmed;
}

function compareDateStrings(dateA, dateB) {
    const normalizedA = normalizeDateString(dateA);
    const normalizedB = normalizeDateString(dateB);

    if (!normalizedA || !normalizedB) {
        return null;
    }

    if (normalizedA === normalizedB) {
        return 0;
    }

    return normalizedA < normalizedB ? -1 : 1;
}

function getPreviousDate(dateStr) {
    const normalized = normalizeDateString(dateStr);
    if (!normalized) {
        return null;
    }

    const [year, month, day] = normalized.split('-').map((v) => Number(v));
    const date = new Date(Date.UTC(year, month - 1, day));
    date.setUTCDate(date.getUTCDate() - 1);
    const prevYear = date.getUTCFullYear();
    const prevMonth = String(date.getUTCMonth() + 1).padStart(2, '0');
    const prevDay = String(date.getUTCDate()).padStart(2, '0');
    return `${prevYear}-${prevMonth}-${prevDay}`;
}

function resolveLeaderboardPeriodStart(value, timezone = CHECKIN_DEFAULTS.timezone) {
    const normalized = normalizeDateString(value);
    if (normalized) {
        return normalized;
    }

    return getTodayDateString(timezone || CHECKIN_DEFAULTS.timezone);
}

function resolveSummaryPeriodStart(value) {
    const normalized = normalizeDateString(value);
    return normalized || null;
}

async function ensureCheckinGroup(chatId) {
    const now = Math.floor(Date.now() / 1000);
    const existing = await dbGet('SELECT chatId FROM checkin_groups WHERE chatId = ?', [chatId]);
    if (existing) {
        return existing.chatId;
    }

    const defaultStart = getTodayDateString(CHECKIN_DEFAULTS.timezone);

    await dbRun(
        `INSERT INTO checkin_groups (chatId, checkinTime, timezone, autoMessageEnabled, dailyPoints, summaryWindow, mathWeight, physicsWeight, chemistryWeight, okxWeight, web3Weight, autoMessageTimes, summaryMessageEnabled, summaryMessageTimes, leaderboardPeriodStart, summaryPeriodStart, createdAt, updatedAt)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
            chatId,
            CHECKIN_DEFAULTS.checkinTime,
            CHECKIN_DEFAULTS.timezone,
            CHECKIN_DEFAULTS.autoMessageEnabled,
            CHECKIN_DEFAULTS.dailyPoints,
            CHECKIN_DEFAULTS.summaryWindow,
            CHECKIN_DEFAULTS.mathWeight,
            CHECKIN_DEFAULTS.physicsWeight,
            CHECKIN_DEFAULTS.chemistryWeight,
            CHECKIN_DEFAULTS.okxWeight,
            CHECKIN_DEFAULTS.web3Weight,
            JSON.stringify(CHECKIN_DEFAULTS.autoMessageTimes),
            CHECKIN_DEFAULTS.summaryMessageEnabled,
            JSON.stringify(CHECKIN_DEFAULTS.summaryMessageTimes),
            defaultStart,
            CHECKIN_DEFAULTS.summaryPeriodStart,
            now,
            now
        ]
    );

    return chatId;
}

async function getCheckinGroup(chatId) {
    const row = await dbGet('SELECT * FROM checkin_groups WHERE chatId = ?', [chatId]);
    if (!row) {
        return {
            chatId,
            ...CHECKIN_DEFAULTS,
            lastAutoMessageDate: null,
            leaderboardPeriodStart: getTodayDateString(CHECKIN_DEFAULTS.timezone)
        };
    }

    return {
        chatId: row.chatId,
        checkinTime: row.checkinTime || CHECKIN_DEFAULTS.checkinTime,
        timezone: row.timezone || CHECKIN_DEFAULTS.timezone,
        autoMessageEnabled: row.autoMessageEnabled ?? CHECKIN_DEFAULTS.autoMessageEnabled,
        dailyPoints: row.dailyPoints ?? CHECKIN_DEFAULTS.dailyPoints,
        summaryWindow: row.summaryWindow ?? CHECKIN_DEFAULTS.summaryWindow,
        mathWeight: row.mathWeight ?? CHECKIN_DEFAULTS.mathWeight,
        physicsWeight: row.physicsWeight ?? CHECKIN_DEFAULTS.physicsWeight,
        chemistryWeight: row.chemistryWeight ?? CHECKIN_DEFAULTS.chemistryWeight,
        okxWeight: row.okxWeight ?? CHECKIN_DEFAULTS.okxWeight,
        web3Weight: row.web3Weight ?? CHECKIN_DEFAULTS.web3Weight,
        lastAutoMessageDate: row.lastAutoMessageDate || null,
        autoMessageTimes: normalizeAutoMessageTimes(row.autoMessageTimes, row.checkinTime || CHECKIN_DEFAULTS.checkinTime),
        summaryMessageEnabled: row.summaryMessageEnabled ?? CHECKIN_DEFAULTS.summaryMessageEnabled,
        summaryMessageTimes: normalizeSummaryMessageTimes(row.summaryMessageTimes),
        leaderboardPeriodStart: resolveLeaderboardPeriodStart(row.leaderboardPeriodStart, row.timezone || CHECKIN_DEFAULTS.timezone),
        summaryPeriodStart: resolveSummaryPeriodStart(row.summaryPeriodStart)
    };
}

async function listCheckinGroups() {
    const rows = await dbAll('SELECT * FROM checkin_groups');
    if (!rows || rows.length === 0) {
        return [];
    }

    return rows.map((row) => ({
        chatId: row.chatId,
        checkinTime: row.checkinTime || CHECKIN_DEFAULTS.checkinTime,
        timezone: row.timezone || CHECKIN_DEFAULTS.timezone,
        autoMessageEnabled: row.autoMessageEnabled ?? CHECKIN_DEFAULTS.autoMessageEnabled,
        dailyPoints: row.dailyPoints ?? CHECKIN_DEFAULTS.dailyPoints,
        summaryWindow: row.summaryWindow ?? CHECKIN_DEFAULTS.summaryWindow,
        mathWeight: row.mathWeight ?? CHECKIN_DEFAULTS.mathWeight,
        physicsWeight: row.physicsWeight ?? CHECKIN_DEFAULTS.physicsWeight,
        chemistryWeight: row.chemistryWeight ?? CHECKIN_DEFAULTS.chemistryWeight,
        okxWeight: row.okxWeight ?? CHECKIN_DEFAULTS.okxWeight,
        web3Weight: row.web3Weight ?? CHECKIN_DEFAULTS.web3Weight,
        lastAutoMessageDate: row.lastAutoMessageDate || null,
        autoMessageTimes: normalizeAutoMessageTimes(row.autoMessageTimes, row.checkinTime || CHECKIN_DEFAULTS.checkinTime),
        summaryMessageEnabled: row.summaryMessageEnabled ?? CHECKIN_DEFAULTS.summaryMessageEnabled,
        summaryMessageTimes: normalizeSummaryMessageTimes(row.summaryMessageTimes),
        leaderboardPeriodStart: resolveLeaderboardPeriodStart(row.leaderboardPeriodStart, row.timezone || CHECKIN_DEFAULTS.timezone),
        summaryPeriodStart: resolveSummaryPeriodStart(row.summaryPeriodStart)
    }));
}

async function updateCheckinGroup(chatId, patch = {}) {
    await ensureCheckinGroup(chatId);
    const fields = [];
    const values = [];
    const allowed = ['checkinTime', 'timezone', 'autoMessageEnabled', 'dailyPoints', 'summaryWindow', 'lastAutoMessageDate', 'mathWeight', 'physicsWeight', 'chemistryWeight', 'okxWeight', 'web3Weight', 'autoMessageTimes', 'leaderboardPeriodStart', 'summaryMessageEnabled', 'summaryMessageTimes', 'summaryPeriodStart'];
    for (const key of allowed) {
        if (Object.prototype.hasOwnProperty.call(patch, key)) {
            let value = patch[key];
            if ((key === 'autoMessageTimes' || key === 'summaryMessageTimes') && Array.isArray(value)) {
                value = JSON.stringify(value);
            }
            fields.push(`${key} = ?`);
            values.push(value);
        }
    }

    if (fields.length === 0) {
        return getCheckinGroup(chatId);
    }

    const now = Math.floor(Date.now() / 1000);
    fields.push('updatedAt = ?');
    values.push(now);
    values.push(chatId);

    const sql = `UPDATE checkin_groups SET ${fields.join(', ')} WHERE chatId = ?`;
    await dbRun(sql, values);
    return getCheckinGroup(chatId);
}

async function updateAutoMessageDate(chatId, dateStr) {
    const normalized = normalizeDateString(dateStr);
    if (!normalized) {
        return updateCheckinGroup(chatId, { lastAutoMessageDate: null });
    }

    return updateCheckinGroup(chatId, { lastAutoMessageDate: normalized });
}

async function setLeaderboardPeriodStart(chatId, dateStr, timezone = CHECKIN_DEFAULTS.timezone) {
    const normalized = normalizeDateString(dateStr);
    const resolved = normalized || getTodayDateString(timezone || CHECKIN_DEFAULTS.timezone);
    return updateCheckinGroup(chatId, { leaderboardPeriodStart: resolved });
}

async function setSummaryPeriodStart(chatId, dateStr, timezone = CHECKIN_DEFAULTS.timezone) {
    if (!dateStr && dateStr !== '0') {
        return updateCheckinGroup(chatId, { summaryPeriodStart: null });
    }

    const normalized = normalizeDateString(dateStr);
    const resolved = normalized || getTodayDateString(timezone || CHECKIN_DEFAULTS.timezone);
    return updateCheckinGroup(chatId, { summaryPeriodStart: resolved });
}

async function getCheckinAttempt(chatId, userId, checkinDate) {
    const normalized = normalizeDateString(checkinDate);
    if (!normalized) {
        return null;
    }

    const row = await dbGet(
        'SELECT attempts, locked FROM checkin_attempts WHERE chatId = ? AND userId = ? AND checkinDate = ?',
        [chatId, userId, normalized]
    );

    if (!row) {
        return { attempts: 0, locked: 0 };
    }

    return { attempts: Number(row.attempts || 0), locked: Number(row.locked || 0) };
}

async function setCheckinAttempt(chatId, userId, checkinDate, attempts, locked) {
    const normalized = normalizeDateString(checkinDate);
    if (!normalized) {
        throw new Error('Invalid checkin date');
    }

    const now = Math.floor(Date.now() / 1000);
    const existing = await dbGet(
        'SELECT chatId FROM checkin_attempts WHERE chatId = ? AND userId = ? AND checkinDate = ?',
        [chatId, userId, normalized]
    );

    if (existing) {
        await dbRun(
            'UPDATE checkin_attempts SET attempts = ?, locked = ?, updatedAt = ? WHERE chatId = ? AND userId = ? AND checkinDate = ?',
            [attempts, locked ? 1 : 0, now, chatId, userId, normalized]
        );
    } else {
        await dbRun(
            'INSERT INTO checkin_attempts (chatId, userId, checkinDate, attempts, locked, updatedAt) VALUES (?, ?, ?, ?, ?, ?)',
            [chatId, userId, normalized, attempts, locked ? 1 : 0, now]
        );
    }
}

async function incrementCheckinAttempt(chatId, userId, checkinDate, maxAttempts = 3) {
    const status = await getCheckinAttempt(chatId, userId, checkinDate);
    const nextAttempts = status ? status.attempts + 1 : 1;
    const shouldLock = nextAttempts >= maxAttempts;
    await setCheckinAttempt(chatId, userId, checkinDate, nextAttempts, shouldLock);
    return { attempts: nextAttempts, locked: shouldLock };
}

async function clearDailyAttempts(chatId, userId, checkinDate) {
    const normalized = normalizeDateString(checkinDate);
    if (!normalized) {
        return;
    }

    await dbRun('DELETE FROM checkin_attempts WHERE chatId = ? AND userId = ? AND checkinDate = ?', [chatId, userId, normalized]);
}

async function getCheckinRecord(chatId, userId, checkinDate) {
    const normalized = normalizeDateString(checkinDate);
    if (!normalized) {
        return null;
    }

    const row = await dbGet(
        'SELECT * FROM checkin_records WHERE chatId = ? AND userId = ? AND checkinDate = ?',
        [chatId, userId, normalized]
    );

    if (!row) {
        return null;
    }

    return { ...row };
}

async function ensureMemberRow(chatId, userId) {
    const existing = await dbGet('SELECT userId FROM checkin_members WHERE chatId = ? AND userId = ?', [chatId, userId]);
    if (existing) {
        return existing.userId;
    }

    const now = Math.floor(Date.now() / 1000);
    await dbRun(
        `INSERT INTO checkin_members (chatId, userId, streak, longestStreak, totalCheckins, totalPoints, createdAt, updatedAt)
         VALUES (?, ?, 0, 0, 0, 0, ?, ?)`
        ,
        [chatId, userId, now, now]
    );
    return userId;
}

function calculateConsecutiveStreak(dates) {
    if (!Array.isArray(dates) || dates.length === 0) {
        return { streak: 0, longest: 0, lastDate: null };
    }

    const sorted = [...dates].sort();
    let longest = 1;
    let current = 1;
    for (let i = 1; i < sorted.length; i++) {
        const prev = sorted[i - 1];
        const prevDate = new Date(`${prev}T00:00:00Z`);
        const currentDate = new Date(`${sorted[i]}T00:00:00Z`);
        const diffMs = currentDate - prevDate;
        const diffDays = Math.round(diffMs / (24 * 60 * 60 * 1000));
        if (diffDays === 1) {
            current += 1;
        } else if (diffDays === 0) {
            continue;
        } else {
            current = 1;
        }

        if (current > longest) {
            longest = current;
        }
    }

    const lastDate = sorted[sorted.length - 1];
    // Calculate streak ending at lastDate
    let streak = 1;
    for (let i = sorted.length - 2; i >= 0; i--) {
        const prev = sorted[i];
        const prevDate = new Date(`${prev}T00:00:00Z`);
        const nextDate = new Date(`${sorted[i + 1]}T00:00:00Z`);
        const diffMs = nextDate - prevDate;
        const diffDays = Math.round(diffMs / (24 * 60 * 60 * 1000));
        if (diffDays === 1) {
            streak += 1;
        } else if (diffDays === 0) {
            continue;
        } else {
            break;
        }
    }

    return { streak, longest, lastDate };
}

async function recalculateMemberStats(chatId, userId) {
    await ensureMemberRow(chatId, userId);
    const records = await dbAll(
        'SELECT checkinDate, pointsAwarded FROM checkin_records WHERE chatId = ? AND userId = ? ORDER BY checkinDate ASC',
        [chatId, userId]
    );

    const dates = records.map((row) => row.checkinDate).filter(Boolean);
    const { streak, longest, lastDate } = calculateConsecutiveStreak(dates);
    const totalCheckins = records.length;
    const totalPoints = records.reduce((sum, row) => sum + Number(row.pointsAwarded || 0), 0);

    const now = Math.floor(Date.now() / 1000);
    await dbRun(
        `UPDATE checkin_members
         SET streak = ?, longestStreak = CASE WHEN ? > longestStreak THEN ? ELSE longestStreak END,
             totalCheckins = ?, totalPoints = ?, lastCheckinDate = ?, updatedAt = ?, lockedUntilDate = NULL
         WHERE chatId = ? AND userId = ?`,
        [streak, longest, longest, totalCheckins, totalPoints, lastDate, now, chatId, userId]
    );

    return { streak, longest, totalCheckins, totalPoints, lastCheckinDate: lastDate };
}

async function completeCheckin({ chatId, userId, checkinDate, walletAddress = null, pointsAwarded = 0 }) {
    const normalized = normalizeDateString(checkinDate);
    if (!normalized) {
        throw new Error('Invalid checkin date');
    }

    await ensureCheckinGroup(chatId);
    await ensureMemberRow(chatId, userId);

    const existingRecord = await getCheckinRecord(chatId, userId, normalized);
    const now = Math.floor(Date.now() / 1000);

    if (existingRecord) {
        await dbRun(
            'UPDATE checkin_records SET walletAddress = ?, pointsAwarded = ?, updatedAt = ? WHERE id = ?',
            [walletAddress, pointsAwarded, now, existingRecord.id]
        );
    } else {
        const id = crypto.randomUUID();
        await dbRun(
            'INSERT INTO checkin_records (id, chatId, userId, checkinDate, walletAddress, pointsAwarded, createdAt, updatedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            [id, chatId, userId, normalized, walletAddress, pointsAwarded, now, now]
        );
    }

    await clearDailyAttempts(chatId, userId, normalized);

    const memberRow = await dbGet('SELECT streak, lastCheckinDate, longestStreak, totalCheckins, totalPoints FROM checkin_members WHERE chatId = ? AND userId = ?', [chatId, userId]);

    let streak = 1;
    let longestStreak = 1;
    let totalCheckins = 1;
    let totalPoints = Number(pointsAwarded || 0);

    if (memberRow) {
        totalCheckins = Number(memberRow.totalCheckins || 0) + (existingRecord ? 0 : 1);
        totalPoints = Number(memberRow.totalPoints || 0) + Number(pointsAwarded || 0) - Number(existingRecord?.pointsAwarded || 0);

        const lastDate = memberRow.lastCheckinDate;
        if (lastDate) {
            if (lastDate === normalized) {
                streak = Number(memberRow.streak || 1);
            } else {
                const prevDate = getPreviousDate(normalized);
                if (prevDate && prevDate === lastDate) {
                    streak = Number(memberRow.streak || 0) + 1;
                } else {
                    streak = 1;
                }
            }
        }

        longestStreak = streak > Number(memberRow.longestStreak || 0) ? streak : Number(memberRow.longestStreak || 0);
        if (existingRecord && !memberRow.lastCheckinDate) {
            streak = Number(memberRow.streak || 1);
        }
    }

    const nowTs = Math.floor(Date.now() / 1000);
    await dbRun(
        `UPDATE checkin_members
         SET streak = ?, longestStreak = CASE WHEN ? > longestStreak THEN ? ELSE longestStreak END,
             totalCheckins = ?, totalPoints = ?, lastCheckinDate = ?, lockedUntilDate = NULL, updatedAt = ?
         WHERE chatId = ? AND userId = ?`,
        [streak, longestStreak, longestStreak, totalCheckins, totalPoints, normalized, nowTs, chatId, userId]
    );

    return { streak, longestStreak, totalCheckins, totalPoints };
}

async function updateCheckinFeedback(chatId, userId, checkinDate, { emotion = null, goal = null } = {}) {
    const normalized = normalizeDateString(checkinDate);
    if (!normalized) {
        throw new Error('Invalid checkin date');
    }

    const record = await getCheckinRecord(chatId, userId, normalized);
    if (!record) {
        throw new Error('Check-in record not found');
    }

    const now = Math.floor(Date.now() / 1000);
    const updates = [];
    const params = [];

    if (emotion !== null) {
        updates.push('emotion = ?');
        params.push(emotion);
    }

    if (goal !== null) {
        updates.push('goal = ?');
        params.push(goal);
    }

    if (updates.length === 0) {
        return record;
    }

    updates.push('updatedAt = ?');
    params.push(now);
    params.push(record.id);

    await dbRun(`UPDATE checkin_records SET ${updates.join(', ')} WHERE id = ?`, params);
    return getCheckinRecord(chatId, userId, normalized);
}

async function getCheckinsForDate(chatId, checkinDate) {
    const normalized = normalizeDateString(checkinDate);
    if (!normalized) {
        return [];
    }

    const rows = await dbAll(
        'SELECT * FROM checkin_records WHERE chatId = ? AND checkinDate = ? ORDER BY updatedAt ASC',
        [chatId, normalized]
    );

    return rows;
}

async function getCheckinsInRange(chatId, startDate, endDate) {
    const normalizedStart = normalizeDateString(startDate);
    const normalizedEnd = normalizeDateString(endDate);
    if (!normalizedStart || !normalizedEnd) {
        return [];
    }

    const rows = await dbAll(
        'SELECT * FROM checkin_records WHERE chatId = ? AND checkinDate BETWEEN ? AND ? ORDER BY checkinDate ASC, updatedAt ASC',
        [chatId, normalizedStart, normalizedEnd]
    );

    return rows || [];
}

async function getCheckinMemberSummary(chatId, userId) {
    if (!chatId || !userId) {
        return null;
    }

    const row = await dbGet(
        'SELECT streak, longestStreak, totalCheckins, totalPoints FROM checkin_members WHERE chatId = ? AND userId = ?',
        [chatId, userId]
    );

    if (!row) {
        return null;
    }

    return {
        streak: Number(row.streak || 0),
        longestStreak: Number(row.longestStreak || 0),
        totalCheckins: Number(row.totalCheckins || 0),
        totalPoints: Number(row.totalPoints || 0)
    };
}

async function getMemberLeaderboardStats(chatId, userId, sinceDate = null) {
    if (!chatId || !userId) {
        return { entries: [] };
    }

    const normalizedSince = normalizeDateString(sinceDate);
    let sql = 'SELECT checkinDate, pointsAwarded, createdAt, updatedAt FROM checkin_records WHERE chatId = ? AND userId = ?';
    const params = [chatId, userId];

    if (normalizedSince) {
        sql += ' AND checkinDate >= ?';
        params.push(normalizedSince);
    }

    sql += ' ORDER BY checkinDate ASC, updatedAt ASC';
    const rows = await dbAll(sql, params);

    if (!rows || rows.length === 0) {
        return { entries: [] };
    }

    const dates = rows.map((row) => row.checkinDate).filter(Boolean);
    const { streak, longest, lastDate } = calculateConsecutiveStreak(dates);
    const totalPoints = rows.reduce((sum, row) => sum + Number(row.pointsAwarded || 0), 0);

    return {
        streak,
        longestStreak: longest,
        totalCheckins: rows.length,
        totalPoints,
        lastCheckinDate: lastDate,
        entries: rows.map((row) => ({
            ...row,
            createdAt: Number(row.createdAt || 0),
            updatedAt: Number(row.updatedAt || 0)
        }))
    };
}

function compareLeaderboardRows(a, b, mode) {
    const metrics = {
        streak: ['streak', 'totalCheckins', 'totalPoints', 'longestStreak'],
        total: ['totalCheckins', 'streak', 'totalPoints', 'longestStreak'],
        points: ['totalPoints', 'streak', 'totalCheckins', 'longestStreak'],
        longest: ['longestStreak', 'totalCheckins', 'totalPoints', 'streak']
    };
    const keys = metrics[mode] || metrics.streak;

    for (const key of keys) {
        const diff = Number(b[key] || 0) - Number(a[key] || 0);
        if (diff !== 0) {
            return diff;
        }
    }

    const lastDiff = Number(b.lastTimestamp || 0) - Number(a.lastTimestamp || 0);
    if (lastDiff !== 0) {
        return lastDiff;
    }

    return String(a.userId || '').localeCompare(String(b.userId || ''));
}

async function getTopCheckins(chatId, limit = 10, mode = 'streak', sinceDate = null) {
    const allowedModes = new Set(['streak', 'total', 'points', 'longest']);
    const finalMode = allowedModes.has(mode) ? mode : 'streak';
    const normalizedLimit = Math.max(Number(limit) || 0, 1);
    const normalizedSince = normalizeDateString(sinceDate);

    if (!normalizedSince) {
        let orderClause = 'streak DESC, totalCheckins DESC';

        if (finalMode === 'total') {
            orderClause = 'totalCheckins DESC, streak DESC';
        } else if (finalMode === 'points') {
            orderClause = 'totalPoints DESC, streak DESC';
        } else if (finalMode === 'longest') {
            orderClause = 'longestStreak DESC, totalCheckins DESC';
        }

        const rows = await dbAll(
            `SELECT * FROM checkin_members WHERE chatId = ? ORDER BY ${orderClause} LIMIT ?`,
            [chatId, normalizedLimit]
        );

        return rows || [];
    }

    const rows = await dbAll(
        `SELECT userId, checkinDate, pointsAwarded, createdAt, updatedAt
         FROM checkin_records
         WHERE chatId = ? AND checkinDate >= ?
         ORDER BY userId ASC, checkinDate ASC, updatedAt ASC`,
        [chatId, normalizedSince]
    );

    if (!rows || rows.length === 0) {
        return [];
    }

    const perUser = new Map();
    for (const row of rows) {
        if (!row || !row.userId) {
            continue;
        }

        if (!perUser.has(row.userId)) {
            perUser.set(row.userId, {
                userId: row.userId,
                dates: [],
                totalPoints: 0,
                totalCheckins: 0,
                lastTimestamp: 0,
                lastCheckinDate: null
            });
        }

        const entry = perUser.get(row.userId);
        if (row.checkinDate) {
            entry.dates.push(row.checkinDate);
            entry.lastCheckinDate = row.checkinDate;
        }

        entry.totalPoints += Number(row.pointsAwarded || 0);
        entry.totalCheckins += 1;
        const updatedAt = Number(row.updatedAt || row.createdAt || 0);
        if (updatedAt > entry.lastTimestamp) {
            entry.lastTimestamp = updatedAt;
        }
    }

    const leaderboard = [];
    for (const entry of perUser.values()) {
        const { streak, longest, lastDate } = calculateConsecutiveStreak(entry.dates);
        leaderboard.push({
            userId: entry.userId,
            streak,
            longestStreak: longest,
            totalCheckins: entry.totalCheckins,
            totalPoints: entry.totalPoints,
            lastCheckinDate: lastDate || entry.lastCheckinDate,
            lastTimestamp: entry.lastTimestamp
        });
    }

    leaderboard.sort((a, b) => compareLeaderboardRows(a, b, finalMode));
    return leaderboard.slice(0, normalizedLimit);
}

async function removeCheckinRecord(chatId, userId, checkinDate) {
    const normalized = normalizeDateString(checkinDate);
    if (!normalized) {
        return false;
    }

    const record = await getCheckinRecord(chatId, userId, normalized);
    if (!record) {
        return false;
    }

    await dbRun('DELETE FROM checkin_records WHERE id = ?', [record.id]);
    await recalculateMemberStats(chatId, userId);
    await clearDailyAttempts(chatId, userId, normalized);
    return true;
}

async function clearMemberLeaderboardEntries(chatId, userId, sinceDate = null) {
    if (!chatId || !userId) {
        return false;
    }

    const normalizedSince = normalizeDateString(sinceDate);
    let sql = 'DELETE FROM checkin_records WHERE chatId = ? AND userId = ?';
    const params = [chatId, userId];

    if (normalizedSince) {
        sql += ' AND checkinDate >= ?';
        params.push(normalizedSince);
    }

    await dbRun(sql, params);
    await recalculateMemberStats(chatId, userId);
    return true;
}

async function unlockMemberCheckin(chatId, userId) {
    const now = Math.floor(Date.now() / 1000);
    await ensureMemberRow(chatId, userId);
    await dbRun(
        'UPDATE checkin_members SET lockedUntilDate = NULL, updatedAt = ? WHERE chatId = ? AND userId = ?',
        [now, chatId, userId]
    );
    await dbRun('UPDATE checkin_attempts SET locked = 0 WHERE chatId = ? AND userId = ?', [chatId, userId]);
}

async function markMemberLocked(chatId, userId, checkinDate) {
    const normalized = normalizeDateString(checkinDate);
    if (!normalized) {
        return;
    }

    await ensureMemberRow(chatId, userId);
    const now = Math.floor(Date.now() / 1000);
    await dbRun(
        'UPDATE checkin_members SET lockedUntilDate = ?, updatedAt = ? WHERE chatId = ? AND userId = ?',
        [normalized, now, chatId, userId]
    );
}

async function hasAutoMessageLog(chatId, checkinDate, slot) {
    const normalizedDate = normalizeDateString(checkinDate);
    const normalizedSlot = sanitizeTimeSlot(slot);
    if (!normalizedDate || !normalizedSlot) {
        return false;
    }

    const row = await dbGet(
        'SELECT 1 FROM checkin_auto_logs WHERE chatId = ? AND checkinDate = ? AND slot = ?',
        [chatId, normalizedDate, normalizedSlot]
    );

    return Boolean(row);
}

async function recordAutoMessageLog(chatId, checkinDate, slot) {
    const normalizedDate = normalizeDateString(checkinDate);
    const normalizedSlot = sanitizeTimeSlot(slot);
    if (!normalizedDate || !normalizedSlot) {
        return;
    }

    const now = Math.floor(Date.now() / 1000);
    await dbRun(
        'INSERT OR IGNORE INTO checkin_auto_logs (chatId, checkinDate, slot, sentAt) VALUES (?, ?, ?, ?)',
        [chatId, normalizedDate, normalizedSlot, now]
    );
}

async function hasSummaryMessageLog(chatId, summaryDate, slot) {
    const normalizedDate = normalizeDateString(summaryDate);
    const normalizedSlot = sanitizeTimeSlot(slot);
    if (!normalizedDate || !normalizedSlot) {
        return false;
    }

    const row = await dbGet(
        'SELECT 1 FROM checkin_summary_logs WHERE chatId = ? AND summaryDate = ? AND slot = ?',
        [chatId, normalizedDate, normalizedSlot]
    );

    return Boolean(row);
}

async function recordSummaryMessageLog(chatId, summaryDate, slot) {
    const normalizedDate = normalizeDateString(summaryDate);
    const normalizedSlot = sanitizeTimeSlot(slot);
    if (!normalizedDate || !normalizedSlot) {
        return;
    }

    const now = Math.floor(Date.now() / 1000);
    await dbRun(
        'INSERT OR IGNORE INTO checkin_summary_logs (chatId, summaryDate, slot, sentAt) VALUES (?, ?, ?, ?)',
        [chatId, normalizedDate, normalizedSlot, now]
    );
}

async function resetSummaryMessageLogs(chatId) {
    if (!chatId) {
        return;
    }

    await dbRun('DELETE FROM checkin_summary_logs WHERE chatId = ?', [chatId]);
}

async function getLockedMembers(chatId, checkinDate) {
    const normalized = normalizeDateString(checkinDate);
    if (!normalized) {
        return [];
    }

    const rows = await dbAll(
        `SELECT attempts.userId, attempts.attempts
         FROM checkin_attempts attempts
         WHERE attempts.chatId = ? AND attempts.checkinDate = ? AND attempts.locked = 1`,
        [chatId, normalized]
    );

    return rows || [];
}

// Hàm khởi tạo database
async function init() {
    console.log("Đang khởi tạo cấu trúc bảng SQLite...");
    await dbRun(`
        CREATE TABLE IF NOT EXISTS users (
            chatId TEXT PRIMARY KEY,
            lang TEXT,
            wallets TEXT,
            lang_source TEXT DEFAULT 'auto',
            first_name TEXT,
            last_name TEXT,
            username TEXT,
            createdAt INTEGER,
            lastActiveAt INTEGER
        );
    `);

    try {
        await dbRun(`ALTER TABLE users ADD COLUMN lang_source TEXT DEFAULT 'auto'`);
    } catch (err) {
        if (!/duplicate column name/i.test(err.message)) {
            throw err;
        }
    }
    for (const column of ['first_name', 'last_name', 'username']) {
        try {
            await dbRun(`ALTER TABLE users ADD COLUMN ${column} TEXT`);
        } catch (err) {
            if (!/duplicate column name/i.test(err.message)) {
                throw err;
            }
        }
    }

    for (const column of ['createdAt', 'lastActiveAt']) {
        try {
            await dbRun(`ALTER TABLE users ADD COLUMN ${column} INTEGER`);
        } catch (err) {
            if (!/duplicate column name/i.test(err.message)) {
                throw err;
            }
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
            title TEXT,
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
    try {
        await dbRun(`ALTER TABLE group_subscriptions ADD COLUMN title TEXT`);
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
        CREATE TABLE IF NOT EXISTS checkin_groups (
            chatId TEXT PRIMARY KEY,
            checkinTime TEXT NOT NULL DEFAULT '08:00',
            timezone TEXT NOT NULL DEFAULT 'UTC',
            autoMessageEnabled INTEGER NOT NULL DEFAULT 1,
            dailyPoints INTEGER NOT NULL DEFAULT 10,
            summaryWindow INTEGER NOT NULL DEFAULT 7,
            mathWeight REAL NOT NULL DEFAULT 2,
            physicsWeight REAL NOT NULL DEFAULT 1,
            chemistryWeight REAL NOT NULL DEFAULT 1,
            okxWeight REAL NOT NULL DEFAULT 1,
            web3Weight REAL NOT NULL DEFAULT 1,
            autoMessageTimes TEXT,
            summaryMessageEnabled INTEGER NOT NULL DEFAULT 0,
            summaryMessageTimes TEXT,
            leaderboardPeriodStart TEXT,
            summaryPeriodStart TEXT,
            lastAutoMessageDate TEXT,
            createdAt INTEGER NOT NULL,
            updatedAt INTEGER NOT NULL
        );
    `);
    try {
        await dbRun(`ALTER TABLE checkin_groups ADD COLUMN autoMessageTimes TEXT`);
    } catch (err) {
        if (!/duplicate column name/i.test(err.message)) {
            throw err;
        }
    }
    try {
        await dbRun(`ALTER TABLE checkin_groups ADD COLUMN leaderboardPeriodStart TEXT`);
    } catch (err) {
        if (!/duplicate column name/i.test(err.message)) {
            throw err;
        }
    }
    try {
        await dbRun(`ALTER TABLE checkin_groups ADD COLUMN summaryPeriodStart TEXT`);
    } catch (err) {
        if (!/duplicate column name/i.test(err.message)) {
            throw err;
        }
    }
    try {
        await dbRun(`ALTER TABLE checkin_groups ADD COLUMN summaryMessageEnabled INTEGER NOT NULL DEFAULT 0`);
    } catch (err) {
        if (!/duplicate column name/i.test(err.message)) {
            throw err;
        }
    }
    try {
        await dbRun(`ALTER TABLE checkin_groups ADD COLUMN summaryMessageTimes TEXT`);
    } catch (err) {
        if (!/duplicate column name/i.test(err.message)) {
            throw err;
        }
    }
    const defaultPeriodStart = getTodayDateString(CHECKIN_DEFAULTS.timezone);
    await dbRun(
        `UPDATE checkin_groups SET leaderboardPeriodStart = COALESCE(leaderboardPeriodStart, ?)`,
        [defaultPeriodStart]
    );
    await dbRun(
        `UPDATE checkin_groups SET summaryMessageEnabled = COALESCE(summaryMessageEnabled, 0)`
    );
    await dbRun(
        `UPDATE checkin_groups SET summaryMessageTimes = CASE
            WHEN summaryMessageTimes IS NULL OR TRIM(summaryMessageTimes) = '' THEN ?
            ELSE summaryMessageTimes
        END`,
        [JSON.stringify(CHECKIN_DEFAULTS.summaryMessageTimes)]
    );
    const weightDefaults = { mathWeight: 2, physicsWeight: 1, chemistryWeight: 1, okxWeight: 1, web3Weight: 1 };
    for (const column of Object.keys(weightDefaults)) {
        try {
            await dbRun(`ALTER TABLE checkin_groups ADD COLUMN ${column} REAL DEFAULT ${weightDefaults[column]}`);
        } catch (err) {
            if (!/duplicate column name/i.test(err.message)) {
                throw err;
            }
        }
    }

    await dbRun(`
        UPDATE checkin_groups
        SET timezone = 'UTC'
        WHERE timezone IS NULL OR timezone = 'Asia/Ho_Chi_Minh'
    `);

    await dbRun(`
        CREATE TABLE IF NOT EXISTS checkin_members (
            chatId TEXT NOT NULL,
            userId TEXT NOT NULL,
            streak INTEGER NOT NULL DEFAULT 0,
            longestStreak INTEGER NOT NULL DEFAULT 0,
            totalCheckins INTEGER NOT NULL DEFAULT 0,
            totalPoints INTEGER NOT NULL DEFAULT 0,
            lastCheckinDate TEXT,
            lockedUntilDate TEXT,
            createdAt INTEGER NOT NULL,
            updatedAt INTEGER NOT NULL,
            PRIMARY KEY (chatId, userId)
        );
    `);

    await dbRun(`
        CREATE TABLE IF NOT EXISTS checkin_records (
            id TEXT PRIMARY KEY,
            chatId TEXT NOT NULL,
            userId TEXT NOT NULL,
            checkinDate TEXT NOT NULL,
            walletAddress TEXT,
            emotion TEXT,
            goal TEXT,
            pointsAwarded INTEGER NOT NULL DEFAULT 0,
            createdAt INTEGER NOT NULL,
            updatedAt INTEGER NOT NULL
        );
    `);

    await dbRun(`
        CREATE TABLE IF NOT EXISTS checkin_attempts (
            chatId TEXT NOT NULL,
            userId TEXT NOT NULL,
            checkinDate TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            locked INTEGER NOT NULL DEFAULT 0,
            updatedAt INTEGER NOT NULL,
            PRIMARY KEY (chatId, userId, checkinDate)
        );
    `);

    await dbRun(`CREATE INDEX IF NOT EXISTS idx_checkin_records_chat_date ON checkin_records (chatId, checkinDate);`);
    await dbRun(`CREATE INDEX IF NOT EXISTS idx_checkin_records_user ON checkin_records (chatId, userId, checkinDate);`);
    await dbRun(`CREATE INDEX IF NOT EXISTS idx_checkin_attempts_locked ON checkin_attempts (chatId, checkinDate, locked);`);
    await dbRun(`
        CREATE TABLE IF NOT EXISTS checkin_auto_logs (
            chatId TEXT NOT NULL,
            checkinDate TEXT NOT NULL,
            slot TEXT NOT NULL,
            sentAt INTEGER NOT NULL,
            PRIMARY KEY (chatId, checkinDate, slot)
        );
    `);
    await dbRun(`
        CREATE TABLE IF NOT EXISTS checkin_summary_logs (
            chatId TEXT NOT NULL,
            summaryDate TEXT NOT NULL,
            slot TEXT NOT NULL,
            sentAt INTEGER NOT NULL,
            PRIMARY KEY (chatId, summaryDate, slot)
        );
    `);
    await dbRun(`
        CREATE TABLE IF NOT EXISTS owners (
            chatId TEXT PRIMARY KEY,
            username TEXT,
            isPrimary INTEGER NOT NULL DEFAULT 0,
            createdAt INTEGER NOT NULL
        );
    `);
    await dbRun(`
        CREATE TABLE IF NOT EXISTS bans (
            identifier TEXT NOT NULL,
            type TEXT NOT NULL,
            createdAt INTEGER NOT NULL,
            createdBy TEXT,
            PRIMARY KEY (identifier, type)
        );
    `);
    await dbRun(`
        CREATE TABLE IF NOT EXISTS message_logs (
            chatId TEXT NOT NULL,
            messageId INTEGER NOT NULL,
            PRIMARY KEY (chatId, messageId)
        );
    `);
    console.log("Cơ sở dữ liệu đã sẵn sàng.");
}

// --- Hàm xử lý User & Wallet ---

function normalizeUsername(username) {
    if (!username || typeof username !== 'string') {
        return null;
    }
    const trimmed = username.trim();
    return trimmed ? trimmed.toLowerCase() : null;
}

async function addWalletToUser(chatId, lang, walletAddress) {
    const normalizedLangInput = normalizeLanguageCode(lang);
    const normalizedAddr = ethers.getAddress(walletAddress);
    const now = Math.floor(Date.now() / 1000);
    let user = await dbGet('SELECT lang, lang_source, wallets, createdAt FROM users WHERE chatId = ?', [chatId]);

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

        await dbRun(
            'UPDATE users SET lang = ?, lang_source = ?, wallets = ?, createdAt = COALESCE(createdAt, ?), lastActiveAt = ? WHERE chatId = ?',
            [langToPersist, nextSource, JSON.stringify(wallets), now, now, chatId]
        );
    } else {
        await dbRun(
            'INSERT INTO users (chatId, lang, wallets, lang_source, createdAt, lastActiveAt) VALUES (?, ?, ?, ?, ?, ?)',
            [chatId, normalizedLangInput, JSON.stringify([normalizedAddr]), 'auto', now, now]
        );
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

async function saveUserProfile(chatId, profile = {}) {
    if (!chatId) {
        return;
    }

    const { first_name = null, last_name = null, username = null } = profile;
    const normalizedUsername = normalizeUsername(username);
    const existing = await dbGet('SELECT chatId, createdAt FROM users WHERE chatId = ?', [chatId]);
    const now = Math.floor(Date.now() / 1000);

    if (existing) {
        await dbRun(
            'UPDATE users SET first_name = ?, last_name = ?, username = COALESCE(?, username), createdAt = COALESCE(createdAt, ?), lastActiveAt = ? WHERE chatId = ?',
            [first_name, last_name, normalizedUsername, now, now, chatId]
        );
    } else {
        await dbRun(
            'INSERT INTO users (chatId, lang, wallets, lang_source, first_name, last_name, username, createdAt, lastActiveAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
            [chatId, null, '[]', 'auto', first_name, last_name, normalizedUsername, now, now]
        );
    }
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

async function upsertGroupSubscription(chatId, lang, minStake, messageThreadId = null, title = null) {
    const now = Math.floor(Date.now() / 1000);
    const normalizedThreadId =
        messageThreadId === undefined || messageThreadId === null
            ? null
            : messageThreadId.toString();
    const normalizedTitle = title || null;
    const existing = await dbGet('SELECT chatId FROM group_subscriptions WHERE chatId = ?', [chatId]);
    if (existing) {
        await dbRun(
            'UPDATE group_subscriptions SET lang = ?, minStake = ?, messageThreadId = ?, title = COALESCE(?, title), updatedAt = ? WHERE chatId = ?',
            [lang, minStake, normalizedThreadId, normalizedTitle, now, chatId]
        );
    } else {
        await dbRun(
            'INSERT INTO group_subscriptions (chatId, lang, minStake, messageThreadId, title, createdAt, updatedAt) VALUES (?, ?, ?, ?, ?, ?, ?)',
            [chatId, lang, minStake, normalizedThreadId, normalizedTitle, now, now]
        );
    }
}

async function removeGroupSubscription(chatId) {
    await dbRun('DELETE FROM group_subscriptions WHERE chatId = ?', [chatId]);
}

async function getGroupSubscription(chatId) {
    const row = await dbGet('SELECT chatId, lang, minStake, messageThreadId, title, createdAt, updatedAt FROM group_subscriptions WHERE chatId = ?', [chatId]);
    if (!row) {
        return null;
    }

    return {
        chatId: row.chatId,
        lang: row.lang,
        minStake: row.minStake,
        messageThreadId: row.messageThreadId == null ? null : row.messageThreadId.toString(),
        title: row.title || null,
        createdAt: row.createdAt || null,
        updatedAt: row.updatedAt || null
    };
}

async function getGroupSubscriptions() {
    const rows = await dbAll('SELECT chatId, lang, minStake, messageThreadId, title, createdAt, updatedAt FROM group_subscriptions');
    return rows.map(row => ({
        chatId: row.chatId,
        lang: row.lang,
        minStake: row.minStake,
        messageThreadId: row.messageThreadId == null ? null : row.messageThreadId.toString(),
        title: row.title || null,
        createdAt: row.createdAt || null,
        updatedAt: row.updatedAt || null
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

async function addOwner(chatId, username, options = {}) {
    if (!chatId) {
        return;
    }

    const normalizedUsername = normalizeUsername(username);
    const isPrimary = options?.isPrimary ? 1 : 0;
    const now = Math.floor(Date.now() / 1000);

    const existing = await dbGet('SELECT chatId FROM owners WHERE chatId = ?', [chatId]);
    if (existing) {
        await dbRun('UPDATE owners SET username = COALESCE(?, username), isPrimary = MAX(isPrimary, ?), createdAt = ? WHERE chatId = ?',
            [normalizedUsername, isPrimary, now, chatId]);
    } else {
        await dbRun('INSERT INTO owners (chatId, username, isPrimary, createdAt) VALUES (?, ?, ?, ?)', [chatId, normalizedUsername, isPrimary, now]);
    }
}

async function isOwner(chatId) {
    if (!chatId) {
        return false;
    }
    const row = await dbGet('SELECT chatId FROM owners WHERE chatId = ?', [chatId]);
    return !!row;
}

async function getOwners() {
    const rows = await dbAll('SELECT chatId, username, isPrimary FROM owners');
    return rows.map((row) => ({
        chatId: row.chatId,
        username: row.username,
        isPrimary: Boolean(row.isPrimary)
    }));
}

async function removeOwner(chatId) {
    if (!chatId) {
        return false;
    }

    const existing = await dbGet('SELECT chatId, isPrimary FROM owners WHERE chatId = ?', [chatId]);
    if (!existing || existing.isPrimary) {
        return false;
    }

    await dbRun('DELETE FROM owners WHERE chatId = ?', [chatId]);
    return true;
}

async function removeAllCoOwners() {
    await dbRun('DELETE FROM owners WHERE isPrimary = 0');
}

async function banUser(chatId, createdBy = null) {
    if (!chatId) {
        return;
    }
    const now = Math.floor(Date.now() / 1000);
    await dbRun('INSERT OR REPLACE INTO bans (identifier, type, createdAt, createdBy) VALUES (?, ?, ?, ?)', [chatId.toString(), 'user', now, createdBy]);
}

async function banWallet(walletAddress, createdBy = null) {
    if (!walletAddress) {
        return;
    }
    const normalizedAddr = ethers.getAddress(walletAddress);
    const now = Math.floor(Date.now() / 1000);
    await dbRun('INSERT OR REPLACE INTO bans (identifier, type, createdAt, createdBy) VALUES (?, ?, ?, ?)', [normalizedAddr, 'wallet', now, createdBy]);
}

async function unbanUser(chatId) {
    await dbRun('DELETE FROM bans WHERE identifier = ? AND type = ?', [chatId.toString(), 'user']);
}

async function unbanWallet(walletAddress) {
    try {
        const normalized = ethers.getAddress(walletAddress);
        await dbRun('DELETE FROM bans WHERE identifier = ? AND type = ?', [normalized, 'wallet']);
    } catch (err) {
        console.error(`[DB] Không thể unban ví ${walletAddress}: ${err.message}`);
    }
}

async function isChatBanned(chatId) {
    if (!chatId) {
        return false;
    }
    const row = await dbGet('SELECT 1 FROM bans WHERE identifier = ? AND type = ? LIMIT 1', [chatId.toString(), 'user']);
    return !!row;
}

async function areWalletsBanned(wallets = []) {
    if (!Array.isArray(wallets) || wallets.length === 0) {
        return false;
    }

    const normalized = [];
    for (const wallet of wallets) {
        try {
            normalized.push(ethers.getAddress(wallet));
        } catch (err) {
            // ignore invalid
        }
    }

    if (normalized.length === 0) {
        return false;
    }

    const placeholders = normalized.map(() => '?').join(',');
    const sql = `SELECT 1 FROM bans WHERE type = 'wallet' AND identifier IN (${placeholders}) LIMIT 1`;
    const row = await dbGet(sql, normalized);
    return !!row;
}

async function listBans() {
    const rows = await dbAll('SELECT identifier, type FROM bans');
    return rows.map((row) => ({ identifier: row.identifier, type: row.type }));
}

async function clearAllBans() {
    await dbRun('DELETE FROM bans');
}

async function logBotMessage(chatId, messageId) {
    if (!chatId || !messageId) {
        return;
    }
    await dbRun('INSERT OR IGNORE INTO message_logs (chatId, messageId) VALUES (?, ?)', [chatId.toString(), messageId]);
}

async function getMessageLogs(chatId) {
    if (!chatId) {
        return [];
    }
    const rows = await dbAll('SELECT messageId FROM message_logs WHERE chatId = ? ORDER BY messageId DESC', [chatId.toString()]);
    return rows.map((row) => row.messageId);
}

async function clearMessageLogs(chatId) {
    if (!chatId) {
        return;
    }
    await dbRun('DELETE FROM message_logs WHERE chatId = ?', [chatId.toString()]);
}

async function clearAllMessageLogs() {
    await dbRun('DELETE FROM message_logs');
}

async function getAllUsersDetailed() {
    const rows = await dbAll('SELECT chatId, lang, wallets, first_name, last_name, username, createdAt, lastActiveAt FROM users');
    return rows.map((row) => {
        let parsedWallets = [];
        try {
            parsedWallets = row.wallets ? JSON.parse(row.wallets) : [];
        } catch (err) {
            parsedWallets = [];
        }
        return {
            chatId: row.chatId,
            lang: normalizeLanguageCode(row.lang),
            wallets: Array.isArray(parsedWallets) ? parsedWallets : [],
            firstName: row.first_name || '',
            lastName: row.last_name || '',
            username: row.username || '',
            createdAt: row.createdAt || null,
            lastActiveAt: row.lastActiveAt || null
        };
    });
}

async function clearUserData(chatId) {
    if (!chatId) {
        return;
    }
    const idText = chatId.toString();
    await dbRun('DELETE FROM users WHERE chatId = ?', [idText]);
    await dbRun('DELETE FROM group_member_languages WHERE userId = ?', [idText]);
    await dbRun('DELETE FROM checkin_members WHERE userId = ?', [idText]);
    await dbRun('DELETE FROM checkin_records WHERE userId = ?', [idText]);
    await dbRun('DELETE FROM checkin_attempts WHERE userId = ?', [idText]);
    await dbRun('DELETE FROM owners WHERE chatId = ? AND isPrimary = 0', [idText]);
    await clearMessageLogs(idText);
    await unbanUser(idText);
}

async function clearAllUserData() {
    await dbRun('DELETE FROM users');
    await dbRun('DELETE FROM pending_tokens');
    await dbRun('DELETE FROM game_stats');
    await dbRun('DELETE FROM group_subscriptions');
    await dbRun('DELETE FROM group_member_languages');
    await dbRun('DELETE FROM checkin_members');
    await dbRun('DELETE FROM checkin_records');
    await dbRun('DELETE FROM checkin_attempts');
    await removeAllCoOwners();
    await clearAllBans();
    await clearAllMessageLogs();
}

module.exports = {
    init,
    ensureCheckinGroup,
    getCheckinGroup,
    listCheckinGroups,
    updateCheckinGroup,
    updateAutoMessageDate,
    incrementCheckinAttempt,
    clearDailyAttempts,
    getCheckinAttempt,
    getCheckinRecord,
    completeCheckin,
    updateCheckinFeedback,
    getCheckinsForDate,
    getCheckinsInRange,
    getCheckinMemberSummary,
    getMemberLeaderboardStats,
    getTopCheckins,
    removeCheckinRecord,
    clearMemberLeaderboardEntries,
    unlockMemberCheckin,
    markMemberLocked,
    hasAutoMessageLog,
    recordAutoMessageLog,
    hasSummaryMessageLog,
    recordSummaryMessageLog,
    resetSummaryMessageLogs,
    getLockedMembers,
    addWalletToUser,
    removeWalletFromUser,
    removeAllWalletsFromUser,
    getWalletsForUser,
    getUsersForWallet,
    getUserLanguage,
    getUserLanguageInfo,
    setUserLanguage,
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
    setLeaderboardPeriodStart,
    setSummaryPeriodStart,
    getGroupMemberLanguage,
    getGroupMemberLanguages,
    setGroupMemberLanguage,
    removeGroupMemberLanguage,
    updateGroupSubscriptionLanguage,
    updateGroupSubscriptionTopic,
    saveUserProfile,
    addOwner,
    isOwner,
    getOwners,
    banUser,
    banWallet,
    unbanUser,
    unbanWallet,
    isChatBanned,
    areWalletsBanned,
    removeOwner,
    removeAllCoOwners,
    listBans,
    getAllUsersDetailed,
    logBotMessage,
    getMessageLogs,
    clearMessageLogs,
    clearAllMessageLogs,
    clearUserData,
    clearAllUserData,
    clearAllBans
};
