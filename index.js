// Đảm bảo dotenv được gọi ĐẦU TIÊN
require('dotenv').config(); 

// --- Import các thư viện ---
const ethers = require('ethers');
const TelegramBot = require('node-telegram-bot-api');
const fs = require('fs');
const express = require('express');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
const https = require('https');
const crypto = require('crypto');
const { t_, normalizeLanguageCode } = require('./i18n.js');
const db = require('./database.js');

// --- CẤU HÌNH ---
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const RPC_URL = process.env.RPC_URL;
const CONTRACT_ADDRESS = process.env.CONTRACT_ADDRESS;
const contractABI = require('./BanmaoRPS_ABI.json');
const API_PORT = 3000;
const WEB_URL = "https://www.banmao.fun";
const defaultLang = 'en';
const roomCache = new Map();
const finalRoomOutcomes = new Map();
const MAX_TELEGRAM_RETRIES = 5;
const OKX_BASE_URL = process.env.OKX_BASE_URL || 'https://web3.okx.com';
const OKX_CHAIN_SHORT_NAME = process.env.OKX_CHAIN_SHORT_NAME || 'x-layer';
const OKX_BANMAO_TOKEN_ADDRESS =
    normalizeOkxConfigAddress(process.env.OKX_BANMAO_TOKEN_ADDRESS) ||
    '0x16d91d1615FC55B76d5f92365Bd60C069B46ef78';
const OKX_QUOTE_TOKEN_ADDRESS =
    normalizeOkxConfigAddress(process.env.OKX_QUOTE_TOKEN_ADDRESS) ||
    '0x779Ded0c9e1022225f8E0630b35a9b54bE713736';
const BANMAO_ADDRESS_LOWER = OKX_BANMAO_TOKEN_ADDRESS ? OKX_BANMAO_TOKEN_ADDRESS.toLowerCase() : null;
const OKX_QUOTE_ADDRESS_LOWER = OKX_QUOTE_TOKEN_ADDRESS ? OKX_QUOTE_TOKEN_ADDRESS.toLowerCase() : null;
const OKX_MARKET_INSTRUMENT = process.env.OKX_MARKET_INSTRUMENT || 'BANMAO-USDT';
const OKX_FETCH_TIMEOUT = Number(process.env.OKX_FETCH_TIMEOUT || 10000);
const OKX_API_KEY = process.env.OKX_API_KEY || null;
const OKX_SECRET_KEY = process.env.OKX_SECRET_KEY || null;
const OKX_API_PASSPHRASE = process.env.OKX_API_PASSPHRASE || null;
const OKX_API_PROJECT = process.env.OKX_API_PROJECT || null;
const OKX_API_SIMULATED = String(process.env.OKX_API_SIMULATED || '').toLowerCase() === 'true';
const OKX_OKB_TOKEN_ADDRESSES = (() => {
    const raw = process.env.OKX_OKB_TOKEN_ADDRESSES
        || '0xe538905cf8410324e03a5a23c1c177a474d59b2b';

    const seen = new Set();
    const result = [];

    for (const value of raw.split(/[|,\s]+/)) {
        if (!value) {
            continue;
        }

        const normalized = normalizeOkxConfigAddress(value);
        if (!normalized) {
            continue;
        }

        const lowered = normalized.toLowerCase();
        if (seen.has(lowered)) {
            continue;
        }

        seen.add(lowered);
        result.push(lowered);
    }

    return result;
})();
const OKX_OKB_SYMBOL_KEYS = ['okb', 'wokb'];
const OKX_CHAIN_INDEX = (() => {
    const value = process.env.OKX_CHAIN_INDEX;
    if (value === undefined || value === null || value === '') {
        return null;
    }

    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : null;
})();
const OKX_CHAIN_CONTEXT_TTL = Number(process.env.OKX_CHAIN_CONTEXT_TTL || 10 * 60 * 1000);
const OKX_CHAIN_INDEX_FALLBACK = Number.isFinite(Number(process.env.OKX_CHAIN_INDEX_FALLBACK))
    ? Number(process.env.OKX_CHAIN_INDEX_FALLBACK)
    : 196;
const OKX_TOKEN_DIRECTORY_TTL = Number(process.env.OKX_TOKEN_DIRECTORY_TTL || 10 * 60 * 1000);
const hasOkxCredentials = Boolean(OKX_API_KEY && OKX_SECRET_KEY && OKX_API_PASSPHRASE);
const OKX_BANMAO_TOKEN_URL =
    process.env.OKX_BANMAO_TOKEN_URL ||
    'https://web3.okx.com/token/x-layer/0x16d91d1615fc55b76d5f92365bd60c069b46ef78';

let okxChainDirectoryCache = null;
let okxChainDirectoryExpiresAt = 0;
let okxChainDirectoryPromise = null;
const okxResolvedChainCache = new Map();
const BANMAO_DECIMALS_DEFAULT = 18;
const BANMAO_DECIMALS_CACHE_TTL = 30 * 60 * 1000;
let banmaoDecimalsCache = null;
let banmaoDecimalsFetchedAt = 0;
const tokenDecimalsCache = new Map();
const okxTokenDirectoryCache = new Map();

const CHECKIN_MAX_ATTEMPTS = 3;
const CHECKIN_SCHEDULER_INTERVAL = 45 * 1000;
const CHECKIN_DEFAULT_TIME = '08:00';
const CHECKIN_DEFAULT_TIMEZONE = 'UTC';
const CHECKIN_EMOTIONS = ['🤩', '👍', '💪', '😴', '😊', '🔥'];
const CHECKIN_GOAL_PRESETS = [
    'checkin_goal_preset_learn',
    'checkin_goal_preset_task',
    'checkin_goal_preset_workout',
    'checkin_goal_preset_rest',
    'checkin_goal_preset_help'
];

const pendingCheckinChallenges = new Map();
const pendingEmotionPrompts = new Map();
const pendingGoalInputs = new Map();
const pendingSecretMessages = new Map();
const checkinAdminStates = new Map();
const checkinAdminMenus = new Map();
const adminHubSessions = new Map();
let checkinSchedulerTimer = null;

function delay(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function scheduleMessageDeletion(chatId, messageId, delayMs = 15000) {
    if (!chatId || !messageId) {
        return;
    }

    const timer = setTimeout(() => {
        bot.deleteMessage(chatId, messageId).catch(() => { /* ignore */ });
    }, Math.max(delayMs, 1000));

    if (typeof timer.unref === 'function') {
        timer.unref();
    }
}

async function sendEphemeralMessage(chatId, text, options = {}, delayMs = 15000) {
    const message = await bot.sendMessage(chatId, text, options);
    scheduleMessageDeletion(chatId, message.message_id, delayMs);
    return message;
}

function normalizeOkxConfigAddress(value) {
    if (!value || typeof value !== 'string') {
        return null;
    }

    const trimmed = value.trim();
    if (!trimmed) {
        return null;
    }

    try {
        return ethers.getAddress(trimmed);
    } catch (error) {
        const basicHexPattern = /^0x[0-9a-fA-F]{40}$/;
        if (basicHexPattern.test(trimmed)) {
            return trimmed;
        }
    }

    return null;
}

function markRoomFinalOutcome(roomId, outcome) {
    const roomIdStr = toRoomIdString(roomId);
    const record = { outcome, recordedAt: Date.now() };
    finalRoomOutcomes.set(roomIdStr, record);

    const timeout = setTimeout(() => {
        const existing = finalRoomOutcomes.get(roomIdStr);
        if (existing && existing.recordedAt === record.recordedAt) {
            finalRoomOutcomes.delete(roomIdStr);
        }
    }, 60 * 60 * 1000);

    if (typeof timeout.unref === 'function') {
        timeout.unref();
    }

    return record;
}

function getRoomFinalOutcome(roomId) {
    return finalRoomOutcomes.get(toRoomIdString(roomId)) || null;
}

function clearRoomFinalOutcome(roomId) {
    finalRoomOutcomes.delete(toRoomIdString(roomId));
}

// --- Kiểm tra Cấu hình ---
if (!TELEGRAM_TOKEN || !RPC_URL || !CONTRACT_ADDRESS) {
    console.error("LỖI NGHIÊM TRỌNG: Thiếu TELEGRAM_TOKEN, RPC_URL, hoặc CONTRACT_ADDRESS trong file .env!");
    process.exit(1);
}

// --- KHỞI TẠO CÁC DỊCH VỤ ---
// db.init() sẽ được gọi trong hàm main()
const app = express();
const bot = new TelegramBot(TELEGRAM_TOKEN, { polling: true });
let provider = null;
let contract = null;
let reconnectTimeout = null;
let reconnectAttempts = 0;

// Hàm 't' (translate) nội bộ
function t(lang_code, key, variables = {}) {
    return t_(lang_code, key, variables);
}

function resolveLangCode(lang_code) {
    return normalizeLanguageCode(lang_code || defaultLang);
}

function escapeHtml(text) {
    if (typeof text !== 'string') {
        return '';
    }
    return text
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

const HELP_COMMAND_DETAILS = {
    start: { command: '/start', icon: '🚀', descKey: 'help_command_start' },
    register: { command: '/register', icon: '📝', descKey: 'help_command_register' },
    mywallet: { command: '/mywallet', icon: '💼', descKey: 'help_command_mywallet' },
    stats: { command: '/stats', icon: '📊', descKey: 'help_command_stats' },
    banmaoprice: { command: '/banmaoprice', icon: '💰', descKey: 'help_command_banmaoprice' },
    okxchains: { command: '/okxchains', icon: '🧭', descKey: 'help_command_okxchains' },
    okx402status: { command: '/okx402status', icon: '🔐', descKey: 'help_command_okx402status' },
    unregister: { command: '/unregister', icon: '🗑️', descKey: 'help_command_unregister' },
    language: { command: '/language', icon: '🌐', descKey: 'help_command_language' },
    feedlang: { command: '/feedlang', icon: '🗣️', descKey: 'help_command_feedlang' },
    help: { command: '/help', icon: '❓', descKey: 'help_command_help' },
    checkin: { command: '/checkin', icon: '✅', descKey: 'help_command_checkin' },
    topcheckin: { command: '/topcheckin', icon: '🏆', descKey: 'help_command_topcheckin' },
    admin: { command: '/admin', icon: '🛠️', descKey: 'help_command_admin' }
};

const HELP_USER_SECTIONS = [
    {
        titleKey: 'help_section_general_title',
        commands: ['start', 'register', 'mywallet', 'stats', 'banmaoprice', 'okxchains', 'okx402status', 'unregister', 'language', 'feedlang', 'help']
    },
    {
        titleKey: 'help_section_checkin_title',
        commands: ['checkin', 'topcheckin']
    }
];

const HELP_ADMIN_SECTIONS = [
    {
        titleKey: 'help_section_admin_title',
        commands: ['admin']
    }
];

function buildHelpRows(lang, commandKeys) {
    const rows = [];
    for (const key of commandKeys) {
        const detail = HELP_COMMAND_DETAILS[key];
        if (!detail) {
            continue;
        }
        const description = t(lang, detail.descKey);
        const label = `${detail.icon} ${detail.command}`.padEnd(20, ' ');
        rows.push(`${label} ${description}`);
    }

    if (rows.length === 0) {
        return '';
    }

    return `<pre>${escapeHtml(rows.join('\n'))}</pre>`;
}

function buildHelpText(lang, view = 'user') {
    const sections = view === 'admin' ? HELP_ADMIN_SECTIONS : HELP_USER_SECTIONS;
    const lines = [];

    lines.push(t(lang, 'help_header'));
    const hintKey = view === 'admin' ? 'help_admin_hint' : 'help_menu_hint';
    lines.push(`<i>${escapeHtml(t(lang, hintKey))}</i>`);

    for (const section of sections) {
        const table = buildHelpRows(lang, section.commands);
        if (!table) {
            continue;
        }

        lines.push('', `<b>${escapeHtml(t(lang, section.titleKey))}</b>`, table);
    }

    if (view === 'admin') {
        lines.push('', `<i>${escapeHtml(t(lang, 'help_admin_features'))}</i>`);
    }

    return lines.filter(Boolean).join('\n');
}

function buildHelpKeyboard(lang, view = 'user') {
    const sections = view === 'admin' ? HELP_ADMIN_SECTIONS : HELP_USER_SECTIONS;
    const commandKeys = sections.flatMap((section) => section.commands);
    const inline_keyboard = [];

    for (let i = 0; i < commandKeys.length; i += 2) {
        const row = [];
        for (let j = i; j < Math.min(i + 2, commandKeys.length); j += 1) {
            const key = commandKeys[j];
            const detail = HELP_COMMAND_DETAILS[key];
            if (!detail) {
                continue;
            }
            row.push({ text: `${detail.icon} ${detail.command}`, callback_data: `help_cmd|${key}` });
        }
        if (row.length > 0) {
            inline_keyboard.push(row);
        }
    }

    if (view === 'admin') {
        inline_keyboard.push([{ text: t(lang, 'help_button_user'), callback_data: 'help_view|user' }]);
    } else {
        inline_keyboard.push([{ text: t(lang, 'help_button_admin'), callback_data: 'help_view|admin' }]);
    }

    inline_keyboard.push([{ text: t(lang, 'help_button_close'), callback_data: 'help_close' }]);
    return { inline_keyboard };
}

function extractThreadId(source) {
    if (!source) {
        return null;
    }

    if (Object.prototype.hasOwnProperty.call(source, 'message_thread_id') && source.message_thread_id !== undefined && source.message_thread_id !== null) {
        return source.message_thread_id;
    }

    if (source.message && Object.prototype.hasOwnProperty.call(source.message, 'message_thread_id') && source.message.message_thread_id !== undefined && source.message.message_thread_id !== null) {
        return source.message.message_thread_id;
    }

    return null;
}

function buildThreadedOptions(source, options = {}) {
    const threadId = extractThreadId(source);
    if (threadId === undefined || threadId === null) {
        return { ...options };
    }

    return { ...options, message_thread_id: threadId };
}

async function sendMessageRespectingThread(chatId, source, text, options = {}) {
    const threadedOptions = buildThreadedOptions(source, options);

    try {
        return await bot.sendMessage(chatId, text, threadedOptions);
    } catch (error) {
        const errorCode = error?.response?.body?.error_code;
        const description = error?.response?.body?.description || '';
        const hasThread = Object.prototype.hasOwnProperty.call(threadedOptions, 'message_thread_id');

        if (hasThread && errorCode === 400) {
            const lowered = description.toLowerCase();
            const shouldFallback =
                lowered.includes('message thread not found') ||
                lowered.includes('topic is closed') ||
                lowered.includes('forum topic is closed') ||
                lowered.includes('forum topics are disabled') ||
                lowered.includes('forum is disabled') ||
                lowered.includes('wrong message thread id specified') ||
                lowered.includes("can't send messages to the topic") ||
                lowered.includes('not enough rights to send in the topic') ||
                lowered.includes('not enough rights to send messages in the topic');

            if (shouldFallback) {
                console.warn(`[ThreadFallback] Gửi tin nhắn tới thread ${threadedOptions.message_thread_id} thất bại (${description}). Thử gửi không chỉ định thread.`);
                const fallbackOptions = { ...options };
                return bot.sendMessage(chatId, text, fallbackOptions);
            }
        }

        throw error;
    }
}

function sendReply(sourceMessage, text, options = {}) {
    if (!sourceMessage || !sourceMessage.chat) {
        throw new Error('sendReply requires a message with chat information');
    }

    return sendMessageRespectingThread(sourceMessage.chat.id, sourceMessage, text, options);
}

function buildUserMention(user) {
    if (!user) {
        return { text: 'user', parseMode: null };
    }

    if (user.username) {
        return { text: `@${user.username}`, parseMode: null };
    }

    const displayName = escapeHtml(user.first_name || user.last_name || 'user');
    return {
        text: `<a href="tg://user?id=${user.id}">${displayName}</a>`,
        parseMode: 'HTML'
    };
}

function formatDateForTimezone(timezone = CHECKIN_DEFAULT_TIMEZONE, date = new Date()) {
    try {
        const formatter = new Intl.DateTimeFormat('en-CA', {
            timeZone: timezone,
            year: 'numeric',
            month: '2-digit',
            day: '2-digit'
        });

        return formatter.format(date);
    } catch (error) {
        console.warn(`[Checkin] Không thể format ngày cho timezone ${timezone}: ${error.message}`);
        const year = date.getUTCFullYear();
        const month = String(date.getUTCMonth() + 1).padStart(2, '0');
        const day = String(date.getUTCDate()).padStart(2, '0');
        return `${year}-${month}-${day}`;
    }
}

function formatTimeForTimezone(timezone = CHECKIN_DEFAULT_TIMEZONE, date = new Date()) {
    try {
        const formatter = new Intl.DateTimeFormat('en-GB', {
            timeZone: timezone,
            hour: '2-digit',
            minute: '2-digit',
            hour12: false
        });

        return formatter.format(date);
    } catch (error) {
        console.warn(`[Checkin] Không thể format giờ cho timezone ${timezone}: ${error.message}`);
        const hours = String(date.getUTCHours()).padStart(2, '0');
        const minutes = String(date.getUTCMinutes()).padStart(2, '0');
        return `${hours}:${minutes}`;
    }
}

function generateMathChallenge(lang = 'en') {
    const resolvedLang = lang || 'en';
    const operations = ['+', '-', '×', '÷'];
    const op = operations[Math.floor(Math.random() * operations.length)];
    let a = Math.floor(Math.random() * 12) + 1;
    let b = Math.floor(Math.random() * 12) + 1;
    let expression = '';
    let answer = 0;

    switch (op) {
        case '+':
            answer = a + b;
            expression = `${a} + ${b}`;
            break;
        case '-':
            if (b > a) {
                [a, b] = [b, a];
            }
            answer = a - b;
            expression = `${a} - ${b}`;
            break;
        case '×':
            answer = a * b;
            expression = `${a} × ${b}`;
            break;
        case '÷':
            answer = a;
            expression = `${a * b} ÷ ${b}`;
            break;
        default:
            answer = a + b;
            expression = `${a} + ${b}`;
            break;
    }

    const options = new Set([answer]);
    while (options.size < 4) {
        const delta = Math.floor(Math.random() * 10) + 1;
        const sign = Math.random() > 0.5 ? 1 : -1;
        const candidate = answer + sign * delta;
        if (candidate >= 0) {
            options.add(candidate);
        }
    }

    const optionArray = Array.from(options);
    for (let i = optionArray.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [optionArray[i], optionArray[j]] = [optionArray[j], optionArray[i]];
    }

    const correctIndex = optionArray.findIndex((value) => value === answer);
    const questionText = t(resolvedLang, 'checkin_math_question', { expression });

    return {
        question: questionText,
        options: optionArray.map((value, index) => ({
            text: value.toString(),
            isCorrect: index === correctIndex,
            index
        })),
        correctIndex
    };
}

function buildEmotionKeyboard(lang, token) {
    const rows = [];
    for (let i = 0; i < CHECKIN_EMOTIONS.length; i += 3) {
        const row = [];
        for (let j = i; j < i + 3 && j < CHECKIN_EMOTIONS.length; j++) {
            const emoji = CHECKIN_EMOTIONS[j];
            row.push({ text: emoji, callback_data: `checkin_emotion|${token}|${encodeURIComponent(emoji)}` });
        }
        rows.push(row);
    }
    rows.push([{ text: t(lang, 'checkin_button_skip'), callback_data: `checkin_emotion_skip|${token}` }]);
    return { inline_keyboard: rows };
}

function buildGoalKeyboard(lang, token) {
    const rows = [];
    for (const preset of CHECKIN_GOAL_PRESETS) {
        const text = t(lang, preset);
        rows.push([{ text, callback_data: `checkin_goal_choose|${token}|${encodeURIComponent(text)}` }]);
    }
    rows.push([
        { text: t(lang, 'checkin_goal_button_custom'), callback_data: `checkin_goal_custom|${token}` },
        { text: t(lang, 'checkin_goal_button_later'), callback_data: `checkin_goal_skip|${token}` }
    ]);
    return { inline_keyboard: rows };
}

function sanitizeGoalInput(text) {
    if (typeof text !== 'string') {
        return null;
    }

    const trimmed = text.trim();
    if (!trimmed) {
        return null;
    }

    if (trimmed.length > 200) {
        return trimmed.slice(0, 200);
    }

    return trimmed;
}

function createShortToken(prefix = 'chk') {
    const raw = uuidv4().replace(/-/g, '');
    const short = raw.slice(0, 16);
    return `${prefix}_${short}`;
}

async function getGroupCheckinSettings(chatId) {
    const chatKey = chatId.toString();
    try {
        return await db.getCheckinGroup(chatKey);
    } catch (error) {
        console.warn(`[Checkin] Không thể đọc cấu hình nhóm ${chatKey}: ${error.message}`);
        return {
            chatId: chatKey,
            checkinTime: CHECKIN_DEFAULT_TIME,
            timezone: CHECKIN_DEFAULT_TIMEZONE,
            autoMessageEnabled: 1,
            dailyPoints: 10,
            summaryWindow: 7,
            lastAutoMessageDate: null
        };
    }
}

function buildCheckinKeyboard(chatId, lang) {
    const chatKey = chatId.toString();
    return {
        inline_keyboard: [
            [{ text: t(lang, 'checkin_button_start'), callback_data: `checkin_start|${chatKey}` }],
            [{ text: t(lang, 'checkin_button_leaderboard'), callback_data: `checkin_leaderboard|${chatKey}` }],
            [{ text: t(lang, 'checkin_button_admin_menu'), callback_data: `checkin_admin|${chatKey}` }]
        ]
    };
}

async function sendCheckinAnnouncement(chatId, { sourceMessage = null, triggeredBy = 'auto' } = {}) {
    const settings = await getGroupCheckinSettings(chatId);
    const timezone = settings.timezone || CHECKIN_DEFAULT_TIMEZONE;
    const today = formatDateForTimezone(timezone);
    const lang = await resolveGroupLanguage(chatId);
    const promptText = t(lang, 'checkin_prompt_text');
    const options = { reply_markup: buildCheckinKeyboard(chatId, lang) };

    try {
        if (sourceMessage) {
            await sendMessageRespectingThread(chatId, sourceMessage, promptText, options);
        } else {
            await bot.sendMessage(chatId, promptText, options);
        }
        await db.updateAutoMessageDate(chatId, today);
        console.log(`[Checkin] Sent check-in announcement to ${chatId} (${triggeredBy}).`);
    } catch (error) {
        console.error(`[Checkin] Failed to send announcement to ${chatId}: ${error.message}`);
    }
}

async function ensureUserCanCheckin(chatId, userId, settings) {
    const timezone = settings?.timezone || CHECKIN_DEFAULT_TIMEZONE;
    const today = formatDateForTimezone(timezone);
    const attempt = await db.getCheckinAttempt(chatId, userId, today);
    if (attempt && Number(attempt.locked) === 1) {
        return { allowed: false, reason: 'locked', attempts: attempt.attempts, date: today };
    }

    const record = await db.getCheckinRecord(chatId, userId, today);
    if (record) {
        return { allowed: false, reason: 'checked', record, date: today };
    }

    return { allowed: true, date: today, attempts: attempt?.attempts || 0 };
}

async function initiateCheckinChallenge(chatId, user, { replyMessage = null } = {}) {
    const settings = await getGroupCheckinSettings(chatId);
    const userId = user.id.toString();
    const userLang = await resolveNotificationLanguage(userId, user.language_code);
    const groupLang = await resolveGroupLanguage(chatId);
    const check = await ensureUserCanCheckin(chatId, userId, settings);

    if (!check.allowed) {
        if (check.reason === 'locked') {
            return { status: 'locked', userLang };
        }

        if (check.reason === 'checked') {
            return { status: 'checked', userLang };
        }
    }

    const challenge = generateMathChallenge(userLang);
    const token = createShortToken('chk');
    pendingCheckinChallenges.set(token, {
        chatId: chatId.toString(),
        userId,
        timezone: settings.timezone || CHECKIN_DEFAULT_TIMEZONE,
        date: check.date,
        attempts: check.attempts || 0,
        correctIndex: challenge.correctIndex,
        settings,
        sourceMessage: replyMessage ? { chatId: replyMessage.chat?.id, messageId: replyMessage.message_id } : null
    });

    const inline_keyboard = challenge.options.map((option) => ([{
        text: option.text,
        callback_data: `checkin_answer|${token}|${option.index}`
    }]));

    const dmText = [
        t(userLang, 'checkin_dm_intro'),
        '',
        challenge.question,
        '',
        t(userLang, 'checkin_dm_choose_option')
    ].join('\n');

    try {
        await bot.sendMessage(userId, dmText, { reply_markup: { inline_keyboard } });
        return { status: 'sent', userLang };
    } catch (error) {
        pendingCheckinChallenges.delete(token);
        console.warn(`[Checkin] Unable to send DM to ${userId}: ${error.message}`);

        if (replyMessage) {
            const mention = buildUserMention(user);
            const noteLines = [
                t(groupLang, 'checkin_dm_failure_line1', { user: mention.text }),
                t(groupLang, 'checkin_dm_failure_line2')
            ];
            const note = noteLines.join('\n');
            const options = {};
            if (mention.parseMode) {
                options.parse_mode = mention.parseMode;
            }
            await sendMessageRespectingThread(replyMessage.chat.id, replyMessage, note, options);
        }

        return { status: 'failed', userLang, failureReason: 'dm_unreachable' };
    }
}

async function concludeCheckinSuccess(token, challenge) {
    const userId = challenge.userId;
    const chatId = challenge.chatId;
    const settings = challenge.settings || await getGroupCheckinSettings(chatId);
    const timezone = challenge.timezone || settings.timezone || CHECKIN_DEFAULT_TIMEZONE;
    const today = challenge.date || formatDateForTimezone(timezone);
    const userLang = await resolveNotificationLanguage(userId);

    let walletAddress = null;
    try {
        const wallets = await db.getWalletsForUser(userId);
        if (Array.isArray(wallets) && wallets.length > 0) {
            walletAddress = wallets[0];
        }
    } catch (error) {
        console.warn(`[Checkin] Không thể lấy ví cho ${userId}: ${error.message}`);
    }

    const points = Number(settings.dailyPoints || 0) || 0;
    const result = await db.completeCheckin({
        chatId,
        userId,
        checkinDate: today,
        walletAddress,
        pointsAwarded: points
    });

    const streak = result?.streak || 1;
    const totalPoints = result?.totalPoints || points;
    const walletNote = walletAddress
        ? t(userLang, 'checkin_success_wallet_note', { wallet: walletAddress })
        : t(userLang, 'checkin_success_wallet_missing');

    const emotionToken = createShortToken('emo');
    pendingEmotionPrompts.set(emotionToken, {
        chatId,
        userId,
        date: today,
        stage: 'emotion'
    });

    const successMessage = [
        t(userLang, 'checkin_success_title'),
        t(userLang, 'checkin_success_streak', { streak }),
        t(userLang, 'checkin_success_total_points', { totalPoints }),
        walletNote,
        '',
        t(userLang, 'checkin_emotion_prompt')
    ].join('\n');

    await bot.sendMessage(userId, successMessage, {
        reply_markup: buildEmotionKeyboard(userLang, emotionToken),
        parse_mode: 'Markdown'
    });

    pendingCheckinChallenges.delete(token);
}

async function handleCheckinAnswerCallback(query, token, answerIndexRaw) {
    const userId = query.from.id.toString();
    const lang = await resolveNotificationLanguage(userId, query.from.language_code);
    const challenge = pendingCheckinChallenges.get(token);
    if (!challenge) {
        bot.answerCallbackQuery(query.id, { text: t(lang, 'checkin_error_question_expired'), show_alert: true });
        return;
    }

    if (userId !== challenge.userId) {
        bot.answerCallbackQuery(query.id, { text: t(lang, 'checkin_error_wrong_user'), show_alert: true });
        return;
    }

    const answerIndex = Number(answerIndexRaw);
    if (!Number.isInteger(answerIndex)) {
        bot.answerCallbackQuery(query.id, { text: t(lang, 'checkin_error_invalid_choice'), show_alert: true });
        return;
    }

    if (answerIndex === challenge.correctIndex) {
        await bot.answerCallbackQuery(query.id, { text: t(lang, 'checkin_alert_correct') });
        try {
            await concludeCheckinSuccess(token, challenge);
        } catch (error) {
            console.error(`[Checkin] Failed to record check-in: ${error.message}`);
            await bot.sendMessage(userId, t(lang, 'checkin_error_record_failed'));
            pendingCheckinChallenges.delete(token);
        }
        return;
    }

    const attempts = await db.incrementCheckinAttempt(challenge.chatId, userId, challenge.date, CHECKIN_MAX_ATTEMPTS);
    challenge.attempts = attempts.attempts;
    const remaining = Math.max(CHECKIN_MAX_ATTEMPTS - attempts.attempts, 0);

    if (attempts.locked) {
        await db.markMemberLocked(challenge.chatId, userId, challenge.date);
        pendingCheckinChallenges.delete(token);
        await bot.answerCallbackQuery(query.id, { text: t(lang, 'checkin_alert_attempts_locked'), show_alert: true });
        await bot.sendMessage(userId, t(lang, 'checkin_dm_locked'));
        return;
    }

    await bot.answerCallbackQuery(query.id, {
        text: t(lang, 'checkin_alert_attempts_remaining', { remaining }),
        show_alert: true
    });

    try {
        await bot.editMessageReplyMarkup({ inline_keyboard: [] }, {
            chat_id: query.message.chat.id,
            message_id: query.message.message_id
        });
    } catch (error) {
        // ignore edit errors
    }

    const newChallenge = generateMathChallenge(lang);
    challenge.correctIndex = newChallenge.correctIndex;
    const inline_keyboard = newChallenge.options.map((option) => ([{
        text: option.text,
        callback_data: `checkin_answer|${token}|${option.index}`
    }]));

    const retryText = [
        t(lang, 'checkin_dm_retry_intro'),
        '',
        newChallenge.question,
        '',
        t(lang, 'checkin_dm_choose_option')
    ].join('\n');

    await bot.sendMessage(userId, retryText, { reply_markup: { inline_keyboard } });
}

async function handleEmotionCallback(query, token, emoji, { skip = false } = {}) {
    const prompt = pendingEmotionPrompts.get(token);
    const userId = query.from.id.toString();
    const lang = await resolveNotificationLanguage(userId, query.from.language_code);

    if (!prompt) {
        bot.answerCallbackQuery(query.id, { text: t(lang, 'checkin_error_session_expired'), show_alert: true });
        return;
    }

    if (userId !== prompt.userId) {
        bot.answerCallbackQuery(query.id, { text: t(lang, 'checkin_error_wrong_user_button'), show_alert: true });
        return;
    }

    if (!skip) {
        const decoded = decodeURIComponent(emoji || '');
        if (!decoded) {
            bot.answerCallbackQuery(query.id, { text: t(lang, 'checkin_error_invalid_emotion'), show_alert: true });
            return;
        }

        try {
            await db.updateCheckinFeedback(prompt.chatId, prompt.userId, prompt.date, { emotion: decoded });
        } catch (error) {
            console.error(`[Checkin] Unable to save emotion: ${error.message}`);
            bot.answerCallbackQuery(query.id, { text: t(lang, 'checkin_error_save_emotion'), show_alert: true });
            return;
        }
        bot.answerCallbackQuery(query.id, { text: t(lang, 'checkin_alert_emotion_saved') });
    } else {
        bot.answerCallbackQuery(query.id, { text: t(lang, 'checkin_alert_emotion_skipped') });
    }

    pendingEmotionPrompts.set(token, { ...prompt, stage: 'goal' });
    await bot.sendMessage(prompt.userId, t(lang, 'checkin_dm_goal_prompt'), {
        reply_markup: buildGoalKeyboard(lang, token)
    });
}

async function handleGoalCallback(query, token, action, value = null) {
    const prompt = pendingEmotionPrompts.get(token);
    const userId = query.from.id.toString();
    const lang = await resolveNotificationLanguage(userId, query.from.language_code);

    if (!prompt) {
        bot.answerCallbackQuery(query.id, { text: t(lang, 'checkin_error_session_expired'), show_alert: true });
        return;
    }

    if (userId !== prompt.userId) {
        bot.answerCallbackQuery(query.id, { text: t(lang, 'checkin_error_wrong_user_button'), show_alert: true });
        return;
    }

    if (prompt.stage !== 'goal') {
        bot.answerCallbackQuery(query.id, { text: t(lang, 'checkin_error_goal_stage'), show_alert: true });
        return;
    }

    if (action === 'choose') {
        const decoded = decodeURIComponent(value || '');
        try {
            await db.updateCheckinFeedback(prompt.chatId, prompt.userId, prompt.date, { goal: decoded });
            bot.answerCallbackQuery(query.id, { text: t(lang, 'checkin_alert_goal_saved') });
            await bot.sendMessage(prompt.userId, t(lang, 'checkin_dm_goal_success'));
            pendingEmotionPrompts.delete(token);
        } catch (error) {
            console.error(`[Checkin] Unable to save preset goal: ${error.message}`);
            bot.answerCallbackQuery(query.id, { text: t(lang, 'checkin_error_goal_save'), show_alert: true });
        }
        return;
    }

    if (action === 'skip') {
        bot.answerCallbackQuery(query.id, { text: t(lang, 'checkin_alert_goal_skipped') });
        await bot.sendMessage(prompt.userId, t(lang, 'checkin_dm_goal_skip'));
        pendingEmotionPrompts.delete(token);
        return;
    }

    if (action === 'custom') {
        pendingGoalInputs.set(prompt.userId, { chatId: prompt.chatId, date: prompt.date, token });
        bot.answerCallbackQuery(query.id, { text: t(lang, 'checkin_goal_custom_prompt') });
        await bot.sendMessage(prompt.userId, t(lang, 'checkin_goal_custom_dm'));
        return;
    }

    bot.answerCallbackQuery(query.id, { text: t(lang, 'checkin_error_invalid_choice'), show_alert: true });
}

async function handleGoalTextInput(msg) {
    const userId = msg.from?.id?.toString();
    if (!userId) {
        return false;
    }

    const pending = pendingGoalInputs.get(userId);
    if (!pending) {
        return false;
    }

    const lang = await resolveNotificationLanguage(userId, msg.from?.language_code);
    const goalText = sanitizeGoalInput(msg.text || '');
    if (!goalText) {
        await bot.sendMessage(userId, t(lang, 'checkin_error_goal_invalid'));
        return true;
    }

    try {
        await db.updateCheckinFeedback(pending.chatId, userId, pending.date, { goal: goalText });
        await bot.sendMessage(userId, t(lang, 'checkin_alert_goal_saved'));
        pendingEmotionPrompts.delete(pending.token);
    } catch (error) {
        console.error(`[Checkin] Unable to save custom goal: ${error.message}`);
        await bot.sendMessage(userId, t(lang, 'checkin_error_goal_save'));
    } finally {
        pendingGoalInputs.delete(userId);
    }

    return true;
}

async function buildLeaderboardText(chatId, mode = 'streak', limit = 10, langOverride = null) {
    const settings = await getGroupCheckinSettings(chatId);
    const rows = await db.getTopCheckins(chatId, limit, mode);
    const lang = langOverride ? resolveLangCode(langOverride) : await resolveGroupLanguage(chatId);

    if (!rows || rows.length === 0) {
        return t(lang, 'checkin_leaderboard_empty');
    }

    let headerKey = 'checkin_leaderboard_header_current';
    if (mode === 'points') {
        headerKey = 'checkin_leaderboard_header_points';
    } else if (mode === 'total') {
        headerKey = 'checkin_leaderboard_header_total';
    } else if (mode === 'longest') {
        headerKey = 'checkin_leaderboard_header_longest';
    }

    const lines = [t(lang, headerKey), ''];

    for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const rank = i + 1;
        let displayName = t(lang, 'checkin_leaderboard_fallback_name', { userId: row.userId });
        try {
            const member = await bot.getChatMember(chatId, row.userId);
            if (member?.user) {
                if (member.user.username) {
                    displayName = `@${member.user.username}`;
                } else if (member.user.first_name || member.user.last_name) {
                    displayName = `${member.user.first_name || ''} ${member.user.last_name || ''}`.trim();
                }
            }
        } catch (error) {
            // ignore fetch errors
        }

        let statText = '';
        if (mode === 'points') {
            statText = t(lang, 'checkin_leaderboard_stat_points', { value: row.totalPoints });
        } else if (mode === 'total') {
            statText = t(lang, 'checkin_leaderboard_stat_total', { value: row.totalCheckins });
        } else if (mode === 'longest') {
            statText = t(lang, 'checkin_leaderboard_stat_longest', { value: row.longestStreak });
        } else {
            statText = t(lang, 'checkin_leaderboard_stat_current', { value: row.streak });
        }

        lines.push(`${rank}. ${displayName} • ${statText}`);
    }

    lines.push('', t(lang, 'checkin_leaderboard_footer_time', { time: settings.checkinTime || CHECKIN_DEFAULT_TIME }));
    return lines.join('\n');
}

async function runCheckinSchedulerTick() {
    let groups = [];
    try {
        groups = await db.listCheckinGroups();
    } catch (error) {
        console.error(`[Checkin] Không thể tải danh sách nhóm: ${error.message}`);
        return;
    }

    if (!groups || groups.length === 0) {
        return;
    }

    const now = new Date();
    for (const group of groups) {
        if (!group || Number(group.autoMessageEnabled) !== 1) {
            continue;
        }

        const timezone = group.timezone || CHECKIN_DEFAULT_TIMEZONE;
        const scheduledTime = group.checkinTime || CHECKIN_DEFAULT_TIME;
        const currentTime = formatTimeForTimezone(timezone, now);
        const today = formatDateForTimezone(timezone, now);

        if (currentTime === scheduledTime && group.lastAutoMessageDate !== today) {
            await sendCheckinAnnouncement(group.chatId, { triggeredBy: 'auto' });
            group.lastAutoMessageDate = today;
        }
    }
}

function startCheckinScheduler() {
    if (checkinSchedulerTimer) {
        clearInterval(checkinSchedulerTimer);
        checkinSchedulerTimer = null;
    }

    const tick = () => {
        runCheckinSchedulerTick().catch((error) => {
            console.error(`[Checkin] Tick lỗi: ${error.message}`);
        });
    };

    tick();
    checkinSchedulerTimer = setInterval(tick, CHECKIN_SCHEDULER_INTERVAL);
    if (typeof checkinSchedulerTimer.unref === 'function') {
        checkinSchedulerTimer.unref();
    }
}

async function getAdminHubGroups(adminId) {
    let groups = [];
    try {
        groups = await db.listCheckinGroups();
    } catch (error) {
        console.error(`[AdminHub] Failed to load group list: ${error.message}`);
        return [];
    }

    if (!groups || groups.length === 0) {
        return [];
    }

    const results = [];
    for (const entry of groups) {
        if (!entry || !entry.chatId) {
            continue;
        }

        const chatId = entry.chatId;
        const isAdmin = await isGroupAdmin(chatId, adminId);
        if (!isAdmin) {
            continue;
        }

        let title = chatId.toString();
        try {
            const chat = await bot.getChat(chatId);
            if (chat?.title) {
                title = chat.title;
            } else if (chat?.username) {
                title = `@${chat.username}`;
            }
        } catch (error) {
            // ignore title lookup errors
        }

        results.push({ chatId, title });
    }

    results.sort((a, b) => a.title.localeCompare(b.title, 'en', { sensitivity: 'base' }));
    return results;
}

function buildAdminHubText(lang, groups) {
    const lines = [t(lang, 'admin_hub_title'), `<i>${escapeHtml(t(lang, 'admin_hub_hint'))}</i>`];

    if (!groups || groups.length === 0) {
        lines.push('', escapeHtml(t(lang, 'admin_hub_empty')));
    } else {
        lines.push('');
        for (let i = 0; i < groups.length; i += 1) {
            const group = groups[i];
            const safeTitle = escapeHtml(group.title || group.chatId.toString());
            lines.push(t(lang, 'admin_hub_group_line', { index: String(i + 1), title: safeTitle }));
        }
    }

    return lines.filter(Boolean).join('\n');
}

function truncateLabel(text, max = 32) {
    if (typeof text !== 'string') {
        return '';
    }
    if (text.length <= max) {
        return text;
    }
    return `${text.slice(0, max - 1)}…`;
}

function buildAdminHubKeyboard(lang, groups) {
    const inline_keyboard = [];

    if (groups && groups.length > 0) {
        for (const group of groups) {
            const label = truncateLabel(group.title || group.chatId.toString());
            inline_keyboard.push([
                {
                    text: t(lang, 'admin_hub_button_manage', { title: label }),
                    callback_data: `admin_hub_open|${group.chatId}`
                }
            ]);
        }
    }

    inline_keyboard.push([
        { text: t(lang, 'admin_hub_refresh'), callback_data: 'admin_hub_refresh' },
        { text: t(lang, 'admin_hub_close'), callback_data: 'admin_hub_close' }
    ]);

    return { inline_keyboard };
}

async function openAdminHub(adminId, { forceRefresh = false } = {}) {
    const lang = await resolveNotificationLanguage(adminId);
    const groups = await getAdminHubGroups(adminId);
    const text = buildAdminHubText(lang, groups);
    const replyMarkup = buildAdminHubKeyboard(lang, groups);

    const existing = adminHubSessions.get(adminId);
    if (existing && !forceRefresh) {
        try {
            await bot.editMessageText(text, {
                chat_id: adminId,
                message_id: existing.messageId,
                parse_mode: 'HTML',
                reply_markup: replyMarkup
            });
            return { messageId: existing.messageId, groups };
        } catch (error) {
            try {
                await bot.deleteMessage(adminId, existing.messageId);
            } catch (deleteError) {
                // ignore cleanup errors
            }
        }
    }

    const message = await bot.sendMessage(adminId, text, {
        parse_mode: 'HTML',
        reply_markup: replyMarkup
    });
    adminHubSessions.set(adminId, { messageId: message.message_id });
    return { messageId: message.message_id, groups };
}

function buildAdminMenuKeyboard(chatId, lang) {
    const chatKey = chatId.toString();
    return {
        inline_keyboard: [
            [{ text: t(lang, 'checkin_admin_board_user_header'), callback_data: 'checkin_admin_noop' }],
            [
                { text: t(lang, 'checkin_admin_button_user_checkin'), callback_data: `checkin_admin_user_prompt|${chatKey}` },
                { text: t(lang, 'checkin_admin_button_user_leaderboard'), callback_data: `checkin_admin_user_leaderboard|${chatKey}` }
            ],
            [{ text: t(lang, 'checkin_admin_board_admin_header'), callback_data: 'checkin_admin_noop' }],
            [{ text: t(lang, 'checkin_admin_button_broadcast'), callback_data: `checkin_admin_broadcast|${chatKey}` }],
            [{ text: t(lang, 'checkin_admin_button_today_list'), callback_data: `checkin_admin_list|${chatKey}` }],
            [{ text: t(lang, 'checkin_admin_button_remove'), callback_data: `checkin_admin_remove|${chatKey}` }],
            [{ text: t(lang, 'checkin_admin_button_unlock'), callback_data: `checkin_admin_unlock|${chatKey}` }],
            [{ text: t(lang, 'checkin_admin_button_secret_message'), callback_data: `checkin_admin_dm|${chatKey}` }],
            [{ text: t(lang, 'checkin_admin_button_points'), callback_data: `checkin_admin_points|${chatKey}` }],
            [{ text: t(lang, 'checkin_admin_button_summary'), callback_data: `checkin_admin_summary|${chatKey}` }],
            [{ text: t(lang, 'checkin_admin_button_reset'), callback_data: `checkin_admin_reset|${chatKey}` }],
            [{ text: t(lang, 'admin_hub_button_home'), callback_data: 'admin_hub_from_menu' }],
            [
                { text: t(lang, 'checkin_admin_button_refresh'), callback_data: `checkin_admin_refresh|${chatKey}` },
                { text: t(lang, 'checkin_admin_button_close'), callback_data: `checkin_admin_close|${chatKey}` }
            ]
        ]
    };
}

async function isGroupAdmin(chatId, userId) {
    try {
        const member = await bot.getChatMember(chatId, userId);
        if (!member) {
            return false;
        }
        return ['creator', 'administrator'].includes(member.status);
    } catch (error) {
        console.warn(`[Checkin] Không thể kiểm tra quyền admin của ${userId} trong ${chatId}: ${error.message}`);
        return false;
    }
}

async function closeAdminMenu(adminId) {
    const current = checkinAdminMenus.get(adminId);
    if (!current) {
        return;
    }

    try {
        await bot.deleteMessage(adminId, current.messageId);
    } catch (error) {
        // ignore deletion errors
    }

    checkinAdminMenus.delete(adminId);
}

async function sendAdminMenu(adminId, chatId) {
    const settings = await getGroupCheckinSettings(chatId);
    const lang = await resolveNotificationLanguage(adminId);
    const textLines = [
        t(lang, 'checkin_admin_menu_header'),
        t(lang, 'checkin_admin_menu_line_time', { time: settings.checkinTime || CHECKIN_DEFAULT_TIME }),
        t(lang, 'checkin_admin_menu_line_points', { points: settings.dailyPoints || 0 }),
        t(lang, 'checkin_admin_menu_line_summary', { days: settings.summaryWindow || 7 }),
        '',
        t(lang, 'checkin_admin_menu_user_section'),
        t(lang, 'checkin_admin_menu_admin_section'),
        '',
        t(lang, 'checkin_admin_menu_choose_action')
    ];

    const payload = {
        parse_mode: 'Markdown',
        reply_markup: buildAdminMenuKeyboard(chatId, lang)
    };

    const session = checkinAdminMenus.get(adminId);
    if (session) {
        try {
            await bot.editMessageText(textLines.join('\n'), {
                chat_id: adminId,
                message_id: session.messageId,
                parse_mode: payload.parse_mode,
                reply_markup: payload.reply_markup
            });
            checkinAdminMenus.set(adminId, { chatId, messageId: session.messageId });
            return session.messageId;
        } catch (error) {
            try {
                await bot.deleteMessage(adminId, session.messageId);
            } catch (deleteError) {
                // ignore
            }
        }
    }

    const message = await bot.sendMessage(adminId, textLines.join('\n'), payload);
    checkinAdminMenus.set(adminId, { chatId, messageId: message.message_id });
    return message.message_id;
}

async function sendTodayCheckinList(chatId, adminId) {
    const settings = await getGroupCheckinSettings(chatId);
    const today = formatDateForTimezone(settings.timezone || CHECKIN_DEFAULT_TIMEZONE);
    const records = await db.getCheckinsForDate(chatId, today);
    const lang = await resolveNotificationLanguage(adminId);
    if (!records || records.length === 0) {
        const message = await bot.sendMessage(adminId, t(lang, 'checkin_admin_today_empty'), {
            reply_markup: {
                inline_keyboard: [[
                    { text: t(lang, 'checkin_admin_button_back'), callback_data: `checkin_admin_back|${chatId}` },
                    { text: t(lang, 'checkin_admin_button_close'), callback_data: `checkin_admin_close|${chatId}` }
                ]]
            }
        });
        scheduleMessageDeletion(adminId, message.message_id, 15000);
        return;
    }

    const lines = [t(lang, 'checkin_admin_today_header'), ''];
    for (const record of records) {
        let displayName = t(lang, 'checkin_leaderboard_fallback_name', { userId: record.userId });
        try {
            const member = await bot.getChatMember(chatId, record.userId);
            if (member?.user) {
                if (member.user.username) {
                    displayName = `@${member.user.username}`;
                } else if (member.user.first_name || member.user.last_name) {
                    displayName = `${member.user.first_name || ''} ${member.user.last_name || ''}`.trim();
                }
            }
        } catch (error) {
            // ignore errors
        }

        const details = [];
        if (record.walletAddress) {
            details.push(t(lang, 'checkin_admin_today_wallet', { wallet: record.walletAddress }));
        }
        if (record.emotion) {
            details.push(t(lang, 'checkin_admin_today_emotion', { emotion: record.emotion }));
        }
        if (record.goal) {
            details.push(t(lang, 'checkin_admin_today_goal', { goal: record.goal }));
        }

        const detailText = details.length > 0 ? ` (${details.join(' • ')})` : '';
        lines.push(`• ${displayName}${detailText}`);
    }

    const message = await bot.sendMessage(adminId, lines.join('\n'), {
        reply_markup: {
            inline_keyboard: [[
                { text: t(lang, 'checkin_admin_button_back'), callback_data: `checkin_admin_back|${chatId}` },
                { text: t(lang, 'checkin_admin_button_close'), callback_data: `checkin_admin_close|${chatId}` }
            ]]
        }
    });
    scheduleMessageDeletion(adminId, message.message_id, 60000);
}

async function promptAdminForRemoval(chatId, adminId) {
    const settings = await getGroupCheckinSettings(chatId);
    const today = formatDateForTimezone(settings.timezone || CHECKIN_DEFAULT_TIMEZONE);
    const records = await db.getCheckinsForDate(chatId, today);
    const lang = await resolveNotificationLanguage(adminId);
    if (!records || records.length === 0) {
        const message = await bot.sendMessage(adminId, t(lang, 'checkin_admin_remove_empty'), {
            reply_markup: {
                inline_keyboard: [[
                    { text: t(lang, 'checkin_admin_button_back'), callback_data: `checkin_admin_back|${chatId}` },
                    { text: t(lang, 'checkin_admin_button_close'), callback_data: `checkin_admin_close|${chatId}` }
                ]]
            }
        });
        scheduleMessageDeletion(adminId, message.message_id, 15000);
        return;
    }

    const inline_keyboard = records.slice(0, 20).map((record) => ([{
        text: t(lang, 'checkin_admin_remove_option', { user: record.userId }),
        callback_data: `checkin_admin_remove_confirm|${chatId}|${record.userId}`
    }]));

    inline_keyboard.push([
        { text: t(lang, 'checkin_admin_button_back'), callback_data: `checkin_admin_back|${chatId}` },
        { text: t(lang, 'checkin_admin_button_close'), callback_data: `checkin_admin_close|${chatId}` }
    ]);

    await bot.sendMessage(adminId, t(lang, 'checkin_admin_remove_prompt'), {
        reply_markup: { inline_keyboard }
    });
}

async function promptAdminUnlock(chatId, adminId) {
    const settings = await getGroupCheckinSettings(chatId);
    const today = formatDateForTimezone(settings.timezone || CHECKIN_DEFAULT_TIMEZONE);
    const locked = await db.getLockedMembers(chatId, today);
    const lang = await resolveNotificationLanguage(adminId);
    if (!locked || locked.length === 0) {
        const message = await bot.sendMessage(adminId, t(lang, 'checkin_admin_unlock_empty'), {
            reply_markup: {
                inline_keyboard: [[
                    { text: t(lang, 'checkin_admin_button_back'), callback_data: `checkin_admin_back|${chatId}` },
                    { text: t(lang, 'checkin_admin_button_close'), callback_data: `checkin_admin_close|${chatId}` }
                ]]
            }
        });
        scheduleMessageDeletion(adminId, message.message_id, 15000);
        return;
    }

    const inline_keyboard = locked.slice(0, 20).map((entry) => ([{
        text: t(lang, 'checkin_admin_unlock_option', { user: entry.userId }),
        callback_data: `checkin_admin_unlock_confirm|${chatId}|${entry.userId}`
    }]));

    inline_keyboard.push([
        { text: t(lang, 'checkin_admin_button_back'), callback_data: `checkin_admin_back|${chatId}` },
        { text: t(lang, 'checkin_admin_button_close'), callback_data: `checkin_admin_close|${chatId}` }
    ]);

    await bot.sendMessage(adminId, t(lang, 'checkin_admin_unlock_prompt'), {
        reply_markup: { inline_keyboard }
    });
}

async function promptAdminSecretMessage(chatId, adminId) {
    const settings = await getGroupCheckinSettings(chatId);
    const today = formatDateForTimezone(settings.timezone || CHECKIN_DEFAULT_TIMEZONE);
    const records = await db.getCheckinsForDate(chatId, today);
    const lang = await resolveNotificationLanguage(adminId);
    if (!records || records.length === 0) {
        const message = await bot.sendMessage(adminId, t(lang, 'checkin_admin_dm_empty'), {
            reply_markup: {
                inline_keyboard: [[
                    { text: t(lang, 'checkin_admin_button_back'), callback_data: `checkin_admin_back|${chatId}` },
                    { text: t(lang, 'checkin_admin_button_close'), callback_data: `checkin_admin_close|${chatId}` }
                ]]
            }
        });
        scheduleMessageDeletion(adminId, message.message_id, 15000);
        return;
    }

    const inline_keyboard = records.slice(0, 20).map((record) => ([{
        text: t(lang, 'checkin_admin_dm_option', { user: record.userId }),
        callback_data: `checkin_admin_dm_target|${chatId}|${record.userId}`
    }]));

    inline_keyboard.push([
        { text: t(lang, 'checkin_admin_button_back'), callback_data: `checkin_admin_back|${chatId}` },
        { text: t(lang, 'checkin_admin_button_close'), callback_data: `checkin_admin_close|${chatId}` }
    ]);

    await bot.sendMessage(adminId, t(lang, 'checkin_admin_dm_prompt'), {
        reply_markup: { inline_keyboard }
    });
}

async function promptAdminPoints(chatId, adminId) {
    const options = [5, 10, 20, 30];
    const lang = await resolveNotificationLanguage(adminId);
    const inline_keyboard = options.map((value) => ([{
        text: t(lang, 'checkin_admin_points_option', { value }),
        callback_data: `checkin_admin_points_set|${chatId}|${value}`
    }]));
    inline_keyboard.push([{ text: t(lang, 'checkin_admin_button_custom'), callback_data: `checkin_admin_points_custom|${chatId}` }]);

    inline_keyboard.push([
        { text: t(lang, 'checkin_admin_button_back'), callback_data: `checkin_admin_back|${chatId}` },
        { text: t(lang, 'checkin_admin_button_close'), callback_data: `checkin_admin_close|${chatId}` }
    ]);

    await bot.sendMessage(adminId, t(lang, 'checkin_admin_points_title'), {
        reply_markup: { inline_keyboard }
    });
}

async function promptAdminSummaryWindow(chatId, adminId) {
    const options = [7, 14, 30];
    const lang = await resolveNotificationLanguage(adminId);
    const inline_keyboard = options.map((value) => ([{
        text: t(lang, 'checkin_admin_summary_option', { value }),
        callback_data: `checkin_admin_summary_set|${chatId}|${value}`
    }]));
    inline_keyboard.push([{ text: t(lang, 'checkin_admin_button_custom'), callback_data: `checkin_admin_summary_custom|${chatId}` }]);

    inline_keyboard.push([
        { text: t(lang, 'checkin_admin_button_back'), callback_data: `checkin_admin_back|${chatId}` },
        { text: t(lang, 'checkin_admin_button_close'), callback_data: `checkin_admin_close|${chatId}` }
    ]);

    await bot.sendMessage(adminId, t(lang, 'checkin_admin_summary_title'), {
        reply_markup: { inline_keyboard }
    });
}

async function promptAdminResetQuestion(chatId, adminId) {
    const settings = await getGroupCheckinSettings(chatId);
    const today = formatDateForTimezone(settings.timezone || CHECKIN_DEFAULT_TIMEZONE);
    const locked = await db.getLockedMembers(chatId, today);
    const lang = await resolveNotificationLanguage(adminId);
    if (!locked || locked.length === 0) {
        const message = await bot.sendMessage(adminId, t(lang, 'checkin_admin_reset_empty'), {
            reply_markup: {
                inline_keyboard: [[
                    { text: t(lang, 'checkin_admin_button_back'), callback_data: `checkin_admin_back|${chatId}` },
                    { text: t(lang, 'checkin_admin_button_close'), callback_data: `checkin_admin_close|${chatId}` }
                ]]
            }
        });
        scheduleMessageDeletion(adminId, message.message_id, 15000);
        return;
    }

    const inline_keyboard = locked.slice(0, 20).map((entry) => ([{
        text: t(lang, 'checkin_admin_reset_option', { user: entry.userId }),
        callback_data: `checkin_admin_reset_confirm|${chatId}|${entry.userId}`
    }]));

    inline_keyboard.push([
        { text: t(lang, 'checkin_admin_button_back'), callback_data: `checkin_admin_back|${chatId}` },
        { text: t(lang, 'checkin_admin_button_close'), callback_data: `checkin_admin_close|${chatId}` }
    ]);

    await bot.sendMessage(adminId, t(lang, 'checkin_admin_reset_prompt'), {
        reply_markup: { inline_keyboard }
    });
}

async function executeAdminRemoval(chatId, adminId, targetUserId) {
    const settings = await getGroupCheckinSettings(chatId);
    const today = formatDateForTimezone(settings.timezone || CHECKIN_DEFAULT_TIMEZONE);
    const success = await db.removeCheckinRecord(chatId, targetUserId, today);
    const adminLang = await resolveNotificationLanguage(adminId);
    if (!success) {
        await sendEphemeralMessage(adminId, t(adminLang, 'checkin_admin_remove_missing'));
        return;
    }

    await sendEphemeralMessage(adminId, t(adminLang, 'checkin_admin_remove_success', { user: targetUserId }));
    try {
        const userLang = await resolveNotificationLanguage(targetUserId);
        await bot.sendMessage(targetUserId, t(userLang, 'checkin_dm_removed'));
    } catch (error) {
        // ignore DM failures
    }

    await sendAdminMenu(adminId, chatId);
}

async function executeAdminUnlock(chatId, adminId, targetUserId) {
    const settings = await getGroupCheckinSettings(chatId);
    const today = formatDateForTimezone(settings.timezone || CHECKIN_DEFAULT_TIMEZONE);
    await db.unlockMemberCheckin(chatId, targetUserId);
    await db.clearDailyAttempts(chatId, targetUserId, today);
    const adminLang = await resolveNotificationLanguage(adminId);
    await sendEphemeralMessage(adminId, t(adminLang, 'checkin_admin_unlock_success', { user: targetUserId }));
    try {
        const userLang = await resolveNotificationLanguage(targetUserId);
        await bot.sendMessage(targetUserId, t(userLang, 'checkin_dm_unlocked'));
    } catch (error) {
        // ignore DM failures
    }

    await sendAdminMenu(adminId, chatId);
}

async function executeAdminReset(chatId, adminId, targetUserId) {
    const settings = await getGroupCheckinSettings(chatId);
    const today = formatDateForTimezone(settings.timezone || CHECKIN_DEFAULT_TIMEZONE);
    await db.unlockMemberCheckin(chatId, targetUserId);
    await db.clearDailyAttempts(chatId, targetUserId, today);
    const adminLang = await resolveNotificationLanguage(adminId);
    await sendEphemeralMessage(adminId, t(adminLang, 'checkin_admin_reset_sending', { user: targetUserId }), {}, 10000);
    try {
        const fakeUser = { id: Number(targetUserId), first_name: '' };
        const result = await initiateCheckinChallenge(chatId, fakeUser);
        if (result.status === 'sent') {
            await sendEphemeralMessage(adminId, t(adminLang, 'checkin_admin_reset_success'));
        } else if (result.status === 'failed') {
            await sendEphemeralMessage(adminId, t(adminLang, 'checkin_admin_reset_dm_failed'));
        } else if (result.status === 'locked') {
            await sendEphemeralMessage(adminId, t(adminLang, 'checkin_admin_reset_still_locked'));
        } else if (result.status === 'checked') {
            await sendEphemeralMessage(adminId, t(adminLang, 'checkin_admin_reset_already_checked'));
        }
    } catch (error) {
        console.error(`[Checkin] Unable to resend question for ${targetUserId}: ${error.message}`);
        await sendEphemeralMessage(adminId, t(adminLang, 'checkin_admin_reset_dm_failed'));
    }

    await sendAdminMenu(adminId, chatId);
}

async function setAdminDailyPoints(chatId, adminId, value) {
    const numeric = Number(value);
    const lang = await resolveNotificationLanguage(adminId);
    if (!Number.isFinite(numeric) || numeric < 0) {
        await sendEphemeralMessage(adminId, t(lang, 'checkin_admin_points_invalid'));
        return;
    }

    await db.updateCheckinGroup(chatId, { dailyPoints: numeric });
    await sendEphemeralMessage(adminId, t(lang, 'checkin_admin_points_updated', { value: numeric }));
    await sendAdminMenu(adminId, chatId);
}

async function setAdminSummaryWindow(chatId, adminId, value) {
    const numeric = Number(value);
    const lang = await resolveNotificationLanguage(adminId);
    if (!Number.isFinite(numeric) || numeric <= 0) {
        await sendEphemeralMessage(adminId, t(lang, 'checkin_admin_summary_invalid'));
        return;
    }

    await db.updateCheckinGroup(chatId, { summaryWindow: Math.round(numeric) });
    await sendEphemeralMessage(adminId, t(lang, 'checkin_admin_summary_updated', { value: Math.round(numeric) }));
    await sendAdminMenu(adminId, chatId);
}

// ===== HÀM HELPER: Dịch Lựa chọn (Kéo/Búa/Bao) =====
function getChoiceString(choice, lang) {
    const choiceNum = Number(choice);
    if (choiceNum === 1) return t(lang, 'choice_rock'); // "Búa ✊"
    if (choiceNum === 2) return t(lang, 'choice_paper'); // "Bao 🖐️"
    if (choiceNum === 3) return t(lang, 'choice_scissors'); // "Kéo ✌️"
    return t(lang, 'choice_none'); // "Chưa rõ"
}
// =======================================================

function applyChoiceTranslations(lang, variables) {
    const mapping = variables.__choiceTranslations;
    if (!Array.isArray(mapping)) {
        return;
    }

    for (const entry of mapping) {
        if (!entry || typeof entry !== 'object') {
            continue;
        }

        const { valueKey, targetKey } = entry;
        if (!valueKey || !targetKey || !(valueKey in variables)) {
            continue;
        }

        const rawValue = variables[valueKey];
        if (Array.isArray(rawValue)) {
            variables[targetKey] = rawValue.map((choiceValue) => getChoiceString(choiceValue, lang));
        } else {
            variables[targetKey] = getChoiceString(rawValue, lang);
        }
    }

    delete variables.__choiceTranslations;
}


async function resolveNotificationLanguage(chatId, fallbackLang) {
    try {
        if (chatId) {
            const info = await db.getUserLanguageInfo(chatId);
            if (info && info.lang) {
                return resolveLangCode(info.lang);
            }
        }
    } catch (error) {
        console.warn(`[Notify] Không thể đọc ngôn ngữ đã lưu cho ${chatId}: ${error.message}`);
    }

    return resolveLangCode(fallbackLang || defaultLang);
}

async function resolveGroupLanguage(chatId, fallbackLang) {
    if (!chatId) {
        return resolveLangCode(fallbackLang || defaultLang);
    }

    try {
        const subscription = await db.getGroupSubscription(chatId.toString());
        if (subscription && subscription.lang) {
            return resolveLangCode(subscription.lang);
        }
    } catch (error) {
        console.warn(`[Notify] Không thể đọc ngôn ngữ nhóm cho ${chatId}: ${error.message}`);
    }

    return resolveLangCode(fallbackLang || defaultLang);
}


function buildDrawNotificationMessage(lang, variables) {
    const lines = [
        t(lang, 'notify_game_draw_header', { roomId: variables.roomId }),
        '',
        t(lang, 'notify_game_draw_overview', {
            refundPercent: variables.refundPercent,
            feePercent: variables.feePercent
        }),
        '',
        t(lang, 'notify_game_draw_breakdown_title'),
        t(lang, 'notify_game_draw_breakdown_you', {
            refundAmount: variables.refundAmount,
            refundPercent: variables.refundPercent,
            stakeAmount: variables.stakeAmount
        }),
        t(lang, 'notify_game_draw_breakdown_fee', {
            feeAmount: variables.feeAmount,
            feePercent: variables.feePercent
        }),
        '',
        t(lang, 'notify_game_draw_reason', { choice: variables.choice })
    ];

    return lines.join('\n');
}

function buildWinNotificationMessage(lang, variables) {
    const lines = [
        t(lang, 'notify_game_win_header', { roomId: variables.roomId }),
        '',
        t(lang, 'notify_game_win_breakdown_title'),
        t(lang, 'notify_game_win_breakdown_you', {
            payout: variables.payout,
            winnerPercent: variables.winnerPercent,
            totalPot: variables.totalPot
        }),
        t(lang, 'notify_game_win_breakdown_opponent', {
            opponentLoss: variables.opponentLoss,
            opponentLossPercent: variables.opponentLossPercent
        }),
        t(lang, 'notify_game_win_breakdown_fee', {
            feeAmount: variables.feeAmount,
            feePercent: variables.feePercent
        }),
        '',
        t(lang, 'notify_game_win_reason', {
            myChoice: variables.myChoice,
            opponentChoice: variables.opponentChoice
        })
    ];

    return lines.join('\n');
}

function buildLoseNotificationMessage(lang, variables) {
    const lines = [
        t(lang, 'notify_game_lose_header', { roomId: variables.roomId }),
        '',
        t(lang, 'notify_game_lose_breakdown_title'),
        t(lang, 'notify_game_lose_breakdown_you', {
            lostAmount: variables.lostAmount,
            lostPercent: variables.lostPercent
        }),
        t(lang, 'notify_game_lose_breakdown_opponent', {
            opponentPayout: variables.opponentPayout,
            opponentPayoutPercent: variables.opponentPayoutPercent,
            totalPot: variables.totalPot
        }),
        t(lang, 'notify_game_lose_breakdown_fee', {
            feeAmount: variables.feeAmount,
            feePercent: variables.feePercent
        }),
        '',
        t(lang, 'notify_game_lose_reason', {
            myChoice: variables.myChoice,
            opponentChoice: variables.opponentChoice
        })
    ];

    return lines.join('\n');
}

function buildForfeitWinNotificationMessage(lang, variables) {
    const lines = [
        t(lang, 'notify_forfeit_win_header', { roomId: variables.roomId }),
        '',
        t(lang, 'notify_forfeit_win_overview'),
        '',
        t(lang, 'notify_forfeit_win_breakdown_title'),
        t(lang, 'notify_forfeit_win_breakdown_you', {
            payoutAmount: variables.payoutAmount,
            winnerPercent: variables.winnerPercent,
            totalPot: variables.totalPot
        }),
        t(lang, 'notify_forfeit_win_breakdown_opponent', {
            opponentLossAmount: variables.opponentLossAmount,
            opponentLossPercent: variables.opponentLossPercent
        }),
        t(lang, 'notify_forfeit_win_breakdown_community', {
            communityAmount: variables.communityAmount,
            communityPercent: variables.communityPercent
        }),
        t(lang, 'notify_forfeit_win_breakdown_burn', {
            burnAmount: variables.burnAmount,
            burnPercent: variables.burnPercent
        }),
        '',
        t(lang, 'notify_forfeit_win_reason', { loser: variables.loser })
    ];

    return lines.join('\n');
}

function buildForfeitLoseNotificationMessage(lang, variables) {
    const lines = [
        t(lang, 'notify_forfeit_lose_header', { roomId: variables.roomId }),
        '',
        t(lang, 'notify_forfeit_lose_overview'),
        '',
        t(lang, 'notify_forfeit_lose_breakdown_title'),
        t(lang, 'notify_forfeit_lose_breakdown_you', {
            lostAmount: variables.lostAmount,
            lostPercent: variables.lostPercent
        }),
        t(lang, 'notify_forfeit_lose_breakdown_opponent', {
            opponentPayout: variables.opponentPayout,
            winnerPercent: variables.winnerPercent,
            totalPot: variables.totalPot
        }),
        t(lang, 'notify_forfeit_lose_breakdown_community', {
            communityAmount: variables.communityAmount,
            communityPercent: variables.communityPercent
        }),
        t(lang, 'notify_forfeit_lose_breakdown_burn', {
            burnAmount: variables.burnAmount,
            burnPercent: variables.burnPercent
        }),
        '',
        t(lang, 'notify_forfeit_lose_reason', { winner: variables.winner })
    ];

    return lines.join('\n');
}


function shortAddress(address) {
    if (!address) return '-';
    try {
        const normalized = ethers.getAddress(address);
        return `${normalized.substring(0, 6)}…${normalized.substring(normalized.length - 4)}`;
    } catch (error) {
        return address.substring(0, 6) + '…';
    }
}

function formatBanmao(amount) {
    const num = Number(amount);
    if (!Number.isFinite(num)) {
        if (typeof amount === 'string' && amount.trim() !== '') {
            return amount;
        }
        return '0.00';
    }
    return num.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function toBigIntSafe(value) {
    if (value === null || value === undefined) {
        return null;
    }

    if (typeof value === 'bigint') {
        return value;
    }

    try {
        if (typeof value === 'number') {
            if (!Number.isFinite(value)) {
                return null;
            }
            return BigInt(Math.trunc(value));
        }

        if (typeof value === 'string') {
            const trimmed = value.trim();
            if (!trimmed) {
                return null;
            }
            return BigInt(trimmed);
        }

        if (value && typeof value.toString === 'function') {
            const asString = value.toString();
            if (asString) {
                return BigInt(asString);
            }
        }
    } catch (error) {
        return null;
    }

    return null;
}

function formatBanmaoFromWei(weiValue) {
    const bigIntValue = toBigIntSafe(weiValue);
    if (bigIntValue === null) {
        return '0.00';
    }

    try {
        const etherString = ethers.formatEther(bigIntValue);
        const numeric = Number(etherString);
        if (Number.isFinite(numeric)) {
            return formatBanmao(numeric);
        }
        return etherString;
    } catch (error) {
        return '0.00';
    }
}

function formatUsdPrice(amount) {
    const numeric = Number(amount);
    if (!Number.isFinite(numeric)) {
        return '0.0000';
    }

    let minimumFractionDigits = 2;
    let maximumFractionDigits = 2;

    if (numeric < 1 && numeric >= 0.01) {
        minimumFractionDigits = 4;
        maximumFractionDigits = 4;
    } else if (numeric < 0.01 && numeric >= 0.0001) {
        minimumFractionDigits = 6;
        maximumFractionDigits = 6;
    } else if (numeric < 0.0001) {
        minimumFractionDigits = 8;
        maximumFractionDigits = 8;
    }

    return numeric.toLocaleString('en-US', {
        minimumFractionDigits,
        maximumFractionDigits
    });
}

function formatUsdCompact(amount) {
    const numeric = Number(amount);
    if (!Number.isFinite(numeric) || numeric === 0) {
        return '—';
    }

    try {
        return numeric.toLocaleString('en-US', {
            style: 'currency',
            currency: 'USD',
            notation: 'compact',
            maximumFractionDigits: 2
        });
    } catch (error) {
        const abs = Math.abs(numeric);
        if (abs >= 1e9) {
            return `$${(numeric / 1e9).toFixed(2)}B`;
        }
        if (abs >= 1e6) {
            return `$${(numeric / 1e6).toFixed(2)}M`;
        }
        if (abs >= 1e3) {
            return `$${(numeric / 1e3).toFixed(2)}K`;
        }
        return `$${numeric.toFixed(2)}`;
    }
}

function formatPercentage(value, options = {}) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
        return '0.00%';
    }

    const { minimumFractionDigits = 2, maximumFractionDigits = 2, includeSign = true } = options;
    const formatter = new Intl.NumberFormat('en-US', {
        minimumFractionDigits,
        maximumFractionDigits
    });

    const formatted = formatter.format(Math.abs(numeric));
    const sign = includeSign ? (numeric >= 0 ? '+' : '-') : '';
    return `${sign}${formatted}%`;
}

function normalizePercentageValue(value) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
        return null;
    }

    if (Math.abs(numeric) <= 1) {
        return numeric * 100;
    }

    return numeric;
}

function formatTokenQuantity(amount, options = {}) {
    const numeric = Number(amount);
    if (!Number.isFinite(numeric)) {
        return '—';
    }

    const { minimumFractionDigits = 2, maximumFractionDigits = 4 } = options;
    return numeric.toLocaleString('en-US', {
        minimumFractionDigits,
        maximumFractionDigits
    });
}

function formatTokenAmountFromUnits(amount, decimals, options = {}) {
    const bigIntValue = parseBigIntValue(amount);
    if (bigIntValue === null) {
        return null;
    }

    const digits = Number.isFinite(decimals) ? Math.max(0, Math.trunc(decimals)) : 0;

    try {
        const formatted = ethers.formatUnits(bigIntValue, digits);
        const numeric = Number(formatted);
        if (Number.isFinite(numeric)) {
            const minimumFractionDigits = Number.isFinite(options.minimumFractionDigits)
                ? options.minimumFractionDigits
                : (Math.abs(numeric) < 1 ? 6 : 2);
            const maximumFractionDigits = Number.isFinite(options.maximumFractionDigits)
                ? options.maximumFractionDigits
                : Math.max(minimumFractionDigits, Math.abs(numeric) < 1 ? 8 : 6);

            return numeric.toLocaleString('en-US', {
                minimumFractionDigits,
                maximumFractionDigits
            });
        }

        return formatted;
    } catch (error) {
        return null;
    }
}

function formatTimestampRange(startMs, endMs) {
    const start = startMs ? new Date(startMs) : null;
    const end = endMs ? new Date(endMs) : null;

    const format = (date) => {
        if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
            return '—';
        }
        return date.toISOString().replace('T', ' ').slice(0, 16);
    };

    return { start: format(start), end: format(end) };
}

function formatRelativeTime(timestampMs) {
    if (!Number.isFinite(Number(timestampMs))) {
        return null;
    }

    const now = Date.now();
    const diffMs = now - Number(timestampMs);
    if (!Number.isFinite(diffMs)) {
        return null;
    }

    const diffSeconds = Math.max(Math.round(diffMs / 1000), 0);
    if (diffSeconds < 60) {
        return `${diffSeconds}s`;
    }

    const diffMinutes = Math.floor(diffSeconds / 60);
    if (diffMinutes < 60) {
        return `${diffMinutes}m`;
    }

    const diffHours = Math.floor(diffMinutes / 60);
    if (diffHours < 48) {
        return `${diffHours}h`;
    }

    const diffDays = Math.floor(diffHours / 24);
    if (diffDays < 30) {
        return `${diffDays}d`;
    }

    const diffMonths = Math.floor(diffDays / 30);
    if (diffMonths < 12) {
        return `${diffMonths}mo`;
    }

    const diffYears = Math.floor(diffMonths / 12);
    return `${diffYears}y`;
}

function renderSparkline(values) {
    if (!Array.isArray(values) || values.length === 0) {
        return null;
    }

    const numericValues = values
        .map((value) => Number(value))
        .filter((value) => Number.isFinite(value));

    if (numericValues.length === 0) {
        return null;
    }

    const min = Math.min(...numericValues);
    const max = Math.max(...numericValues);

    if (min === max) {
        return '▅'.repeat(numericValues.length);
    }

    const blocks = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
    const scale = (value) => {
        const normalized = (value - min) / (max - min);
        const index = Math.min(blocks.length - 1, Math.max(0, Math.round(normalized * (blocks.length - 1))));
        return blocks[index];
    };

    return numericValues.map((value) => scale(value)).join('');
}

async function fetchBanmaoQuoteSnapshot(options = {}) {
    if (!OKX_BANMAO_TOKEN_ADDRESS || !OKX_QUOTE_TOKEN_ADDRESS) {
        throw new Error('Missing OKX token addresses');
    }

    const { chainName, slippagePercent = '0.5', amount: amountOverride } = options;
    const query = await buildOkxDexQuery(chainName, { includeToken: false, includeQuote: false });
    const context = await resolveOkxChainContext(chainName);

    const chainIndex = Number.isFinite(query.chainIndex)
        ? Number(query.chainIndex)
        : (Number.isFinite(context?.chainIndex) ? Number(context.chainIndex) : null);

    if (!Number.isFinite(chainIndex)) {
        throw new Error('Unable to resolve OKX chain index');
    }

    const amount = amountOverride || await resolveBanmaoQuoteAmount(chainName);
    const requestQuery = {
        chainIndex,
        fromTokenAddress: OKX_BANMAO_TOKEN_ADDRESS,
        toTokenAddress: OKX_QUOTE_TOKEN_ADDRESS,
        amount,
        swapMode: 'exactIn',
        slippagePercent
    };

    const payload = await okxJsonRequest('GET', '/api/v6/dex/aggregator/quote', {
        query: requestQuery
    });

    const quoteEntries = unwrapOkxData(payload);
    const quoteEntry = selectOkxQuoteByLiquidity(quoteEntries) || unwrapOkxFirst(payload);
    if (!quoteEntry) {
        return null;
    }

    const priceInfo = extractOkxQuotePrice(quoteEntry, { requestAmount: amount });
    if (!Number.isFinite(priceInfo.price) || priceInfo.price <= 0) {
        return null;
    }

    const chainLabel = context?.chainName || context?.chainShortName || query.chainShortName || chainName || '(default)';
    const okbUsd = resolveOkbUsdPrice(priceInfo.tokenUnitPrices);
    const priceOkb = Number.isFinite(priceInfo.price) && Number.isFinite(okbUsd) && okbUsd > 0
        ? priceInfo.price / okbUsd
        : null;

    const extractSymbol = (token, fallback) => {
        if (!token || typeof token !== 'object') {
            return fallback;
        }

        const candidate = typeof token.tokenSymbol === 'string'
            ? token.tokenSymbol
            : (typeof token.symbol === 'string' ? token.symbol : null);

        if (candidate && candidate.trim()) {
            return candidate.trim().toUpperCase();
        }

        return fallback;
    };

    const routerList = Array.isArray(quoteEntry.dexRouterList) ? quoteEntry.dexRouterList : [];
    const firstRoute = routerList[0] || null;
    const lastRoute = routerList.length > 0 ? routerList[routerList.length - 1] : null;

    const fromSymbol = extractSymbol(quoteEntry.fromToken, extractSymbol(firstRoute?.fromToken, 'BANMAO'));
    const toSymbol = extractSymbol(quoteEntry.toToken, extractSymbol(lastRoute?.toToken, 'USDT'));

    const tradeFeeUsd = normalizeNumeric(quoteEntry.tradeFee);
    const priceImpactPercent = normalizeNumeric(quoteEntry.priceImpactPercent);
    const routeLabel = summarizeOkxQuoteRoute(quoteEntry);

    return {
        price: priceInfo.price,
        priceOkb: Number.isFinite(priceOkb) ? priceOkb : null,
        okbUsd: Number.isFinite(okbUsd) ? okbUsd : null,
        chain: chainLabel,
        chainIndex,
        source: 'OKX DEX quote',
        amount,
        decimals: priceInfo.fromDecimals,
        quoteDecimals: priceInfo.toDecimals,
        fromAmount: priceInfo.fromAmount,
        toAmount: priceInfo.toAmount,
        fromSymbol,
        toSymbol,
        tradeFeeUsd: Number.isFinite(tradeFeeUsd) ? tradeFeeUsd : null,
        priceImpactPercent: Number.isFinite(priceImpactPercent) ? priceImpactPercent : null,
        routeLabel,
        tokenPrices: priceInfo.tokenUnitPrices,
        derivedPrice: priceInfo.amountPrice,
        raw: quoteEntry
    };
}

async function resolveBanmaoQuoteAmount(chainName) {
    const decimals = await getBanmaoTokenDecimals(chainName);
    const safeDecimals = Number.isFinite(decimals) ? Math.max(0, Math.min(36, Math.trunc(decimals))) : BANMAO_DECIMALS_DEFAULT;

    try {
        return (BigInt(10) ** BigInt(safeDecimals)).toString();
    } catch (error) {
        // Fallback to 1 * 10^18 if exponentiation fails for any reason
        return '1000000000000000000';
    }
}

async function getBanmaoTokenDecimals(chainName) {
    const now = Date.now();
    if (banmaoDecimalsCache !== null && banmaoDecimalsFetchedAt > 0 && now - banmaoDecimalsFetchedAt < BANMAO_DECIMALS_CACHE_TTL) {
        return banmaoDecimalsCache;
    }

    try {
        const profile = await fetchBanmaoTokenProfile({ chainName });
        const decimals = pickOkxNumeric(profile || {}, ['decimals', 'tokenDecimals', 'tokenDecimal', 'decimal']);
        if (Number.isFinite(decimals)) {
            banmaoDecimalsCache = Math.max(0, Math.trunc(decimals));
            banmaoDecimalsFetchedAt = now;
            return banmaoDecimalsCache;
        }
    } catch (error) {
        console.warn(`[BanmaoDecimals] Failed to load token profile: ${error.message}`);
    }

    try {
        const directory = await fetchOkxTokenDirectory(chainName);
        const match = directory?.byAddressLower?.get(BANMAO_ADDRESS_LOWER);
        if (match && Number.isFinite(match.decimals)) {
            banmaoDecimalsCache = Math.max(0, Math.trunc(match.decimals));
            banmaoDecimalsFetchedAt = now;
            return banmaoDecimalsCache;
        }
    } catch (error) {
        console.warn(`[BanmaoDecimals] Failed to load token directory: ${error.message}`);
    }

    return banmaoDecimalsCache !== null ? banmaoDecimalsCache : BANMAO_DECIMALS_DEFAULT;
}

async function resolveTokenDecimals(tokenAddress, options = {}) {
    const { chainName, chainIndex, fallback = null } = options;

    if (!tokenAddress || typeof tokenAddress !== 'string') {
        return fallback;
    }

    const normalized = normalizeOkxConfigAddress(tokenAddress);
    const addressText = normalized || tokenAddress;
    const lower = addressText.toLowerCase();

    if (BANMAO_ADDRESS_LOWER && lower === BANMAO_ADDRESS_LOWER) {
        return getBanmaoTokenDecimals(chainName);
    }

    if (OKX_QUOTE_ADDRESS_LOWER && lower === OKX_QUOTE_ADDRESS_LOWER) {
        return 6;
    }

    if (OKX_OKB_TOKEN_ADDRESSES.includes(lower)) {
        return 18;
    }

    const cached = tokenDecimalsCache.get(lower);
    const now = Date.now();
    if (cached && cached.expiresAt > now) {
        return cached.value;
    }

    try {
        const profile = await fetchBanmaoTokenProfile({ chainName, tokenAddress: addressText });
        const decimals = pickOkxNumeric(profile || {}, ['decimals', 'tokenDecimals', 'tokenDecimal', 'decimal']);
        if (Number.isFinite(decimals)) {
            tokenDecimalsCache.set(lower, { value: Math.max(0, Math.trunc(decimals)), expiresAt: now + BANMAO_DECIMALS_CACHE_TTL });
            return Math.max(0, Math.trunc(decimals));
        }
    } catch (error) {
        console.warn(`[TokenDecimals] Failed to resolve decimals for ${tokenAddress}: ${error.message}`);
    }

    try {
        const directory = await fetchOkxTokenDirectory(chainName, { chainIndex });
        const match = directory?.byAddressLower?.get(lower);
        if (match && Number.isFinite(match.decimals)) {
            const normalizedDecimals = Math.max(0, Math.trunc(match.decimals));
            tokenDecimalsCache.set(lower, { value: normalizedDecimals, expiresAt: now + BANMAO_DECIMALS_CACHE_TTL });
            return normalizedDecimals;
        }
    } catch (error) {
        console.warn(`[TokenDecimals] Failed to query directory for ${tokenAddress}: ${error.message}`);
    }

    tokenDecimalsCache.set(lower, { value: fallback, expiresAt: now + (BANMAO_DECIMALS_CACHE_TTL / 2) });
    return fallback;
}

function parseBigIntValue(value) {
    if (value === null || value === undefined) {
        return null;
    }

    if (typeof value === 'bigint') {
        return value;
    }

    if (typeof value === 'number') {
        if (!Number.isFinite(value)) {
            return null;
        }
        return BigInt(Math.trunc(value));
    }

    if (typeof value === 'string') {
        const cleaned = value.replace(/[,_\s]/g, '');
        if (!cleaned) {
            return null;
        }

        if (/^-?\d+$/.test(cleaned)) {
            try {
                return BigInt(cleaned);
            } catch (error) {
                return null;
            }
        }
    }

    return null;
}

function extractOkxTokenUnitPrice(token) {
    if (!token || typeof token !== 'object') {
        return null;
    }

    const keys = ['tokenUnitPrice', 'unitPrice', 'priceUsd', 'usdPrice', 'price'];
    for (const key of keys) {
        if (!Object.prototype.hasOwnProperty.call(token, key)) {
            continue;
        }

        const numeric = normalizeNumeric(token[key]);
        if (Number.isFinite(numeric)) {
            return numeric;
        }
    }

    return null;
}

function normalizeOkxTokenAddress(value) {
    if (!value || typeof value !== 'string') {
        return null;
    }

    const normalized = normalizeOkxConfigAddress(value);
    if (normalized) {
        return normalized.toLowerCase();
    }

    const trimmed = value.trim();
    return trimmed ? trimmed.toLowerCase() : null;
}

function collectOkxTokenUnitPrices(entry, routerList = []) {
    const tokens = [];
    const byAddress = new Map();
    const bySymbol = new Map();

    const register = (token, meta = {}) => {
        if (!token || typeof token !== 'object') {
            return;
        }

        const unitPrice = extractOkxTokenUnitPrice(token);
        if (!Number.isFinite(unitPrice)) {
            return;
        }

        const symbolRaw = typeof token.tokenSymbol === 'string'
            ? token.tokenSymbol
            : (typeof token.symbol === 'string' ? token.symbol : null);
        const symbol = symbolRaw ? symbolRaw.trim() : null;

        const addressCandidates = [
            token.tokenContractAddress,
            token.tokenAddress,
            token.contractAddress,
            token.address,
            token.contract,
            token.mintAddress
        ];

        let normalizedAddress = null;
        for (const candidate of addressCandidates) {
            const normalized = normalizeOkxTokenAddress(candidate);
            if (normalized) {
                normalizedAddress = normalized;
                break;
            }
        }

        const record = {
            unitPrice,
            symbol,
            address: normalizedAddress,
            meta,
            raw: token
        };

        tokens.push(record);

        if (normalizedAddress && !byAddress.has(normalizedAddress)) {
            byAddress.set(normalizedAddress, record);
        }

        if (symbol) {
            const symbolKey = symbol.toLowerCase();
            if (!bySymbol.has(symbolKey)) {
                bySymbol.set(symbolKey, record);
            }
        }
    };

    register(entry?.fromToken, { source: 'fromToken' });
    register(entry?.toToken, { source: 'toToken' });
    register(entry?.sellToken, { source: 'sellToken' });
    register(entry?.buyToken, { source: 'buyToken' });

    routerList.forEach((route, index) => {
        register(route?.fromToken, { source: 'router', hop: index, side: 'from' });
        register(route?.toToken, { source: 'router', hop: index, side: 'to' });
    });

    const fromTokenEntry = BANMAO_ADDRESS_LOWER && byAddress.has(BANMAO_ADDRESS_LOWER)
        ? byAddress.get(BANMAO_ADDRESS_LOWER)
        : null;
    const quoteTokenEntry = OKX_QUOTE_ADDRESS_LOWER && byAddress.has(OKX_QUOTE_ADDRESS_LOWER)
        ? byAddress.get(OKX_QUOTE_ADDRESS_LOWER)
        : null;

    return {
        list: tokens,
        byAddress,
        bySymbol,
        fromTokenUsd: fromTokenEntry && Number.isFinite(fromTokenEntry.unitPrice)
            ? fromTokenEntry.unitPrice
            : null,
        quoteTokenUsd: quoteTokenEntry && Number.isFinite(quoteTokenEntry.unitPrice)
            ? quoteTokenEntry.unitPrice
            : null
    };
}

function summarizeOkxQuoteRoute(entry) {
    const list = Array.isArray(entry?.dexRouterList) ? entry.dexRouterList : [];
    if (list.length === 0) {
        return null;
    }

    const seen = new Set();
    const names = [];

    const normalizeName = (value) => {
        if (!value || typeof value !== 'string') {
            return null;
        }

        const trimmed = value.trim();
        return trimmed ? trimmed : null;
    };

    const extractDexName = (hop) => {
        const nameCandidates = [
            hop?.dexProtocol?.dexName,
            hop?.dexProtocol?.name,
            hop?.dexName
        ];

        for (const candidate of nameCandidates) {
            const normalized = normalizeName(candidate);
            if (normalized) {
                return normalized;
            }
        }

        return null;
    };

    const extractTokenAddress = (token) => {
        if (!token || typeof token !== 'object') {
            return null;
        }

        const candidates = [
            token.tokenContractAddress,
            token.tokenAddress,
            token.contractAddress,
            token.address
        ];

        for (const candidate of candidates) {
            if (typeof candidate === 'string' && candidate) {
                return candidate.trim().toLowerCase();
            }
        }

        return null;
    };

    const pushName = (name) => {
        if (!name) {
            return;
        }

        const key = name.toLowerCase();
        if (seen.has(key)) {
            return;
        }

        seen.add(key);
        names.push(name);
    };

    // Prioritize the hop that handles BANMAO so the main route highlights DYOR Swap.
    if (BANMAO_ADDRESS_LOWER) {
        for (const hop of list) {
            const fromAddress = extractTokenAddress(hop?.fromToken);
            if (fromAddress && fromAddress === BANMAO_ADDRESS_LOWER) {
                pushName(extractDexName(hop));
                break;
            }
        }
    }

    for (const hop of list) {
        pushName(extractDexName(hop));
    }

    if (names.length === 0) {
        return null;
    }

    return names.join(' → ');
}

function resolveOkbUsdPrice(tokenPrices) {
    if (!tokenPrices) {
        return null;
    }

    const { byAddress, bySymbol, list } = tokenPrices;

    if (byAddress instanceof Map) {
        for (const address of OKX_OKB_TOKEN_ADDRESSES) {
            if (!address) {
                continue;
            }

            const entry = byAddress.get(address);
            if (entry && Number.isFinite(entry.unitPrice)) {
                return entry.unitPrice;
            }
        }
    }

    if (bySymbol instanceof Map) {
        for (const key of OKX_OKB_SYMBOL_KEYS) {
            if (!key) {
                continue;
            }

            const entry = bySymbol.get(key);
            if (entry && Number.isFinite(entry.unitPrice)) {
                return entry.unitPrice;
            }
        }
    }

    if (Array.isArray(list)) {
        for (const entry of list) {
            if (!entry || !Number.isFinite(entry.unitPrice)) {
                continue;
            }

            const symbol = typeof entry.symbol === 'string' ? entry.symbol.toUpperCase() : '';
            if (symbol.includes('OKB')) {
                return entry.unitPrice;
            }
        }
    }

    return null;
}

function selectOkxQuoteByLiquidity(quotes) {
    if (!Array.isArray(quotes) || quotes.length === 0) {
        return null;
    }

    let bestEntry = null;
    let bestScore = Number.NEGATIVE_INFINITY;

    for (const entry of quotes) {
        if (!entry || typeof entry !== 'object') {
            continue;
        }

        const score = computeOkxQuoteLiquidityScore(entry);
        if (Number.isFinite(score)) {
            if (!Number.isFinite(bestScore) || score > bestScore) {
                bestScore = score;
                bestEntry = entry;
            }
        } else if (bestEntry === null) {
            bestEntry = entry;
        }
    }

    return bestEntry;
}

function computeOkxQuoteLiquidityScore(entry) {
    const routerList = Array.isArray(entry?.dexRouterList) ? entry.dexRouterList : [];
    let bestLiquidity = null;

    for (const hop of routerList) {
        const hopLiquidity = pickOkxNumeric(hop, [
            'liquidityUsd',
            'usdLiquidity',
            'poolLiquidity',
            'liquidity',
            'reserveUsd',
            'valueUsd'
        ]);

        if (Number.isFinite(hopLiquidity)) {
            bestLiquidity = Number.isFinite(bestLiquidity)
                ? Math.max(bestLiquidity, hopLiquidity)
                : hopLiquidity;
        }
    }

    if (Number.isFinite(bestLiquidity)) {
        return bestLiquidity;
    }

    const decimalsCandidates = [
        pickOkxNumeric(entry, ['toTokenDecimals', 'buyTokenDecimals', 'toDecimals', 'toTokenDecimal']),
        pickOkxNumeric(entry?.toToken, ['decimal', 'decimals', 'tokenDecimals', 'tokenDecimal']),
        pickOkxNumeric(entry?.buyToken, ['decimal', 'decimals', 'tokenDecimals', 'tokenDecimal']),
        pickOkxNumeric(routerList.length > 0 ? routerList[routerList.length - 1]?.toToken : null, ['decimal', 'decimals', 'tokenDecimals', 'tokenDecimal'])
    ];

    const toDecimals = normalizeDecimalsCandidate(decimalsCandidates);
    const toAmount = parseBigIntValue(
        entry?.toTokenAmount
        ?? entry?.buyTokenAmount
        ?? entry?.toAmount
        ?? entry?.outputAmount
    );

    if (toAmount === null) {
        return null;
    }

    const decimals = Number.isFinite(toDecimals) ? Math.max(0, Math.trunc(toDecimals)) : 0;
    let quantity = null;

    try {
        quantity = Number(ethers.formatUnits(toAmount, decimals));
    } catch (error) {
        quantity = null;
    }

    if (!Number.isFinite(quantity)) {
        const numeric = Number(toAmount);
        if (Number.isFinite(numeric)) {
            quantity = numeric / Math.pow(10, decimals);
        }
    }

    if (!Number.isFinite(quantity) || quantity <= 0) {
        return null;
    }

    const tokenPrices = collectOkxTokenUnitPrices(entry, routerList);
    const quoteUsd = Number.isFinite(tokenPrices?.quoteTokenUsd) && tokenPrices.quoteTokenUsd > 0
        ? tokenPrices.quoteTokenUsd
        : 1;

    return quantity * quoteUsd;
}

function extractOkxQuotePrice(entry, options = {}) {
    if (!entry || typeof entry !== 'object') {
        return {
            price: null,
            fromDecimals: null,
            toDecimals: null,
            fromAmount: null,
            toAmount: null,
            tokenUnitPrices: null,
            quotePrice: null,
            amountPrice: null
        };
    }

    const directPrice = extractOkxPriceValue(entry);
    const routerList = Array.isArray(entry.dexRouterList) ? entry.dexRouterList : [];
    const firstRoute = routerList[0] || null;
    const lastRoute = routerList.length > 0 ? routerList[routerList.length - 1] : null;

    const fromDecimalsCandidates = [
        pickOkxNumeric(entry, ['fromTokenDecimals', 'sellTokenDecimals', 'fromDecimals', 'fromTokenDecimal']),
        pickOkxNumeric(entry.fromToken, ['decimal', 'decimals', 'tokenDecimals', 'tokenDecimal']),
        pickOkxNumeric(entry.sellToken, ['decimal', 'decimals', 'tokenDecimals', 'tokenDecimal']),
        pickOkxNumeric(firstRoute?.fromToken, ['decimal', 'decimals', 'tokenDecimals', 'tokenDecimal'])
    ];

    const toDecimalsCandidates = [
        pickOkxNumeric(entry, ['toTokenDecimals', 'buyTokenDecimals', 'toDecimals', 'toTokenDecimal']),
        pickOkxNumeric(entry.toToken, ['decimal', 'decimals', 'tokenDecimals', 'tokenDecimal']),
        pickOkxNumeric(entry.buyToken, ['decimal', 'decimals', 'tokenDecimals', 'tokenDecimal']),
        pickOkxNumeric(lastRoute?.toToken, ['decimal', 'decimals', 'tokenDecimals', 'tokenDecimal'])
    ];

    const fromDecimals = normalizeDecimalsCandidate(fromDecimalsCandidates);
    const toDecimals = normalizeDecimalsCandidate(toDecimalsCandidates);

    const tokenPrices = collectOkxTokenUnitPrices(entry, routerList);

    const fromAmount = parseBigIntValue(
        entry.fromTokenAmount
        ?? entry.sellTokenAmount
        ?? entry.fromAmount
        ?? entry.inputAmount
        ?? options.requestAmount
    );

    const toAmount = parseBigIntValue(
        entry.toTokenAmount
        ?? entry.buyTokenAmount
        ?? entry.toAmount
        ?? entry.outputAmount
    );

    const priceFromAmounts = (fromAmount !== null && toAmount !== null)
        ? computePriceFromTokenAmounts(fromAmount, toAmount, fromDecimals, toDecimals)
        : null;

    let price = tokenPrices && Number.isFinite(tokenPrices.fromTokenUsd)
        ? tokenPrices.fromTokenUsd
        : null;

    if (!Number.isFinite(price) && Number.isFinite(directPrice)) {
        price = Number(directPrice);
    }

    if (!Number.isFinite(price) && Number.isFinite(priceFromAmounts)) {
        price = priceFromAmounts;
    }

    const toAmountUsd = pickOkxNumeric(entry, ['toAmountUsd', 'toUsdAmount', 'toAmountInUsd', 'usdAmount']);
    if (!Number.isFinite(price) && Number.isFinite(toAmountUsd) && fromAmount !== null) {
        const decimals = Number.isFinite(fromDecimals) ? fromDecimals : 0;
        const fromNumeric = Number(fromAmount);
        if (Number.isFinite(fromNumeric) && fromNumeric > 0) {
            const scale = Math.pow(10, decimals);
            price = (toAmountUsd / fromNumeric) * scale;
        }
    }

    if (!Number.isFinite(price)) {
        price = null;
    }

    return {
        price,
        fromDecimals,
        toDecimals,
        fromAmount,
        toAmount,
        tokenUnitPrices: tokenPrices,
        quotePrice: Number.isFinite(directPrice) ? Number(directPrice) : null,
        amountPrice: Number.isFinite(priceFromAmounts) ? priceFromAmounts : null
    };
}

function normalizeDecimalsCandidate(candidates) {
    if (!Array.isArray(candidates)) {
        return null;
    }

    for (const candidate of candidates) {
        const numeric = normalizeNumeric(candidate);
        if (Number.isFinite(numeric)) {
            return Math.max(0, Math.trunc(numeric));
        }
    }

    return null;
}

function computePriceFromTokenAmounts(fromAmount, toAmount, fromDecimals, toDecimals) {
    if (fromAmount === null || toAmount === null) {
        return null;
    }

    const hasFromDecimals = Number.isFinite(fromDecimals);
    const hasToDecimals = Number.isFinite(toDecimals);
    const fromDigits = hasFromDecimals ? Math.max(0, Math.trunc(fromDecimals)) : 0;
    const toDigits = hasToDecimals ? Math.max(0, Math.trunc(toDecimals)) : 0;

    try {
        const numerator = toAmount * (BigInt(10) ** BigInt(fromDigits));
        const denominator = fromAmount * (BigInt(10) ** BigInt(toDigits));
        if (denominator === 0n) {
            return null;
        }

        const ratio = Number(numerator) / Number(denominator);
        if (Number.isFinite(ratio)) {
            return ratio;
        }
    } catch (error) {
        // Fallback to floating point math below
    }

    const fromNumeric = Number(fromAmount);
    const toNumeric = Number(toAmount);
    if (Number.isFinite(fromNumeric) && fromNumeric > 0 && Number.isFinite(toNumeric)) {
        let ratio = toNumeric / fromNumeric;
        if (hasFromDecimals || hasToDecimals) {
            const decimalsDiff = fromDigits - toDigits;
            if (decimalsDiff !== 0) {
                ratio *= Math.pow(10, decimalsDiff);
            }
        }
        return Number.isFinite(ratio) ? ratio : null;
    }

    return null;
}

async function fetchBanmaoPrice() {
    const errors = [];

    try {
        const quoteSnapshot = await fetchBanmaoQuoteSnapshot();
        if (quoteSnapshot && Number.isFinite(quoteSnapshot.price)) {
            return quoteSnapshot;
        }
    } catch (error) {
        console.warn(`[BanmaoPrice] Quote snapshot failed: ${error.message}`);
        errors.push(error);
    }

    try {
        const snapshot = await fetchBanmaoMarketSnapshot();
        if (snapshot && Number.isFinite(snapshot.price)) {
            return snapshot;
        }
    } catch (error) {
        console.warn(`[BanmaoPrice] Market snapshot failed: ${error.message}`);
        errors.push(error);
    }

    try {
        const fallbackTicker = await tryFetchOkxMarketTicker();
        if (fallbackTicker) {
            return fallbackTicker;
        }
    } catch (error) {
        console.warn(`[BanmaoPrice] Market ticker fallback failed: ${error.message}`);
        errors.push(error);
    }

    if (errors.length > 0) {
        throw errors[errors.length - 1];
    }

    throw new Error('No price data available');
}

async function fetchBanmaoMarketSnapshot() {
    const chainNames = getOkxChainShortNameCandidates();
    const errors = [];

    for (const chainName of chainNames) {
        try {
            const snapshot = await fetchBanmaoMarketSnapshotForChain(chainName);
            if (snapshot) {
                return snapshot;
            }
        } catch (error) {
            errors.push(error);
        }
    }

    try {
        const fallbackSnapshot = await fetchBanmaoMarketSnapshotForChain();
        if (fallbackSnapshot) {
            return fallbackSnapshot;
        }
    } catch (error) {
        errors.push(error);
    }

    if (errors.length > 0) {
        throw errors[errors.length - 1];
    }

    return null;
}

async function fetchBanmaoMarketSnapshotForChain(chainName) {
    const query = await buildOkxDexQuery(chainName);
    const chainLabel = query.chainShortName || chainName || '(default)';
    const errors = [];

    let priceInfoEntry = null;
    try {
        const payload = await okxJsonRequest('GET', '/api/v6/dex/market/price-info', { query });
        priceInfoEntry = unwrapOkxFirst(payload);
    } catch (error) {
        errors.push(new Error(`[price-info:${chainLabel}] ${error.message}`));
    }

    let priceEntry = priceInfoEntry;
    let source = 'OKX DEX price-info';

    if (!Number.isFinite(extractOkxPriceValue(priceEntry))) {
        try {
            const payload = await okxJsonRequest('GET', '/api/v6/dex/market/price', { query });
            priceEntry = unwrapOkxFirst(payload);
            source = 'OKX DEX price';
        } catch (error) {
            errors.push(new Error(`[price:${chainLabel}] ${error.message}`));
        }
    }

    if (!Number.isFinite(extractOkxPriceValue(priceEntry))) {
        try {
            const payload = await okxJsonRequest('GET', '/api/v6/dex/aggregator/tokenPrice', { query });
            priceEntry = unwrapOkxFirst(payload);
            source = 'OKX DEX tokenPrice';
        } catch (error) {
            errors.push(new Error(`[tokenPrice:${chainLabel}] ${error.message}`));
        }
    }

    const tokenPrices = collectOkxTokenUnitPrices(priceEntry || priceInfoEntry);

    let price = extractOkxPriceValue(priceEntry);
    if (!Number.isFinite(price) && tokenPrices && Number.isFinite(tokenPrices.fromTokenUsd)) {
        price = tokenPrices.fromTokenUsd;
    }

    if (!Number.isFinite(price)) {
        if (errors.length > 0) {
            throw errors[errors.length - 1];
        }
        return null;
    }

    const metricsSource = priceInfoEntry || priceEntry || {};
    const changeAbs = pickOkxNumeric(metricsSource, ['usdChange24h', 'change24h', 'priceChangeUsd', 'priceChange', 'usdChange']);
    const changePercent = pickOkxNumeric(metricsSource, ['changeRate', 'changePercent', 'priceChangePercent', 'percentChange24h', 'change24hPercent']);
    const volume = pickOkxNumeric(metricsSource, ['usdVolume24h', 'volumeUsd24h', 'volume24h', 'turnover24h', 'usdTurnover24h']);
    const liquidity = pickOkxNumeric(metricsSource, ['usdLiquidity', 'liquidityUsd', 'poolLiquidity', 'liquidity']);
    const marketCap = pickOkxNumeric(metricsSource, ['usdMarketCap', 'marketCap', 'fdvUsd', 'fullyDilutedMarketCap', 'marketCapUsd']);
    const supply = pickOkxNumeric(metricsSource, ['totalSupply', 'supply', 'circulatingSupply']);
    const okbUsd = resolveOkbUsdPrice(tokenPrices);
    const priceOkb = Number.isFinite(price) && Number.isFinite(okbUsd) && okbUsd > 0
        ? price / okbUsd
        : null;

    return {
        price,
        priceOkb: Number.isFinite(priceOkb) ? priceOkb : null,
        okbUsd: Number.isFinite(okbUsd) ? okbUsd : null,
        changeAbs,
        changePercent,
        volume,
        liquidity,
        marketCap,
        supply,
        chain: chainLabel,
        source,
        tokenPrices,
        raw: { priceEntry, priceInfoEntry }
    };
}

async function fetchBanmaoTokenProfile(options = {}) {
    const { chainName, tokenAddress } = options;
    const query = await buildOkxDexQuery(chainName, { includeToken: false });
    const normalizedAddress = tokenAddress
        ? normalizeOkxConfigAddress(tokenAddress) || tokenAddress
        : OKX_BANMAO_TOKEN_ADDRESS;

    if (normalizedAddress) {
        query.tokenAddress = normalizedAddress;
        query.tokenContractAddress = normalizedAddress;
    }

    const payload = await okxJsonRequest('GET', '/api/v6/dex/market/token/basic-info', { query });
    return unwrapOkxFirst(payload);
}

async function fetchOkxTokenDirectory(chainName, options = {}) {
    const { chainIndex } = options;
    const query = await buildOkxDexQuery(chainName, {
        includeToken: false,
        includeQuote: false,
        explicitChainIndex: chainIndex
    });

    const cacheKey = `${query.chainIndex || 'na'}|${(query.chainShortName || '').toLowerCase()}`;
    const now = Date.now();
    const cached = okxTokenDirectoryCache.get(cacheKey);
    if (cached && cached.expiresAt > now) {
        return cached.value;
    }

    const payload = await okxJsonRequest('GET', '/api/v6/dex/aggregator/all-tokens', { query });
    const rows = unwrapOkxData(payload);

    const tokens = [];
    const byAddressLower = new Map();

    if (Array.isArray(rows)) {
        for (const row of rows) {
            const token = normalizeOkxTokenDirectoryToken(row);
            if (!token) {
                continue;
            }

            tokens.push(token);
            if (!byAddressLower.has(token.addressLower)) {
                byAddressLower.set(token.addressLower, token);
            }
        }
    }

    const directory = {
        tokens,
        byAddressLower,
        chainIndex: query.chainIndex ?? null,
        chainShortName: query.chainShortName || null
    };

    okxTokenDirectoryCache.set(cacheKey, {
        value: directory,
        expiresAt: now + OKX_TOKEN_DIRECTORY_TTL
    });

    return directory;
}

async function fetchOkxSupportedChains() {
    const directory = await ensureOkxChainDirectory();

    const formatList = (list) => {
        if (!Array.isArray(list) || list.length === 0) {
            return [];
        }

        const seen = new Set();
        const result = [];

        for (const entry of list) {
            if (!entry) {
                continue;
            }

            const key = entry.primaryKey
                || (Number.isFinite(entry.chainIndex) ? `idx:${entry.chainIndex}` : null)
                || (entry.chainShortName ? normalizeChainKey(entry.chainShortName) : null);

            if (key && seen.has(key)) {
                continue;
            }

            if (key) {
                seen.add(key);
            }

            const names = [];
            if (entry.chainName) {
                names.push(entry.chainName);
            }
            if (entry.chainShortName && entry.chainShortName !== entry.chainName) {
                names.push(entry.chainShortName);
            }

            const baseLabel = names.length > 1
                ? `${names[0]} (${names[1]})`
                : (names[0] || entry.aliases?.[0] || 'Unknown');

            const meta = [];
            if (Number.isFinite(entry.chainIndex)) {
                meta.push(`#${entry.chainIndex}`);
            }
            if (Number.isFinite(entry.chainId) && entry.chainId !== entry.chainIndex) {
                meta.push(`id ${entry.chainId}`);
            }

            const metaText = meta.length > 0 ? ` [${meta.join(' · ')}]` : '';
            result.push(`${baseLabel}${metaText}`);
        }

        return result;
    };

    return {
        aggregator: formatList(directory?.aggregator || []),
        market: formatList(directory?.market || [])
    };
}

async function fetchOkx402Supported() {
    const payload = await okxJsonRequest('GET', '/api/v6/x402/supported', { query: {} });
    const data = unwrapOkxData(payload);
    if (!data || data.length === 0) {
        return [];
    }

    return data
        .map((entry) => {
            if (!entry) {
                return null;
            }
            if (typeof entry === 'string') {
                return entry;
            }
            if (typeof entry === 'object') {
                return entry.chainShortName || entry.chainName || entry.name || null;
            }
            return null;
        })
        .filter(Boolean);
}

async function tryFetchOkxMarketTicker() {
    if (!OKX_MARKET_INSTRUMENT) {
        return null;
    }

    const payload = await okxJsonRequest('GET', '/api/v5/market/ticker', {
        query: { instId: OKX_MARKET_INSTRUMENT },
        expectOkCode: true,
        auth: hasOkxCredentials
    });

    const tickerEntry = unwrapOkxFirst(payload);
    const price = extractOkxPriceValue(tickerEntry);
    const tokenPrices = collectOkxTokenUnitPrices(tickerEntry || {});
    const okbUsd = resolveOkbUsdPrice(tokenPrices);
    const priceOkb = Number.isFinite(price) && Number.isFinite(okbUsd) && okbUsd > 0
        ? price / okbUsd
        : null;

    if (Number.isFinite(price)) {
        return {
            price,
            priceOkb: Number.isFinite(priceOkb) ? priceOkb : null,
            okbUsd: Number.isFinite(okbUsd) ? okbUsd : null,
            source: 'OKX market ticker',
            chain: null
        };
    }

    return null;
}

function getOkxChainShortNameCandidates() {
    const configured = typeof OKX_CHAIN_SHORT_NAME === 'string'
        ? OKX_CHAIN_SHORT_NAME.split(/[|,]+/)
        : [];

    const defaults = [
        'x-layer',
        'xlayer',
        'X Layer',
        'X-Layer',
        'X_LAYER',
        'Xlayer'
    ];

    const seen = new Set();
    const result = [];

    for (const value of [...configured, ...defaults]) {
        if (!value) {
            continue;
        }

        const trimmed = value.trim();
        if (!trimmed) {
            continue;
        }

        const dedupeKey = trimmed.toLowerCase();
        if (seen.has(dedupeKey)) {
            continue;
        }

        seen.add(dedupeKey);
        result.push(trimmed);
    }

    if (result.length === 0) {
        result.push('x-layer');
    }

    return result;
}

function normalizeChainKey(value) {
    if (!value || typeof value !== 'string') {
        return null;
    }

    const trimmed = value.trim();
    if (!trimmed) {
        return null;
    }

    return trimmed.toLowerCase().replace(/[^a-z0-9]/g, '');
}

function normalizeOkxChainDirectoryEntry(entry) {
    if (!entry) {
        return null;
    }

    if (typeof entry === 'string') {
        const trimmed = entry.trim();
        if (!trimmed) {
            return null;
        }

        const key = normalizeChainKey(trimmed);
        return {
            chainShortName: trimmed,
            chainName: trimmed,
            chainIndex: null,
            chainId: null,
            aliases: [trimmed],
            keys: key ? [key] : [],
            primaryKey: key,
            raw: entry
        };
    }

    if (typeof entry !== 'object') {
        return null;
    }

    const aliasFields = [
        entry.chainShortName,
        entry.chainName,
        entry.chain,
        entry.name,
        entry.shortName,
        entry.short_name,
        entry.short,
        entry.symbol,
        entry.chainSymbol,
        entry.chainAlias,
        entry.alias,
        entry.displayName,
        entry.label,
        entry.networkName
    ];

    const aliases = Array.from(new Set(aliasFields
        .map((value) => (typeof value === 'string' ? value.trim() : null))
        .filter(Boolean)));

    const chainShortName = aliases[0] || null;
    const chainName = (typeof entry.chainName === 'string' && entry.chainName.trim())
        ? entry.chainName.trim()
        : (aliases[1] || chainShortName || null);

    const chainIndexCandidate = entry.chainIndex ?? entry.index ?? entry.chain_id ?? entry.chainId ?? entry.id;
    const chainIdCandidate = entry.chainId ?? entry.chain_id ?? entry.chainID ?? entry.id ?? entry.networkId;

    const chainIndexNumeric = normalizeNumeric(chainIndexCandidate);
    const chainIdNumeric = normalizeNumeric(chainIdCandidate);

    const chainIndex = Number.isFinite(chainIndexNumeric) ? Math.trunc(chainIndexNumeric) : null;
    const chainId = Number.isFinite(chainIdNumeric) ? Math.trunc(chainIdNumeric) : null;

    const keys = Array.from(new Set(aliases
        .map((alias) => normalizeChainKey(alias))
        .filter(Boolean)));

    const primaryKey = keys[0] || (Number.isFinite(chainIndex) ? `idx:${chainIndex}` : null);

    return {
        chainShortName: chainShortName || chainName || null,
        chainName: chainName || chainShortName || null,
        chainIndex,
        chainId,
        aliases,
        keys,
        primaryKey,
        raw: entry
    };
}

function dedupeOkxChainEntries(entries) {
    if (!Array.isArray(entries) || entries.length === 0) {
        return [];
    }

    const seen = new Set();
    const result = [];

    for (const entry of entries) {
        if (!entry) {
            continue;
        }

        const key = entry.primaryKey
            || (Number.isFinite(entry.chainIndex) ? `idx:${entry.chainIndex}` : null)
            || (entry.chainShortName ? normalizeChainKey(entry.chainShortName) : null);

        if (key && seen.has(key)) {
            continue;
        }

        if (key) {
            seen.add(key);
        }

        result.push(entry);
    }

    return result;
}

async function ensureOkxChainDirectory() {
    const now = Date.now();
    if (okxChainDirectoryCache && okxChainDirectoryExpiresAt > now) {
        return okxChainDirectoryCache;
    }

    if (!okxChainDirectoryPromise) {
        okxChainDirectoryPromise = loadOkxChainDirectory()
            .then((directory) => {
                okxChainDirectoryCache = directory;
                okxChainDirectoryExpiresAt = Date.now() + OKX_CHAIN_CONTEXT_TTL;
                return directory;
            })
            .catch((error) => {
                okxChainDirectoryCache = null;
                okxChainDirectoryExpiresAt = 0;
                throw error;
            })
            .finally(() => {
                okxChainDirectoryPromise = null;
            });
    }

    return okxChainDirectoryPromise;
}

async function loadOkxChainDirectory() {
    const [aggregator, market] = await Promise.allSettled([
        okxJsonRequest('GET', '/api/v6/dex/aggregator/supported/chain', { query: {}, expectOkCode: false }),
        okxJsonRequest('GET', '/api/v6/dex/market/supported/chain', { query: {}, expectOkCode: false })
    ]);

    const normalizeList = (payload) => {
        const rawList = payload.status === 'fulfilled' ? unwrapOkxData(payload.value) : [];
        const normalized = [];
        for (const item of rawList || []) {
            const entry = normalizeOkxChainDirectoryEntry(item);
            if (entry) {
                normalized.push(entry);
            }
        }
        return dedupeOkxChainEntries(normalized);
    };

    return {
        aggregator: normalizeList(aggregator),
        market: normalizeList(market)
    };
}

function findChainEntryByIndex(list, index) {
    if (!Array.isArray(list) || !Number.isFinite(index)) {
        return null;
    }

    const numericIndex = Number(index);
    for (const entry of list) {
        if (!entry) {
            continue;
        }

        if (Number.isFinite(entry.chainIndex) && Number(entry.chainIndex) === numericIndex) {
            return entry;
        }
    }

    return null;
}

function findChainEntryByKeys(list, keys) {
    if (!Array.isArray(list) || !Array.isArray(keys) || keys.length === 0) {
        return null;
    }

    for (const entry of list) {
        if (!entry || !Array.isArray(entry.keys)) {
            continue;
        }

        for (const key of entry.keys) {
            if (keys.includes(key)) {
                return entry;
            }
        }
    }

    return null;
}

function collectChainSearchKeys(chainName) {
    const names = [];

    if (chainName) {
        names.push(chainName);
    }

    if (OKX_CHAIN_SHORT_NAME) {
        names.push(OKX_CHAIN_SHORT_NAME);
    }

    const configured = typeof OKX_CHAIN_SHORT_NAME === 'string'
        ? OKX_CHAIN_SHORT_NAME.split(/[|,]+/)
        : [];

    for (const value of configured) {
        names.push(value);
    }

    names.push('x-layer', 'xlayer', 'X Layer', 'okx xlayer', 'okbchain', 'okxchain');

    const normalized = [];
    const seen = new Set();

    for (const name of names) {
        if (!name || typeof name !== 'string') {
            continue;
        }

        const variants = [
            name,
            name.replace(/[_\s-]+/g, ''),
            name.replace(/[_\s]+/g, '-'),
            name.replace(/[-]+/g, ' ')
        ];

        for (const variant of variants) {
            const key = normalizeChainKey(variant);
            if (key && !seen.has(key)) {
                seen.add(key);
                normalized.push(key);
            }
        }
    }

    return normalized;
}

async function resolveOkxChainContext(chainName) {
    const cacheKey = chainName ? chainName.toLowerCase().trim() : '(default)';
    const cached = okxResolvedChainCache.get(cacheKey);
    const now = Date.now();

    if (cached && cached.expiresAt > now) {
        return cached.value;
    }

    let directory = null;
    try {
        directory = await ensureOkxChainDirectory();
    } catch (error) {
        console.warn(`[OKX] Failed to load chain directory: ${error.message}`);
    }

    const aggregator = directory?.aggregator || [];
    const market = directory?.market || [];

    const searchKeys = collectChainSearchKeys(chainName);

    let match = null;

    if (Number.isFinite(OKX_CHAIN_INDEX)) {
        match = findChainEntryByIndex(aggregator, OKX_CHAIN_INDEX)
            || findChainEntryByIndex(market, OKX_CHAIN_INDEX);
    }

    if (!match && searchKeys.length > 0) {
        match = findChainEntryByKeys(aggregator, searchKeys)
            || findChainEntryByKeys(market, searchKeys);
    }

    if (!match) {
        const xlayerKey = 'xlayer';
        match = findChainEntryByKeys(aggregator, [xlayerKey])
            || findChainEntryByKeys(market, [xlayerKey]);
    }

    if (!match) {
        match = aggregator[0] || market[0] || null;
    }

    if (!match) {
        const fallbackShortName = OKX_CHAIN_SHORT_NAME || 'x-layer';
        const fallbackKeys = collectChainSearchKeys(fallbackShortName);
        match = {
            chainShortName: fallbackShortName,
            chainName: fallbackShortName,
            chainIndex: Number.isFinite(OKX_CHAIN_INDEX)
                ? Number(OKX_CHAIN_INDEX)
                : OKX_CHAIN_INDEX_FALLBACK,
            chainId: null,
            aliases: [fallbackShortName],
            keys: fallbackKeys,
            primaryKey: fallbackKeys[0] || null,
            raw: null
        };
    }

    if (okxResolvedChainCache.size > 50) {
        okxResolvedChainCache.clear();
    }

    okxResolvedChainCache.set(cacheKey, {
        value: match,
        expiresAt: now + OKX_CHAIN_CONTEXT_TTL
    });

    return match;
}

async function buildOkxDexQuery(chainName, options = {}) {
    const query = {};
    const context = await resolveOkxChainContext(chainName);
    const explicitChainIndex = options.explicitChainIndex;
    const explicitChainShortName = options.explicitChainShortName;

    if (explicitChainShortName) {
        query.chainShortName = explicitChainShortName;
    }

    if (Number.isFinite(explicitChainIndex)) {
        query.chainIndex = Number(explicitChainIndex);
    }

    if (context) {
        if (!query.chainShortName) {
            if (context.chainShortName) {
                query.chainShortName = context.chainShortName;
            } else if (chainName) {
                query.chainShortName = chainName;
            }
        }

        if (!Number.isFinite(query.chainIndex)) {
            if (Number.isFinite(context.chainIndex)) {
                query.chainIndex = Number(context.chainIndex);
            } else if (Number.isFinite(OKX_CHAIN_INDEX)) {
                query.chainIndex = Number(OKX_CHAIN_INDEX);
            }
        }

        if (Number.isFinite(context.chainId)) {
            query.chainId = context.chainId;
        }
    } else if (chainName && !query.chainShortName) {
        query.chainShortName = chainName;
    }

    if (!query.chainShortName) {
        query.chainShortName = OKX_CHAIN_SHORT_NAME || 'x-layer';
    }

    if (!Number.isFinite(query.chainIndex)) {
        if (Number.isFinite(OKX_CHAIN_INDEX)) {
            query.chainIndex = Number(OKX_CHAIN_INDEX);
        } else if (Number.isFinite(OKX_CHAIN_INDEX_FALLBACK)) {
            query.chainIndex = OKX_CHAIN_INDEX_FALLBACK;
        }
    }

    const includeToken = options.includeToken !== false;
    const includeQuote = options.includeQuote !== false;

    if (includeToken && OKX_BANMAO_TOKEN_ADDRESS) {
        query.tokenAddress = OKX_BANMAO_TOKEN_ADDRESS;
        query.baseTokenAddress = query.baseTokenAddress || OKX_BANMAO_TOKEN_ADDRESS;
        query.baseCurrency = query.baseCurrency || OKX_BANMAO_TOKEN_ADDRESS;
        query.baseToken = query.baseToken || OKX_BANMAO_TOKEN_ADDRESS;
        query.tokenContractAddress = query.tokenContractAddress || OKX_BANMAO_TOKEN_ADDRESS;
    }

    if (includeQuote && OKX_QUOTE_TOKEN_ADDRESS) {
        query.quoteTokenAddress = OKX_QUOTE_TOKEN_ADDRESS;
        query.quoteCurrency = query.quoteCurrency || OKX_QUOTE_TOKEN_ADDRESS;
        query.quoteToken = query.quoteToken || OKX_QUOTE_TOKEN_ADDRESS;
        if (!query.toTokenAddress) {
            query.toTokenAddress = OKX_QUOTE_TOKEN_ADDRESS;
        }
    }

    return query;
}

async function okxJsonRequest(method, path, options = {}) {
    const { query, body, auth = hasOkxCredentials, expectOkCode = true } = options;
    const url = new URL(path, OKX_BASE_URL);

    if (query && typeof query === 'object') {
        for (const [key, value] of Object.entries(query)) {
            if (value === undefined || value === null || value === '') {
                continue;
            }
            url.searchParams.set(key, String(value));
        }
    }

    const methodUpper = method.toUpperCase();
    const requestPath = url.pathname + url.search;
    const bodyString = body ? JSON.stringify(body) : '';

    const headers = {
        'Accept': 'application/json',
        'User-Agent': 'banmao-bot/2.0 (+https://www.banmao.fun)'
    };

    if (bodyString) {
        headers['Content-Type'] = 'application/json';
    }

    if (auth && hasOkxCredentials) {
        const timestamp = new Date().toISOString();
        const signPayload = `${timestamp}${methodUpper}${requestPath}${bodyString}`;
        const signature = crypto
            .createHmac('sha256', OKX_SECRET_KEY)
            .update(signPayload)
            .digest('base64');

        headers['OK-ACCESS-KEY'] = OKX_API_KEY;
        headers['OK-ACCESS-SIGN'] = signature;
        headers['OK-ACCESS-TIMESTAMP'] = timestamp;
        headers['OK-ACCESS-PASSPHRASE'] = OKX_API_PASSPHRASE;
        if (OKX_API_PROJECT) {
            headers['OK-ACCESS-PROJECT'] = OKX_API_PROJECT;
        }
        if (OKX_API_SIMULATED) {
            headers['x-simulated-trading'] = '1';
        }
    }

    const response = await fetchJsonWithTimeout(url.toString(), {
        method: methodUpper,
        headers,
        body: bodyString || undefined
    }, OKX_FETCH_TIMEOUT);

    if (!response) {
        return null;
    }

    if (expectOkCode && response.code && response.code !== '0') {
        const msg = typeof response.msg === 'string' ? response.msg : 'Unknown error';
        throw new Error(`OKX response code ${response.code}: ${msg}`);
    }

    return response;
}

async function fetchJsonWithTimeout(urlString, requestOptions, timeoutMs) {
    const options = requestOptions || {};

    if (typeof fetch === 'function') {
        const supportsAbort = typeof AbortController === 'function';
        const controller = supportsAbort ? new AbortController() : null;
        let timeoutId = null;
        let timedOut = false;

        const timeoutPromise = new Promise((_, reject) => {
            timeoutId = setTimeout(() => {
                timedOut = true;
                if (controller) {
                    controller.abort();
                }
                reject(new Error('Request timed out'));
            }, timeoutMs);
        });

        try {
            const response = await Promise.race([
                fetch(urlString, {
                    ...options,
                    ...(controller ? { signal: controller.signal } : {})
                }),
                timeoutPromise
            ]);

            if (!response) {
                throw new Error('Invalid response from fetch');
            }

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }

            const text = await response.text();
            if (!text) {
                return null;
            }

            try {
                return JSON.parse(text);
            } catch (error) {
                throw new Error('Failed to parse OKX response');
            }
        } catch (error) {
            if (timedOut || (controller && error && error.name === 'AbortError')) {
                throw new Error('Request timed out');
            }
            throw error;
        } finally {
            if (timeoutId) {
                clearTimeout(timeoutId);
            }
        }
    }

    return await fetchJsonWithHttps(urlString, options, timeoutMs);
}

function fetchJsonWithHttps(urlString, options, timeoutMs) {
    return new Promise((resolve, reject) => {
        const requestOptions = {
            method: options.method || 'GET',
            headers: options.headers || {}
        };

        const req = https.request(urlString, requestOptions, (response) => {
            const { statusCode } = response;
            const chunks = [];

            response.setEncoding('utf8');
            response.on('error', reject);

            if (!statusCode || statusCode < 200 || statusCode >= 300) {
                if (typeof response.resume === 'function') {
                    response.resume();
                }
                reject(new Error(`HTTP ${statusCode || 'ERR'}`));
                return;
            }

            response.on('data', (chunk) => chunks.push(chunk));
            response.on('end', () => {
                const body = chunks.join('');

                if (!body) {
                    resolve(null);
                    return;
                }

                try {
                    resolve(JSON.parse(body));
                } catch (error) {
                    reject(new Error('Failed to parse OKX response'));
                }
            });
        });

        req.on('error', (error) => {
            reject(error);
        });

        req.setTimeout(timeoutMs, () => {
            req.destroy(new Error('Request timed out'));
        });

        if (options.body) {
            req.write(options.body);
        }

        req.end();
    });
}

function extractOkxPriceValue(entry) {
    if (!entry || typeof entry !== 'object') {
        return null;
    }

    const priceKeys = [
        'usdPrice',
        'price',
        'priceUsd',
        'lastPrice',
        'last',
        'close',
        'markPrice',
        'quotePrice',
        'tokenPrice',
        'tokenUnitPrice',
        'usdValue',
        'value',
        'bestAskPrice',
        'bestBidPrice',
        'bestAsk',
        'bestBid',
        'askPx',
        'bidPx'
    ];

    for (const key of priceKeys) {
        if (Object.prototype.hasOwnProperty.call(entry, key)) {
            const numeric = normalizeNumeric(entry[key]);
            if (Number.isFinite(numeric)) {
                return numeric;
            }
        }
    }

    const nestedKeys = ['prices', 'priceInfo', 'tokenPrices', 'ticker', 'bestAsk', 'bestBid'];
    for (const nestedKey of nestedKeys) {
        const nested = entry[nestedKey];
        const numeric = extractFromNested(nested);
        if (Number.isFinite(numeric)) {
            return numeric;
        }
    }

    if (Array.isArray(entry.data)) {
        const numeric = extractFromNested(entry.data);
        if (Number.isFinite(numeric)) {
            return numeric;
        }
    }

    return null;
}

function extractFromNested(value) {
    if (!value) {
        return null;
    }

    if (Array.isArray(value)) {
        for (const item of value) {
            const numeric = extractOkxPriceValue(item);
            if (Number.isFinite(numeric)) {
                return numeric;
            }
        }
        return null;
    }

    if (typeof value === 'object') {
        const nestedValues = Object.values(value);
        for (const nested of nestedValues) {
            const numeric = extractOkxPriceValue(nested);
            if (Number.isFinite(numeric)) {
                return numeric;
            }
        }
    }

    return normalizeNumeric(value);
}

function pickOkxNumeric(entry, keys) {
    if (!entry || typeof entry !== 'object' || !Array.isArray(keys)) {
        return null;
    }

    for (const key of keys) {
        if (!key || typeof key !== 'string') {
            continue;
        }

        if (Object.prototype.hasOwnProperty.call(entry, key)) {
            const numeric = normalizeNumeric(entry[key]);
            if (Number.isFinite(numeric)) {
                return numeric;
            }
        }
    }

    return null;
}

function unwrapOkxData(payload) {
    if (!payload || typeof payload !== 'object') {
        return [];
    }

    const directData = payload.data !== undefined ? payload.data : payload.result;

    if (Array.isArray(directData)) {
        return directData;
    }

    if (directData && typeof directData === 'object') {
        const candidates = [
            directData.data,
            directData.items,
            directData.list,
            directData.rows,
            directData.result,
            directData.candles,
            directData.records,
            directData.trades,
            directData.pools,
            directData.liquidityList,
            directData.tokens,
            directData.tokenList
        ];
        for (const candidate of candidates) {
            if (Array.isArray(candidate)) {
                return candidate;
            }
        }
    }

    return [];
}

function unwrapOkxFirst(payload) {
    if (!payload || typeof payload !== 'object') {
        return null;
    }

    const data = unwrapOkxData(payload);
    if (data.length > 0) {
        return data[0] || null;
    }

    if (payload.data && typeof payload.data === 'object') {
        return payload.data;
    }

    return null;
}

function normalizeNumeric(value) {
    if (value === null || value === undefined) {
        return null;
    }

    if (typeof value === 'number') {
        return Number.isFinite(value) ? value : null;
    }

    if (typeof value === 'string') {
        const cleaned = value.replace(/[,\s]/g, '');
        if (!cleaned) {
            return null;
        }
        const numeric = Number(cleaned);
        return Number.isFinite(numeric) ? numeric : null;
    }

    return null;
}

function normalizeOkxTokenDirectoryToken(entry) {
    if (!entry || typeof entry !== 'object') {
        return null;
    }

    const addressCandidate = entry.tokenContractAddress
        || entry.tokenAddress
        || entry.contractAddress
        || entry.address
        || entry.baseTokenAddress
        || entry.token;

    if (!addressCandidate || typeof addressCandidate !== 'string') {
        return null;
    }

    const normalizedAddress = normalizeOkxConfigAddress(addressCandidate) || addressCandidate.trim();
    if (!normalizedAddress) {
        return null;
    }

    const decimals = pickOkxNumeric(entry, ['decimals', 'decimal', 'tokenDecimal']);
    const symbolCandidate = typeof entry.tokenSymbol === 'string'
        ? entry.tokenSymbol
        : (typeof entry.symbol === 'string' ? entry.symbol : null);
    const nameCandidate = typeof entry.tokenName === 'string'
        ? entry.tokenName
        : (typeof entry.name === 'string' ? entry.name : null);
    const logoCandidate = typeof entry.tokenLogoUrl === 'string'
        ? entry.tokenLogoUrl
        : (typeof entry.logoUrl === 'string' ? entry.logoUrl : (typeof entry.logo === 'string' ? entry.logo : null));

    return {
        address: normalizedAddress,
        addressLower: normalizedAddress.toLowerCase(),
        decimals: Number.isFinite(decimals) ? Math.max(0, Math.trunc(decimals)) : null,
        symbol: symbolCandidate ? symbolCandidate.trim() : null,
        name: nameCandidate ? nameCandidate.trim() : null,
        logo: logoCandidate ? logoCandidate.trim() : null,
        raw: entry
    };
}

function toRoomIdString(roomId) {
    try {
        return roomId.toString();
    } catch (error) {
        return `${roomId}`;
    }
}

function normalizeAddress(value) {
    if (!value || value === ethers.ZeroAddress) {
        return null;
    }
    try {
        return ethers.getAddress(value);
    } catch (error) {
        return null;
    }
}

function normalizeRoomStruct(room) {
    if (!room) {
        return null;
    }

    let stakeWei;
    if (room.stake !== undefined) {
        try {
            if (typeof room.stake === 'bigint') {
                stakeWei = room.stake;
            } else if (typeof room.stake === 'number') {
                stakeWei = BigInt(Math.trunc(room.stake));
            } else if (typeof room.stake === 'string') {
                stakeWei = BigInt(room.stake);
            } else if (room.stake && typeof room.stake.toString === 'function') {
                stakeWei = BigInt(room.stake.toString());
            }
        } catch (error) {
            stakeWei = undefined;
        }
    }

    return {
        creator: normalizeAddress(room.creator),
        opponent: normalizeAddress(room.opponent),
        stakeWei,
        commitA: room.commitA,
        commitB: room.commitB,
        revealA: room.revealA !== undefined ? Number(room.revealA) : undefined,
        revealB: room.revealB !== undefined ? Number(room.revealB) : undefined
    };
}

function mergeRoomData(existing = {}, incoming = {}) {
    const merged = { ...existing };

    for (const [key, value] of Object.entries(incoming)) {
        if (value === undefined) {
            continue;
        }

        if ((key === 'creator' || key === 'opponent') && !value) {
            continue;
        }

        if (key === 'stakeWei') {
            if (value === null || value === undefined) {
                continue;
            }
            if (value === 0n && existing && existing.stakeWei !== undefined) {
                continue;
            }
        }

        if ((key === 'commitA' || key === 'commitB') && existing) {
            const existingValue = existing[key];
            const incomingValue = value;
            const isExistingSet = Boolean(existingValue && existingValue !== ethers.ZeroHash);
            const isIncomingUnset = !incomingValue || incomingValue === ethers.ZeroHash;

            if (isExistingSet && isIncomingUnset) {
                continue;
            }
        }

        if ((key === 'revealA' || key === 'revealB') && value === 0 && existing && typeof existing[key] === 'number' && existing[key] !== 0) {
            continue;
        }

        merged[key] = value;
    }

    return merged;
}

function updateRoomCache(roomId, incoming = {}) {
    const roomIdStr = toRoomIdString(roomId);
    const existing = roomCache.get(roomIdStr) || {};
    const merged = mergeRoomData(existing, incoming);
    roomCache.set(roomIdStr, merged);
    return merged;
}

function getCachedRoom(roomId) {
    return roomCache.get(toRoomIdString(roomId)) || null;
}

async function getRoomState(roomId, { refresh = true } = {}) {
    let latest = null;
    if (refresh && contract) {
        try {
            const room = await contract.rooms(roomId);
            const normalized = normalizeRoomStruct(room);
            if (normalized) {
                latest = updateRoomCache(roomId, normalized);
            }
        } catch (error) {
            console.warn(`[RoomCache] Không thể tải dữ liệu phòng ${toRoomIdString(roomId)}: ${error.message}`);
        }
    }

    if (!latest) {
        latest = getCachedRoom(roomId);
    }

    return latest;
}

function clearRoomCache(roomId) {
    roomCache.delete(toRoomIdString(roomId));
}

async function finalizeDrawOutcome(roomId, roomState, { source = 'DrawCheck' } = {}) {
    const roomIdStr = toRoomIdString(roomId);

    if (!roomState) {
        return false;
    }

    const creatorAddress = roomState.creator || null;
    const opponentAddress = roomState.opponent || null;

    if (!creatorAddress || !opponentAddress) {
        return false;
    }

    const creatorChoice = Number(roomState.revealA ?? 0);
    const opponentChoice = Number(roomState.revealB ?? 0);

    if (!creatorChoice || !opponentChoice || creatorChoice !== opponentChoice) {
        return false;
    }

    const existingOutcome = getRoomFinalOutcome(roomId);
    if (existingOutcome && existingOutcome.outcome === 'draw') {
        console.log(`[${source}] Room ${roomIdStr} đã được đánh dấu hòa trước đó, bỏ qua.`);
        return true;
    }

    console.log(`[${source}] Room ${roomIdStr} được xác nhận hòa dựa trên dữ liệu cache.`);

    markRoomFinalOutcome(roomId, 'draw');

    const stakeWeiValue = roomState.stakeWei !== undefined ? roomState.stakeWei : null;
    const stakeAmount = stakeWeiValue !== null
        ? parseFloat(ethers.formatEther(stakeWeiValue))
        : 0;
    const drawRefundWei = stakeWeiValue !== null ? (stakeWeiValue * 98n) / 100n : null;
    const drawFeeWei = stakeWeiValue !== null && drawRefundWei !== null ? stakeWeiValue - drawRefundWei : null;
    const refundAmountText = formatBanmaoFromWei(drawRefundWei);
    const stakeAmountText = formatBanmaoFromWei(stakeWeiValue);
    const feeAmountText = formatBanmaoFromWei(drawFeeWei);

    const baseDrawVariables = {
        roomId: roomIdStr,
        refundAmount: refundAmountText,
        refundPercent: '98%',
        stakeAmount: stakeAmountText,
        feePercent: '2%',
        feeAmount: feeAmountText,
        __choiceTranslations: [{ valueKey: 'choiceValue', targetKey: 'choice' }]
    };

    const notifyTasks = [
        sendInstantNotification(creatorAddress, 'notify_game_draw', {
            ...baseDrawVariables,
            choiceValue: creatorChoice,
            __messageBuilder: (lang, vars) => buildDrawNotificationMessage(lang, vars)
        })
    ];

    if (opponentAddress) {
        notifyTasks.push(
            sendInstantNotification(opponentAddress, 'notify_game_draw', {
                ...baseDrawVariables,
                choiceValue: opponentChoice,
                __messageBuilder: (lang, vars) => buildDrawNotificationMessage(lang, vars)
            })
        );
    }

    await Promise.all(notifyTasks);

    if (opponentAddress && stakeAmount > 0) {
        await Promise.all([
            db.writeGameResult(creatorAddress, 'draw', stakeAmount),
            db.writeGameResult(opponentAddress, 'draw', stakeAmount)
        ]);
    }

    await broadcastGroupGameUpdate('draw', {
        roomId: roomIdStr,
        creatorAddress,
        opponentAddress,
        stakeAmount,
        creatorChoice,
        opponentChoice
    });

    clearRoomCache(roomId);
    return true;
}


// ==========================================================
// 🚀 PHẦN 1: API SERVER
// ==========================================================
function startApiServer() {
    app.use(cors());
    app.use(express.json());

    // API cho DApp (Deep Link) - Cần async
    app.post('/api/generate-token', async (req, res) => {
        try {
            const { walletAddress } = req.body;
            if (!walletAddress) return res.status(400).json({ error: 'walletAddress là bắt buộc' });
            const token = uuidv4();
            await db.addPendingToken(token, walletAddress); // <-- AWAIT
            console.log(`[API] Đã tạo token cho ví: ${walletAddress}`);
            res.json({ token: token });
        } catch (error) {
            console.error("[API] Lỗi generate-token:", error.message);
            res.status(500).json({ error: 'Địa chỉ ví không hợp lệ' });
        }
    });

    // API cho DApp kiểm tra trạng thái - Cần async
    app.get('/api/check-status', async (req, res) => {
        try {
            const { walletAddress } = req.query;
            if (!walletAddress) return res.status(400).json({ error: 'walletAddress là bắt buộc' });
            const users = await db.getUsersForWallet(walletAddress); // <-- AWAIT
            res.json({ isConnected: users.length > 0, count: users.length });
        } catch (error) {
            res.status(500).json({ error: 'Địa chỉ ví không hợp lệ' });
        }
    });

    app.listen(API_PORT, '0.0.0.0', () => {
        console.log(`✅ [API Server] Đang chạy tại http://0.0.0.0:${API_PORT}`);
    });
}


// ==========================================================
// 🤖 PHẦN 2: LOGIC BOT TELEGRAM (ĐÃ SỬA LỖI LOGIC NGÔN NGỮ)
// ==========================================================

// ===== HÀM HELPER MỚI (SỬA LỖI) =====
// Lấy ngôn ngữ ĐÃ LƯU của user, nếu không có thì set ngôn ngữ mặc định
async function getLang(msg) {
    if (!msg || !msg.chat) {
        return defaultLang;
    }

    const chatId = msg.chat.id.toString();
    const detectedLang = resolveLangCode(msg?.from?.language_code);

    const info = await db.getUserLanguageInfo(chatId);
    if (info) {
        const savedLang = resolveLangCode(info.lang);
        const source = info.source || 'auto';

        if (source === 'manual') {
            if (savedLang !== info.lang) {
                await db.setLanguage(chatId, savedLang);
            }
            return savedLang;
        }

        if (savedLang !== detectedLang) {
            await db.setLanguage(chatId, savedLang);
            return savedLang;
        }

        if (savedLang !== info.lang || source !== info.source) {
            await db.setLanguageAuto(chatId, savedLang);
        }

        return detectedLang;
    }

    await db.setLanguageAuto(chatId, detectedLang);
    return detectedLang;
}
// ======================================

function startTelegramBot() {
    
    // Xử lý /start CÓ token (Từ DApp) - Cần async
    bot.onText(/\/start (.+)/, async (msg, match) => {
        const chatId = msg.chat.id.toString();
        const token = match[1];
        // Khi /start, luôn ưu tiên ngôn ngữ của thiết bị
        const lang = resolveLangCode(msg.from.language_code);
        const walletAddress = await db.getPendingWallet(token); 
        if (walletAddress) {
            await db.addWalletToUser(chatId, lang, walletAddress);
            await db.deletePendingToken(token);
            const message = t(lang, 'connect_success', { walletAddress: walletAddress });
            sendReply(msg, message, { parse_mode: "Markdown" });
            console.log(`[BOT] Liên kết (DApp): ${walletAddress} -> ${chatId} (lang: ${lang})`);
        } else {
            const message = t(lang, 'connect_fail_token');
            sendReply(msg, message, { parse_mode: "Markdown" });
            console.log(`[BOT] Token không hợp lệ: ${token}`);
        }
    });

    // Xử lý /start KHÔNG CÓ token (Gõ tay) - Cần async
    bot.onText(/\/start$/, async (msg) => {
        const chatId = msg.chat.id.toString();
        // Lấy ngôn ngữ (hoặc tạo user mới nếu chưa có)
        const lang = await getLang(msg); // <-- SỬA LỖI
        const message = t(lang, 'welcome_generic');
        sendReply(msg, message, { parse_mode: "Markdown" });
    });

    // COMMAND: /register - Cần async
    bot.onText(/\/register (.+)/, async (msg, match) => {
        const chatId = msg.chat.id.toString();
        const lang = await getLang(msg); // <-- SỬA LỖI
        const address = match[1];
        try {
            const normalizedAddr = ethers.getAddress(address);
            await db.addWalletToUser(chatId, lang, normalizedAddr);
            const message = t(lang, 'register_success', { walletAddress: normalizedAddr });
            sendReply(msg, message, { parse_mode: "Markdown" });
            console.log(`[BOT] Thêm ví (Manual): ${normalizedAddr} -> ${chatId} (lang: ${lang})`);
        } catch (error) {
            const message = t(lang, 'register_invalid_address');
            sendReply(msg, message, { parse_mode: "Markdown" });
        }
    });

    // COMMAND: /mywallet - Cần async
    bot.onText(/\/mywallet/, async (msg) => { 
        const chatId = msg.chat.id.toString();
        const lang = await getLang(msg); // <-- SỬA LỖI
        const wallets = await db.getWalletsForUser(chatId);
        if (wallets.length > 0) {
            let message = t(lang, 'mywallet_list_header', { count: wallets.length }) + "\n\n";
            wallets.forEach(wallet => { message += `• \`${wallet}\`\n`; });
            message += `\n` + t(lang, 'mywallet_list_footer');
            sendReply(msg, message, { parse_mode: "Markdown" });
        } else {
            const message = t(lang, 'mywallet_not_linked');
            sendReply(msg, message, { parse_mode: "Markdown" });
        }
    });

    // COMMAND: /stats - Cần async
    bot.onText(/\/stats/, async (msg) => {
        const chatId = msg.chat.id.toString();
        const lang = await getLang(msg); // <-- SỬA LỖI
        const wallets = await db.getWalletsForUser(chatId);
        if (wallets.length === 0) {
            sendReply(msg, t(lang, 'stats_no_wallet'));
            return;
        }
        let totalStats = { games: 0, wins: 0, losses: 0, draws: 0, totalWon: 0, totalLost: 0 };
        for (const wallet of wallets) {
            const stats = await db.getStats(wallet);
            totalStats.games += stats.games;
            totalStats.wins += stats.wins;
            totalStats.losses += stats.losses;
            totalStats.draws += stats.draws;
            totalStats.totalWon += stats.totalWon;
            totalStats.totalLost += stats.totalLost;
        };
        if (totalStats.games === 0) {
            sendReply(msg, t(lang, 'stats_no_games'));
            return;
        }
        const winRate = (totalStats.games > 0) ? (totalStats.wins / totalStats.games * 100).toFixed(0) : 0;
        const netProfit = totalStats.totalWon - totalStats.totalLost;
        let message = t(lang, 'stats_header', { wallets: wallets.length, games: totalStats.games }) + "\n\n";
        message += `• ${t(lang, 'stats_line_1', { wins: totalStats.wins, losses: totalStats.losses, draws: totalStats.draws })}\n`;
        message += `• ${t(lang, 'stats_line_2', { rate: winRate })}\n`;
        message += `• ${t(lang, 'stats_line_3', { amount: totalStats.totalWon.toFixed(2) })}\n`;
        message += `• ${t(lang, 'stats_line_4', { amount: totalStats.totalLost.toFixed(2) })}\n`;
        message += `• **${t(lang, 'stats_line_5', { amount: netProfit.toFixed(2) })} $BANMAO**`;
        sendReply(msg, message, { parse_mode: "Markdown" });
    });

    bot.onText(/^\/checkin(?:@[\w_]+)?$/, async (msg) => {
        const chatType = msg.chat?.type;
        const chatId = msg.chat.id.toString();
        const userLang = await resolveNotificationLanguage(msg.from.id.toString(), msg.from.language_code);
        if (chatType === 'private') {
            await bot.sendMessage(chatId, t(userLang, 'checkin_dm_use_button'));
            return;
        }

        const result = await initiateCheckinChallenge(chatId, msg.from, { replyMessage: msg });
        const responseLang = result.userLang || userLang;
        if (result.status === 'locked') {
            await bot.sendMessage(msg.from.id, t(responseLang, 'checkin_error_locked'));
        } else if (result.status === 'checked') {
            await bot.sendMessage(msg.from.id, t(responseLang, 'checkin_error_already_checked'));
        } else if (result.status === 'failed') {
            await bot.sendMessage(msg.from.id, t(responseLang, 'checkin_error_dm_failed'));
        } else {
            await bot.sendMessage(msg.from.id, t(responseLang, 'checkin_answer_sent_dm'));
        }
    });

    bot.onText(/^\/topcheckin(?:@[\w_]+)?(?:\s+(streak|total|points|longest))?$/, async (msg, match) => {
        const chatId = msg.chat.id.toString();
        const chatType = msg.chat?.type;
        const mode = (match && match[1]) ? match[1] : 'streak';
        const userLang = await resolveNotificationLanguage(msg.from.id.toString(), msg.from.language_code);
        if (chatType === 'private') {
            await bot.sendMessage(chatId, t(userLang, 'checkin_error_group_only'));
            return;
        }

        const boardLang = await resolveGroupLanguage(chatId);
        const text = await buildLeaderboardText(chatId, mode, 10, boardLang);
        await sendMessageRespectingThread(chatId, msg, text);
    });

    bot.onText(/\/okxchains/, async (msg) => {
        const lang = await getLang(msg);
        try {
            const directory = await fetchOkxSupportedChains();
            if (!directory) {
                sendReply(msg, t(lang, 'okxchains_error'), { parse_mode: 'Markdown' });
                return;
            }

            const aggregatorLines = (directory.aggregator || []).slice(0, 20);
            const marketLines = (directory.market || []).slice(0, 20);

            const lines = [
                t(lang, 'okxchains_title'),
                t(lang, 'okxchains_aggregator_heading'),
                aggregatorLines.length > 0 ? aggregatorLines.map((line) => `• ${line}`).join('\n') : t(lang, 'okxchains_no_data'),
                '',
                t(lang, 'okxchains_market_heading'),
                marketLines.length > 0 ? marketLines.map((line) => `• ${line}`).join('\n') : t(lang, 'okxchains_no_data')
            ];

            sendReply(msg, lines.join('\n'), { parse_mode: 'Markdown' });
        } catch (error) {
            console.error(`[OkxChains] Failed to load supported chains: ${error.message}`);
            sendReply(msg, t(lang, 'okxchains_error'), { parse_mode: 'Markdown' });
        }
    });

    async function handleAdminCommand(msg) {
        const chatId = msg.chat.id;
        const userId = msg.from?.id;
        const chatType = msg.chat.type;

        if (!userId) {
            return;
        }

        if (chatType === 'private') {
            try {
                await openAdminHub(userId, {});
            } catch (error) {
                console.error(`[AdminHub] Failed to open hub for ${userId}: ${error.message}`);
                const lang = await getLang(msg);
                await sendReply(msg, t(lang, 'checkin_admin_command_error'));
            }
            return;
        }

        const isGroupChat = ['group', 'supergroup'].includes(chatType);
        const replyLang = isGroupChat
            ? await resolveGroupLanguage(chatId, defaultLang)
            : await getLang(msg);

        if (!isGroupChat) {
            await sendReply(msg, t(replyLang, 'checkin_admin_command_group_only'));
            return;
        }

        const isAdmin = await isGroupAdmin(chatId, userId);
        if (!isAdmin) {
            await bot.sendMessage(chatId, t(replyLang, 'checkin_admin_menu_no_permission'), {
                reply_to_message_id: msg.message_id,
                allow_sending_without_reply: true
            });
            return;
        }

        try {
            await openAdminHub(userId, {});
            await sendAdminMenu(userId, chatId);
            await bot.sendMessage(chatId, t(replyLang, 'checkin_admin_command_dm_notice'), {
                reply_to_message_id: msg.message_id,
                allow_sending_without_reply: true
            });
        } catch (error) {
            console.error(`[AdminHub] Failed to send admin hub for ${userId} in ${chatId}: ${error.message}`);
            const statusCode = error?.response?.statusCode;
            const errorKey = statusCode === 403
                ? 'checkin_admin_command_dm_error'
                : 'checkin_admin_command_error';

            await bot.sendMessage(chatId, t(replyLang, errorKey), {
                reply_to_message_id: msg.message_id,
                allow_sending_without_reply: true
            });
        }
    }

    bot.onText(/^\/checkinadmin(?:@[\w_]+)?$/, async (msg) => {
        await handleAdminCommand(msg);
    });

    bot.onText(/^\/admin(?:@[\w_]+)?$/, async (msg) => {
        await handleAdminCommand(msg);
    });

    bot.onText(/\/okx402status/, async (msg) => {
        const lang = await getLang(msg);
        try {
            const supported = await fetchOkx402Supported();
            const lines = [
                t(lang, 'okx402_title'),
                supported && supported.length > 0
                    ? t(lang, 'okx402_supported', { chains: supported.join(', ') })
                    : t(lang, 'okx402_not_supported')
            ];
            sendReply(msg, lines.join('\n'), { parse_mode: 'Markdown' });
        } catch (error) {
            console.error(`[Okx402] Failed to check x402 support: ${error.message}`);
            sendReply(msg, t(lang, 'okx402_error'), { parse_mode: 'Markdown' });
        }
    });

    bot.onText(/\/banmaoprice/, async (msg) => {
        const chatId = msg.chat.id.toString();
        const lang = await getLang(msg);

        try {
            const snapshot = await fetchBanmaoQuoteSnapshot();
            if (!snapshot || !Number.isFinite(snapshot.price)) {
                throw new Error('No price returned');
            }

            const priceUsdText = formatUsdPrice(snapshot.price);
            const priceOkbNumeric = Number(snapshot.priceOkb);
            const priceOkbText = Number.isFinite(priceOkbNumeric)
                ? formatTokenQuantity(priceOkbNumeric, { minimumFractionDigits: 8, maximumFractionDigits: 8 })
                : null;

            const fromAmountText = formatTokenAmountFromUnits(
                snapshot.fromAmount ?? snapshot.amount,
                snapshot.decimals,
                { minimumFractionDigits: 0, maximumFractionDigits: 6 }
            ) || '1';

            const toAmountText = formatTokenAmountFromUnits(
                snapshot.toAmount,
                snapshot.quoteDecimals,
                { minimumFractionDigits: 6, maximumFractionDigits: 8 }
            );

            const priceImpactText = Number.isFinite(snapshot.priceImpactPercent)
                ? formatPercentage(snapshot.priceImpactPercent, {
                    includeSign: true,
                    minimumFractionDigits: 2,
                    maximumFractionDigits: 2
                })
                : null;

            const feeText = Number.isFinite(Number(snapshot.tradeFeeUsd))
                ? formatUsdPrice(snapshot.tradeFeeUsd)
                : null;

            const sourceLabelParts = [];
            if (typeof snapshot.source === 'string' && snapshot.source.trim()) {
                sourceLabelParts.push(snapshot.source.trim());
            } else {
                sourceLabelParts.push('OKX DEX');
            }
            if (snapshot.chain && snapshot.chain !== '(default)') {
                sourceLabelParts.push(`chain: ${snapshot.chain}`);
            }
            const sourceLabel = sourceLabelParts.join(' · ');

            const fallbackValue = t(lang, 'okx_generic_no_data');

            const lines = [
                t(lang, 'banmaoprice_title'),
                t(lang, 'banmaoprice_price_usd', { priceUsd: priceUsdText }),
                priceOkbText ? t(lang, 'banmaoprice_price_okb', { priceOkb: priceOkbText }) : null,
                t(lang, 'banmaoprice_quote_line', {
                    fromAmount: fromAmountText,
                    fromSymbol: snapshot.fromSymbol || 'BANMAO',
                    toAmount: toAmountText || fallbackValue,
                    toSymbol: snapshot.toSymbol || 'USDT'
                }),
                feeText ? t(lang, 'banmaoprice_fee_line', { feeUsd: feeText }) : null,
                priceImpactText ? t(lang, 'banmaoprice_price_impact_line', { impact: priceImpactText }) : null,
                snapshot.routeLabel ? t(lang, 'banmaoprice_route_line', { route: snapshot.routeLabel }) : null,
                t(lang, 'banmaoprice_source_line', { source: sourceLabel })
            ].filter(Boolean);

            const successMessage = lines.join('\n');
            sendReply(msg, successMessage, { parse_mode: "Markdown" });
        } catch (error) {
            console.error(`[BanmaoPrice] Failed to fetch price: ${error.message}`);
            const errorMessage = t(lang, 'banmaoprice_error');
            sendReply(msg, errorMessage, { parse_mode: "Markdown" });
        }
    });

    // COMMAND: /banmaofeed - Chỉ dùng cho group
    bot.onText(/\/banmaofeed(?:\s+(.+))?/, async (msg, match) => {
        const chatId = msg.chat.id.toString();
        const chatType = msg.chat.type;
        const userLang = await getLang(msg);
        const sendInThread = (text, options = {}) => sendReply(msg, text, options);

        if (chatType !== 'group' && chatType !== 'supergroup') {
            await sendInThread(t(userLang, 'group_feed_group_only'), { parse_mode: "Markdown" });
            return;
        }

        let memberInfo;
        try {
            memberInfo = await bot.getChatMember(chatId, msg.from.id);
        } catch (error) {
            console.error(`[GroupFeed] Không thể lấy thông tin admin cho ${chatId}:`, error.message);
        }

        const isAdmin = memberInfo && ['administrator', 'creator'].includes(memberInfo.status);
        if (!isAdmin) {
            await sendInThread(t(userLang, 'group_feed_admin_only'), { parse_mode: "Markdown" });
            return;
        }

        const arg = (match && match[1]) ? match[1].trim() : '';

        try {
            if (!arg) {
                const current = await db.getGroupSubscription(chatId);
                const statusLine = current
                    ? t(userLang, 'group_feed_current_threshold', { amount: formatBanmao(current.minStake || 0) })
                    : t(userLang, 'group_feed_not_configured');
                const usage = t(userLang, 'group_feed_usage');
                await sendInThread(`${statusLine}\n\n${usage}`, { parse_mode: "Markdown" });
                return;
            }

            const lowered = arg.toLowerCase();
            if (['off', 'disable', 'stop', 'cancel'].includes(lowered)) {
                await db.removeGroupSubscription(chatId);
                await sendInThread(t(userLang, 'group_feed_disabled'), { parse_mode: "Markdown" });
                return;
            }

            const normalizedArg = arg.replace(',', '.');
            const minStake = parseFloat(normalizedArg);
            if (!Number.isFinite(minStake) || minStake < 0) {
                await sendInThread(t(userLang, 'group_feed_invalid_amount'), { parse_mode: "Markdown" });
                return;
            }

            const threadId = msg.message_thread_id;
            await db.upsertGroupSubscription(chatId, userLang, minStake, threadId);
            await sendInThread(t(userLang, 'group_feed_enabled', { amount: formatBanmao(minStake) }), { parse_mode: "Markdown" });
        } catch (error) {
            console.error(`[GroupFeed] Lỗi cấu hình cho nhóm ${chatId}:`, error.message);
            await sendInThread(t(userLang, 'group_feed_error'), { parse_mode: "Markdown" });
        }
    });

    // COMMAND: /feedtopic - cấu hình topic nhận thông báo nhóm
    bot.onText(/\/feedtopic(?:\s+(.+))?/, async (msg, match) => {
        const chatId = msg.chat.id.toString();
        const chatType = msg.chat.type;
        const lang = await getLang(msg);
        const sendInThread = (text, options = {}) => sendReply(msg, text, options);

        if (chatType !== 'group' && chatType !== 'supergroup') {
            await sendInThread(t(lang, 'feedtopic_group_only'), { parse_mode: "Markdown" });
            return;
        }

        let memberInfo = null;
        try {
            memberInfo = await bot.getChatMember(chatId, msg.from.id);
        } catch (error) {
            console.warn(`[FeedTopic] Không thể kiểm tra quyền admin cho ${chatId}: ${error.message}`);
        }

        const isAdmin = memberInfo && ['administrator', 'creator'].includes(memberInfo.status);
        if (!isAdmin) {
            await sendInThread(t(lang, 'feedtopic_admin_only'), { parse_mode: "Markdown" });
            return;
        }

        let subscription = null;
        try {
            subscription = await db.getGroupSubscription(chatId);
        } catch (error) {
            console.warn(`[FeedTopic] Không thể đọc cấu hình nhóm ${chatId}: ${error.message}`);
        }

        if (!subscription) {
            await sendInThread(t(lang, 'feedtopic_not_configured'), { parse_mode: "Markdown" });
            return;
        }

        const arg = (match && match[1]) ? match[1].trim() : '';
        if (!arg) {
            const currentThread = subscription.messageThreadId;
            const status = currentThread
                ? t(lang, 'feedtopic_current_set', { threadId: currentThread })
                : t(lang, 'feedtopic_current_default');
            const usageKey = msg.message_thread_id === undefined || msg.message_thread_id === null
                ? 'feedtopic_usage_no_thread'
                : 'feedtopic_usage_with_thread';
            const usage = t(lang, usageKey);
            await sendInThread(`${status}\n\n${usage}`, { parse_mode: "Markdown" });
            return;
        }

        const lowered = arg.toLowerCase();
        let desiredThread;
        let resolved = true;

        if (['general', 'default', 'clear', 'reset', 'off', 'none'].includes(lowered)) {
            desiredThread = null;
        } else if (['here', 'this', 'topic', 'thread'].includes(lowered)) {
            const currentThread = msg.message_thread_id;
            desiredThread = currentThread === undefined ? null : currentThread;
        } else {
            const trimmed = arg.replace(/^#/, '');
            if (/^\d+$/.test(trimmed)) {
                const parsed = Number(trimmed);
                if (Number.isInteger(parsed) && parsed > 0) {
                    desiredThread = parsed;
                } else {
                    resolved = false;
                }
            } else {
                resolved = false;
            }
        }

        if (!resolved) {
            await sendInThread(t(lang, 'feedtopic_invalid'), { parse_mode: "Markdown" });
            return;
        }

        try {
            await db.updateGroupSubscriptionTopic(chatId, desiredThread);
        } catch (error) {
            console.error(`[FeedTopic] Không thể cập nhật topic cho ${chatId}: ${error.message}`);
            await sendInThread(t(lang, 'feedtopic_error'), { parse_mode: "Markdown" });
            return;
        }

        const currentThread = desiredThread === null ? null : desiredThread.toString();
        const successMessage = desiredThread === null
            ? t(lang, 'feedtopic_cleared')
            : (msg.message_thread_id !== undefined && msg.message_thread_id !== null && desiredThread === msg.message_thread_id)
                ? t(lang, 'feedtopic_set_success_here')
                : t(lang, 'feedtopic_set_success_id', { threadId: currentThread });

        await sendInThread(successMessage, { parse_mode: "Markdown" });
    });

    // COMMAND: /feedlang - Cấu hình ngôn ngữ cá nhân cho thông báo nhóm
    bot.onText(/\/feedlang(?:\s+(.+))?/, async (msg, match) => {
        const chatId = msg.chat.id.toString();
        const chatType = msg.chat.type;
        const userId = msg.from.id.toString();
        const fallbackLang = resolveLangCode(msg.from.language_code);

        if (chatType !== 'group' && chatType !== 'supergroup') {
            sendReply(msg, t(fallbackLang, 'group_feed_member_language_group_only'), { parse_mode: "Markdown" });
            return;
        }

        let storedLang = null;
        let preferredLang = fallbackLang;
        try {
            storedLang = await db.getGroupMemberLanguage(chatId, userId);
            if (storedLang) {
                preferredLang = resolveLangCode(storedLang);
            }
        } catch (error) {
            console.warn(`[GroupFeed] Không thể đọc ngôn ngữ cá nhân cho ${userId} trong ${chatId}: ${error.message}`);
        }

        const arg = (match && match[1]) ? match[1].trim() : '';

        if (arg) {
            const lowered = arg.toLowerCase();
            if (['off', 'disable', 'stop', 'cancel', 'clear', 'remove'].includes(lowered)) {
                try {
                    await db.removeGroupMemberLanguage(chatId, userId);
                    sendReply(msg, t(preferredLang, 'group_feed_member_language_removed'), { parse_mode: "Markdown" });
                } catch (error) {
                    console.warn(`[GroupFeed] Không thể xóa ngôn ngữ cá nhân cho ${userId} trong ${chatId}: ${error.message}`);
                    sendReply(msg, t(preferredLang, 'group_feed_member_language_error'), { parse_mode: "Markdown" });
                }
                return;
            }
        }

        const keyboard = [
            [
                { text: "🇻🇳 Tiếng Việt", callback_data: `feedlang|vi|${chatId}` },
                { text: "🇺🇸 English", callback_data: `feedlang|en|${chatId}` }
            ],
            [
                { text: "🇨🇳 中文", callback_data: `feedlang|zh|${chatId}` },
                { text: "🇷🇺 Русский", callback_data: `feedlang|ru|${chatId}` }
            ],
            [
                { text: "🇰🇷 한국어", callback_data: `feedlang|ko|${chatId}` },
                { text: "🇮🇩 Indonesia", callback_data: `feedlang|id|${chatId}` }
            ]
        ];

        keyboard.push([
            { text: t(preferredLang, 'group_feed_member_language_disable_button'), callback_data: `feedlang|clear|${chatId}` }
        ]);

        const message = t(preferredLang, 'group_feed_member_language_prompt');
        sendReply(msg, message, {
            reply_markup: { inline_keyboard: keyboard },
            reply_to_message_id: msg.message_id,
            parse_mode: "Markdown"
        });
    });

    // COMMAND: /unregister - Cần async
    bot.onText(/\/unregister/, async (msg) => {
        const chatId = msg.chat.id.toString();
        const lang = await getLang(msg); // <-- SỬA LỖI
        const wallets = await db.getWalletsForUser(chatId);
        if (wallets.length === 0) {
            sendReply(msg, t(lang, 'mywallet_not_linked'));
            return;
        }
        const keyboard = wallets.map(wallet => {
            const shortWallet = `${wallet.substring(0, 5)}...${wallet.substring(wallet.length - 4)}`;
            return [{ text: `❌ ${shortWallet}`, callback_data: `delete_${wallet}` }];
        });
        keyboard.push([{ text: `🔥🔥 ${t(lang, 'unregister_all')} 🔥🔥`, callback_data: 'delete_all' }]);
        sendReply(msg, t(lang, 'unregister_header'), {
            reply_markup: { inline_keyboard: keyboard }
        });
    });

    // LỆNH: /language - Cần async
    bot.onText(/\/language/, async (msg) => {
        const chatId = msg.chat.id.toString();
        const chatType = msg.chat.type;
        const lang = await getLang(msg); // <-- SỬA LỖI
        const isGroupChat = chatType === 'group' || chatType === 'supergroup';

        if (isGroupChat) {
            let memberInfo = null;
            try {
                memberInfo = await bot.getChatMember(chatId, msg.from.id);
            } catch (error) {
                console.warn(`[GroupLanguage] Không thể kiểm tra quyền admin cho ${chatId}: ${error.message}`);
            }

            const isAdmin = memberInfo && ['administrator', 'creator'].includes(memberInfo.status);
            if (!isAdmin) {
                const feedbackLang = resolveLangCode(msg.from.language_code || lang);
                sendReply(msg, t(feedbackLang, 'group_language_admin_only'), { parse_mode: "Markdown" });
                return;
            }
        }

        const textKey = isGroupChat ? 'select_group_language' : 'select_language';
        const text = t(lang, textKey);
        const options = {
            reply_markup: {
                inline_keyboard: [
                    [ { text: "🇻🇳 Tiếng Việt", callback_data: 'lang_vi' }, { text: "🇺🇸 English", callback_data: 'lang_en' } ],
                    [ { text: "🇨🇳 中文", callback_data: 'lang_zh' }, { text: "🇷🇺 Русский", callback_data: 'lang_ru' } ],
                    [ { text: "🇰🇷 한국어", callback_data: 'lang_ko' }, { text: "🇮🇩 Indonesia", callback_data: 'lang_id' } ]
                ]
            }
        };
        sendReply(msg, text, options);
    });

    // LỆNH: /help - Cần async
    bot.onText(/\/help/, async (msg) => {
        const lang = await getLang(msg);
        const helpText = buildHelpText(lang, 'user');
        const replyMarkup = buildHelpKeyboard(lang, 'user');
        await sendReply(msg, helpText, { parse_mode: 'HTML', reply_markup: replyMarkup });
    });

    // Xử lý tất cả CALLBACK QUERY (Nút bấm) - Cần async
    bot.on('callback_query', async (query) => {
        const queryId = query.id;
        const messageChatId = query.message?.chat?.id;
        const chatId = messageChatId ? messageChatId.toString() : null;
        const fallbackLang = resolveLangCode(query.from?.language_code || defaultLang);
        const lang = query.message ? await getLang(query.message) : fallbackLang; // <-- SỬA LỖI
        const callbackLang = await resolveNotificationLanguage(query.from.id, lang || fallbackLang);

        try {
            if (query.data === 'help_close') {
                if (query.message?.chat?.id && query.message?.message_id) {
                    try {
                        await bot.deleteMessage(query.message.chat.id, query.message.message_id);
                    } catch (error) {
                        // ignore deletion errors
                    }
                }
                await bot.answerCallbackQuery(queryId);
                return;
            }

            if (query.data.startsWith('help_view|')) {
                const [, requestedView] = query.data.split('|');
                const view = requestedView === 'admin' ? 'admin' : 'user';
                const helpText = buildHelpText(callbackLang, view);
                const replyMarkup = buildHelpKeyboard(callbackLang, view);

                if (query.message?.chat?.id && query.message?.message_id) {
                    try {
                        await bot.editMessageText(helpText, {
                            chat_id: query.message.chat.id,
                            message_id: query.message.message_id,
                            parse_mode: 'HTML',
                            reply_markup: replyMarkup
                        });
                    } catch (error) {
                        // ignore edit errors
                    }
                }

                await bot.answerCallbackQuery(queryId);
                return;
            }

            if (query.data.startsWith('help_cmd|')) {
                const [, commandKey] = query.data.split('|');
                const detail = HELP_COMMAND_DETAILS[commandKey];
                if (detail) {
                    const description = t(callbackLang, detail.descKey);
                    const maxLength = 195;
                    const trimmed = description.length > maxLength
                        ? `${description.slice(0, maxLength).trimEnd()}…`
                        : description;
                    await bot.answerCallbackQuery(queryId, {
                        text: trimmed || detail.command,
                        show_alert: trimmed.length > 120
                    });
                } else {
                    await bot.answerCallbackQuery(queryId);
                }
                return;
            }

            if (query.data === 'admin_hub_refresh') {
                try {
                    await openAdminHub(query.from.id, {});
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'admin_hub_refreshed') });
                } catch (error) {
                    console.error(`[AdminHub] Failed to refresh hub for ${query.from.id}: ${error.message}`);
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_command_error'), show_alert: true });
                }
                return;
            }

            if (query.data === 'admin_hub_from_menu') {
                try {
                    await openAdminHub(query.from.id, {});
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'admin_hub_refreshed') });
                } catch (error) {
                    console.error(`[AdminHub] Failed to open hub from menu for ${query.from.id}: ${error.message}`);
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_command_error'), show_alert: true });
                }
                return;
            }

            if (query.data === 'admin_hub_close') {
                const session = adminHubSessions.get(query.from.id);
                if (session?.messageId) {
                    try {
                        await bot.deleteMessage(query.from.id, session.messageId);
                    } catch (error) {
                        // ignore errors
                    }
                }
                adminHubSessions.delete(query.from.id);
                await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'admin_hub_closed') });
                return;
            }

            if (query.data.startsWith('admin_hub_open|')) {
                const [, targetChatId] = query.data.split('|');
                if (!targetChatId) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }

                const isAdminUser = await isGroupAdmin(targetChatId, query.from.id);
                if (!isAdminUser) {
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_error_no_permission'), show_alert: true });
                    return;
                }

                try {
                    await sendAdminMenu(query.from.id, targetChatId);
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_menu_opening') });
                } catch (error) {
                    console.error(`[AdminHub] Failed to open menu for ${query.from.id} in ${targetChatId}: ${error.message}`);
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_command_error'), show_alert: true });
                }
                return;
            }

            if (query.data.startsWith('checkin_start|')) {
                const parts = query.data.split('|');
                const targetChatId = (parts[1] || chatId || '').toString();
                if (!targetChatId) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }
                const result = await initiateCheckinChallenge(targetChatId, query.from, { replyMessage: query.message });
                const responseLang = result.userLang || callbackLang;

                if (result.status === 'locked') {
                    await bot.answerCallbackQuery(queryId, { text: t(responseLang, 'checkin_error_locked'), show_alert: true });
                } else if (result.status === 'checked') {
                    await bot.answerCallbackQuery(queryId, { text: t(responseLang, 'checkin_error_already_checked'), show_alert: true });
                } else if (result.status === 'failed') {
                    await bot.answerCallbackQuery(queryId, { text: t(responseLang, 'checkin_error_dm_failed'), show_alert: true });
                } else {
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_answer_sent_alert') });
                }
                return;
            }

            if (query.data.startsWith('checkin_answer|')) {
                const parts = query.data.split('|');
                const token = parts[1];
                const answerIndex = parts[2];
                await handleCheckinAnswerCallback(query, token, answerIndex);
                return;
            }

            if (query.data.startsWith('checkin_emotion_skip|')) {
                const parts = query.data.split('|');
                const token = parts[1];
                await handleEmotionCallback(query, token, null, { skip: true });
                return;
            }

            if (query.data.startsWith('checkin_emotion|')) {
                const parts = query.data.split('|');
                const token = parts[1];
                const emoji = parts[2] || '';
                await handleEmotionCallback(query, token, emoji);
                return;
            }

            if (query.data.startsWith('checkin_goal_choose|')) {
                const parts = query.data.split('|');
                await handleGoalCallback(query, parts[1], 'choose', parts[2] || '');
                return;
            }

            if (query.data.startsWith('checkin_goal_skip|')) {
                const parts = query.data.split('|');
                await handleGoalCallback(query, parts[1], 'skip');
                return;
            }

            if (query.data.startsWith('checkin_goal_custom|')) {
                const parts = query.data.split('|');
                await handleGoalCallback(query, parts[1], 'custom');
                return;
            }

            if (query.data.startsWith('checkin_leaderboard|')) {
                const parts = query.data.split('|');
                const targetChatId = (parts[1] || chatId || '').toString();
                if (!targetChatId) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }
                const mode = parts[2] || 'streak';
                const boardLang = await resolveGroupLanguage(targetChatId);
                const boardText = await buildLeaderboardText(targetChatId, mode, 10, boardLang);
                await sendMessageRespectingThread(targetChatId, query.message, boardText);
                await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_leaderboard_sent_alert') });
                return;
            }

            if (query.data === 'checkin_admin_noop') {
                await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_menu_board_hint') });
                return;
            }

            if (query.data.startsWith('checkin_admin_close|')) {
                const parts = query.data.split('|');
                const targetChatId = (parts[1] || chatId || '').toString();
                if (!targetChatId) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }
                const userKey = query.from.id.toString();
                checkinAdminStates.delete(userKey);
                pendingSecretMessages.delete(userKey);
                await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_menu_closed') });
                if (query.message?.chat?.id && query.message?.message_id) {
                    try {
                        await bot.deleteMessage(query.message.chat.id, query.message.message_id);
                    } catch (error) {
                        // ignore
                    }
                }
                await closeAdminMenu(query.from.id);
                return;
            }

            if (query.data.startsWith('checkin_admin_back|')) {
                const parts = query.data.split('|');
                const targetChatId = (parts[1] || chatId || '').toString();
                if (!targetChatId) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }
                const isAdminUser = await isGroupAdmin(targetChatId, query.from.id);
                if (!isAdminUser) {
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_error_no_permission'), show_alert: true });
                    return;
                }

                const userKey = query.from.id.toString();
                checkinAdminStates.delete(userKey);
                pendingSecretMessages.delete(userKey);

                if (query.message?.chat?.id && query.message?.message_id) {
                    try {
                        await bot.deleteMessage(query.message.chat.id, query.message.message_id);
                    } catch (error) {
                        // ignore
                    }
                }

                await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_menu_backing') });
                await sendAdminMenu(query.from.id, targetChatId);
                return;
            }

            if (query.data.startsWith('checkin_admin_refresh|')) {
                const parts = query.data.split('|');
                const targetChatId = (parts[1] || chatId || '').toString();
                if (!targetChatId) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }
                const isAdminUser = await isGroupAdmin(targetChatId, query.from.id);
                if (!isAdminUser) {
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_error_no_permission'), show_alert: true });
                    return;
                }

                await sendAdminMenu(query.from.id, targetChatId);
                await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_menu_refreshed') });
                return;
            }

            if (query.data.startsWith('checkin_admin_cancel_input|')) {
                const parts = query.data.split('|');
                const targetChatId = (parts[1] || chatId || '').toString();
                if (!targetChatId) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }
                const isAdminUser = await isGroupAdmin(targetChatId, query.from.id);
                if (!isAdminUser) {
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_error_no_permission'), show_alert: true });
                    return;
                }
                const userKey = query.from.id.toString();
                const adminState = checkinAdminStates.get(userKey);
                const secretState = pendingSecretMessages.get(userKey);
                if (adminState?.promptMessageId) {
                    try {
                        await bot.deleteMessage(query.from.id, adminState.promptMessageId);
                    } catch (error) {
                        // ignore
                    }
                }
                if (secretState?.promptMessageId) {
                    try {
                        await bot.deleteMessage(query.from.id, secretState.promptMessageId);
                    } catch (error) {
                        // ignore
                    }
                }
                checkinAdminStates.delete(userKey);
                pendingSecretMessages.delete(userKey);

                if (query.message?.chat?.id && query.message?.message_id) {
                    try {
                        await bot.deleteMessage(query.message.chat.id, query.message.message_id);
                    } catch (error) {
                        // ignore
                    }
                }

                await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_menu_cancelled') });
                await sendAdminMenu(query.from.id, targetChatId);
                return;
            }

            if (query.data.startsWith('checkin_admin_user_prompt|')) {
                const parts = query.data.split('|');
                const targetChatId = (parts[1] || chatId || '').toString();
                if (!targetChatId) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }
                const isAdminUser = await isGroupAdmin(targetChatId, query.from.id);
                if (!isAdminUser) {
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_error_no_permission'), show_alert: true });
                    return;
                }

                try {
                    const groupLang = await resolveGroupLanguage(targetChatId);
                    await bot.sendMessage(targetChatId, t(groupLang, 'checkin_admin_user_prompt_text'), {
                        disable_web_page_preview: true
                    });

                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_user_prompt_alert') });
                } catch (error) {
                    console.error(`[Checkin] Failed to broadcast member guide for ${targetChatId}: ${error.message}`);
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_command_error'), show_alert: true });
                }
                return;
            }

            if (query.data.startsWith('checkin_admin_user_leaderboard|')) {
                const parts = query.data.split('|');
                const targetChatId = (parts[1] || chatId || '').toString();
                if (!targetChatId) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }
                const isAdminUser = await isGroupAdmin(targetChatId, query.from.id);
                if (!isAdminUser) {
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_error_no_permission'), show_alert: true });
                    return;
                }

                try {
                    const groupLang = await resolveGroupLanguage(targetChatId);
                    await bot.sendMessage(targetChatId, t(groupLang, 'checkin_admin_user_leaderboard_text'), {
                        disable_web_page_preview: true
                    });

                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_user_leaderboard_alert') });
                } catch (error) {
                    console.error(`[Checkin] Failed to broadcast leaderboard guide for ${targetChatId}: ${error.message}`);
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_command_error'), show_alert: true });
                }
                return;
            }

            if (query.data.startsWith('checkin_admin_list|')) {
                const parts = query.data.split('|');
                const targetChatId = (parts[1] || chatId || '').toString();
                if (!targetChatId) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }
                const isAdminUser = await isGroupAdmin(targetChatId, query.from.id);
                if (!isAdminUser) {
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_error_no_permission_action'), show_alert: true });
                    return;
                }
                await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_list_progress_alert') });
                await sendTodayCheckinList(targetChatId, query.from.id);
                return;
            }

            if (query.data.startsWith('checkin_admin_broadcast|')) {
                const parts = query.data.split('|');
                const targetChatId = (parts[1] || chatId || '').toString();
                if (!targetChatId) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }
                const isAdminUser = await isGroupAdmin(targetChatId, query.from.id);
                if (!isAdminUser) {
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_error_no_permission'), show_alert: true });
                    return;
                }
                await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_broadcast_progress_alert') });
                await sendCheckinAnnouncement(targetChatId, { triggeredBy: 'manual' });
                return;
            }

            if (query.data.startsWith('checkin_admin_remove_confirm|')) {
                const parts = query.data.split('|');
                const targetChatId = (parts[1] || chatId || '').toString();
                if (!targetChatId) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }
                const targetUserId = parts[2];
                const isAdminUser = await isGroupAdmin(targetChatId, query.from.id);
                if (!isAdminUser) {
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_error_no_permission'), show_alert: true });
                    return;
                }
                if (query.message?.chat?.id && query.message?.message_id) {
                    try {
                        await bot.deleteMessage(query.message.chat.id, query.message.message_id);
                    } catch (error) {
                        // ignore
                    }
                }
                await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_remove_progress_alert') });
                await executeAdminRemoval(targetChatId, query.from.id, targetUserId);
                return;
            }

            if (query.data.startsWith('checkin_admin_remove|')) {
                const parts = query.data.split('|');
                const targetChatId = (parts[1] || chatId || '').toString();
                if (!targetChatId) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }
                const isAdminUser = await isGroupAdmin(targetChatId, query.from.id);
                if (!isAdminUser) {
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_error_no_permission'), show_alert: true });
                    return;
                }
                await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_remove_choose_prompt') });
                await promptAdminForRemoval(targetChatId, query.from.id);
                return;
            }

            if (query.data.startsWith('checkin_admin_unlock_confirm|')) {
                const parts = query.data.split('|');
                const targetChatId = (parts[1] || chatId || '').toString();
                if (!targetChatId) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }
                const targetUserId = parts[2];
                const isAdminUser = await isGroupAdmin(targetChatId, query.from.id);
                if (!isAdminUser) {
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_error_no_permission'), show_alert: true });
                    return;
                }
                if (query.message?.chat?.id && query.message?.message_id) {
                    try {
                        await bot.deleteMessage(query.message.chat.id, query.message.message_id);
                    } catch (error) {
                        // ignore
                    }
                }
                await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_unlock_progress_alert') });
                await executeAdminUnlock(targetChatId, query.from.id, targetUserId);
                return;
            }

            if (query.data.startsWith('checkin_admin_unlock|')) {
                const parts = query.data.split('|');
                const targetChatId = (parts[1] || chatId || '').toString();
                if (!targetChatId) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }
                const isAdminUser = await isGroupAdmin(targetChatId, query.from.id);
                if (!isAdminUser) {
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_error_no_permission'), show_alert: true });
                    return;
                }
                await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_unlock_choose_prompt') });
                await promptAdminUnlock(targetChatId, query.from.id);
                return;
            }

            if (query.data.startsWith('checkin_admin_dm_target|')) {
                const parts = query.data.split('|');
                const targetChatId = (parts[1] || chatId || '').toString();
                if (!targetChatId) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }
                const targetUserId = parts[2];
                const isAdminUser = await isGroupAdmin(targetChatId, query.from.id);
                if (!isAdminUser) {
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_error_no_permission'), show_alert: true });
                    return;
                }
                if (query.message?.chat?.id && query.message?.message_id) {
                    try {
                        await bot.deleteMessage(query.message.chat.id, query.message.message_id);
                    } catch (error) {
                        // ignore
                    }
                }
                const promptMessage = await bot.sendMessage(query.from.id, t(callbackLang, 'checkin_admin_dm_enter_message', { user: targetUserId }), {
                    reply_markup: {
                        inline_keyboard: [
                            [
                                { text: t(callbackLang, 'checkin_admin_button_back'), callback_data: `checkin_admin_back|${targetChatId}` },
                                { text: t(callbackLang, 'checkin_admin_button_close'), callback_data: `checkin_admin_close|${targetChatId}` }
                            ],
                            [{ text: t(callbackLang, 'checkin_admin_button_cancel'), callback_data: `checkin_admin_cancel_input|${targetChatId}` }]
                        ]
                    }
                });
                pendingSecretMessages.set(query.from.id.toString(), {
                    chatId: targetChatId,
                    targetUserId,
                    promptMessageId: promptMessage.message_id
                });
                await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_dm_enter_prompt_alert') });
                return;
            }

            if (query.data.startsWith('checkin_admin_dm|')) {
                const parts = query.data.split('|');
                const targetChatId = (parts[1] || chatId || '').toString();
                if (!targetChatId) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }
                const isAdminUser = await isGroupAdmin(targetChatId, query.from.id);
                if (!isAdminUser) {
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_error_no_permission'), show_alert: true });
                    return;
                }
                await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_dm_choose_prompt_alert') });
                await promptAdminSecretMessage(targetChatId, query.from.id);
                return;
            }

            if (query.data.startsWith('checkin_admin_points_set|')) {
                const parts = query.data.split('|');
                const targetChatId = (parts[1] || chatId || '').toString();
                if (!targetChatId) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }
                const value = parts[2];
                const isAdminUser = await isGroupAdmin(targetChatId, query.from.id);
                if (!isAdminUser) {
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_error_no_permission'), show_alert: true });
                    return;
                }
                if (query.message?.chat?.id && query.message?.message_id) {
                    try {
                        await bot.deleteMessage(query.message.chat.id, query.message.message_id);
                    } catch (error) {
                        // ignore
                    }
                }
                await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_points_updated_alert') });
                await setAdminDailyPoints(targetChatId, query.from.id, value);
                return;
            }

            if (query.data.startsWith('checkin_admin_points_custom|')) {
                const parts = query.data.split('|');
                const targetChatId = (parts[1] || chatId || '').toString();
                if (!targetChatId) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }
                const isAdminUser = await isGroupAdmin(targetChatId, query.from.id);
                if (!isAdminUser) {
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_error_no_permission'), show_alert: true });
                    return;
                }
                if (query.message?.chat?.id && query.message?.message_id) {
                    try {
                        await bot.deleteMessage(query.message.chat.id, query.message.message_id);
                    } catch (error) {
                        // ignore
                    }
                }
                const promptMessage = await bot.sendMessage(query.from.id, t(callbackLang, 'checkin_admin_points_prompt'), {
                    reply_markup: {
                        inline_keyboard: [
                            [
                                { text: t(callbackLang, 'checkin_admin_button_back'), callback_data: `checkin_admin_back|${targetChatId}` },
                                { text: t(callbackLang, 'checkin_admin_button_close'), callback_data: `checkin_admin_close|${targetChatId}` }
                            ],
                            [{ text: t(callbackLang, 'checkin_admin_button_cancel'), callback_data: `checkin_admin_cancel_input|${targetChatId}` }]
                        ]
                    }
                });
                checkinAdminStates.set(query.from.id.toString(), {
                    type: 'points_custom',
                    chatId: targetChatId,
                    promptMessageId: promptMessage.message_id
                });
                await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_points_prompt_alert') });
                return;
            }

            if (query.data.startsWith('checkin_admin_points|')) {
                const parts = query.data.split('|');
                const targetChatId = (parts[1] || chatId || '').toString();
                if (!targetChatId) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }
                const isAdminUser = await isGroupAdmin(targetChatId, query.from.id);
                if (!isAdminUser) {
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_error_no_permission'), show_alert: true });
                    return;
                }
                await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_points_choose_prompt') });
                await promptAdminPoints(targetChatId, query.from.id);
                return;
            }

            if (query.data.startsWith('checkin_admin_summary_set|')) {
                const parts = query.data.split('|');
                const targetChatId = (parts[1] || chatId || '').toString();
                if (!targetChatId) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }
                const value = parts[2];
                const isAdminUser = await isGroupAdmin(targetChatId, query.from.id);
                if (!isAdminUser) {
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_error_no_permission'), show_alert: true });
                    return;
                }
                if (query.message?.chat?.id && query.message?.message_id) {
                    try {
                        await bot.deleteMessage(query.message.chat.id, query.message.message_id);
                    } catch (error) {
                        // ignore
                    }
                }
                await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_summary_updated_alert') });
                await setAdminSummaryWindow(targetChatId, query.from.id, value);
                return;
            }

            if (query.data.startsWith('checkin_admin_summary_custom|')) {
                const parts = query.data.split('|');
                const targetChatId = (parts[1] || chatId || '').toString();
                if (!targetChatId) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }
                const isAdminUser = await isGroupAdmin(targetChatId, query.from.id);
                if (!isAdminUser) {
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_error_no_permission'), show_alert: true });
                    return;
                }
                if (query.message?.chat?.id && query.message?.message_id) {
                    try {
                        await bot.deleteMessage(query.message.chat.id, query.message.message_id);
                    } catch (error) {
                        // ignore
                    }
                }
                const promptMessage = await bot.sendMessage(query.from.id, t(callbackLang, 'checkin_admin_summary_prompt'), {
                    reply_markup: {
                        inline_keyboard: [
                            [
                                { text: t(callbackLang, 'checkin_admin_button_back'), callback_data: `checkin_admin_back|${targetChatId}` },
                                { text: t(callbackLang, 'checkin_admin_button_close'), callback_data: `checkin_admin_close|${targetChatId}` }
                            ],
                            [{ text: t(callbackLang, 'checkin_admin_button_cancel'), callback_data: `checkin_admin_cancel_input|${targetChatId}` }]
                        ]
                    }
                });
                checkinAdminStates.set(query.from.id.toString(), {
                    type: 'summary_custom',
                    chatId: targetChatId,
                    promptMessageId: promptMessage.message_id
                });
                await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_summary_prompt_alert') });
                return;
            }

            if (query.data.startsWith('checkin_admin_summary|')) {
                const parts = query.data.split('|');
                const targetChatId = (parts[1] || chatId || '').toString();
                if (!targetChatId) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }
                const isAdminUser = await isGroupAdmin(targetChatId, query.from.id);
                if (!isAdminUser) {
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_error_no_permission'), show_alert: true });
                    return;
                }
                await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_summary_choose_prompt') });
                await promptAdminSummaryWindow(targetChatId, query.from.id);
                return;
            }

            if (query.data.startsWith('checkin_admin_reset_confirm|')) {
                const parts = query.data.split('|');
                const targetChatId = (parts[1] || chatId || '').toString();
                if (!targetChatId) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }
                const targetUserId = parts[2];
                const isAdminUser = await isGroupAdmin(targetChatId, query.from.id);
                if (!isAdminUser) {
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_error_no_permission'), show_alert: true });
                    return;
                }
                if (query.message?.chat?.id && query.message?.message_id) {
                    try {
                        await bot.deleteMessage(query.message.chat.id, query.message.message_id);
                    } catch (error) {
                        // ignore
                    }
                }
                await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_reset_progress_alert') });
                await executeAdminReset(targetChatId, query.from.id, targetUserId);
                return;
            }

            if (query.data.startsWith('checkin_admin_reset|')) {
                const parts = query.data.split('|');
                const targetChatId = (parts[1] || chatId || '').toString();
                if (!targetChatId) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }
                const isAdminUser = await isGroupAdmin(targetChatId, query.from.id);
                if (!isAdminUser) {
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_error_no_permission'), show_alert: true });
                    return;
                }
                await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_reset_choose_prompt') });
                await promptAdminResetQuestion(targetChatId, query.from.id);
                return;
            }

            if (query.data.startsWith('checkin_admin|')) {
                const parts = query.data.split('|');
                const targetChatId = (parts[1] || chatId || '').toString();
                if (!targetChatId) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }
                const isAdminUser = await isGroupAdmin(targetChatId, query.from.id);
                if (!isAdminUser) {
                    await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_menu_no_permission'), show_alert: true });
                    return;
                }
                await bot.answerCallbackQuery(queryId, { text: t(callbackLang, 'checkin_admin_menu_opening') });
                try {
                    await sendAdminMenu(query.from.id, targetChatId);
                } catch (error) {
                    console.error(`[Checkin] Không thể gửi menu quản lý: ${error.message}`);
                }
                return;
            }

            if (query.data.startsWith('lang_')) {
                const newLang = resolveLangCode(query.data.split('_')[1]);
                if (!chatId) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }
                const chatType = query.message.chat?.type;
                const isGroupChat = chatType === 'group' || chatType === 'supergroup';

                if (isGroupChat) {
                    let memberInfo = null;
                    try {
                        memberInfo = await bot.getChatMember(chatId, query.from.id);
                    } catch (error) {
                        console.warn(`[GroupLanguage] Không thể kiểm tra quyền admin cho ${chatId}: ${error.message}`);
                    }

                    const isAdmin = memberInfo && ['administrator', 'creator'].includes(memberInfo.status);
                    if (!isAdmin) {
                        const feedbackLang = resolveLangCode(query.from.language_code || newLang);
                        bot.answerCallbackQuery(queryId, { text: t(feedbackLang, 'group_language_admin_only'), show_alert: true });
                        return;
                    }
                }

                await db.setLanguage(chatId, newLang);

                if (isGroupChat) {
                    try {
                        const subscription = await db.getGroupSubscription(chatId);
                        if (subscription) {
                            await db.updateGroupSubscriptionLanguage(chatId, newLang);
                        }
                    } catch (error) {
                        console.warn(`[GroupLanguage] Không thể cập nhật ngôn ngữ broadcast cho nhóm ${chatId}: ${error.message}`);
                    }
                }

                const messageKey = isGroupChat ? 'group_language_changed_success' : 'language_changed_success';
                const message = t(newLang, messageKey); // Dùng newLang
                sendReply(query.message, message);
                console.log(`[BOT] ChatID ${chatId} đã đổi ngôn ngữ sang: ${newLang}`);
                bot.answerCallbackQuery(queryId, { text: message });
            }
            else if (query.data.startsWith('feedlang|')) {
                const parts = query.data.split('|');
                const actionOrLang = parts[1] || '';
                const targetGroupId = (parts[2] || query.message.chat?.id || '').toString();
                const memberId = query.from.id.toString();
                const fallbackMemberLang = resolveLangCode(query.from.language_code || defaultLang);

                if (!targetGroupId) {
                    bot.answerCallbackQuery(queryId, { text: t(fallbackMemberLang, 'group_feed_member_language_error') || 'Error' });
                    return;
                }

                if (actionOrLang === 'clear') {
                    try {
                        await db.removeGroupMemberLanguage(targetGroupId, memberId);
                        const clearedMessage = t(fallbackMemberLang, 'group_feed_member_language_removed');
                        try {
                            await bot.answerCallbackQuery(queryId, { text: clearedMessage });
                        } catch (answerErr) {
                            console.warn(`[GroupFeed] Không thể phản hồi callback: ${answerErr.message}`);
                        }

                        try {
                            await sendTelegramMessageWithRetry(memberId, clearedMessage, { parse_mode: "Markdown" });
                        } catch (error) {
                            const errorCode = error?.response?.body?.error_code;
                            if (errorCode === 403) {
                                console.warn(`[GroupFeed] Thành viên ${memberId} đã chặn bot khi thông báo hủy ngôn ngữ cá nhân.`);
                            } else {
                                console.warn(`[GroupFeed] Không thể gửi xác nhận hủy cho ${memberId}: ${error.message}`);
                            }
                        }
                    } catch (error) {
                        console.error(`[GroupFeed] Không thể xóa ngôn ngữ cá nhân cho ${memberId} tại ${targetGroupId}: ${error.message}`);
                        bot.answerCallbackQuery(queryId, { text: t(fallbackMemberLang, 'group_feed_member_language_error') || 'Error' });
                    }
                    return;
                }

                const selectedLang = resolveLangCode(actionOrLang || defaultLang);
                await db.setGroupMemberLanguage(targetGroupId, memberId, selectedLang);

                const successMessage = t(selectedLang, 'group_feed_member_language_saved');
                try {
                    await bot.answerCallbackQuery(queryId, { text: successMessage });
                } catch (answerErr) {
                    console.warn(`[GroupFeed] Không thể phản hồi callback: ${answerErr.message}`);
                }

                try {
                    await sendTelegramMessageWithRetry(memberId, successMessage, { parse_mode: "Markdown" });
                } catch (error) {
                    const errorCode = error?.response?.body?.error_code;
                    console.warn(`[GroupFeed] Không thể gửi tin nhắn riêng cho ${memberId}: ${error.message}`);
                    if (errorCode === 403) {
                        let groupLang = selectedLang;
                        try {
                            const subscription = await db.getGroupSubscription(targetGroupId);
                            if (subscription?.lang) {
                                groupLang = resolveLangCode(subscription.lang);
                            }
                        } catch (langErr) {
                            console.warn(`[GroupFeed] Không thể lấy ngôn ngữ nhóm ${targetGroupId}: ${langErr.message}`);
                        }

                        const mentionInfo = buildUserMention(query.from);
                        const warnMessage = t(groupLang, 'group_feed_member_language_dm_required', { user: mentionInfo.text });
                        const sendOptions = mentionInfo.parseMode ? { parse_mode: mentionInfo.parseMode } : undefined;
                        if (sendOptions) {
                            bot.sendMessage(targetGroupId, warnMessage, sendOptions);
                        } else {
                            bot.sendMessage(targetGroupId, warnMessage);
                        }
                    }
                }
            }
            else if (query.data.startsWith('delete_')) {
                if (!chatId || !query.message?.message_id) {
                    await bot.answerCallbackQuery(queryId);
                    return;
                }

                const walletToDelete = query.data.substring(7);
                if (walletToDelete === 'all') {
                    await db.removeAllWalletsFromUser(chatId);
                    const message = t(lang, 'unregister_all_success'); // Dùng lang đã lưu
                    bot.editMessageText(message, { chat_id: chatId, message_id: query.message.message_id });
                    bot.answerCallbackQuery(queryId, { text: message });
                } else {
                    await db.removeWalletFromUser(chatId, walletToDelete);
                    const message = t(lang, 'unregister_one_success', { wallet: walletToDelete }); // Dùng lang đã lưu
                    bot.editMessageText(message, { chat_id: chatId, message_id: query.message.message_id });
                    bot.answerCallbackQuery(queryId, { text: message });
                }
            }
        } catch (error) {
            console.error("Lỗi khi xử lý callback_query:", error);
            bot.answerCallbackQuery(queryId, { text: "Error!" });
        }
    });

    bot.on('message', async (msg) => {
        if (await handleGoalTextInput(msg)) {
            return;
        }

        const userId = msg.from?.id?.toString();
        if (!userId) {
            return;
        }

        const chatType = msg.chat?.type || '';

        if (chatType === 'private') {
            const lang = await resolveNotificationLanguage(userId, msg.from?.language_code);
            const secretState = pendingSecretMessages.get(userId);
            if (secretState) {
                const rawText = (msg.text || '').trim();
                if (!rawText) {
                    await sendEphemeralMessage(userId, t(lang, 'checkin_dm_secret_invalid'));
                    return;
                }

                const clipped = rawText.length > 500 ? rawText.slice(0, 500) : rawText;

                try {
                    if (secretState.promptMessageId) {
                        try {
                            await bot.deleteMessage(msg.chat.id, secretState.promptMessageId);
                        } catch (error) {
                            // ignore
                        }
                    }
                    const targetLang = await resolveNotificationLanguage(secretState.targetUserId);
                    await bot.sendMessage(secretState.targetUserId, t(targetLang, 'checkin_dm_secret_forward', { message: clipped }));
                    await sendEphemeralMessage(userId, t(lang, 'checkin_dm_secret_confirm'));
                } catch (error) {
                    console.error(`[Checkin] Không thể chuyển tiếp tin nhắn bí mật: ${error.message}`);
                    await sendEphemeralMessage(userId, t(lang, 'checkin_dm_secret_error'));
                } finally {
                    pendingSecretMessages.delete(userId);
                }

                if (secretState.chatId) {
                    await sendAdminMenu(msg.from.id, secretState.chatId);
                }
                return;
            }

            const adminState = checkinAdminStates.get(userId);
            if (adminState) {
                const rawText = (msg.text || '').trim();
                if (!rawText) {
                    await sendEphemeralMessage(userId, t(lang, 'checkin_error_input_invalid'));
                    return;
                }

                if (adminState.type === 'points_custom') {
                    const normalized = Number(rawText.replace(',', '.'));
                    if (!Number.isFinite(normalized) || normalized < 0) {
                        await sendEphemeralMessage(userId, t(lang, 'checkin_admin_points_invalid'));
                        return;
                    }
                    if (adminState.promptMessageId) {
                        try {
                            await bot.deleteMessage(msg.chat.id, adminState.promptMessageId);
                        } catch (error) {
                            // ignore
                        }
                    }
                    await setAdminDailyPoints(adminState.chatId, msg.from.id, normalized);
                    checkinAdminStates.delete(userId);
                    return;
                }

                if (adminState.type === 'summary_custom') {
                    const normalized = Number(rawText.replace(',', '.'));
                    if (!Number.isFinite(normalized) || normalized <= 0) {
                        await sendEphemeralMessage(userId, t(lang, 'checkin_admin_summary_invalid'));
                        return;
                    }
                    if (adminState.promptMessageId) {
                        try {
                            await bot.deleteMessage(msg.chat.id, adminState.promptMessageId);
                        } catch (error) {
                            // ignore
                        }
                    }
                    await setAdminSummaryWindow(adminState.chatId, msg.from.id, normalized);
                    checkinAdminStates.delete(userId);
                    return;
                }
            }
        }
    });

    bot.on('polling_error', (error) => {
        console.error(`[LỖI BOT POLLING]: ${error.message}`);
    });

    console.log('✅ [Telegram Bot] Đang chạy...');
}

// ==========================================================
// 🎧 PHẦN 4: LOGIC LẮNG NGHE BLOCKCHAIN (Cần async)
// ==========================================================
async function waitForNetworkConnection(wsProvider) {
    const timeoutMs = 10000;
    const networkPromise = wsProvider.getNetwork();
    const timeoutPromise = new Promise((_, reject) =>
        setTimeout(() => reject(new Error(`WSS connection timed out after ${timeoutMs / 1000} seconds`)), timeoutMs)
    );
    await Promise.race([networkPromise, timeoutPromise]);
}

async function cleanupBlockchainResources() {
    if (contract) {
        contract.removeAllListeners();
        contract = null;
    }
    if (provider) {
        provider.removeAllListeners?.();
        try {
            await provider.destroy();
        } catch (error) {
            console.warn(`[Blockchain] Lỗi khi hủy provider: ${error.message}`);
        }
        provider = null;
    }
    roomCache.clear();
}

function scheduleReconnect() {
    if (reconnectTimeout) {
        return;
    }
    reconnectAttempts += 1;
    const delay = Math.min(30000, 2000 * reconnectAttempts);
    console.warn(`[Blockchain] Mất kết nối WSS. Thử kết nối lại sau ${Math.round(delay / 1000)}s (lần ${reconnectAttempts}).`);
    reconnectTimeout = setTimeout(async () => {
        reconnectTimeout = null;
        try {
            await startBlockchainListener(true);
        } catch (error) {
            console.error(`[Blockchain] Lỗi khi kết nối lại: ${error.message}`);
            scheduleReconnect();
        }
    }, delay);
}

function attachWebSocketHandlers(wsProvider) {
    try {
        const socket = wsProvider.websocket;
        if (socket && typeof socket.on === 'function') {
            socket.on('close', (event) => {
                const code = event?.code ?? 'unknown';
                console.warn(`[Blockchain] WebSocket đóng (code: ${code}).`);
                scheduleReconnect();
            });
            socket.on('error', (error) => {
                const message = error?.message || error;
                console.error(`[Blockchain] WebSocket lỗi: ${message}`);
                scheduleReconnect();
            });
        } else if (socket) {
            socket.onclose = (event) => {
                const code = event?.code ?? 'unknown';
                console.warn(`[Blockchain] WebSocket đóng (code: ${code}).`);
                scheduleReconnect();
            };
            socket.onerror = (error) => {
                const message = error?.message || error;
                console.error(`[Blockchain] WebSocket lỗi: ${message}`);
                scheduleReconnect();
            };
        }
    } catch (error) {
        console.warn(`[Blockchain] Không thể gắn handler WebSocket: ${error.message}`);
    }
}

async function startBlockchainListener(isReconnect = false) {
    try {
        await cleanupBlockchainResources();

        provider = new ethers.WebSocketProvider(RPC_URL);
        provider.on('error', (error) => {
            console.error(`[LỖI WSS Provider]: ${error.message}. Bot sẽ tự động thử kết nối lại.`);
            scheduleReconnect();
        });

        attachWebSocketHandlers(provider);

        await waitForNetworkConnection(provider);

        contract = new ethers.Contract(CONTRACT_ADDRESS, contractABI, provider);
        registerBlockchainEvents();

        reconnectAttempts = 0;
        const prefix = isReconnect ? '🔁' : '🎧';
        console.log(`${prefix} [Blockchain] Đang lắng nghe sự kiện từ contract: ${CONTRACT_ADDRESS}`);
    } catch (error) {
        console.error(`[Blockchain] Lỗi khi khởi tạo listener: ${error.message}`);
        await cleanupBlockchainResources();
        if (!isReconnect) {
            throw error;
        }
        scheduleReconnect();
    }
}

function registerBlockchainEvents() {
    if (!contract) return;

    contract.on("RoomCreated", handleRoomCreatedEvent);
    contract.on("Joined", handleJoinedEvent);
    contract.on("Committed", handleCommittedEvent);
    contract.on("Revealed", handleRevealedEvent);
    contract.on("Resolved", handleResolvedEvent);
    contract.on("Canceled", handleCanceledEvent);
    contract.on("Forfeited", handleForfeitedEvent);
}

async function handleRoomCreatedEvent(roomId, creator, stake) {
    const roomIdStr = toRoomIdString(roomId);
    console.log(`[SỰ KIỆN] Room ${roomIdStr} được tạo bởi ${creator}`);
    try {
        clearRoomFinalOutcome(roomId);

        const creatorAddress = normalizeAddress(creator);
        let stakeWei;
        if (typeof stake === 'bigint') {
            stakeWei = stake;
        } else if (stake && typeof stake.toString === 'function') {
            stakeWei = BigInt(stake.toString());
        }

        const snapshot = {};
        if (creatorAddress) snapshot.creator = creatorAddress;
        if (stakeWei !== undefined) snapshot.stakeWei = stakeWei;
        updateRoomCache(roomId, snapshot);

        await getRoomState(roomId, { refresh: true });
    } catch (err) {
        console.warn(`[RoomCreated] Không thể cập nhật cache cho phòng ${roomIdStr}: ${err.message}`);
    }
}

async function handleJoinedEvent(roomId, opponent) {
    const roomIdStr = toRoomIdString(roomId);
    console.log(`[SỰ KIỆN] Room ${roomIdStr} đã có người tham gia: ${opponent}`);
    try {
        const roomState = await getRoomState(roomId, { refresh: true });
        if (!roomState) {
            console.warn(`[Joined] Không thể xác định thông tin phòng ${roomIdStr}.`);
            return;
        }

        let opponentAddress = roomState.opponent;
        if (!opponentAddress) {
            opponentAddress = normalizeAddress(opponent);
            if (opponentAddress) {
                updateRoomCache(roomId, { opponent: opponentAddress });
            }
        }

        const creatorAddress = roomState.creator;
        if (!creatorAddress || !opponentAddress) {
            console.warn(`[Joined] Thiếu thông tin người chơi cho phòng ${roomIdStr}.`);
            return;
        }

        const stake = roomState.stakeWei !== undefined ? ethers.formatEther(roomState.stakeWei) : '0';

        await Promise.all([
            sendInstantNotification(creatorAddress, 'notify_opponent_joined', { roomId: roomIdStr, opponent: opponentAddress, stake }),
            sendInstantNotification(opponentAddress, 'notify_self_joined', { roomId: roomIdStr, creator: creatorAddress, stake })
        ]);
    } catch (err) {
        console.error(`[Lỗi] Không thể lấy thông tin phòng ${roomIdStr}:`, err.message);
    }
}

async function handleCommittedEvent(roomId, player) {
    const roomIdStr = toRoomIdString(roomId);
    console.log(`[SỰ KIỆN] Room ${roomIdStr} có người commit: ${player}`);
    try {
        const roomState = await getRoomState(roomId, { refresh: true });
        if (!roomState) {
            console.warn(`[Commit] Không thể xác định thông tin phòng ${roomIdStr}.`);
            return;
        }

        const playerAddress = normalizeAddress(player);
        const creatorAddress = roomState.creator;
        const opponentAddress = roomState.opponent;

        if (!playerAddress || !creatorAddress) {
            console.warn(`[Commit] Thiếu dữ liệu người chơi ở phòng ${roomIdStr}.`);
            return;
        }

        const otherPlayer = (playerAddress === creatorAddress) ? opponentAddress : creatorAddress;
        if (otherPlayer) {
            const stake = roomState.stakeWei !== undefined ? ethers.formatEther(roomState.stakeWei) : '0';
            await sendInstantNotification(otherPlayer, 'notify_opponent_committed', { roomId: roomIdStr, opponent: playerAddress, stake });
        }
    } catch (err) {
        console.error(`[Lỗi] Không thể lấy thông tin phòng ${roomIdStr} (sau commit):`, err.message);
    }
}

async function handleRevealedEvent(roomId, player, choice) {
    const roomIdStr = toRoomIdString(roomId);
    console.log(`[SỰ KIỆN] Room ${roomIdStr} có người reveal: ${player}`);
    try {
        const roomState = await getRoomState(roomId, { refresh: true });
        if (!roomState) {
            console.warn(`[Reveal] Không thể xác định thông tin phòng ${roomIdStr}.`);
            return;
        }

        const playerAddress = normalizeAddress(player);
        const creatorAddress = roomState.creator;
        const opponentAddress = roomState.opponent;

        if (!playerAddress || !creatorAddress) {
            console.warn(`[Reveal] Thiếu dữ liệu người chơi ở phòng ${roomIdStr}.`);
            return;
        }

        const numericChoice = choice !== undefined ? Number(choice) : undefined;
        if (numericChoice !== undefined && !Number.isNaN(numericChoice)) {
            if (creatorAddress === playerAddress) {
                updateRoomCache(roomId, { revealA: numericChoice });
            } else if (opponentAddress === playerAddress) {
                updateRoomCache(roomId, { revealB: numericChoice });
            }
        }

        const otherPlayer = (playerAddress === creatorAddress) ? opponentAddress : creatorAddress;

        if (otherPlayer) {
            const stake = roomState.stakeWei !== undefined ? ethers.formatEther(roomState.stakeWei) : '0';
            await sendInstantNotification(otherPlayer, 'notify_opponent_revealed', { roomId: roomIdStr, opponent: playerAddress, stake });
        }
    } catch (err) {
        console.error(`[Lỗi] Không thể lấy thông tin phòng ${roomIdStr} (sau reveal):`, err.message);
    }
}

async function handleResolvedEvent(roomId, winner, payout, fee) {
    const roomIdStr = toRoomIdString(roomId);
    try {
        const priorOutcome = getRoomFinalOutcome(roomId);
        if (priorOutcome && priorOutcome.outcome !== 'timeout') {
            console.log(`[Resolve] Room ${roomIdStr} đã có kết quả cuối cùng '${priorOutcome.outcome}', bỏ qua sự kiện.`);
            return;
        }

        const roomState = await getRoomState(roomId, { refresh: true }) || await getRoomState(roomId, { refresh: false });
        if (!roomState) {
            console.warn(`[Resolve] Không tìm thấy dữ liệu phòng ${roomIdStr}.`);
            return;
        }

        const creatorAddress = roomState.creator;
        const opponentAddress = roomState.opponent;
        const normalizedWinner = winner === ethers.ZeroAddress ? null : normalizeAddress(winner);
        const payoutWeiValue = toBigIntSafe(payout);
        const stakeWeiValue = roomState.stakeWei !== undefined ? roomState.stakeWei : null;
        const payoutAmount = payoutWeiValue !== null ? ethers.formatEther(payoutWeiValue) : '0';
        const stakeAmount = stakeWeiValue !== null ? parseFloat(ethers.formatEther(stakeWeiValue)) : 0;
        const totalPotWei = stakeWeiValue !== null ? stakeWeiValue * 2n : null;
        const feeWei = payoutWeiValue !== null && totalPotWei !== null ? totalPotWei - payoutWeiValue : null;
        const loserLossWei = stakeWeiValue;
        const totalPotText = formatBanmaoFromWei(totalPotWei);
        const winnerPayoutText = formatBanmaoFromWei(payoutWeiValue);
        const feeAmountText = formatBanmaoFromWei(feeWei);
        const loserLossText = formatBanmaoFromWei(loserLossWei);
        const creatorChoice = Number(roomState.revealA ?? 0);
        const opponentChoice = Number(roomState.revealB ?? 0);

        if (!creatorAddress) {
            console.warn(`[Resolve] Thiếu địa chỉ creator cho phòng ${roomIdStr}.`);
            clearRoomCache(roomId);
            return;
        }

        const hasOpponent = Boolean(opponentAddress);
        const isDraw = hasOpponent && (!normalizedWinner || (creatorChoice !== 0 && creatorChoice === opponentChoice));

        if (isDraw) {
            const handled = await finalizeDrawOutcome(roomId, roomState, { source: 'Resolve' });
            if (!handled) {
                console.warn(`[Resolve] Không thể xác nhận kết quả hòa cho phòng ${roomIdStr}.`);
            }
            return;
        }

        if (!normalizedWinner) {
            console.log(`[SỰ KIỆN] Room ${roomIdStr} có kết quả nhưng không xác định được người thắng.`);
            clearRoomCache(roomId);
            return;
        }

        console.log(`[SỰ KIỆN] Room ${roomIdStr} có kết quả: ${normalizedWinner} thắng`);

        markRoomFinalOutcome(roomId, 'win');

        const loserAddress = normalizedWinner === creatorAddress ? opponentAddress : creatorAddress;
        if (!loserAddress) {
            console.warn(`[SỰ KIỆN] Room ${roomIdStr} không xác định được người thua.`);
            clearRoomCache(roomId);
            return;
        }

        const winnerIsCreator = normalizedWinner === creatorAddress;
        const winnerChoice = winnerIsCreator ? creatorChoice : opponentChoice;
        const loserChoice = winnerIsCreator ? opponentChoice : creatorChoice;

        await Promise.all([
            sendInstantNotification(normalizedWinner, 'notify_game_win', {
                __messageBuilder: buildWinNotificationMessage,
                roomId: roomIdStr,
                payout: winnerPayoutText,
                myChoiceValue: winnerChoice,
                opponentChoiceValue: loserChoice,
                winnerPercent: '98%',
                totalPot: totalPotText,
                feePercent: '2%',
                feeAmount: feeAmountText,
                opponentLoss: loserLossText,
                opponentLossPercent: '100%',
                __choiceTranslations: [
                    { valueKey: 'myChoiceValue', targetKey: 'myChoice' },
                    { valueKey: 'opponentChoiceValue', targetKey: 'opponentChoice' }
                ]
            }),
            sendInstantNotification(loserAddress, 'notify_game_lose', {
                __messageBuilder: buildLoseNotificationMessage,
                roomId: roomIdStr,
                winner: normalizedWinner,
                myChoiceValue: loserChoice,
                opponentChoiceValue: winnerChoice,
                lostAmount: loserLossText,
                lostPercent: '100%',
                opponentPayout: winnerPayoutText,
                opponentPayoutPercent: '98%',
                totalPot: totalPotText,
                feePercent: '2%',
                feeAmount: feeAmountText,
                __choiceTranslations: [
                    { valueKey: 'myChoiceValue', targetKey: 'myChoice' },
                    { valueKey: 'opponentChoiceValue', targetKey: 'opponentChoice' }
                ]
            })
        ]);

        if (stakeAmount > 0) {
            await Promise.all([
                db.writeGameResult(normalizedWinner, 'win', stakeAmount),
                db.writeGameResult(loserAddress, 'lose', stakeAmount)
            ]);
        }

        await broadcastGroupGameUpdate('win', {
            roomId: roomIdStr,
            creatorAddress,
            opponentAddress,
            winnerAddress: normalizedWinner,
            loserAddress,
            stakeAmount,
            payoutAmount: parseFloat(payoutAmount),
            creatorChoice,
            opponentChoice
        });

        clearRoomCache(roomId);
    } catch (err) {
        console.error(`[Lỗi] Không thể lấy thông tin phòng ${roomIdStr} (sau resolve):`, err.message);
        clearRoomCache(roomId);
    }
}

function determineClaimTimeoutReason(room) {
    if (!room) {
        return { type: 'room_expired' };
    }

    const opponentValue = room.opponent ?? room.opponentAddress ?? null;
    const hasOpponent = Boolean(opponentValue && opponentValue !== ethers.ZeroAddress);

    const creatorCommitValue = room.commitA ?? room.commitCreator ?? null;
    const opponentCommitValue = room.commitB ?? room.commitOpponent ?? null;
    const creatorCommitted = Boolean(creatorCommitValue && creatorCommitValue !== ethers.ZeroHash);
    const opponentCommitted = Boolean(opponentCommitValue && opponentCommitValue !== ethers.ZeroHash);

    const creatorRevealValue = Number(room.revealA ?? room.creatorReveal ?? 0);
    const opponentRevealValue = Number(room.revealB ?? room.opponentReveal ?? 0);
    const creatorRevealed = creatorRevealValue !== 0;
    const opponentRevealed = opponentRevealValue !== 0;

    if (!hasOpponent) {
        return { type: 'no_opponent' };
    }

    if (!creatorCommitted && !opponentCommitted) {
        return { type: 'missing_commit', subject: 'both' };
    }
    if (!creatorCommitted) {
        return { type: 'missing_commit', subject: 'creator' };
    }
    if (!opponentCommitted) {
        return { type: 'missing_commit', subject: 'opponent' };
    }

    if (!creatorRevealed && !opponentRevealed) {
        return { type: 'missing_reveal', subject: 'both' };
    }
    if (!creatorRevealed) {
        return { type: 'missing_reveal', subject: 'creator' };
    }
    if (!opponentRevealed) {
        return { type: 'missing_reveal', subject: 'opponent' };
    }

    return { type: 'room_expired' };
}

function translateClaimTimeoutReason(lang, reasonInfo, perspective, addresses = {}) {
    if (!reasonInfo) {
        return t(lang, 'timeout_reason_room_expired');
    }

    if (reasonInfo.type === 'no_opponent') {
        const baseReason = t(lang, 'timeout_reason_no_opponent');
        const refundText = t(lang, 'timeout_refund_no_opponent');
        return `${baseReason} ${refundText}`.trim();
    }

    if (reasonInfo.type === 'room_expired') {
        return t(lang, 'timeout_reason_room_expired');
    }

    let subjectText;

    let claimerText = null;

    if (reasonInfo.subject === 'both') {
        subjectText = t(lang, 'timeout_subject_both');
        if (perspective === 'group') {
            const shortCreator = addresses.creator ? shortAddress(addresses.creator) : null;
            const shortOpponent = addresses.opponent ? shortAddress(addresses.opponent) : null;
            if (shortCreator && shortOpponent) {
                subjectText += ` (${shortCreator} & ${shortOpponent})`;
            }
        }
    } else if (perspective === 'group') {
        const subjectKey = reasonInfo.subject === 'creator' ? 'timeout_subject_creator' : 'timeout_subject_challenger';
        subjectText = t(lang, subjectKey);
        const address = reasonInfo.subject === 'creator' ? addresses.creator : addresses.opponent;
        if (address) {
            subjectText += ` (${shortAddress(address)})`;
        }

        const claimerRole = reasonInfo.subject === 'creator' ? 'challenger' : 'creator';
        const claimerKey = claimerRole === 'creator' ? 'timeout_subject_creator' : 'timeout_subject_challenger';
        claimerText = t(lang, claimerKey);
        const claimerAddress = claimerRole === 'creator' ? addresses.creator : addresses.opponent;
        if (claimerAddress) {
            claimerText += ` (${shortAddress(claimerAddress)})`;
        }
    } else {
        const isSelf = (reasonInfo.subject === 'creator' && perspective === 'creator') ||
            (reasonInfo.subject === 'opponent' && perspective === 'opponent');
        const subjectKey = isSelf ? 'timeout_subject_you' : 'timeout_subject_opponent';
        subjectText = t(lang, subjectKey);
    }

    const reasonKey = reasonInfo.type === 'missing_commit'
        ? 'timeout_reason_missing_commit'
        : 'timeout_reason_missing_reveal';

    const baseReason = t(lang, reasonKey, { subject: subjectText });

    let refundKey = null;
    let refundVariables = {};
    const isSelf = (reasonInfo.subject === 'creator' && perspective === 'creator') ||
        (reasonInfo.subject === 'opponent' && perspective === 'opponent');

    if (reasonInfo.type === 'missing_commit' && reasonInfo.subject === 'both') {
        refundKey = 'timeout_refund_missing_commit_both';
    } else if (reasonInfo.type === 'missing_reveal' && reasonInfo.subject === 'both') {
        refundKey = 'timeout_refund_missing_reveal_both';
    } else if (reasonInfo.type === 'missing_commit' || reasonInfo.type === 'missing_reveal') {
        if (reasonInfo.subject === 'creator' || reasonInfo.subject === 'opponent') {
            if (perspective === 'group') {
                refundKey = reasonInfo.subject === 'creator'
                    ? 'timeout_refund_missing_single_creator'
                    : 'timeout_refund_missing_single_challenger';
                refundVariables = { claimer: claimerText || '' };
            } else {
                refundKey = isSelf
                    ? 'timeout_refund_missing_single_self'
                    : 'timeout_refund_missing_single_opponent';
            }
        }
    }

    if (refundKey) {
        const refundText = t(lang, refundKey, refundVariables);
        return `${baseReason} ${refundText}`.trim();
    }

    return baseReason;
}

async function handleCanceledEvent(roomId) {
    const roomIdStr = toRoomIdString(roomId);
    console.log(`[SỰ KIỆN] Room ${roomIdStr} đã bị hủy (Claim Timeout)`);

    const existingOutcome = getRoomFinalOutcome(roomId);
    if (existingOutcome && existingOutcome.outcome !== 'timeout') {
        console.log(`[Timeout] Room ${roomIdStr} đã được đánh dấu '${existingOutcome.outcome}', bỏ qua thông báo claim timeout.`);
        return;
    }

    markRoomFinalOutcome(roomId, 'timeout');

    try {
        const roomState = await getRoomState(roomId, { refresh: true }) || await getRoomState(roomId, { refresh: false });
        if (!roomState) {
            console.warn(`[Timeout] Không tìm thấy dữ liệu phòng ${roomIdStr}.`);
            return;
        }

        const drawHandled = await finalizeDrawOutcome(roomId, roomState, { source: 'Cancel' });
        if (drawHandled) {
            return;
        }

        const stakeAmount = roomState.stakeWei !== undefined ? parseFloat(ethers.formatEther(roomState.stakeWei)) : 0;
        const creatorAddress = roomState.creator;
        const opponentAddress = roomState.opponent || null;

        if (!creatorAddress) {
            console.warn(`[Timeout] Thiếu địa chỉ creator cho phòng ${roomIdStr}.`);
            clearRoomCache(roomId);
            return;
        }

        const reasonInfo = determineClaimTimeoutReason(roomState);
        const addresses = { creator: creatorAddress, opponent: opponentAddress };

        const notificationTasks = [
            sendInstantNotification(creatorAddress, 'notify_claim_timeout', {
                roomId: roomIdStr,
                reasonInfo: { info: reasonInfo, perspective: 'creator', addresses }
            })
        ];

        if (opponentAddress) {
            notificationTasks.push(
                sendInstantNotification(opponentAddress, 'notify_claim_timeout', {
                    roomId: roomIdStr,
                    reasonInfo: { info: reasonInfo, perspective: 'opponent', addresses }
                })
            );
        }

        await Promise.all(notificationTasks);

        if (opponentAddress && stakeAmount > 0) {
            await Promise.all([
                db.writeGameResult(creatorAddress, 'draw', stakeAmount),
                db.writeGameResult(opponentAddress, 'draw', stakeAmount)
            ]);

            await broadcastGroupGameUpdate('timeout', {
                roomId: roomIdStr,
                creatorAddress,
                opponentAddress,
                stakeAmount,
                reasonInfo: { info: reasonInfo, addresses }
            });
        }
        clearRoomCache(roomId);
    } catch (err) {
        console.error(`[Lỗi] Không thể lấy thông tin phòng ${roomIdStr} (sau cancel):`, err.message);
        clearRoomCache(roomId);
    }
}

async function handleForfeitedEvent(roomId, loser, winner, winnerPayout) {
    const roomIdStr = toRoomIdString(roomId);
    console.log(`[SỰ KIỆN] Room ${roomIdStr} có người bỏ cuộc: ${loser}`);
    const winnerPayoutWei = toBigIntSafe(winnerPayout);
    const payoutAmount = formatBanmaoFromWei(winnerPayoutWei);
    const totalPotWei = winnerPayoutWei !== null ? (winnerPayoutWei * 10n) / 9n : null;
    const stakePerPlayerWei = totalPotWei !== null ? totalPotWei / 2n : null;
    const communityShareWei = totalPotWei !== null ? totalPotWei / 20n : null;
    const burnShareWei = communityShareWei;
    const totalPotAmount = formatBanmaoFromWei(totalPotWei);
    const opponentLossAmount = formatBanmaoFromWei(stakePerPlayerWei);
    const communityAmount = formatBanmaoFromWei(communityShareWei);
    const burnAmount = formatBanmaoFromWei(burnShareWei);
    const stakeAmount = totalPotWei !== null ? Number(ethers.formatEther(totalPotWei)) / 2 : 0;

    markRoomFinalOutcome(roomId, 'forfeit');

    try {
        const winnerAddress = ethers.getAddress(winner);
        const loserAddress = ethers.getAddress(loser);

        let creatorAddress = null;
        let opponentAddress = null;
        let creatorChoice = null;
        let opponentChoice = null;

        try {
            const roomState = await getRoomState(roomId, { refresh: true }) || await getRoomState(roomId, { refresh: false });
            if (roomState) {
                creatorAddress = roomState.creator || creatorAddress;
                opponentAddress = roomState.opponent || opponentAddress;
                creatorChoice = Number(roomState.revealA ?? creatorChoice ?? 0);
                opponentChoice = Number(roomState.revealB ?? opponentChoice ?? 0);
            }
        } catch (fetchErr) {
            console.warn(`[Forfeit] Không thể lấy dữ liệu phòng ${roomIdStr}:`, fetchErr.message);
        }

        if (!creatorAddress) creatorAddress = winnerAddress;
        if (!opponentAddress) opponentAddress = loserAddress;

        const winnerShort = shortAddress(winnerAddress);
        const loserShort = shortAddress(loserAddress);

        await Promise.all([
            sendInstantNotification(winnerAddress, 'notify_forfeit_win', {
                __messageBuilder: buildForfeitWinNotificationMessage,
                roomId: roomIdStr,
                loser: loserShort,
                payout: payoutAmount,
                payoutAmount,
                winnerPercent: '90%',
                totalPot: totalPotAmount,
                opponentLossAmount,
                opponentLossPercent: '100%',
                communityAmount,
                communityPercent: '5%',
                burnAmount,
                burnPercent: '5%'
            }),
            sendInstantNotification(loserAddress, 'notify_forfeit_lose', {
                __messageBuilder: buildForfeitLoseNotificationMessage,
                roomId: roomIdStr,
                winner: winnerShort,
                lostAmount: opponentLossAmount,
                lostPercent: '100%',
                opponentPayout: payoutAmount,
                winnerPercent: '90%',
                totalPot: totalPotAmount,
                communityAmount,
                communityPercent: '5%',
                burnAmount,
                burnPercent: '5%'
            })
        ]);

        if (stakeAmount > 0) {
            await Promise.all([
                db.writeGameResult(winnerAddress, 'win', stakeAmount),
                db.writeGameResult(loserAddress, 'lose', stakeAmount)
            ]);
        }

        await broadcastGroupGameUpdate('forfeit', {
            roomId: roomIdStr,
            creatorAddress,
            opponentAddress,
            winnerAddress,
            loserAddress,
            stakeAmount,
            payoutAmount: parseFloat(payoutAmount),
            creatorChoice,
            opponentChoice
        });
        clearRoomCache(roomId);
    } catch (error) {
        console.error(`[Lỗi] Khi xử lý sự kiện Forfeited cho room ${roomIdStr}:`, error.message);
        clearRoomCache(roomId);
    }
}

// ==========================================================
// 🚀 PHẦN 5: HÀM GỬI THÔNG BÁO (CHỈ GỬI TEXT)
// ==========================================================
async function sendTelegramMessageWithRetry(chatId, message, options, attempt = 1) {
    try {
        return await bot.sendMessage(chatId, message, options);
    } catch (error) {
        const errorCode = error?.response?.body?.error_code;
        const parameters = error?.response?.body?.parameters || {};
        const shouldRetry = (errorCode === 429 || errorCode === 500) && attempt < MAX_TELEGRAM_RETRIES;

        if (shouldRetry) {
            let waitSeconds = 1;
            if (typeof parameters.retry_after === 'number') {
                waitSeconds = parameters.retry_after;
            } else {
                waitSeconds = Math.min(2 ** attempt, 30);
            }

            const waitMs = Math.max(waitSeconds, 1) * 1000;
            console.warn(`[Notify] Gửi tin tới ${chatId} thất bại (mã ${errorCode}). Thử lại sau ${Math.round(waitMs / 1000)}s (lần ${attempt + 1}/${MAX_TELEGRAM_RETRIES}).`);
            await delay(waitMs);
            return sendTelegramMessageWithRetry(chatId, message, options, attempt + 1);
        }

        throw error;
    }
}

async function sendInstantNotification(playerAddress, langKey, variables = {}) {
    if (!playerAddress || playerAddress === ethers.ZeroAddress) return;

    let normalizedAddress;
    try {
        normalizedAddress = ethers.getAddress(playerAddress);
    } catch (error) {
        console.warn(`[Notify] Địa chỉ không hợp lệ: ${playerAddress}`);
        return;
    }

    const users = await db.getUsersForWallet(normalizedAddress);
    if (!users || users.length === 0) {
        console.log(`[Notify] Không tìm thấy user nào theo dõi ví ${normalizedAddress}. Bỏ qua.`);
        return;
    }

    for (const { chatId, lang } of users) {
        const langCode = await resolveNotificationLanguage(chatId, lang);
        const resolvedVariables = { ...variables };

        if (resolvedVariables.reasonInfo) {
            const info = resolvedVariables.reasonInfo.info;
            const perspective = resolvedVariables.reasonInfo.perspective || 'creator';
            const addresses = resolvedVariables.reasonInfo.addresses || {};
            resolvedVariables.reason = translateClaimTimeoutReason(langCode, info, perspective, addresses);
            delete resolvedVariables.reasonInfo;
        }

        applyChoiceTranslations(langCode, resolvedVariables);

        let message;

        if (typeof resolvedVariables.__messageBuilder === 'function') {
            const builder = resolvedVariables.__messageBuilder;
            delete resolvedVariables.__messageBuilder;
            message = builder(langCode, resolvedVariables);
        }

        if (!message) {
            message = t(langCode, langKey, resolvedVariables);
        }

        const button = {
            text: `🎮 ${t(langCode, 'action_button_play')}`,
            url: `${WEB_URL}/?join=${variables.roomId || ''}`
        };

        const options = {
            parse_mode: "Markdown",
            reply_markup: {
                inline_keyboard: [[button]]
            }
        };

        const isGameOver = langKey.startsWith('notify_game_') ||
            langKey.startsWith('notify_forfeit_') ||
            langKey.startsWith('notify_timeout_') ||
            langKey === 'notify_claim_timeout';
        if (isGameOver) {
            delete options.reply_markup;
        }

        try {
            await sendTelegramMessageWithRetry(chatId, message, options);
            console.log(`[Notify] Đã gửi thông báo TEXT '${langKey}' tới ${chatId}`);
        } catch (error) {
            console.error(`[Lỗi Gửi Text]: ${error.message}`);
        }
    }
}

async function broadcastGroupGameUpdate(eventType, payload) {
    const groups = await db.getGroupSubscriptions();
    if (!groups || groups.length === 0) {
        return;
    }

    const stakeAmount = Number(payload.stakeAmount || 0);

    const tasks = groups.map(async (group) => {
        const minStake = Number(group.minStake || 0);
        if (Number.isFinite(minStake) && stakeAmount < minStake) {
            return;
        }

        const lang = resolveLangCode(group.lang);
        const messagePayload = buildGroupBroadcastMessage(eventType, lang, payload);
        if (!messagePayload) {
            return;
        }

        const options = {
            parse_mode: "Markdown",
            disable_web_page_preview: true
        };

        if (group.messageThreadId !== undefined && group.messageThreadId !== null) {
            const numericThreadId = Number(group.messageThreadId);
            if (Number.isInteger(numericThreadId)) {
                options.message_thread_id = numericThreadId;
            }
        }

        if (messagePayload.withButton) {
            options.reply_markup = {
                inline_keyboard: [[{ text: `🔥 ${t(lang, 'group_broadcast_cta')}`, url: WEB_URL }]]
            };
        }

        try {
            await bot.sendMessage(group.chatId, messagePayload.text, options);
            console.log(`[Group Broadcast] Đã gửi ${eventType} tới nhóm ${group.chatId}`);
        } catch (error) {
            console.error(`[Group Broadcast] Lỗi gửi tới ${group.chatId}: ${error.message}`);
            const errorCode = error?.response?.body?.error_code;
            if (errorCode === 403 || errorCode === 400) {
                await db.removeGroupSubscription(group.chatId);
                console.warn(`[Group Broadcast] Đã xóa đăng ký nhóm ${group.chatId} (bot bị chặn/rời nhóm).`);
            }
        }

        let memberLanguages = [];
        try {
            memberLanguages = await db.getGroupMemberLanguages(group.chatId);
        } catch (error) {
            console.warn(`[Group Broadcast] Không thể lấy danh sách ngôn ngữ thành viên của nhóm ${group.chatId}: ${error.message}`);
        }

        if (Array.isArray(memberLanguages) && memberLanguages.length > 0) {
            const seenMembers = new Set();
            const payloadCache = new Map();

            for (const member of memberLanguages) {
                if (!member || !member.userId) {
                    continue;
                }

                const memberId = member.userId.toString();
                if (seenMembers.has(memberId)) {
                    continue;
                }
                seenMembers.add(memberId);

                const memberLang = resolveLangCode(member.lang || group.lang);
                if (!payloadCache.has(memberLang)) {
                    const built = buildGroupBroadcastMessage(eventType, memberLang, payload);
                    if (!built) {
                        continue;
                    }
                    payloadCache.set(memberLang, built);
                }

                const personalPayload = payloadCache.get(memberLang);
                if (!personalPayload) {
                    continue;
                }

                const dmOptions = { parse_mode: "Markdown", disable_web_page_preview: true };
                if (personalPayload.withButton) {
                    dmOptions.reply_markup = { inline_keyboard: [[{ text: `🔥 ${t(memberLang, 'group_broadcast_cta')}`, url: WEB_URL }]] };
                }

                try {
                    await sendTelegramMessageWithRetry(memberId, personalPayload.text, dmOptions);
                } catch (error) {
                    const errorCode = error?.response?.body?.error_code;
                    if (errorCode === 403) {
                        console.warn(`[Group Broadcast] Thành viên ${memberId} đã chặn bot khi gửi DM.`);
                    } else {
                        console.error(`[Group Broadcast] Lỗi gửi DM tới ${memberId}: ${error.message}`);
                    }
                }
            }
        }
    });

    await Promise.allSettled(tasks);
}

function buildGroupBroadcastMessage(eventType, lang, payload) {
    if (!payload) return null;

    const lines = [];
    const header = `🔥 *${t(lang, 'group_broadcast_header')}* 🔥`;
    lines.push(`🆔 ${t(lang, 'group_broadcast_room', { roomId: payload.roomId })}`);

    if (payload.creatorAddress && payload.opponentAddress) {
        lines.push(`🤼 ${t(lang, 'group_broadcast_players', {
            creator: shortAddress(payload.creatorAddress),
            opponent: shortAddress(payload.opponentAddress)
        })}`);
    }

    if (payload.creatorChoice !== undefined || payload.opponentChoice !== undefined) {
        const creatorChoiceStr = getChoiceString(payload.creatorChoice, lang);
        const opponentChoiceStr = getChoiceString(payload.opponentChoice, lang);
        if (payload.creatorChoice !== undefined || payload.opponentChoice !== undefined) {
            lines.push(`🃏 ${t(lang, 'group_broadcast_choices', {
                creatorChoice: creatorChoiceStr,
                opponentChoice: opponentChoiceStr
            })}`);
        }
    }

    if (payload.stakeAmount !== undefined) {
        lines.push(`💰 ${t(lang, 'group_broadcast_stake', { amount: formatBanmao(payload.stakeAmount) })}`);
    }

    let resultLine = null;
    if (eventType === 'win') {
        resultLine = `🏆 ${t(lang, 'group_broadcast_win', {
            winner: shortAddress(payload.winnerAddress),
            payout: formatBanmao(payload.payoutAmount)
        })}`;
    } else if (eventType === 'draw') {
        resultLine = `🤝 ${t(lang, 'group_broadcast_draw')}`;
    } else if (eventType === 'forfeit') {
        resultLine = `🚨 ${t(lang, 'group_broadcast_forfeit', {
            winner: shortAddress(payload.winnerAddress),
            loser: shortAddress(payload.loserAddress),
            payout: formatBanmao(payload.payoutAmount)
        })}`;
    } else if (eventType === 'timeout') {
        const reasonInfo = payload.reasonInfo || {};
        const info = reasonInfo.info;
        const addresses = reasonInfo.addresses || {
            creator: payload.creatorAddress,
            opponent: payload.opponentAddress
        };
        const reasonText = translateClaimTimeoutReason(lang, info, 'group', addresses);
        resultLine = `⏰ ${t(lang, 'group_broadcast_timeout', { reason: reasonText })}`;
    }

    if (!resultLine) {
        return null;
    }

    lines.push(resultLine);
    lines.push(`🔥 ${t(lang, 'group_broadcast_footer')}`);

    const text = [header, '', ...lines].join('\n');
    return { text, withButton: true };
}

// ==========================================================
// 🚀 KHỞI ĐỘNG TẤT CẢ DỊCH VỤ (CÁCH MỚI, AN TOÀN)
// ==========================================================
async function main() {
    try {
        console.log("Đang khởi động...");
        
        // Bước 1: Khởi tạo DB
        await db.init(); 

        // Bước 2: Kết nối Blockchain (WSS) và gắn listener
        console.log("Đang kết nối tới Blockchain (WSS)...");
        await startBlockchainListener();
        console.log("✅ [Blockchain] Kết nối WSS thành công.");

        // Bước 3: Bật API
        startApiServer();

        // Bước 4: Bật Bot (bộ 'miệng')
        startTelegramBot();
        startCheckinScheduler();

        console.log("🚀 TẤT CẢ DỊCH VỤ ĐÃ SẴN SÀNG!");

    } catch (error) {
        console.error("LỖI KHỞI ĐỘNG NGHIÊM TRỌNG:", error);
        process.exit(1);
    }
}

main(); // Chạy hàm khởi động chính