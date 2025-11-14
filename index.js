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
const OKX_BASE_URL = process.env.OKX_BASE_URL || 'https://www.okx.com';
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

function delay(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
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

    if (!query.chainShortName && !Number.isFinite(query.chainIndex) && !Number.isFinite(query.chainId)) {
        throw new Error('Unable to resolve OKX chain context');
    }

    const amount = amountOverride || await resolveBanmaoQuoteAmount(chainName);
    const requestQuery = {
        ...('chainShortName' in query ? { chainShortName: query.chainShortName } : {}),
        ...('chainIndex' in query ? { chainIndex: query.chainIndex } : {}),
        ...('chainId' in query ? { chainId: query.chainId } : {}),
        fromTokenAddress: OKX_BANMAO_TOKEN_ADDRESS,
        toTokenAddress: OKX_QUOTE_TOKEN_ADDRESS,
        amount,
        slippagePercent
    };

    const payload = await okxJsonRequest('GET', '/api/v6/dex/aggregator/quote', {
        query: requestQuery
    });

    const quoteEntry = unwrapOkxFirst(payload);
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

    return {
        price: priceInfo.price,
        priceOkb: Number.isFinite(priceOkb) ? priceOkb : null,
        okbUsd: Number.isFinite(okbUsd) ? okbUsd : null,
        chain: chainLabel,
        source: 'OKX DEX quote',
        amount,
        decimals: priceInfo.fromDecimals,
        quoteDecimals: priceInfo.toDecimals,
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

    return banmaoDecimalsCache !== null ? banmaoDecimalsCache : BANMAO_DECIMALS_DEFAULT;
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

async function fetchBanmaoLiquidityOverview() {
    const chainNames = getOkxChainShortNameCandidates();
    const errors = [];

    for (const chainName of chainNames) {
        try {
            const overview = await fetchBanmaoLiquidityForChain(chainName);
            if (overview) {
                return overview;
            }
        } catch (error) {
            errors.push(error);
        }
    }

    try {
        const fallbackOverview = await fetchBanmaoLiquidityForChain();
        if (fallbackOverview) {
            return fallbackOverview;
        }
    } catch (error) {
        errors.push(error);
    }

    if (errors.length > 0) {
        throw errors[errors.length - 1];
    }

    return null;
}

async function fetchBanmaoLiquidityForChain(chainName) {
    const query = await buildOkxDexQuery(chainName);
    const chainLabel = query.chainShortName || chainName || '(default)';
    const payload = await okxJsonRequest('GET', '/api/v6/dex/aggregator/get-liquidity', { query });
    const rows = unwrapOkxData(payload);

    if (!rows || rows.length === 0) {
        return {
            totalLiquidity: null,
            poolCount: 0,
            pools: [],
            chain: chainLabel
        };
    }

    const pools = rows.map((row, index) => {
        const liquidityUsd = pickOkxNumeric(row, ['usdLiquidity', 'liquidityUsd', 'valueUsd', 'usdValue', 'liquidity']);
        const dexName = typeof row?.dexName === 'string' ? row.dexName : null;
        const poolLabel =
            row?.poolName ||
            row?.poolId ||
            (dexName ? `${dexName} #${index + 1}` : `Pool #${index + 1}`);
        const quoteSymbol = row?.quoteTokenSymbol || row?.quoteToken || row?.quoteTokenAddress || null;

        return {
            name: poolLabel,
            liquidityUsd: Number.isFinite(liquidityUsd) ? liquidityUsd : null,
            dexName,
            quoteSymbol
        };
    });

    pools.sort((a, b) => (b.liquidityUsd || 0) - (a.liquidityUsd || 0));

    const totalLiquidity = pools.reduce((sum, pool) => {
        return sum + (Number.isFinite(pool.liquidityUsd) ? pool.liquidityUsd : 0);
    }, 0);

    return {
        totalLiquidity: Number.isFinite(totalLiquidity) && totalLiquidity > 0 ? totalLiquidity : null,
        poolCount: pools.length,
        pools,
        chain: chainLabel
    };
}

async function fetchBanmaoCandles(options = {}) {
    const { chainName, bar = '1H', limit = 24 } = options;
    const baseQuery = await buildOkxDexQuery(chainName);
    const query = { ...baseQuery, bar };
    if (limit) {
        query.limit = Math.min(Math.max(Number(limit) || 0, 1), 200);
    }

    const endpoints = [
        '/api/v6/dex/market/candles',
        '/api/v6/dex/market/historical-candles'
    ];

    const errors = [];

    for (const path of endpoints) {
        try {
            const payload = await okxJsonRequest('GET', path, { query });
            const rows = unwrapOkxData(payload);

            if (!rows || rows.length === 0) {
                continue;
            }

            const candles = rows
                .map((row) => normalizeOkxCandle(row))
                .filter((candle) => candle && Number.isFinite(candle.close) && Number.isFinite(candle.timestamp));

            if (candles.length === 0) {
                continue;
            }

            candles.sort((a, b) => a.timestamp - b.timestamp);
            return candles;
        } catch (error) {
            errors.push(new Error(`[${path}] ${error.message}`));
        }
    }

    if (errors.length > 0) {
        throw errors[errors.length - 1];
    }

    return [];
}

async function fetchBanmaoTokenProfile(options = {}) {
    const { chainName } = options;
    const query = await buildOkxDexQuery(chainName);
    const payload = await okxJsonRequest('GET', '/api/v6/dex/market/token/basic-info', { query });
    return unwrapOkxFirst(payload);
}

async function fetchBanmaoRecentTrades(options = {}) {
    const { chainName, limit = 1 } = options;
    const query = await buildOkxDexQuery(chainName);
    query.limit = Math.min(Math.max(Number(limit) || 0, 1), 50);

    const payload = await okxJsonRequest('GET', '/api/v6/dex/market/trades', { query });
    const data = unwrapOkxData(payload);
    if (!data || data.length === 0) {
        return [];
    }

    return data
        .map((row) => normalizeOkxTrade(row))
        .filter(Boolean)
        .sort((a, b) => b.timestamp - a.timestamp);
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

    const directory = await ensureOkxChainDirectory();
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

    if (context) {
        if (context.chainShortName) {
            query.chainShortName = context.chainShortName;
        } else if (chainName) {
            query.chainShortName = chainName;
        }

        if (Number.isFinite(context.chainIndex)) {
            query.chainIndex = context.chainIndex;
        } else if (Number.isFinite(OKX_CHAIN_INDEX)) {
            query.chainIndex = Number(OKX_CHAIN_INDEX);
        }

        if (Number.isFinite(context.chainId)) {
            query.chainId = context.chainId;
        }
    } else if (chainName) {
        query.chainShortName = chainName;
    }

    const includeToken = options.includeToken !== false;
    const includeQuote = options.includeQuote !== false;

    if (includeToken && OKX_BANMAO_TOKEN_ADDRESS) {
        query.tokenAddress = OKX_BANMAO_TOKEN_ADDRESS;
    }

    if (includeQuote && OKX_QUOTE_TOKEN_ADDRESS) {
        query.quoteTokenAddress = OKX_QUOTE_TOKEN_ADDRESS;
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
        const candidates = [directData.data, directData.items, directData.list, directData.rows, directData.result];
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

function normalizeOkxTimestamp(value) {
    const numeric = normalizeNumeric(value);
    if (Number.isFinite(numeric)) {
        if (numeric > 1e12) {
            return numeric;
        }
        if (numeric > 1e10) {
            return numeric;
        }
        if (numeric > 1e3) {
            return numeric * 1000;
        }
    }

    if (typeof value === 'string') {
        const parsed = Date.parse(value);
        if (!Number.isNaN(parsed)) {
            return parsed;
        }
    }

    return null;
}

function normalizeOkxCandle(entry) {
    if (!entry) {
        return null;
    }

    if (Array.isArray(entry)) {
        const [ts, open, high, low, close, volume] = entry;
        const timestamp = normalizeOkxTimestamp(ts);
        return {
            timestamp,
            open: normalizeNumeric(open),
            high: normalizeNumeric(high),
            low: normalizeNumeric(low),
            close: normalizeNumeric(close),
            volume: normalizeNumeric(volume)
        };
    }

    if (typeof entry === 'object') {
        const timestamp = normalizeOkxTimestamp(entry.timestamp || entry.ts || entry.time || entry[0]);
        return {
            timestamp,
            open: normalizeNumeric(entry.open ?? entry.o),
            high: normalizeNumeric(entry.high ?? entry.h),
            low: normalizeNumeric(entry.low ?? entry.l),
            close: normalizeNumeric(entry.close ?? entry.c ?? entry.last),
            volume: normalizeNumeric(entry.volume ?? entry.v ?? entry.vol ?? entry.quoteVolume)
        };
    }

    return null;
}

function normalizeOkxTrade(entry) {
    if (!entry) {
        return null;
    }

    if (Array.isArray(entry)) {
        const [ts, price, size, sideRaw] = entry;
        const timestamp = normalizeOkxTimestamp(ts);
        const amount = normalizeNumeric(size);
        const normalizedPrice = normalizeNumeric(price);
        const side = typeof sideRaw === 'string' ? sideRaw.toLowerCase() : null;
        return {
            timestamp,
            side,
            price: Number.isFinite(normalizedPrice) ? normalizedPrice : null,
            amount: Number.isFinite(amount) ? amount : null
        };
    }

    if (typeof entry === 'object') {
        const timestamp = normalizeOkxTimestamp(entry.timestamp || entry.ts || entry.time || entry.tradeTs);
        const sideRaw = entry.side || entry.direction || entry.tradeSide || entry.orderSide;
        const side = typeof sideRaw === 'string' ? sideRaw.toLowerCase() : null;
        const price = extractOkxPriceValue(entry);
        const amount = pickOkxNumeric(entry, ['size', 'volume', 'qty', 'amount', 'tradeSize', 'tokenAmount']);
        return {
            timestamp,
            side,
            price: Number.isFinite(price) ? price : null,
            amount: Number.isFinite(amount) ? amount : null
        };
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

    bot.onText(/\/pricebanmao/, async (msg) => {
        const chatId = msg.chat.id.toString();
        const lang = await getLang(msg);

        try {
            const snapshot = await fetchBanmaoPrice();
            if (!snapshot || !Number.isFinite(snapshot.price)) {
                throw new Error('No price returned');
            }

            const priceFormatted = formatUsdPrice(snapshot.price);
            const changeAbsValue = Number.isFinite(Number(snapshot.changeAbs))
                ? formatUsdPrice(Math.abs(Number(snapshot.changeAbs)))
                : null;
            const changePercentValue = normalizePercentageValue(snapshot.changePercent);
            const changePercentText = Number.isFinite(changePercentValue)
                ? formatPercentage(changePercentValue, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
                : null;
            const changeDirection = Number.isFinite(changePercentValue)
                ? (changePercentValue >= 0 ? '▲ ' : '▼ ')
                : '';
            const priceOkbNumeric = Number(snapshot.priceOkb);
            const priceOkbText = Number.isFinite(priceOkbNumeric)
                ? formatTokenQuantity(priceOkbNumeric, { minimumFractionDigits: 8, maximumFractionDigits: 8 })
                : null;

            const volumeText = Number.isFinite(Number(snapshot.volume))
                ? formatUsdCompact(snapshot.volume)
                : null;
            const liquidityText = Number.isFinite(Number(snapshot.liquidity))
                ? formatUsdCompact(snapshot.liquidity)
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
                t(lang, 'price_banmao_title'),
                t(lang, 'price_banmao_price_line', {
                    price: priceFormatted,
                    changeAbs: changeAbsValue || fallbackValue,
                    changePercent: changePercentText || fallbackValue,
                    changeDirection
                }),
                priceOkbText ? t(lang, 'price_banmao_price_okb_line', { price: priceOkbText }) : null,
                volumeText ? t(lang, 'price_banmao_volume_line', { volume: volumeText }) : null,
                liquidityText ? t(lang, 'price_banmao_liquidity_line', { liquidity: liquidityText }) : null,
                t(lang, 'price_banmao_source_line', { source: sourceLabel }),
                t(lang, 'price_banmao_chart_hint')
            ].filter(Boolean);

            const successMessage = lines.join('\n');
            sendReply(msg, successMessage, { parse_mode: "Markdown" });
        } catch (error) {
            console.error(`[BanmaoPrice] Failed to fetch price: ${error.message}`);
            const errorMessage = t(lang, 'price_banmao_error');
            sendReply(msg, errorMessage, { parse_mode: "Markdown" });
        }
    });

    bot.onText(/\/banmaochart(?:\s+(\d+))?/, async (msg, match) => {
        const lang = await getLang(msg);
        const requestedHours = match && match[1] ? parseInt(match[1], 10) : 24;
        const hours = Number.isFinite(requestedHours) && requestedHours > 0 ? Math.min(requestedHours, 240) : 24;

        try {
            const candles = await fetchBanmaoCandles({ limit: hours, bar: '1H' });
            if (!candles || candles.length === 0) {
                throw new Error('No candle data');
            }

            const closingPrices = candles.map((candle) => candle.close).filter((value) => Number.isFinite(value));
            const sparkline = renderSparkline(closingPrices);
            if (!sparkline) {
                throw new Error('Unable to render sparkline');
            }

            const minPrice = Math.min(...closingPrices);
            const maxPrice = Math.max(...closingPrices);
            const firstCandle = candles[0];
            const lastCandle = candles[candles.length - 1];
            const range = formatTimestampRange(firstCandle?.timestamp, lastCandle?.timestamp);

            const lines = [
                t(lang, 'banmao_chart_title', { hours }),
                '```',
                sparkline,
                '```',
                t(lang, 'banmao_chart_range', { minPrice: formatUsdPrice(minPrice), maxPrice: formatUsdPrice(maxPrice) }),
                t(lang, 'banmao_chart_updated', { start: range.start, end: range.end })
            ];

            const message = lines.join('\n');
            sendReply(msg, message, { parse_mode: "Markdown" });
        } catch (error) {
            console.error(`[BanmaoChart] Failed to build chart: ${error.message}`);
            const errorMessage = t(lang, 'banmao_chart_error');
            sendReply(msg, errorMessage, { parse_mode: "Markdown" });
        }
    });

    bot.onText(/\/banmaoliquidity/, async (msg) => {
        const lang = await getLang(msg);

        try {
            const overview = await fetchBanmaoLiquidityOverview();
            if (!overview) {
                throw new Error('No liquidity data');
            }

            const totalLiquidityText = Number.isFinite(Number(overview.totalLiquidity))
                ? formatUsdCompact(overview.totalLiquidity)
                : t(lang, 'okx_generic_no_data');

            const topPool = Array.isArray(overview.pools)
                ? overview.pools.find((pool) => Number.isFinite(pool?.liquidityUsd))
                : null;

            const lines = [
                t(lang, 'banmao_liquidity_title'),
                t(lang, 'banmao_liquidity_summary', {
                    liquidity: totalLiquidityText,
                    poolCount: overview.poolCount || 0
                })
            ];

            if (topPool) {
                lines.push(t(lang, 'banmao_liquidity_top_pool', {
                    poolName: topPool.name || 'Pool',
                    liquidity: formatUsdCompact(topPool.liquidityUsd || 0)
                }));

                if (topPool.quoteSymbol) {
                    lines.push(t(lang, 'banmao_liquidity_quote', { quote: topPool.quoteSymbol }));
                }
            }

            const chainLabel = overview.chain && overview.chain !== '(default)'
                ? overview.chain
                : 'OKX DEX';
            lines.push(t(lang, 'banmao_liquidity_note', { chain: chainLabel }));

            const message = lines.join('\n');
            sendReply(msg, message, { parse_mode: "Markdown" });
        } catch (error) {
            console.error(`[BanmaoLiquidity] Failed to fetch liquidity: ${error.message}`);
            const errorMessage = t(lang, 'banmao_liquidity_error');
            sendReply(msg, errorMessage, { parse_mode: "Markdown" });
        }
    });

    bot.onText(/\/banmaomarket/, async (msg) => {
        const lang = await getLang(msg);

        try {
            const snapshot = await fetchBanmaoMarketSnapshot();
            if (!snapshot || !Number.isFinite(snapshot.price)) {
                throw new Error('No price data');
            }

            const [liquidityResult, tokenProfileResult, tradesResult] = await Promise.allSettled([
                fetchBanmaoLiquidityOverview(),
                fetchBanmaoTokenProfile(),
                fetchBanmaoRecentTrades({ limit: 1 })
            ]);

            const liquidity = liquidityResult.status === 'fulfilled' ? liquidityResult.value : null;
            const tokenProfile = tokenProfileResult.status === 'fulfilled' ? tokenProfileResult.value : null;
            const trades = tradesResult.status === 'fulfilled' ? tradesResult.value : [];

            const fallbackValue = t(lang, 'okx_generic_no_data');
            const priceText = formatUsdPrice(snapshot.price);
            const priceOkbNumeric = Number(snapshot.priceOkb);
            const priceOkbText = Number.isFinite(priceOkbNumeric)
                ? formatTokenQuantity(priceOkbNumeric, { minimumFractionDigits: 8, maximumFractionDigits: 8 })
                : null;
            const changePercentValue = normalizePercentageValue(snapshot.changePercent);
            const changePercentText = Number.isFinite(changePercentValue)
                ? formatPercentage(changePercentValue, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
                : fallbackValue;
            const changeDirection = Number.isFinite(changePercentValue)
                ? (changePercentValue >= 0 ? '▲' : '▼')
                : '';
            const changeAbsText = Number.isFinite(Number(snapshot.changeAbs))
                ? formatUsdPrice(Math.abs(Number(snapshot.changeAbs)))
                : fallbackValue;

            const volumeText = Number.isFinite(Number(snapshot.volume))
                ? formatUsdCompact(snapshot.volume)
                : fallbackValue;

            const liquidityValue = liquidity && Number.isFinite(Number(liquidity.totalLiquidity))
                ? liquidity.totalLiquidity
                : snapshot.liquidity;
            const liquidityText = Number.isFinite(Number(liquidityValue))
                ? formatUsdCompact(liquidityValue)
                : fallbackValue;

            const marketCapValue = Number.isFinite(Number(snapshot.marketCap))
                ? snapshot.marketCap
                : pickOkxNumeric(tokenProfile, ['marketCapUsd', 'marketCap', 'fdvUsd', 'fullyDilutedMarketCap']);
            const marketCapText = Number.isFinite(Number(marketCapValue))
                ? formatUsdCompact(marketCapValue)
                : fallbackValue;

            const symbol = (tokenProfile && (tokenProfile.symbol || tokenProfile.tokenSymbol)) || 'BANMAO';
            const supplyValue = pickOkxNumeric(tokenProfile || {}, ['totalSupply', 'supply', 'circulatingSupply', 'maxSupply']);
            const supplyText = Number.isFinite(Number(supplyValue))
                ? formatTokenQuantity(supplyValue, { minimumFractionDigits: 0, maximumFractionDigits: 0 })
                : fallbackValue;

            const trade = Array.isArray(trades) && trades.length > 0 ? trades[0] : null;
            let tradeLine = null;
            if (trade) {
                const sideKey = trade.side === 'sell'
                    ? 'banmao_market_trade_sell'
                    : 'banmao_market_trade_buy';
                const sideLabel = t(lang, sideKey);
                const amountText = Number.isFinite(Number(trade.amount))
                    ? formatTokenQuantity(trade.amount, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
                    : fallbackValue;
                const priceTextTrade = Number.isFinite(Number(trade.price))
                    ? formatUsdPrice(trade.price)
                    : fallbackValue;
                const agoText = formatRelativeTime(trade.timestamp) || fallbackValue;
                tradeLine = t(lang, 'banmao_market_last_trade', {
                    side: sideLabel,
                    amount: amountText,
                    symbol,
                    price: priceTextTrade,
                    ago: agoText
                });
            }

            const tokenLink = (tokenProfile && (tokenProfile.tokenUrl || tokenProfile.url)) || OKX_BANMAO_TOKEN_URL;

            const lines = [
                t(lang, 'banmao_market_title'),
                t(lang, 'banmao_market_price', {
                    price: priceText,
                    changePercent: changePercentText,
                    changeDirection,
                    changeAbs: changeAbsText
                }),
                priceOkbText ? t(lang, 'banmao_market_price_okb', { priceOkb: priceOkbText }) : null,
                t(lang, 'banmao_market_volume', { volume: volumeText }),
                t(lang, 'banmao_market_liquidity', { liquidity: liquidityText }),
                t(lang, 'banmao_market_marketcap', { marketCap: marketCapText }),
                t(lang, 'banmao_market_supply', { supply: supplyText, symbol }),
                tradeLine,
                t(lang, 'banmao_market_token_link', { link: tokenLink })
            ].filter(Boolean);

            const message = lines.join('\n');
            sendReply(msg, message, { parse_mode: "Markdown" });
        } catch (error) {
            console.error(`[BanmaoMarket] Failed to assemble market data: ${error.message}`);
            const errorMessage = t(lang, 'banmao_market_error');
            sendReply(msg, errorMessage, { parse_mode: "Markdown" });
        }
    });

    bot.onText(/\/okxchains/, async (msg) => {
        const lang = await getLang(msg);

        try {
            const { aggregator, market } = await fetchOkxSupportedChains();
            const fallback = t(lang, 'okx_generic_no_data');
            const aggregatorText = aggregator && aggregator.length > 0 ? aggregator.join(', ') : fallback;
            const marketText = market && market.length > 0 ? market.join(', ') : fallback;

            const lines = [
                t(lang, 'okx_chains_title'),
                aggregator && aggregator.length > 0
                    ? t(lang, 'okx_chains_aggregator', { chains: aggregatorText })
                    : null,
                market && market.length > 0
                    ? t(lang, 'okx_chains_market', { chains: marketText })
                    : null
            ].filter(Boolean);

            if (lines.length === 1) {
                lines.push(t(lang, 'okx_chains_none'));
            }

            const message = lines.join('\n');
            sendReply(msg, message, { parse_mode: "Markdown" });
        } catch (error) {
            console.error(`[OkxChains] Failed to load supported chains: ${error.message}`);
            const errorMessage = t(lang, 'okx_chains_error');
            sendReply(msg, errorMessage, { parse_mode: "Markdown" });
        }
    });

    bot.onText(/\/okx402status/, async (msg) => {
        const lang = await getLang(msg);

        try {
            const supported = await fetchOkx402Supported();
            const fallback = t(lang, 'okx_generic_no_data');
            const chainsText = supported && supported.length > 0 ? supported.join(', ') : fallback;

            const lines = [
                t(lang, 'okx_x402_title'),
                t(lang, 'okx_x402_supported', { chains: chainsText }),
                t(lang, 'okx_x402_note')
            ];

            const message = lines.join('\n');
            sendReply(msg, message, { parse_mode: "Markdown" });
        } catch (error) {
            console.error(`[Okx402] Failed to query x402 support: ${error.message}`);
            const errorMessage = t(lang, 'okx_x402_error');
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
        const chatId = msg.chat.id.toString();
        const lang = await getLang(msg);
        const generalCommands = [
            t(lang, 'help_command_start'),
            t(lang, 'help_command_register'),
            t(lang, 'help_command_mywallet'),
            t(lang, 'help_command_stats'),
            t(lang, 'help_command_pricebanmao'),
            t(lang, 'help_command_banmaochart'),
            t(lang, 'help_command_banmaomarket'),
            t(lang, 'help_command_banmaoliquidity'),
            t(lang, 'help_command_okxchains'),
            t(lang, 'help_command_okx402status'),
            t(lang, 'help_command_unregister'),
            t(lang, 'help_command_language'),
            t(lang, 'help_command_feedlang'),
            t(lang, 'help_command_help')
        ];

        const adminCommands = [
            t(lang, 'help_command_banmaofeed'),
            t(lang, 'help_command_feedtopic')
        ];

        const sections = [
            t(lang, 'help_header'),
            '',
            t(lang, 'help_section_general_title'),
            generalCommands.join('\n')
        ];

        if (adminCommands.length > 0) {
            sections.push('', t(lang, 'help_section_admin_title'), adminCommands.join('\n'));
        }

        const helpMessage = sections.join('\n');
        sendReply(msg, helpMessage, { parse_mode: "Markdown" });
    });

    // Xử lý tất cả CALLBACK QUERY (Nút bấm) - Cần async
    bot.on('callback_query', async (query) => {
        const chatId = query.message.chat.id.toString();
        const queryId = query.id;
        const lang = await getLang(query.message); // <-- SỬA LỖI
        
        try {
            if (query.data.startsWith('lang_')) {
                const newLang = resolveLangCode(query.data.split('_')[1]);
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

        console.log("🚀 TẤT CẢ DỊCH VỤ ĐÃ SẴN SÀNG!");

    } catch (error) {
        console.error("LỖI KHỞI ĐỘNG NGHIÊM TRỌNG:", error);
        process.exit(1);
    }
}

main(); // Chạy hàm khởi động chính