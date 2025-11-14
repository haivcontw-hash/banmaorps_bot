// Đảm bảo dotenv được gọi ĐẦU TIÊN
require('dotenv').config(); 

// --- Import các thư viện ---
const ethers = require('ethers');
const TelegramBot = require('node-telegram-bot-api');
const fs = require('fs');
const express = require('express');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
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

function delay(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
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