// Đảm bảo dotenv được gọi ĐẦU TIÊN
require('dotenv').config(); 

// --- Import các thư viện ---
const ethers = require('ethers');
const TelegramBot = require('node-telegram-bot-api');
const fs = require('fs');
const path = require('path');
const express = require('express');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
const { t_ } = require('./i18n.js');
const db = require('./database.js');

// --- CẤU HÌNH ---
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const RPC_URL = process.env.RPC_URL;
const CONTRACT_ADDRESS = process.env.CONTRACT_ADDRESS;
const contractABI = require('./BanmaoRPS_ABI.json');
const API_PORT = 3000;
const WEB_URL = "https://www.banmao.fun";
const defaultLang = 'en';

function normalizeOwnerToken(token) {
    if (!token) {
        return null;
    }
    const trimmed = token.trim().replace(/^['"\[]+|['"\]]+$/g, '');
    if (!trimmed) {
        return null;
    }
    return trimmed;
}

function collectOwnerEntries(rawValue, collector) {
    if (!rawValue) {
        return;
    }
    const parts = rawValue.split(/[,\s]+/);
    for (const part of parts) {
        const cleaned = normalizeOwnerToken(part);
        if (!cleaned) {
            continue;
        }
        collector(cleaned);
    }
}

const ownerIdSet = new Set();
const ownerUsernameSet = new Set();

function registerOwnerEntry(entry, options = { allowUsername: true }) {
    if (!entry) {
        return;
    }
    if (/^-?\d+$/.test(entry)) {
        ownerIdSet.add(entry.replace(/^[+]/, ''));
        return;
    }
    if (!options.allowUsername) {
        return;
    }
    ownerUsernameSet.add(entry.replace(/^@/, '').toLowerCase());
}

collectOwnerEntries(process.env.BOT_OWNER_IDS, (entry) => registerOwnerEntry(entry));
collectOwnerEntries(process.env.BOT_OWNER_ID, (entry) => registerOwnerEntry(entry));
collectOwnerEntries(process.env.BOT_OWNER_USERNAMES, (entry) => registerOwnerEntry(entry));
collectOwnerEntries(process.env.BOT_OWNER_USERNAME, (entry) => registerOwnerEntry(entry));
collectOwnerEntries(process.env.OWNER_TELEGRAM_ID, (entry) => registerOwnerEntry(entry, { allowUsername: false }));
collectOwnerEntries(process.env.OWNER_TG_ID, (entry) => registerOwnerEntry(entry, { allowUsername: false }));

const localesDir = path.join(__dirname, 'locales');
const supportedLanguages = fs.readdirSync(localesDir)
    .filter((file) => file.endsWith('.json'))
    .map((file) => file.replace('.json', ''));

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
const customMessageCache = new Map();

const overrideKey = (lang, key) => `${lang}::${key}`;

function applyVariables(template, variables = {}) {
    let text = template;
    for (const [varName, varValue] of Object.entries(variables)) {
        const placeholder = `{${varName}}`;
        text = text.split(placeholder).join(varValue);
    }
    return text;
}

function t(lang_code, key, variables = {}) {
    const langsToTry = [lang_code];
    if (lang_code !== defaultLang) {
        langsToTry.push(defaultLang);
    }

    for (const lang of langsToTry) {
        const cached = customMessageCache.get(overrideKey(lang, key));
        if (cached) {
            return applyVariables(cached, variables);
        }
    }

    return t_(lang_code, key, variables);
}

async function refreshCustomMessageCache() {
    const overrides = await db.getAllCustomMessages();
    customMessageCache.clear();
    overrides.forEach((item) => {
        customMessageCache.set(overrideKey(item.lang, item.messageKey), item.messageText);
    });
}

function isSupportedLanguage(lang) {
    return supportedLanguages.includes(lang);
}

function isBotOwner(user) {
    if (!user) {
        return false;
    }
    const userId = String(user.id);
    if (ownerIdSet.has(userId) || ownerIdSet.has(userId.replace(/^[+]/, ''))) {
        return true;
    }
    const username = (user.username || '').toLowerCase();
    if (username && ownerUsernameSet.has(username)) {
        return true;
    }
    return false;
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
    const chatId = msg.chat.id.toString();
    const detectedLang = msg.from.language_code || defaultLang; // Ngôn ngữ từ TG

    let savedLang = await db.getUserLanguage(chatId); // Thử đọc từ DB
    
    if (savedLang) {
        return savedLang; // Đã tìm thấy, trả về ngôn ngữ đã lưu
    } else {
        // User mới, hoặc user cũ nhưng chưa có lang
        await db.setLanguage(chatId, detectedLang); // Lưu ngôn ngữ mặc định
        return detectedLang; // Trả về ngôn ngữ mặc định
    }
}
// ======================================

function startTelegramBot() {

    async function ensureOwner(msg) {
        if (isBotOwner(msg.from)) {
            return true;
        }
        const chatId = msg.chat.id.toString();
        const lang = await getLang(msg);
        await bot.sendMessage(chatId, t(lang, 'owner_only_command'), { parse_mode: "Markdown" });
        return false;
    }

    // Xử lý /start CÓ token (Từ DApp) - Cần async
    bot.onText(/\/start (.+)/, async (msg, match) => {
        const chatId = msg.chat.id.toString();
        const token = match[1];
        // Khi /start, luôn ưu tiên ngôn ngữ của thiết bị
        const lang = msg.from.language_code || defaultLang; 
        const walletAddress = await db.getPendingWallet(token); 
        if (walletAddress) {
            await db.addWalletToUser(chatId, lang, walletAddress);
            await db.deletePendingToken(token);
            const message = t(lang, 'connect_success', { walletAddress: walletAddress });
            bot.sendMessage(chatId, message, { parse_mode: "Markdown" });
            console.log(`[BOT] Liên kết (DApp): ${walletAddress} -> ${chatId} (lang: ${lang})`);
        } else {
            const message = t(lang, 'connect_fail_token');
            bot.sendMessage(chatId, message, { parse_mode: "Markdown" });
            console.log(`[BOT] Token không hợp lệ: ${token}`);
        }
    });

    // Xử lý /start KHÔNG CÓ token (Gõ tay) - Cần async
    bot.onText(/\/start$/, async (msg) => {
        const chatId = msg.chat.id.toString();
        // Lấy ngôn ngữ (hoặc tạo user mới nếu chưa có)
        const lang = await getLang(msg); // <-- SỬA LỖI
        const message = t(lang, 'welcome_generic');
        bot.sendMessage(chatId, message, { parse_mode: "Markdown" });
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
            bot.sendMessage(chatId, message, { parse_mode: "Markdown" });
            console.log(`[BOT] Thêm ví (Manual): ${normalizedAddr} -> ${chatId} (lang: ${lang})`);
        } catch (error) {
            const message = t(lang, 'register_invalid_address');
            bot.sendMessage(chatId, message, { parse_mode: "Markdown" });
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
            bot.sendMessage(chatId, message, { parse_mode: "Markdown" });
        } else {
            const message = t(lang, 'mywallet_not_linked');
            bot.sendMessage(chatId, message, { parse_mode: "Markdown" });
        }
    });

    // COMMAND: /stats - Cần async
    bot.onText(/\/stats/, async (msg) => {
        const chatId = msg.chat.id.toString();
        const lang = await getLang(msg); // <-- SỬA LỖI
        const wallets = await db.getWalletsForUser(chatId);
        if (wallets.length === 0) {
            bot.sendMessage(chatId, t(lang, 'stats_no_wallet'));
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
            bot.sendMessage(chatId, t(lang, 'stats_no_games'));
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
            bot.sendMessage(chatId, message, { parse_mode: "Markdown" });
    });

    bot.onText(/\/help/, async (msg) => {
        const chatId = msg.chat.id.toString();
        const lang = await getLang(msg);

        const lines = [t(lang, 'help_title'), ''];

        lines.push(t(lang, 'help_section_user'));
        lines.push(`• ${t(lang, 'help_cmd_start')}`);
        lines.push(`• ${t(lang, 'help_cmd_register')}`);
        lines.push(`• ${t(lang, 'help_cmd_mywallet')}`);
        lines.push(`• ${t(lang, 'help_cmd_stats')}`);
        lines.push(`• ${t(lang, 'help_cmd_language')}`);
        lines.push(`• ${t(lang, 'help_cmd_unregister')}`);

        lines.push('');
        lines.push(t(lang, 'help_section_group'));
        lines.push(`• ${t(lang, 'help_cmd_banmaofeed')}`);

        lines.push('');
        lines.push(t(lang, 'help_section_owner'));
        if (isBotOwner(msg.from)) {
            lines.push(`• ${t(lang, 'help_cmd_setmessage')}`);
            lines.push(`• ${t(lang, 'help_cmd_resetmessage')}`);
            lines.push(`• ${t(lang, 'help_cmd_showmessage')}`);
        } else {
            lines.push(`• ${t(lang, 'help_owner_only_hint')}`);
        }

        const message = lines.join('\n');
        bot.sendMessage(chatId, message, { parse_mode: "Markdown", disable_web_page_preview: true });
    });

    // COMMAND: /banmaofeed - Chỉ dùng cho group
    bot.onText(/\/banmaofeed(?:\s+(.+))?/, async (msg, match) => {
        const chatId = msg.chat.id.toString();
        const chatType = msg.chat.type;
        const userLang = msg.from.language_code || defaultLang;

        if (chatType !== 'group' && chatType !== 'supergroup') {
            bot.sendMessage(chatId, t(userLang, 'group_feed_group_only'), { parse_mode: "Markdown" });
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
            bot.sendMessage(chatId, t(userLang, 'group_feed_admin_only'), { parse_mode: "Markdown" });
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
                bot.sendMessage(chatId, `${statusLine}\n\n${usage}`, { parse_mode: "Markdown" });
                return;
            }

            const lowered = arg.toLowerCase();
            if (['off', 'disable', 'stop', 'cancel'].includes(lowered)) {
                await db.removeGroupSubscription(chatId);
                bot.sendMessage(chatId, t(userLang, 'group_feed_disabled'), { parse_mode: "Markdown" });
                return;
            }

            const normalizedArg = arg.replace(',', '.');
            const minStake = parseFloat(normalizedArg);
            if (!Number.isFinite(minStake) || minStake < 0) {
                bot.sendMessage(chatId, t(userLang, 'group_feed_invalid_amount'), { parse_mode: "Markdown" });
                return;
            }

            await db.upsertGroupSubscription(chatId, userLang, minStake);
            bot.sendMessage(chatId, t(userLang, 'group_feed_enabled', { amount: formatBanmao(minStake) }), { parse_mode: "Markdown" });
        } catch (error) {
            console.error(`[GroupFeed] Lỗi cấu hình cho nhóm ${chatId}:`, error.message);
            bot.sendMessage(chatId, t(userLang, 'group_feed_error'), { parse_mode: "Markdown" });
        }
    });

    // COMMAND: /unregister - Cần async
    bot.onText(/\/unregister/, async (msg) => {
        const chatId = msg.chat.id.toString();
        const lang = await getLang(msg); // <-- SỬA LỖI
        const wallets = await db.getWalletsForUser(chatId);
        if (wallets.length === 0) {
            bot.sendMessage(chatId, t(lang, 'mywallet_not_linked'));
            return;
        }
        const keyboard = wallets.map(wallet => {
            const shortWallet = `${wallet.substring(0, 5)}...${wallet.substring(wallet.length - 4)}`;
            return [{ text: `❌ ${shortWallet}`, callback_data: `delete_${wallet}` }];
        });
        keyboard.push([{ text: `🔥🔥 ${t(lang, 'unregister_all')} 🔥🔥`, callback_data: 'delete_all' }]);
            bot.sendMessage(chatId, t(lang, 'unregister_header'), {
            reply_markup: { inline_keyboard: keyboard }
        });
    });

    bot.onText(/^\/setmessage\s+(\S+)\s+(\S+)\s+([\s\S]+)/, async (msg, match) => {
        if (!(await ensureOwner(msg))) {
            return;
        }
        const chatId = msg.chat.id.toString();
        const lang = await getLang(msg);
        const targetLang = match[1].toLowerCase();
        const messageKey = match[2];
        const messageText = match[3].trim();

        if (!isSupportedLanguage(targetLang)) {
            bot.sendMessage(chatId, t(lang, 'custom_message_invalid_language', { lang: targetLang }));
            return;
        }

        if (!messageText) {
            bot.sendMessage(chatId, t(lang, 'custom_message_missing_text'));
            return;
        }

        await db.upsertCustomMessage(targetLang, messageKey, messageText, String(msg.from.id));
        customMessageCache.set(overrideKey(targetLang, messageKey), messageText);
        bot.sendMessage(chatId, t(lang, 'custom_message_saved', { lang: targetLang, key: messageKey }));
    });

    bot.onText(/^\/resetmessage\s+(\S+)\s+(\S+)/, async (msg, match) => {
        if (!(await ensureOwner(msg))) {
            return;
        }
        const chatId = msg.chat.id.toString();
        const lang = await getLang(msg);
        const targetLang = match[1].toLowerCase();
        const messageKey = match[2];

        if (!isSupportedLanguage(targetLang)) {
            bot.sendMessage(chatId, t(lang, 'custom_message_invalid_language', { lang: targetLang }));
            return;
        }

        const removed = await db.removeCustomMessage(targetLang, messageKey);
        customMessageCache.delete(overrideKey(targetLang, messageKey));
        if (removed) {
            bot.sendMessage(chatId, t(lang, 'custom_message_removed', { lang: targetLang, key: messageKey }));
        } else {
            bot.sendMessage(chatId, t(lang, 'custom_message_not_found', { lang: targetLang, key: messageKey }));
        }
    });

    bot.onText(/^\/showmessage\s+(\S+)\s+(\S+)/, async (msg, match) => {
        if (!(await ensureOwner(msg))) {
            return;
        }
        const chatId = msg.chat.id.toString();
        const lang = await getLang(msg);
        const targetLang = match[1].toLowerCase();
        const messageKey = match[2];

        if (!isSupportedLanguage(targetLang)) {
            bot.sendMessage(chatId, t(lang, 'custom_message_invalid_language', { lang: targetLang }));
            return;
        }

        const cacheKey = overrideKey(targetLang, messageKey);
        const overrideText = customMessageCache.get(cacheKey);
        let response;
        if (overrideText) {
            response = `${t(lang, 'custom_message_preview_override', { lang: targetLang, key: messageKey })}\n\n${overrideText}`;
        } else {
            const defaultText = t_(targetLang, messageKey, {});
            if (defaultText === messageKey) {
                response = t(lang, 'custom_message_not_found', { lang: targetLang, key: messageKey });
            } else {
                response = `${t(lang, 'custom_message_preview_default', { lang: targetLang, key: messageKey })}\n\n${defaultText}`;
            }
        }

        bot.sendMessage(chatId, response);
    });

    // LỆNH: /language - Cần async
    bot.onText(/\/language/, async (msg) => {
        const chatId = msg.chat.id.toString();
        const lang = await getLang(msg); // <-- SỬA LỖI
        const text = t(lang, 'select_language');
        const options = {
            reply_markup: {
                inline_keyboard: [
                    [ { text: "🇻🇳 Tiếng Việt", callback_data: 'lang_vi' }, { text: "🇺🇸 English", callback_data: 'lang_en' } ],
                    [ { text: "🇨🇳 中文", callback_data: 'lang_zh' }, { text: "🇷🇺 Русский", callback_data: 'lang_ru' } ],
                    [ { text: "🇰🇷 한국어", callback_data: 'lang_ko' }, { text: "🇮🇩 Indonesia", callback_data: 'lang_id' } ]
                ]
            }
        };
        bot.sendMessage(chatId, text, options);
    });

    // Xử lý tất cả CALLBACK QUERY (Nút bấm) - Cần async
    bot.on('callback_query', async (query) => {
        const chatId = query.message.chat.id.toString();
        const queryId = query.id;
        const lang = await getLang(query.message); // <-- SỬA LỖI
        
        try {
            if (query.data.startsWith('lang_')) {
                const newLang = query.data.split('_')[1];
                await db.setLanguage(chatId, newLang);
                const message = t(newLang, 'language_changed_success'); // Dùng newLang
                bot.sendMessage(chatId, message);
                console.log(`[BOT] ChatID ${chatId} đã đổi ngôn ngữ sang: ${newLang}`);
                bot.answerCallbackQuery(queryId, { text: message });
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

    contract.on("Joined", handleJoinedEvent);
    contract.on("Committed", handleCommittedEvent);
    contract.on("Revealed", handleRevealedEvent);
    contract.on("Resolved", handleResolvedEvent);
    contract.on("Canceled", handleCanceledEvent);
    contract.on("Forfeited", handleForfeitedEvent);
}

function toRoomIdString(roomId) {
    try {
        return roomId.toString();
    } catch (error) {
        return `${roomId}`;
    }
}

async function handleJoinedEvent(roomId, opponent) {
    const roomIdStr = toRoomIdString(roomId);
    console.log(`[SỰ KIỆN] Room ${roomIdStr} đã có người tham gia: ${opponent}`);
    try {
        if (!contract) return;
        const room = await contract.rooms(roomId);
        const stake = ethers.formatEther(room.stake);
        const creatorAddress = ethers.getAddress(room.creator);
        const opponentAddress = ethers.getAddress(room.opponent);

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
        if (!contract) return;
        const room = await contract.rooms(roomId);
        const playerAddress = ethers.getAddress(player);
        const creatorAddress = ethers.getAddress(room.creator);
        const opponentAddress = ethers.getAddress(room.opponent);
        const stake = ethers.formatEther(room.stake);
        const otherPlayer = (playerAddress === creatorAddress) ? opponentAddress : creatorAddress;

        if (otherPlayer && otherPlayer !== ethers.ZeroAddress) {
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
        if (!contract) return;
        const room = await contract.rooms(roomId);
        const playerAddress = ethers.getAddress(player);
        const creatorAddress = ethers.getAddress(room.creator);
        const opponentAddress = ethers.getAddress(room.opponent);
        const stake = ethers.formatEther(room.stake);
        const otherPlayer = (playerAddress === creatorAddress) ? opponentAddress : creatorAddress;

        if (otherPlayer && otherPlayer !== ethers.ZeroAddress) {
            await sendInstantNotification(otherPlayer, 'notify_opponent_revealed', { roomId: roomIdStr, opponent: playerAddress, stake });
        }
    } catch (err) {
        console.error(`[Lỗi] Không thể lấy thông tin phòng ${roomIdStr} (sau reveal):`, err.message);
    }
}

async function handleResolvedEvent(roomId, winner, payout, fee) {
    const roomIdStr = toRoomIdString(roomId);
    console.log(`[SỰ KIỆN] Room ${roomIdStr} có kết quả: ${winner} thắng`);
    try {
        if (!contract) return;
        const room = await contract.rooms(roomId);
        const winnerAddress = ethers.getAddress(winner);
        const creatorAddress = ethers.getAddress(room.creator);
        const opponentAddress = ethers.getAddress(room.opponent);
        const payoutAmount = ethers.formatEther(payout);
        const stakeAmount = parseFloat(ethers.formatEther(room.stake));
        const loserAddress = (winnerAddress === creatorAddress) ? opponentAddress : creatorAddress;

        const winnerIsCreator = (winnerAddress === creatorAddress);
        const winnerChoice = winnerIsCreator ? room.revealA : room.revealB;
        const loserChoice = winnerIsCreator ? room.revealB : room.revealA;

        const winnerLangs = await db.getUsersForWallet(winnerAddress);
        const loserLangs = await db.getUsersForWallet(loserAddress);
        const winnerLang = (winnerLangs[0] || {}).lang || defaultLang;
        const loserLang = (loserLangs[0] || {}).lang || defaultLang;

        const winnerChoiceStr = getChoiceString(winnerChoice, winnerLang);
        const loserChoiceStr = getChoiceString(loserChoice, loserLang);

        await Promise.all([
            sendInstantNotification(winnerAddress, 'notify_game_win',
                { roomId: roomIdStr, payout: payoutAmount, myChoice: winnerChoiceStr, opponentChoice: loserChoiceStr }
            ),
            sendInstantNotification(loserAddress, 'notify_game_lose',
                { roomId: roomIdStr, winner: winnerAddress, myChoice: loserChoiceStr, opponentChoice: winnerChoiceStr }
            )
        ]);

        await Promise.all([
            db.writeGameResult(winnerAddress, 'win', stakeAmount),
            db.writeGameResult(loserAddress, 'lose', stakeAmount)
        ]);

        await broadcastGroupGameUpdate('win', {
            roomId: roomIdStr,
            creatorAddress,
            opponentAddress,
            winnerAddress,
            loserAddress,
            stakeAmount,
            payoutAmount: parseFloat(payoutAmount),
            creatorChoice: Number(room.revealA),
            opponentChoice: Number(room.revealB)
        });
    } catch (err) {
        console.error(`[Lỗi] Không thể lấy thông tin phòng ${roomIdStr} (sau resolve):`, err.message);
    }
}

async function handleCanceledEvent(roomId) {
    const roomIdStr = toRoomIdString(roomId);
    console.log(`[SỰ KIỆN] Room ${roomIdStr} đã bị hủy (Hòa/Timeout)`);
    try {
        if (!contract) return;
        const room = await contract.rooms(roomId);
        const stakeAmount = parseFloat(ethers.formatEther(room.stake));
        const creatorAddress = ethers.getAddress(room.creator);

        const creatorLangs = await db.getUsersForWallet(creatorAddress);
        const creatorLang = (creatorLangs[0] || {}).lang || defaultLang;
        const choiceStr = getChoiceString(room.revealA, creatorLang);

        const tasks = [
            sendInstantNotification(creatorAddress, 'notify_game_draw', { roomId: roomIdStr, choice: choiceStr })
        ];

        let opponentAddress = null;
        let opponentChoiceValue = null;
        if (room.opponent !== ethers.ZeroAddress) {
            opponentAddress = ethers.getAddress(room.opponent);
            const opponentLangs = await db.getUsersForWallet(opponentAddress);
            const opponentLang = (opponentLangs[0] || {}).lang || defaultLang;
            opponentChoiceValue = Number(room.revealB);
            const choiceStrOpp = getChoiceString(room.revealB, opponentLang);
            tasks.push(sendInstantNotification(opponentAddress, 'notify_game_draw', { roomId: roomIdStr, choice: choiceStrOpp }));

            await Promise.all([
                db.writeGameResult(creatorAddress, 'draw', stakeAmount),
                db.writeGameResult(opponentAddress, 'draw', stakeAmount)
            ]);
        }

        await Promise.all(tasks);

        if (opponentAddress) {
            await broadcastGroupGameUpdate('draw', {
                roomId: roomIdStr,
                creatorAddress,
                opponentAddress,
                stakeAmount,
                creatorChoice: Number(room.revealA),
                opponentChoice: opponentChoiceValue
            });
        }
    } catch (err) {
        console.error(`[Lỗi] Không thể lấy thông tin phòng ${roomIdStr} (sau cancel):`, err.message);
    }
}

async function handleForfeitedEvent(roomId, loser, winner, winnerPayout) {
    const roomIdStr = toRoomIdString(roomId);
    console.log(`[SỰ KIỆN] Room ${roomIdStr} có người bỏ cuộc: ${loser}`);
    const payoutAmount = ethers.formatEther(winnerPayout);
    const stakeAmount = parseFloat(ethers.formatEther(winnerPayout)) / 1.8;

    try {
        const winnerAddress = ethers.getAddress(winner);
        const loserAddress = ethers.getAddress(loser);

        let creatorAddress = null;
        let opponentAddress = null;
        let creatorChoice = null;
        let opponentChoice = null;

        if (contract) {
            try {
                const room = await contract.rooms(roomId);
                if (room.creator && room.creator !== ethers.ZeroAddress) {
                    creatorAddress = ethers.getAddress(room.creator);
                }
                if (room.opponent && room.opponent !== ethers.ZeroAddress) {
                    opponentAddress = ethers.getAddress(room.opponent);
                }
                creatorChoice = Number(room.revealA);
                opponentChoice = Number(room.revealB);
            } catch (fetchErr) {
                console.warn(`[Forfeit] Không thể lấy dữ liệu phòng ${roomIdStr}:`, fetchErr.message);
            }
        }

        if (!creatorAddress) creatorAddress = winnerAddress;
        if (!opponentAddress) opponentAddress = loserAddress;

        await Promise.all([
            sendInstantNotification(winnerAddress, 'notify_forfeit_win', { roomId: roomIdStr, loser: loserAddress, payout: payoutAmount }),
            sendInstantNotification(loserAddress, 'notify_forfeit_lose', { roomId: roomIdStr, winner: winnerAddress })
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
    } catch (error) {
        console.error(`[Lỗi] Khi xử lý sự kiện Forfeited cho room ${roomIdStr}:`, error.message);
    }
}

// ==========================================================
// 🚀 PHẦN 5: HÀM GỬI THÔNG BÁO (CHỈ GỬI TEXT)
// ==========================================================
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

    const tasks = users.map(async ({ chatId, lang }) => {
        const message = t(lang, langKey, variables);

        const button = {
            text: `🎮 ${t(lang, 'action_button_play')}`,
            url: `${WEB_URL}/?join=${variables.roomId || ''}`
        };

        let options = {
            parse_mode: "Markdown",
            reply_markup: {
                inline_keyboard: [[button]]
            }
        };

        const isGameOver = langKey.startsWith('notify_game_') || langKey.startsWith('notify_forfeit_');
        if (isGameOver) {
            delete options.reply_markup;
        }

        try {
            await bot.sendMessage(chatId, message, options);
            console.log(`[Notify] Đã gửi thông báo TEXT '${langKey}' tới ${chatId}`);
        } catch (error) {
            console.error(`[Lỗi Gửi Text]: ${error.message}`);
        }
    });

    await Promise.allSettled(tasks);
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

        const lang = group.lang || defaultLang;
        const messagePayload = buildGroupBroadcastMessage(eventType, lang, payload);
        if (!messagePayload) {
            return;
        }

        const options = {
            parse_mode: "Markdown",
            disable_web_page_preview: true
        };

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

        // Bước 1.5: Nạp các thông điệp tùy chỉnh (nếu có)
        await refreshCustomMessageCache();

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