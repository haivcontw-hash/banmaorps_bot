// Đảm bảo dotenv được gọi ĐẦU TIÊN
require('dotenv').config(); 

// --- Import các thư viện ---
const ethers = require('ethers');
const TelegramBot = require('node-telegram-bot-api');
const fs = require('fs');
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

// --- Kiểm tra Cấu hình ---
if (!TELEGRAM_TOKEN || !RPC_URL || !CONTRACT_ADDRESS) {
    console.error("LỖI NGHIÊM TRỌNG: Thiếu TELEGRAM_TOKEN, RPC_URL, hoặc CONTRACT_ADDRESS trong file .env!");
    process.exit(1);
}

// --- KHỞI TẠO CÁC DỊCH VỤ ---
// db.init() sẽ được gọi trong hàm main()
const app = express();
const bot = new TelegramBot(TELEGRAM_TOKEN, { polling: true });
const provider = new ethers.WebSocketProvider(RPC_URL); 
const contract = new ethers.Contract(CONTRACT_ADDRESS, contractABI, provider);

// Hàm 't' (translate) nội bộ
function t(lang_code, key, variables = {}) {
    return t_(lang_code, key, variables);
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
function startBlockchainListener() {
    console.log(`🎧 [Blockchain] Đang lắng nghe sự kiện từ contract: ${CONTRACT_ADDRESS}`);

    contract.on("Joined", async (roomId, opponent) => {
        console.log(`[SỰ KIỆN] Room ${roomId} đã có người tham gia: ${opponent}`);
        try {
            const room = await contract.rooms(roomId);
            const stake = ethers.formatEther(room.stake);
            await sendInstantNotification(room.creator, 'notify_opponent_joined', { roomId: roomId, opponent: room.opponent, stake: stake });
            await sendInstantNotification(room.opponent, 'notify_self_joined', { roomId: roomId, creator: room.creator, stake: stake });
        } catch (err) {
            console.error(`[Lỗi] Không thể lấy thông tin phòng ${roomId}:`, err.message);
        }
    });

    contract.on("Committed", async (roomId, player) => {
        console.log(`[SỰ KIỆN] Room ${roomId} có người commit: ${player}`);
        try {
            const room = await contract.rooms(roomId);
            const playerAddress = ethers.getAddress(player);
            const creatorAddress = ethers.getAddress(room.creator);
            const opponentAddress = ethers.getAddress(room.opponent);
            const stake = ethers.formatEther(room.stake);
            let otherPlayer = (playerAddress === creatorAddress) ? opponentAddress : creatorAddress;

            if(otherPlayer) {
                await sendInstantNotification(otherPlayer, 'notify_opponent_committed', { roomId: roomId, opponent: playerAddress, stake: stake });
            }
        } catch (err) {
            console.error(`[Lỗi] Không thể lấy thông tin phòng ${roomId} (sau commit):`, err.message);
        }
    });

    contract.on("Revealed", async (roomId, player, choice) => {
        console.log(`[SỰ KIỆN] Room ${roomId} có người reveal: ${player}`);
        try {
            const room = await contract.rooms(roomId);
            const playerAddress = ethers.getAddress(player);
            const creatorAddress = ethers.getAddress(room.creator);
            const opponentAddress = ethers.getAddress(room.opponent);
            const stake = ethers.formatEther(room.stake);
            let otherPlayer = (playerAddress === creatorAddress) ? opponentAddress : creatorAddress;

            if(otherPlayer) {
                await sendInstantNotification(otherPlayer, 'notify_opponent_revealed', { roomId: roomId, opponent: playerAddress, stake: stake });
            }
        } catch (err) {
            console.error(`[Lỗi] Không thể lấy thông tin phòng ${roomId} (sau reveal):`, err.message);
        }
    });
    
    contract.on("Resolved", async (roomId, winner, payout, fee) => {
        console.log(`[SỰ KIỆN] Room ${roomId} có kết quả: ${winner} thắng`);
        try {
            const room = await contract.rooms(roomId);
            const winnerAddress = ethers.getAddress(winner);
            const creatorAddress = ethers.getAddress(room.creator);
            const opponentAddress = ethers.getAddress(room.opponent);
            const payoutAmount = ethers.formatEther(payout);
            const stakeAmount = parseFloat(ethers.formatEther(room.stake));
            let loserAddress = (winnerAddress === creatorAddress) ? opponentAddress : creatorAddress;

            const winnerIsCreator = (winnerAddress === creatorAddress);
            const winnerChoice = winnerIsCreator ? room.revealA : room.revealB;
            const loserChoice = winnerIsCreator ? room.revealB : room.revealA;
            
            // Lấy ngôn ngữ đã lưu
            const winnerLangs = await db.getUsersForWallet(winnerAddress);
            const loserLangs = await db.getUsersForWallet(loserAddress);
            const winnerLang = (winnerLangs[0] || {}).lang || defaultLang;
            const loserLang = (loserLangs[0] || {}).lang || defaultLang;
            
            const winnerChoiceStr = getChoiceString(winnerChoice, winnerLang);
            const loserChoiceStr = getChoiceString(loserChoice, loserLang);

            await sendInstantNotification(winnerAddress, 'notify_game_win', 
                { roomId: roomId, payout: payoutAmount, myChoice: winnerChoiceStr, opponentChoice: loserChoiceStr }
            );
            await sendInstantNotification(loserAddress, 'notify_game_lose', 
                { roomId: roomId, winner: winnerAddress, myChoice: loserChoiceStr, opponentChoice: winnerChoiceStr }
            );
            
            await db.writeGameResult(winnerAddress, 'win', stakeAmount);
            await db.writeGameResult(loserAddress, 'lose', stakeAmount);
        } catch (err) {
            console.error(`[Lỗi] Không thể lấy thông tin phòng ${roomId} (sau resolve):`, err.message);
        }
    });

    contract.on("Canceled", async (roomId) => {
        console.log(`[SỰ KIỆN] Room ${roomId} đã bị hủy (Hòa/Timeout)`);
        try {
            const room = await contract.rooms(roomId);
            const stakeAmount = parseFloat(ethers.formatEther(room.stake));
            
            const creatorLangs = await db.getUsersForWallet(room.creator);
            const creatorLang = (creatorLangs[0] || {}).lang || defaultLang;
            const choiceStr = getChoiceString(room.revealA, creatorLang); 

            await sendInstantNotification(room.creator, 'notify_game_draw', { roomId: roomId, choice: choiceStr });
            if (room.opponent !== ethers.ZeroAddress) {
                const opponentLangs = await db.getUsersForWallet(room.opponent);
                const opponentLang = (opponentLangs[0] || {}).lang || defaultLang;
                const choiceStr_opp = getChoiceString(room.revealA, opponentLang);
                await sendInstantNotification(room.opponent, 'notify_game_draw', { roomId: roomId, choice: choiceStr_opp });
                
                await db.writeGameResult(room.creator, 'draw', stakeAmount);
                await db.writeGameResult(room.opponent, 'draw', stakeAmount);
            }
        } catch (err) {
            console.error(`[Lỗi] Không thể lấy thông tin phòng ${roomId} (sau cancel):`, err.message);
        }
    });

    contract.on("Forfeited", async (roomId, loser, winner, winnerPayout) => {
        console.log(`[SỰ KIỆN] Room ${roomId} có người bỏ cuộc: ${loser}`);
        const payoutAmount = ethers.formatEther(winnerPayout);
        const stakeAmount = parseFloat(ethers.formatEther(winnerPayout)) / 1.8;
        
        await sendInstantNotification(winner, 'notify_forfeit_win', { roomId: roomId, loser: loser, payout: payoutAmount });
        await sendInstantNotification(loser, 'notify_forfeit_lose', { roomId: roomId, winner: winner });
        
        if (stakeAmount > 0) {
            await db.writeGameResult(winner, 'win', stakeAmount);
            await db.writeGameResult(loser, 'lose', stakeAmount);
        }
    });

    provider.on("error", (error) => {
        console.error(`[LỖI WSS Provider]: ${error.message}. Bot sẽ tự động thử kết nối lại.`);
    });
}

// ==========================================================
// 🚀 PHẦN 5: HÀM GỬI THÔNG BÁO (CHỈ GỬI TEXT)
// ==========================================================
async function sendInstantNotification(playerAddress, langKey, variables = {}) {
    if (!playerAddress || playerAddress === ethers.ZeroAddress) return;
    
    const users = await db.getUsersForWallet(playerAddress); 
    if (!users || users.length === 0) {
        console.log(`[Notify] Không tìm thấy user nào theo dõi ví ${playerAddress}. Bỏ qua.`);
        return;
    }

    for (const user of users) {
        const { chatId, lang } = user;
        const message = t(lang, langKey, variables);

        const button = {
            text: `🎮 ${t(lang, 'action_button_play')}`, 
            url: `${WEB_URL}/?join=${variables.roomId || ''}`
        };
        
        let options = {
            parse_mode: "Markdown",
            reply_markup: {
                inline_keyboard: [ [ button ] ]
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
    }
}

// ==========================================================
// 🚀 KHỞI ĐỘNG TẤT CẢ DỊCH VỤ (CÁCH MỚI, AN TOÀN)
// ==========================================================
async function main() {
    try {
        console.log("Đang khởi động...");
        
        // Bước 1: Khởi tạo DB
        await db.init(); 

        // Bước 2: Xác thực kết nối WSS (bộ 'tai')
        console.log("Đang kết nối tới Blockchain (WSS)...");
        const networkPromise = provider.getNetwork();
        const timeoutPromise = new Promise((_, reject) => 
            setTimeout(() => reject(new Error("WSS connection timed out after 10 seconds")), 10000)
        );
        await Promise.race([networkPromise, timeoutPromise]);
        console.log("✅ [Blockchain] Kết nối WSS thành công.");
        
        // Bước 3: Bật bộ 'tai' (listener)
        startBlockchainListener(); 
        
        // Bước 4: Bật API
        startApiServer();
        
        // Bước 5: Bật Bot (bộ 'miệng')
        startTelegramBot();

        console.log("🚀 TẤT CẢ DỊCH VỤ ĐÃ SẴN SÀNG!");

    } catch (error) {
        console.error("LỖI KHỞI ĐỘNG NGHIÊM TRỌNG:", error);
        process.exit(1);
    }
}

main(); // Chạy hàm khởi động chính