const fs = require('fs');
const path = require('path');

// Nạp các file dịch vào bộ nhớ
const locales = {};
const localesDir = path.join(__dirname, 'locales');
fs.readdirSync(localesDir).forEach(file => {
    if (file.endsWith('.json')) {
        const lang = file.split('.')[0];
        const content = fs.readFileSync(path.join(localesDir, file), 'utf8');
        locales[lang] = JSON.parse(content);
    }
});

// ===== THAY ĐỔI Ở ĐÂY =====
const defaultLang = 'en'; // Chọn tiếng Anh làm mặc định
// ==========================

/**
 * Lấy chuỗi dịch
 * @param {string} lang_code Mã ngôn ngữ (ví dụ: 'en', 'vi')
 * @param {string} key Khóa dịch (ví dụ: 'welcome_generic')
 * @param {Object} [variables={}] Biến để thay thế (ví dụ: { walletAddress: "0x..." })
 * @returns {string} Chuỗi đã dịch
 */
function t(lang_code, key, variables = {}) {
    // Ưu tiên ngôn ngữ của user, nếu không có thì dùng 'en'
    const lang = locales[lang_code] ? lang_code : defaultLang;
    
    let translation = '';

    // Thử lấy từ ngôn ngữ của user
    if (locales[lang] && locales[lang][key]) {
        translation = locales[lang][key];
    }
    // Nếu không có, thử ngôn ngữ mặc định (en)
    else if (locales[defaultLang] && locales[defaultLang][key]) {
        translation = locales[defaultLang][key];
    }
    // Nếu vẫn không có, trả về chính cái key
    else {
        return key;
    }

    // Thay thế biến (ví dụ: {walletAddress})
    for (const [varName, varValue] of Object.entries(variables)) {
        translation = translation.replace(`{${varName}}`, varValue);
    }
    
    return translation;
}

module.exports = { t_ : t }; // Đổi tên export để tránh xung đột