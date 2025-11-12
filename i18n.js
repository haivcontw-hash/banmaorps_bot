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

const defaultLang = 'en'; // Chọn tiếng Anh làm mặc định

function normalizeLanguageCode(lang_code) {
    if (!lang_code) {
        return defaultLang;
    }

    let candidate = String(lang_code).trim();
    if (!candidate) {
        return defaultLang;
    }

    candidate = candidate.toLowerCase();

    if (locales[candidate]) {
        return candidate;
    }

    const hyphenIndex = candidate.indexOf('-');
    if (hyphenIndex !== -1) {
        const base = candidate.substring(0, hyphenIndex);
        if (locales[base]) {
            return base;
        }
    }

    return defaultLang;
}

/**
 * Lấy chuỗi dịch
 * @param {string} lang_code Mã ngôn ngữ (ví dụ: 'en', 'vi')
 * @param {string} key Khóa dịch (ví dụ: 'welcome_generic')
 * @param {Object} [variables={}] Biến để thay thế (ví dụ: { walletAddress: "0x..." })
 * @returns {string} Chuỗi đã dịch
 */
function t(lang_code, key, variables = {}) {
    const lang = normalizeLanguageCode(lang_code);

    let translation = '';

    if (locales[lang] && locales[lang][key]) {
        translation = locales[lang][key];
    }
    else if (locales[defaultLang] && locales[defaultLang][key]) {
        translation = locales[defaultLang][key];
    }
    else {
        return key;
    }

    for (const [varName, varValue] of Object.entries(variables)) {
        translation = translation.replace(`{${varName}}`, varValue);
    }

    return translation;
}

module.exports = { t_: t, normalizeLanguageCode };
