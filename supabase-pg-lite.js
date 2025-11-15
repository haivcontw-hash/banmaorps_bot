'use strict';

const tls = require('tls');
const crypto = require('crypto');
const dns = require('dns');

function parseConnectionString(connectionString) {
    const parsed = new URL(connectionString);

    const config = {
        host: parsed.hostname,
        port: parsed.port ? Number(parsed.port) : 5432,
        user: decodeURIComponent(parsed.username || 'postgres'),
        password: decodeURIComponent(parsed.password || ''),
        database: parsed.pathname ? decodeURIComponent(parsed.pathname.replace(/^\//, '')) || undefined : undefined,
        ssl: parsed.searchParams.get('sslmode') === 'disable'
            ? null
            : { rejectUnauthorized: false }
    };

    if (!config.database) {
        config.database = config.user;
    }

    return config;
}

function escapeLiteral(value) {
    if (value === null || value === undefined) {
        return 'NULL';
    }

    if (typeof value === 'number') {
        if (!Number.isFinite(value)) {
            throw new Error('Cannot convert non-finite number to SQL literal');
        }
        return String(value);
    }

    if (typeof value === 'boolean') {
        return value ? 'TRUE' : 'FALSE';
    }

    if (value instanceof Date) {
        return `'${value.toISOString().replace(/'/g, "''")}'`;
    }

    const str = String(value);
    const escaped = str.replace(/\\/g, '\\\\').replace(/'/g, "''");
    return `'${escaped}'`;
}

function formatQuery(text, params) {
    if (!params || !params.length) {
        return text;
    }

    return text.replace(/\$(\d+)/g, (match, indexStr) => {
        const index = Number(indexStr) - 1;
        if (index < 0 || index >= params.length) {
            throw new Error(`Parameter index $${index + 1} out of range`);
        }

        return escapeLiteral(params[index]);
    });
}

function md5(input) {
    return crypto.createHash('md5').update(input).digest('hex');
}

function xorBuffers(a, b) {
    const length = Math.min(a.length, b.length);
    const output = Buffer.allocUnsafe(length);

    for (let i = 0; i < length; i++) {
        output[i] = a[i] ^ b[i];
    }

    return output;
}

function parseErrorResponse(buffer) {
    const fields = {};
    let offset = 0;

    while (offset < buffer.length) {
        const code = buffer[offset];
        offset += 1;

        if (code === 0) {
            break;
        }

        let end = offset;
        while (end < buffer.length && buffer[end] !== 0) {
            end += 1;
        }

        const value = buffer.toString('utf8', offset, end);
        fields[String.fromCharCode(code)] = value;
        offset = end + 1;
    }

    const message = fields.M || 'PostgreSQL error';
    const error = new Error(message);
    error.code = fields.C || undefined;
    error.detail = fields.D || undefined;
    error.schema = fields.s || undefined;
    error.table = fields.t || undefined;
    error.column = fields.c || undefined;
    error.fields = fields;
    return error;
}

class SupabasePgLiteClient {
    constructor(config) {
        this.config = config;
        this.socket = null;
        this.buffer = Buffer.alloc(0);
        this.connected = false;
        this.connectPromise = null;
        this.connectResolve = null;
        this.connectReject = null;
        this.queue = [];
        this.activeQuery = null;
        this.scramSession = null;
        this.expectedServerSignature = null;
        this.destroyed = false;
    }

    connect() {
        if (this.connectPromise) {
            return this.connectPromise;
        }

        this.connectPromise = new Promise((resolve, reject) => {
            this.connectResolve = resolve;
            this.connectReject = reject;

            const options = {
                host: this.config.host,
                port: this.config.port,
                servername: this.config.host,
                rejectUnauthorized: this.config.ssl?.rejectUnauthorized !== false
                    ? true
                    : false,
                lookup: (hostname, opts, cb) => {
                    const callback = typeof opts === 'function' ? opts : cb;
                    const options = typeof opts === 'function' ? {} : (opts || {});
                    dns.lookup(hostname, { ...options, family: 4, all: false }, callback);
                }
            };

            const onError = (err) => {
                if (!this.socket) {
                    this.failConnect(err);
                } else {
                    this.handleSocketError(err);
                }
            };

            const socket = tls.connect(options, () => {
                this.socket = socket;
                this.socket.on('data', (chunk) => this.handleData(chunk));
                this.socket.on('error', (err) => this.handleSocketError(err));
                this.socket.on('close', () => this.handleSocketClose());
                this.sendStartupMessage();
            });

            socket.once('error', onError);
        });

        return this.connectPromise;
    }

    failConnect(err) {
        if (this.connectReject) {
            this.connectReject(err);
            this.connectReject = null;
            this.connectResolve = null;
        }
    }

    handleSocketError(err) {
        if (this.destroyed) {
            return;
        }

        if (this.activeQuery && this.activeQuery.reject) {
            this.activeQuery.reject(err);
            this.activeQuery = null;
        }

        while (this.queue.length) {
            const pending = this.queue.shift();
            pending.reject(err);
        }

        if (!this.connected) {
            this.failConnect(err);
        }
    }

    handleSocketClose() {
        this.destroyed = true;

        if (this.activeQuery && this.activeQuery.reject) {
            this.activeQuery.reject(new Error('PostgreSQL connection closed'));
            this.activeQuery = null;
        }

        while (this.queue.length) {
            const pending = this.queue.shift();
            pending.reject(new Error('PostgreSQL connection closed'));
        }

        if (!this.connected) {
            this.failConnect(new Error('PostgreSQL connection closed'));
        }
    }

    sendStartupMessage() {
        const params = [
            ['user', this.config.user],
            ['database', this.config.database],
            ['client_encoding', 'UTF8'],
            ['application_name', 'banmaorps-bot']
        ];

        let length = 4 + 4 + 1;
        for (const [key, value] of params) {
            if (!value) {
                continue;
            }
            length += Buffer.byteLength(key) + 1;
            length += Buffer.byteLength(String(value)) + 1;
        }

        const buffer = Buffer.alloc(length);
        buffer.writeInt32BE(length, 0);
        buffer.writeInt32BE(196608, 4);
        let offset = 8;

        for (const [key, rawValue] of params) {
            const value = rawValue === undefined || rawValue === null ? '' : String(rawValue);
            if (!value) {
                continue;
            }

            buffer.write(key, offset, 'utf8');
            offset += Buffer.byteLength(key);
            buffer[offset] = 0;
            offset += 1;

            buffer.write(value, offset, 'utf8');
            offset += Buffer.byteLength(value);
            buffer[offset] = 0;
            offset += 1;
        }

        buffer[offset] = 0;
        this.socket.write(buffer);
    }

    handleData(chunk) {
        this.buffer = Buffer.concat([this.buffer, chunk]);

        while (this.buffer.length >= 5) {
            const messageType = this.buffer[0];
            const length = this.buffer.readInt32BE(1);
            const total = length + 1;

            if (this.buffer.length < total) {
                return;
            }

            const payload = this.buffer.slice(5, total);
            this.buffer = this.buffer.slice(total);

            this.handleMessage(messageType, payload);
        }
    }

    handleMessage(type, payload) {
        switch (type) {
            case 0x52:
                this.handleAuthentication(payload);
                break;
            case 0x53:
                break;
            case 0x4b:
                break;
            case 0x43:
                this.handleCommandComplete(payload);
                break;
            case 0x44:
                this.handleDataRow(payload);
                break;
            case 0x54:
                this.handleRowDescription(payload);
                break;
            case 0x5a:
                this.handleReadyForQuery(payload);
                break;
            case 0x45:
                this.handleErrorResponse(payload);
                break;
            case 0x4e:
                this.handleNotice(payload);
                break;
            default:
                break;
        }
    }

    handleNotice(payload) {
        try {
            const notice = parseErrorResponse(payload);
            console.warn('[Supabase] NOTICE:', notice.message);
        } catch (error) {
            // ignore
        }
    }

    handleAuthentication(payload) {
        const type = payload.readInt32BE(0);

        switch (type) {
            case 0:
                break;
            case 3:
                this.sendPasswordMessage(Buffer.from(`${this.config.password}\0`, 'utf8'));
                break;
            case 5: {
                const salt = payload.slice(4, 8);
                const inner = md5(`${this.config.password}${this.config.user}`);
                const innerBuffer = Buffer.concat([Buffer.from(inner), salt]);
                const hash = crypto.createHash('md5').update(innerBuffer).digest('hex');
                const response = Buffer.from(`md5${hash}\0`, 'utf8');
                this.sendPasswordMessage(response);
                break;
            }
            case 10: {
                this.startScramSession(payload.slice(4));
                break;
            }
            case 11: {
                this.continueScramSession(payload.slice(4));
                break;
            }
            case 12: {
                this.finaliseScramSession(payload.slice(4));
                break;
            }
            default:
                this.handleSocketError(new Error(`Unsupported authentication type: ${type}`));
                break;
        }
    }

    sendPasswordMessage(buffer) {
        const length = buffer.length + 4;
        const out = Buffer.alloc(1 + 4 + buffer.length);
        out[0] = 0x70;
        out.writeInt32BE(length, 1);
        buffer.copy(out, 5);
        this.socket.write(out);
    }

    startScramSession(mechanismsBuffer) {
        const mechanisms = [];
        let offset = 0;

        while (offset < mechanismsBuffer.length) {
            let end = offset;
            while (end < mechanismsBuffer.length && mechanismsBuffer[end] !== 0) {
                end += 1;
            }

            if (end === offset) {
                break;
            }

            mechanisms.push(mechanismsBuffer.toString('utf8', offset, end));
            offset = end + 1;
        }

        if (!mechanisms.includes('SCRAM-SHA-256')) {
            throw new Error('Server does not support SCRAM-SHA-256 authentication');
        }

        const clientNonce = crypto.randomBytes(18).toString('base64');
        const clientFirstBare = `n=,r=${clientNonce}`;
        const clientFirstMessage = `n,,${clientFirstBare}`;

        this.scramSession = {
            clientNonce,
            clientFirstBare,
            clientFirstMessage
        };

        const mechanism = Buffer.from('SCRAM-SHA-256\0', 'utf8');
        const response = Buffer.from(clientFirstMessage, 'utf8');
        const totalLength = mechanism.length + response.length + 8;
        const message = Buffer.alloc(1 + 4 + mechanism.length + 4 + response.length);
        message[0] = 0x70;
        message.writeInt32BE(totalLength, 1);
        let pos = 5;
        mechanism.copy(message, pos);
        pos += mechanism.length;
        message.writeInt32BE(response.length, pos);
        pos += 4;
        response.copy(message, pos);
        this.socket.write(message);
    }

    continueScramSession(payload) {
        if (!this.scramSession) {
            throw new Error('SCRAM session not initialised');
        }

        const message = payload.toString('utf8').replace(/\0+$/, '');
        const attributes = {};

        for (const part of message.split(',')) {
            if (!part) {
                continue;
            }
            const key = part[0];
            const value = part.slice(2);
            attributes[key] = value;
        }

        const nonce = attributes.r;
        const saltB64 = attributes.s;
        const iterations = Number(attributes.i || '0');

        if (!nonce || !saltB64 || !iterations) {
            throw new Error('Invalid SCRAM server response');
        }

        if (!nonce.startsWith(this.scramSession.clientNonce)) {
            throw new Error('Invalid SCRAM nonce from server');
        }

        const salt = Buffer.from(saltB64, 'base64');
        const saltedPassword = crypto.pbkdf2Sync(
            Buffer.from(this.config.password),
            salt,
            iterations,
            32,
            'sha256'
        );

        const clientKey = crypto.createHmac('sha256', saltedPassword).update('Client Key').digest();
        const storedKey = crypto.createHash('sha256').update(clientKey).digest();

        const clientFinalWithoutProof = `c=biws,r=${nonce}`;
        const authMessage = `${this.scramSession.clientFirstBare},${message},${clientFinalWithoutProof}`;

        const clientSignature = crypto.createHmac('sha256', storedKey).update(authMessage).digest();
        const clientProof = xorBuffers(clientKey, clientSignature).toString('base64');

        const serverKey = crypto.createHmac('sha256', saltedPassword).update('Server Key').digest();
        const serverSignature = crypto.createHmac('sha256', serverKey).update(authMessage).digest('base64');

        this.expectedServerSignature = serverSignature;

        const finalMessage = `${clientFinalWithoutProof},p=${clientProof}`;
        const response = Buffer.from(finalMessage, 'utf8');
        const out = Buffer.alloc(1 + 4 + response.length);
        out[0] = 0x70;
        out.writeInt32BE(response.length + 4, 1);
        response.copy(out, 5);
        this.socket.write(out);
    }

    finaliseScramSession(payload) {
        const message = payload.toString('utf8').replace(/\0+$/, '');

        if (message.startsWith('e=')) {
            throw new Error(`SCRAM authentication error: ${message.slice(2)}`);
        }

        const expected = this.expectedServerSignature;
        const received = message.startsWith('v=') ? message.slice(2) : message;

        if (expected && expected !== received) {
            throw new Error('SCRAM server signature mismatch');
        }

        this.scramSession = null;
        this.expectedServerSignature = null;
    }

    handleRowDescription(payload) {
        if (!this.activeQuery) {
            return;
        }

        const fieldCount = payload.readInt16BE(0);
        const fields = [];
        let offset = 2;

        for (let i = 0; i < fieldCount; i++) {
            let end = offset;
            while (payload[end] !== 0) {
                end += 1;
            }

            const name = payload.toString('utf8', offset, end);
            offset = end + 19; // skip name null + metadata (4 + 2 + 4 + 2 + 4 + 2)

            fields.push(name);
        }

        this.activeQuery.fields = fields;
    }

    handleDataRow(payload) {
        if (!this.activeQuery) {
            return;
        }

        const columnCount = payload.readInt16BE(0);
        const row = {};
        let offset = 2;

        for (let i = 0; i < columnCount; i++) {
            const length = payload.readInt32BE(offset);
            offset += 4;

            let value = null;
            if (length >= 0) {
                value = payload.toString('utf8', offset, offset + length);
                offset += length;
            }

            const fieldName = this.activeQuery.fields?.[i] || `column${i + 1}`;
            row[fieldName] = value;
        }

        this.activeQuery.rows.push(row);
    }

    handleCommandComplete(payload) {
        if (this.activeQuery) {
            const command = payload.toString('utf8', 0, payload.length - 1);
            this.activeQuery.command = command;
        }
    }

    handleErrorResponse(payload) {
        const error = parseErrorResponse(payload);

        if (!this.connected) {
            this.failConnect(error);
            return;
        }

        if (this.activeQuery) {
            this.activeQuery.error = error;
        }
    }

    handleReadyForQuery() {
        if (!this.connected) {
            this.connected = true;
            if (this.connectResolve) {
                this.connectResolve();
                this.connectResolve = null;
                this.connectReject = null;
            }
        }

        if (this.activeQuery) {
            const current = this.activeQuery;
            this.activeQuery = null;

            if (current.error) {
                current.reject(current.error);
            } else {
                const result = {
                    rows: current.rows,
                    rowCount: this.deriveRowCount(current),
                    command: current.command
                };
                current.resolve(result);
            }
        }

        this.processQueue();
    }

    deriveRowCount(query) {
        if (query.command) {
            const match = query.command.match(/(INSERT|UPDATE|DELETE)\s+\d+\s+(\d+)/i);
            if (match) {
                return Number(match[2]);
            }

            const selectMatch = query.command.match(/SELECT\s+(\d+)/i);
            if (selectMatch && query.rows) {
                return query.rows.length;
            }
        }

        return Array.isArray(query.rows) ? query.rows.length : 0;
    }

    query(text, params) {
        if (this.destroyed) {
            return Promise.reject(new Error('PostgreSQL connection closed'));
        }

        const task = {
            text,
            params,
            resolve: null,
            reject: null
        };

        const promise = new Promise((resolve, reject) => {
            task.resolve = resolve;
            task.reject = reject;
        });

        this.queue.push(task);
        this.processQueue();
        return promise;
    }

    async processQueue() {
        if (!this.connected || this.activeQuery || !this.queue.length) {
            return;
        }

        const next = this.queue.shift();

        try {
            const sql = formatQuery(next.text, next.params);
            const queryBuffer = Buffer.from(sql + '\0', 'utf8');
            const out = Buffer.alloc(1 + 4 + queryBuffer.length);
            out[0] = 0x51;
            out.writeInt32BE(queryBuffer.length + 4, 1);
            queryBuffer.copy(out, 5);

            this.activeQuery = {
                resolve: next.resolve,
                reject: next.reject,
                rows: [],
                fields: [],
                command: null,
                error: null
            };

            this.socket.write(out);
        } catch (error) {
            next.reject(error);
            this.processQueue();
        }
    }

    async close() {
        this.destroyed = true;
        if (this.socket) {
            this.socket.end();
        }
    }
}

class SupabasePgLitePool {
    constructor(config) {
        this.client = new SupabasePgLiteClient(config);
        this.ready = this.client.connect();
    }

    async query(text, params) {
        await this.ready;
        return this.client.query(text, params);
    }

    async end() {
        await this.client.close();
    }
}

function createPgLitePool(connectionString) {
    const config = parseConnectionString(connectionString);
    return new SupabasePgLitePool(config);
}

module.exports = {
    createPgLitePool
};

