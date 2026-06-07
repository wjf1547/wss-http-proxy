const WebSocket = require('ws');
const http = require('http');
const url = require('url');

const EXPECTED_TOKEN = process.env.EXPECTED_TOKEN
const PORT = process.env.PORT || 3001;

// 💡 核心改动：使用 Map 存储多设备代理通道 [deviceId -> socket]
const agentSockets = new Map();
// 存储正在挂起等待内网响应的 HTTP 请求 [requestId -> res]
const pendingRequests = new Map();
let requestCounter = 0;

const server = http.createServer((req, res) => {
    // 允许跨域
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', '*');
    if (req.method === 'OPTIONS') {
        res.writeHead(200);
        return res.end();
    }

    const parsedUrl = url.parse(req.url);
    const pathSegments = parsedUrl.pathname.split('/').filter(Boolean);

    // 💡 多设备路由规则：/p/{deviceId}/{内网IP}/{端口}/{真实路径...}
    // 例如访问 Server-A 上的青龙面板：http://render-host/p/Server-A/127.0.0.1/5700/
    if (pathSegments[0] === 'p' && pathSegments.length >= 4) {
        const deviceId = pathSegments[1];
        const intranetIp = pathSegments[2];
        const intranetPort = pathSegments[3];
        const realPath = '/' + pathSegments.slice(4).join('/');

        // 获取目标设备的 WebSocket 连接
        const targetAgent = agentSockets.get(deviceId);

        if (!targetAgent || targetAgent.readyState !== WebSocket.OPEN) {
            res.writeHead(502, { 'Content-Type': 'text/plain; charset=utf-8' });
            return res.end(`错误：设备 [${deviceId}] 当前不在线或代理通道未建立。`);
        }

        const targetUrl = `http://${intranetIp}:${intranetPort}${realPath}${parsedUrl.search || ''}`;
        const requestId = `${Date.now()}-${++requestCounter}`;
        pendingRequests.set(requestId, res);

        // 读取请求体（支持 POST 登录等操作）
        let bodyChunks = [];
        req.on('data', chunk => bodyChunks.push(chunk));
        req.on('end', () => {
            const bodyBase64 = Buffer.concat(bodyChunks).toString('base64');
            
            const proxyPayload = {
                type: 'HTTP_REQ',
                requestId,
                url: targetUrl,
                method: req.method,
                headers: req.headers,
                body: bodyBase64
            };

            // 清理可能导致内网服务器拒绝访问的头部
            delete proxyPayload.headers['host'];
            delete proxyPayload.headers['referer'];

            targetAgent.send(JSON.stringify(proxyPayload));
        });

        // 30秒超时保护
        setTimeout(() => {
            if (pendingRequests.has(requestId)) {
                res.writeHead(504);
                res.end('Gateway Timeout: 内网设备响应超时');
                pendingRequests.delete(requestId);
            }
        }, 30000);

    } else {
        // 根目录：动态展示当前有哪些设备在线，并提供快捷访问链接
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        
        const onlineDevices = Array.from(agentSockets.keys());
        let deviceListHtml = onlineDevices.length === 0 
            ? '<li>当前暂无内网设备在线...</li>' 
            : onlineDevices.map(id => `<li><b>${id}</b> (已就绪)</li>`).join('');

        res.end(`
            <h3>🌐 WSS 多设备内网代理网关已启动</h3>
            <p><b>当前在线的设备列表：</b></p>
            <ul>${deviceListHtml}</ul>
            <hr/>
            <p><b>访问路径格式：</b></p>
            <code>http://${req.headers.host}/p/<b>{设备ID}</b>/<b>{内网IP}</b>/<b>{端口}</b>/</code>
            <br/><br/>
            <p>示例（假设设备 Server-A 在线，访问其本地 5700 端口）：</p>
            <a href="/p/Server-A/127.0.0.1/5700/">/p/Server-A/127.0.0.1/5700/</a>
        `);
    }
});

const wss = new WebSocket.Server({ server });

wss.on('connection', (ws, req) => {
    const location = url.parse(req.url, true);
    const token = location.query.token;
    const deviceId = location.query.deviceId; // 💡 接收客户端传来的设备ID

    if (token !== EXPECTED_TOKEN) {
        return ws.close(4001, "Unauthorized");
    }
    if (!deviceId) {
        return ws.close(4002, "Missing deviceId");
    }

    // 将设备连接存入 Map
    agentSockets.set(deviceId, ws);
    console.log(`=== 代理通道：内网设备 [${deviceId}] 已成功绑定 ===`);

    ws.on('message', (message) => {
        try {
            const responseData = JSON.parse(message.toString());
            if (responseData.type === 'HTTP_RES') {
                const { requestId, status, headers, body } = responseData;
                const res = pendingRequests.get(requestId);
                
                if (res) {
                    pendingRequests.delete(requestId);
                    const binaryBody = Buffer.from(body, 'base64');

                    // 转发响应头
                    Object.keys(headers).forEach(key => {
                        if (!['content-length', 'transfer-encoding'].includes(key.toLowerCase())) {
                            res.setHeader(key, headers[key]);
                        }
                    });

                    res.writeHead(status);
                    res.end(binaryBody);
                }
            }
        } catch (e) {
            console.error("解析 Agent 响应错误:", e);
        }
    });

    ws.on('close', () => {
        console.log(`=== 代理通道：内网设备 [${deviceId}] 已断开 ===`);
        agentSockets.delete(deviceId);
    });
});

server.listen(PORT, () => {
    console.log(`独立多设备中转网关已在端口 ${PORT} 启动...`);
});
