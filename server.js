
const express = require('express');
const http = require('http');
const crypto = require('crypto');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const sessions = new Map();

const SESSION_DURATION = 5 * 60 * 1000;
const REQUEST_DURATION = 60 * 1000;

// إنهاء جلسة
function deleteSession(code, reason = 'انتهت الجلسة') {
    const session = sessions.get(code);

    if (!session) return;

    io.to(code).emit('session-ended', {
        message: reason
    });

    if (session.owner) {
        const owner = io.sockets.sockets.get(session.owner);

        if (owner) {
            owner.leave(code);
            owner.emit('session-closed', {
                code,
                message: reason
            });
        }
    }

    for (const deviceId of session.participants) {
        const device = io.sockets.sockets.get(deviceId);

        if (device) {
            device.leave(code);
        }
    }

    sessions.delete(code);

    console.log('Session deleted:', code);
}

// تنظيف الجلسات والطلبات المنتهية
setInterval(() => {
    const now = Date.now();

    for (const [code, session] of sessions) {
        if (now - session.createdAt >= SESSION_DURATION) {
            deleteSession(code, 'انتهى وقت الجلسة');
            continue;
        }

        for (const [requestId, request] of session.requests) {
            if (now >= request.expiresAt) {
                session.requests.delete(requestId);

                const device = io.sockets.sockets.get(request.deviceId);

                if (device) {
                    device.emit('session-rejected', {
                        message: 'انتهت مهلة طلب الانضمام'
                    });
                }

                const owner = io.sockets.sockets.get(session.owner);

                if (owner) {
                    owner.emit('request-expired', { requestId });
                }
            }
        }
    }
}, 10000);

// اتصال الأجهزة
io.on('connection', (socket) => {
    console.log('Device connected:', socket.id);

    // اختبار الاتصال
    socket.on('test-message', (message) => {
        console.log('Message received:', message);

        socket.emit(
            'server-reply',
            'Message received successfully!'
        );
    });

    // إنشاء جلسة
    socket.on('create-session', () => {
        for (const [code, session] of sessions) {
            if (session.owner === socket.id) {
                deleteSession(code, 'تم إنشاء جلسة جديدة');
            }
        }

        const sessionCode = crypto
            .randomBytes(4)
            .toString('hex')
            .toUpperCase();

        const session = {
            owner: socket.id,
            createdAt: Date.now(),
            requests: new Map(),
            participants: new Set()
        };

        sessions.set(sessionCode, session);

        socket.join(sessionCode);

        socket.emit('session-created', {
            code: sessionCode,
            expiresIn: 300
        });

        console.log('New session created:', sessionCode);
    });


    // طلب الانضمام
    socket.on('join-session', (receivedCode) => {
        if (typeof receivedCode !== 'string') {
            socket.emit('session-error', 'كود الجلسة غير صحيح');
            return;
        }

        const code = receivedCode.trim().toUpperCase();
        const session = sessions.get(code);

        if (!session || Date.now() - session.createdAt >= SESSION_DURATION) {
            socket.emit(
                'session-error',
                'الجلسة غير موجودة أو انتهت صلاحيتها'
            );
            return;
        }

        if (session.owner === socket.id) {
            socket.emit(
                'session-error',
                'لا يمكنك الانضمام إلى جلستك كضيف'
            );
            return;
        }

        if (session.participants.has(socket.id)) {
            socket.emit('session-approved', {
                message: 'أنت متصل بالفعل'
            });
            return;
        }

        const existingRequest = [...session.requests.values()]
            .find(request => request.deviceId === socket.id);

        if (existingRequest) {
            socket.emit('join-pending', {
                message: 'طلبك قيد الانتظار'
            });
            return;
        }

        const requestId = crypto.randomUUID();

        session.requests.set(requestId, {
            deviceId: socket.id,
            createdAt: Date.now(),
            expiresAt: Date.now() + REQUEST_DURATION
        });

        socket.emit('join-pending', {
            requestId,
            message: 'تم إرسال طلب الانضمام وينتظر موافقة اللابتوب'
        });

        io.to(session.owner).emit('join-request', {
            requestId,
            deviceId: socket.id,
            message: 'يوجد جهاز يطلب الانضمام'
        });

        console.log('Join request:', code, socket.id);
    });

    // موافقة صاحب اللابتوب
    socket.on('approve-device', ({ code, requestId } = {}) => {
        const session = sessions.get(code);

        if (!session || session.owner !== socket.id) {
            socket.emit('session-error', 'غير مسموح لك بالموافقة');
            return;
        }

        const request = session.requests.get(requestId);

        if (!request) {
            socket.emit('session-error', 'طلب الانضمام غير موجود أو انتهى');
            return;
        }

        if (Date.now() >= request.expiresAt) {
            session.requests.delete(requestId);
            socket.emit('session-error', 'انتهت مهلة طلب الانضمام');
            return;
        }

        const device = io.sockets.sockets.get(request.deviceId);

        session.requests.delete(requestId);

        if (!device) {
            socket.emit('session-error', 'الجهاز انقطع عن الاتصال');
            return;
        }

        device.join(code);
        session.participants.add(device.id);

        device.emit('session-approved', {
            message: 'تمت الموافقة على انضمامك'
        });

        socket.emit('device-approved', {
            deviceId: device.id,
            message: 'تم قبول الجهاز'
        });

        console.log('Device approved:', device.id);
    });

    // رفض طلب الانضمام
    socket.on('reject-device', ({ code, requestId } = {}) => {
        const session = sessions.get(code);

        if (!session || session.owner !== socket.id) {
            socket.emit('session-error', 'غير مسموح لك برفض الطلب');
            return;
        }

        const request = session.requests.get(requestId);

        if (!request) {
            socket.emit('session-error', 'طلب الانضمام غير موجود');
            return;
        }

        session.requests.delete(requestId);

        const device = io.sockets.sockets.get(request.deviceId);

        if (device) {
            device.emit('session-rejected', {
                message: 'تم رفض طلب الانضمام'
            });
        }

        socket.emit('device-rejected', {
            deviceId: request.deviceId
        });

        console.log('Device rejected:', request.deviceId);
    });

    // إنهاء الجلسة يدويًا من اللابتوب
    socket.on('end-session', (code) => {
        const session = sessions.get(code);

        if (!session || session.owner !== socket.id) {
            socket.emit('session-error', 'غير مسموح بإنهاء هذه الجلسة');
            return;
        }

        deleteSession(code, 'قام صاحب اللابتوب بإنهاء الجلسة');
    });

    // إرسال رسالة داخل الجلسة
    socket.on('session-message', ({ code, message } = {}) => {
        const session = sessions.get(code);

        if (!session || !session.participants.has(socket.id)) {
            socket.emit('session-error', 'يجب الانضمام للجلسة أولًا');
            return;
        }

        if (typeof message !== 'string' || message.length > 1000) {
            return;
        }

        io.to(code).emit('session-message', {
            sender: socket.id,
            message
        });
    });

    // انقطاع الجهاز
    socket.on('disconnect', () => {
        console.log('Device disconnected:', socket.id);

        for (const [code, session] of sessions) {
            if (session.owner === socket.id) {
                deleteSession(code, 'انقطع اتصال صاحب اللابتوب');
                continue;
            }

            for (const [requestId, request] of session.requests) {
                if (request.deviceId === socket.id) {
                    session.requests.delete(requestId);

                    io.to(session.owner).emit('request-expired', {
                        requestId
                    });
                }
            }

            if (session.participants.has(socket.id)) {
                session.participants.delete(socket.id);

                io.to(session.owner).emit('device-disconnected', {
                    deviceId: socket.id
                });
            }
        }
    });
});

// صفحة لوحة التحكم
app.get('/', (req, res) => {
    res.send(`
<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Phone Check - لوحة التحكم</title>

<style>
* {
    box-sizing: border-box;
}

body {
    background: #101827;
    color: white;
    font-family: Arial, sans-serif;
    padding: 20px;
    margin: 0;
}

h2 {
    color: #38bdf8;
    text-align: center;
}

.card {
    background: #1e293b;
    padding: 16px;
    margin: 12px 0;
    border-radius: 12px;
    overflow-wrap: anywhere;
}

button {
    background: #0284c7;
    color: white;
    border: 0;
    padding: 11px 17px;
    border-radius: 8px;
    cursor: pointer;
    margin: 5px;
}

button:disabled {
    opacity: .5;
    cursor: not-allowed;
}

.danger {
    background: #dc2626;
}

.success {
    background: #16a34a;
}

input {
    width: 100%;
    padding: 12px;
    margin: 8px 0;
    border: 0;
    border-radius: 8px;
}

.info-title {
    color: #38bdf8;
    font-weight: bold;
}

#requests {
    margin-top: 12px;
}

small {
    color: #94a3b8;
}
</style>
</head>

<body>

<h2>Phone Check - لوحة التحكم</h2>

<div class="card">
    <h3>حالة الاتصال</h3>
    <p id="connection">جاري الاتصال بالسيرفر...</p>
</div>

<div class="card">
    <h3>إدارة الجلسة</h3>

    <button id="createBtn">إنشاء جلسة جديدة</button>

    <p>كود الجلسة:</p>
    <h2 id="sessionCode">---</h2>

    <p id="sessionTimer">لم يتم إنشاء جلسة</p>

    <button id="endBtn" class="danger" disabled>
        إنهاء الجلسة
    </button>
</div>

<div class="card">
    <h3>طلبات الانضمام</h3>
    <div id="requests">لا توجد طلبات حاليًا</div>
</div>

<div class="card">
    <h3>الأجهزة المتصلة</h3>
    <div id="devices">لا توجد أجهزة متصلة</div>
</div>

<div class="card">
    <h3>اختبار الرسائل</h3>

    <input id="testInput" placeholder="اكتب رسالة للاختبار">

    <button id="sendTest">إرسال رسالة</button>

    <p id="testResult"></p>
</div>

<div class="card">
    <h3>معلومات المتصفح والجهاز</h3>
    <div id="info">جاري الفحص...</div>
</div>

<script src="/socket.io/socket.io.js"></script>

<script>
const socket = io();

let currentCode = null;
let timerInterval = null;
let sessionEndTime = null;

const connection = document.getElementById('connection');
const sessionCode = document.getElementById('sessionCode');
const sessionTimer = document.getElementById('sessionTimer');
const requests = document.getElementById('requests');
const devices = document.getElementById('devices');

const createBtn = document.getElementById('createBtn');
const endBtn = document.getElementById('endBtn');

const pendingRequests = new Map();
const approvedDevices = new Set();

socket.on('connect', () => {
    connection.textContent = 'متصل بالسيرفر';
});

socket.on('disconnect', () => {
    connection.textContent = 'انقطع الاتصال بالسيرفر';
});

createBtn.addEventListener('click', () => {
    socket.emit('create-session');
});

socket.on('session-created', (data) => {
    currentCode = data.code;

    sessionCode.textContent = data.code;
    sessionTimer.textContent = 'الجلسة نشطة';

    createBtn.disabled = true;
    endBtn.disabled = false;

    sessionEndTime = Date.now() + data.expiresIn * 1000;

    clearInterval(timerInterval);

    timerInterval = setInterval(() => {
        const remaining = Math.max(
            0,
            Math.ceil((sessionEndTime - Date.now()) / 1000)
        );

        sessionTimer.textContent =
            'الوقت المتبقي: ' + remaining + ' ثانية';

        if (remaining <= 0) {
            clearInterval(timerInterval);
        }
    }, 1000);
});

socket.on('join-request', (data) => {
    if (!currentCode) return;

    pendingRequests.set(data.requestId, data);

    renderRequests();
});


function renderRequests() {
    requests.innerHTML = '';

    if (pendingRequests.size === 0) {
        requests.textContent = 'لا توجد طلبات حاليًا';
        return;
    }

    for (const [requestId, request] of pendingRequests) {
        const card = document.createElement('div');
        card.className = 'card';

        const title = document.createElement('p');
        title.textContent = request.message;

        const deviceId = document.createElement('small');
        deviceId.textContent = 'معرّف الجهاز: ' + request.deviceId;

        const approve = document.createElement('button');
        approve.className = 'success';
        approve.textContent = 'موافقة';

        approve.addEventListener('click', () => {
            socket.emit('approve-device', {
                code: currentCode,
                requestId
            });
        });

        const reject = document.createElement('button');
        reject.className = 'danger';
        reject.textContent = 'رفض';

        reject.addEventListener('click', () => {
            socket.emit('reject-device', {
                code: currentCode,
                requestId
            });
        });

        card.append(title, deviceId, approve, reject);
        requests.appendChild(card);
    }
}

socket.on('device-approved', (data) => {
    pendingRequests.forEach((request, requestId) => {
        if (request.deviceId === data.deviceId) {
            pendingRequests.delete(requestId);
        }
    });

    approvedDevices.add(data.deviceId);
    renderRequests();
    renderDevices();
});

socket.on('device-rejected', (data) => {
    pendingRequests.forEach((request, requestId) => {
        if (request.deviceId === data.deviceId) {
            pendingRequests.delete(requestId);
        }
    });

    renderRequests();
});

socket.on('request-expired', (data) => {
    pendingRequests.delete(data.requestId);
    renderRequests();
});

socket.on('device-disconnected', (data) => {
    approvedDevices.delete(data.deviceId);
    renderDevices();
});

function renderDevices() {
    devices.innerHTML = '';

    if (approvedDevices.size === 0) {
        devices.textContent = 'لا توجد أجهزة متصلة';
        return;
    }

    approvedDevices.forEach((deviceId) => {
        const item = document.createElement('div');
        item.className = 'card';
        item.textContent = 'جهاز متصل: ' + deviceId;
        devices.appendChild(item);
    });
}

endBtn.addEventListener('click', () => {
    if (!currentCode) return;

    socket.emit('end-session', currentCode);
});

socket.on('session-ended', (data) => {
    sessionTimer.textContent = data.message;
});

socket.on('session-closed', (data) => {
    currentCode = null;

    sessionCode.textContent = '---';
    sessionTimer.textContent = data.message;

    pendingRequests.clear();
    approvedDevices.clear();

    renderRequests();
    renderDevices();

    createBtn.disabled = false;
    endBtn.disabled = true;

    clearInterval(timerInterval);
});

socket.on('session-error', (message) => {
    alert(message);
});

document.getElementById('sendTest').addEventListener('click', () => {
    const message = document.getElementById('testInput').value.trim();

    if (!message) return;

    socket.emit('test-message', message);
});

socket.on('server-reply', (message) => {
    document.getElementById('testResult').textContent = message;
});

// معلومات المتصفح المتاحة فقط
const info = {
    'المتصفح': navigator.userAgent,
    'اللغة': navigator.language,
    'اللغات': navigator.languages.join(', '),
    'نظام التشغيل المعلن': navigator.platform,
    'عدد المعالجات': navigator.hardwareConcurrency || 'غير متاح',
    'الذاكرة التقريبية': navigator.deviceMemory || 'غير متاح',
    'حجم الشاشة': screen.width + ' × ' + screen.height,
    'عمق الألوان': screen.colorDepth,
    'المنطقة الزمنية':
        Intl.DateTimeFormat().resolvedOptions().timeZone,
    'حالة الاتصال': navigator.onLine ? 'متصل' : 'غير متصل'
};

const infoContainer = document.getElementById('info');

for (const [key, value] of Object.entries(info)) {
    const card = document.createElement('div');
    card.className = 'card';

    const title = document.createElement('span');
    title.className = 'info-title';
    title.textContent = key;

    const content = document.createElement('p');
    content.textContent = String(value);

    card.append(title, content);
    infoContainer.appendChild(card);
}
</script>

</body>
</html>
    `);
});

// تشغيل السيرفر
const PORT = process.env.PORT || 3000;

server.listen(PORT, '0.0.0.0', () => {
    console.log(`Server running on port ${PORT}`);
});
