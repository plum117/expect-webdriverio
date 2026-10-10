// Reproduction with WebDriver BiDi only (Node.js 22+, no dependency): how long Firefox takes to answer
// `network.getData` for the request body of a POST that a `network.addIntercept` blocks at `beforeRequestSent` and
// `responseStarted`, as the network mocks of WebdriverIO do.
//
// Usage: node request-body-getdata.mjs <geckodriver path> [firefox binary] [iterations] [mode]
//   mode: `intercept` (default, as WebdriverIO mocks) or `no-intercept` (only the data collector)
import { spawn } from 'node:child_process'

const [geckodriverPath, firefoxBinary, iterationsArg = '10', mode = 'intercept'] = process.argv.slice(2)
const iterations = Number(iterationsArg)
const port = 4400 + Math.floor(Math.random() * 500)
const pageUrl = 'https://guinea-pig.webdriver.io/'
const apiUrl = 'https://guinea-pig.webdriver.io/api/foo'

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
const log = (...args) => console.log(new Date().toISOString(), ...args)

const geckodriver = spawn(geckodriverPath, ['--port', String(port), '--websocket-port', String(port + 1000)], { stdio: 'ignore' })
let session
let ws

try {
    for (let i = 0; i < 50; i++) {
        try { await fetch(`http://127.0.0.1:${port}/status`); break } catch { await sleep(100) }
    }
    const firefoxOptions = { args: ['-headless'], ...(firefoxBinary && { binary: firefoxBinary }) }
    const response = await fetch(`http://127.0.0.1:${port}/session`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ capabilities: { alwaysMatch: { browserName: 'firefox', webSocketUrl: true, 'moz:firefoxOptions': firefoxOptions } } }),
    })
    const created = await response.json()
    if (!created.value?.capabilities) {
        throw new Error(`new session: ${JSON.stringify(created)}`)
    }
    session = created.value
    log('browser', session.capabilities.browserName, session.capabilities.browserVersion, process.platform, 'mode', mode)

    ws = new WebSocket(session.capabilities.webSocketUrl)
    await new Promise((resolve, reject) => { ws.onopen = resolve; ws.onerror = reject })

    let nextId = 1
    const pending = new Map()
    const handlers = []
    ws.onmessage = ({ data }) => {
        const message = JSON.parse(data)
        if (message.id !== undefined && pending.has(message.id)) {
            pending.get(message.id)(message)
            pending.delete(message.id)
        } else if (message.type === 'event') {
            handlers.forEach((handler) => handler(message))
        }
    }
    const send = (method, params) => new Promise((resolve) => {
        const id = nextId++
        pending.set(id, resolve)
        ws.send(JSON.stringify({ id, method, params }))
    })
    const command = async (method, params) => {
        const message = await send(method, params)
        if (message.type === 'error') {
            throw new Error(`${method}: ${message.error} ${message.message}`)
        }
        return message.result
    }

    await command('session.subscribe', { events: ['network.beforeRequestSent', 'network.responseStarted', 'network.responseCompleted'] })
    await command('network.addDataCollector', { dataTypes: ['request', 'response'], maxEncodedDataSize: 10485760 })
    const { contexts: [{ context }] } = await command('browsingContext.getTree', {})
    await command('browsingContext.navigate', { context, url: pageUrl, wait: 'complete' })
    if (mode === 'intercept') {
        await command('network.addIntercept', {
            phases: ['beforeRequestSent', 'responseStarted'],
            urlPatterns: [{ type: 'pattern', protocol: 'https', hostname: 'guinea-pig.webdriver.io', pathname: '/api/foo', port: '443' }],
        })
    }

    // Each completed POST to the API: time the `getData` of its request body, then of its response body
    const results = []
    let resolveCompleted
    handlers.push(async ({ method, params }) => {
        if (params.request?.url !== apiUrl) {
            return
        }
        const request = params.request.request
        if (method === 'network.beforeRequestSent' && params.isBlocked) {
            await send('network.continueRequest', { request })
        } else if (method === 'network.responseStarted' && params.isBlocked) {
            await send('network.provideResponse', { request })
        } else if (method === 'network.responseCompleted') {
            const start = performance.now()
            const requestData = await send('network.getData', { request, dataType: 'request' })
            const requestMs = Math.round(performance.now() - start)
            const responseStart = performance.now()
            const responseData = await send('network.getData', { request, dataType: 'response' })
            const responseMs = Math.round(performance.now() - responseStart)
            const describe = (message) => message.type === 'error' ? `error ${message.error}` : `${message.result.bytes.value.length} chars`
            results.push({ requestMs, responseMs })
            log(`request ${request}: getData request ${requestMs} ms (${describe(requestData)}), response ${responseMs} ms (${describe(responseData)})`)
            resolveCompleted()
        }
    })

    for (let i = 0; i < iterations; i++) {
        const completed = new Promise((resolve) => { resolveCompleted = resolve })
        await command('script.evaluate', {
            target: { context },
            awaitPromise: true,
            expression: `fetch(${JSON.stringify(apiUrl)}, { method: 'POST', headers: { Authorization: 'foo' }, body: JSON.stringify({ title: 'foo', description: 'bar' }) }).then((r) => r.status)`,
        })
        let timer
        await Promise.race([completed, new Promise((resolve) => { timer = setTimeout(() => resolve(log('no network.responseCompleted in 20 s')), 20000) })])
        clearTimeout(timer)
    }

    const requestTimes = results.map((r) => r.requestMs).sort((a, b) => a - b)
    log(`getData request body, ${requestTimes.length} calls: min ${requestTimes[0]} ms, median ${requestTimes[Math.floor(requestTimes.length / 2)]} ms, max ${requestTimes.at(-1)} ms`)
} finally {
    ws?.close()
    if (session) {
        await fetch(`http://127.0.0.1:${port}/session/${session.sessionId}`, { method: 'DELETE' }).catch(() => {})
    }
    geckodriver.kill()
}
