'use strict';

const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const { createServer } = require('node:http');

async function browserOrigin(base) {
  if (process.platform !== 'win32') return { origin: base };
  try {
    const reply = await fetch(`${base}/api/profiles`, { signal: AbortSignal.timeout(3000) });
    if (!reply.ok || !Array.isArray(await reply.json())) throw new Error('Runner is unavailable on Windows loopback');
    return { origin: base };
  } catch {
    // Some WSL configurations keep the Runner's loopback private to Linux.
    // Relay only this Runner origin over WSL, without publishing its port.
    const server = createServer((request, response) => {
      if (!request.url.startsWith('/') || request.url.startsWith('//')) {
        response.writeHead(400).end();
        return;
      }
      const args = ['--exec', 'curl', '-sS', '-i', '--max-time', '20', '-X', request.method];
      if (request.headers['content-type']) args.push('-H', `Content-Type: ${request.headers['content-type']}`);
      if (request.method === 'POST') args.push('--data-binary', '@-');
      args.push('--url', `${base}${request.url}`);
      const curl = spawn('wsl.exe', args, { windowsHide: true });
      const chunks = [];
      curl.stdout.on('data', chunk => chunks.push(chunk));
      curl.stderr.resume();
      curl.stdin.on('error', () => {});
      request.pipe(curl.stdin);
      curl.on('error', error => response.writeHead(502).end(error.message));
      curl.on('close', code => {
        if (response.writableEnded) return;
        const data = Buffer.concat(chunks);
        const boundary = data.indexOf('\r\n\r\n');
        const status = Number(/^HTTP\/\S+ (\d+)/.exec(data.toString('latin1', 0, boundary))?.[1]);
        if (code !== 0 || boundary < 0 || !status) {
          response.writeHead(502).end('WSL Runner did not reply');
          return;
        }
        const contentType = /^content-type:\s*(.+)$/im.exec(data.toString('latin1', 0, boundary))?.[1];
        response.writeHead(status, contentType ? { 'Content-Type': contentType } : {}).end(data.subarray(boundary + 4));
      });
    });
    await new Promise((resolve, reject) => server.once('error', reject).listen(0, '127.0.0.1', resolve));
    return { origin: `http://127.0.0.1:${server.address().port}`, server };
  }
}

async function main() {
  const [base, profile, modulePath, channel] = process.argv.slice(2);
  assert(base && profile, 'Usage: acceptance-ui.cjs <runner-origin> <scenario.yaml> [playwright-module] [browser-channel]');
  const { chromium } = require(modulePath || 'playwright');
  const { origin, server } = await browserOrigin(base);
  let browser;
  try {
    browser = await chromium.launch({ channel: channel || undefined });
    const page = await browser.newPage();
    await page.goto(origin, { waitUntil: 'domcontentloaded' });
    await page.locator('#scenario').selectOption({ value: profile });
    const [response] = await Promise.all([
      page.waitForResponse(reply => reply.url() === `${origin}/api/experiments` && reply.request().method() === 'POST'),
      page.locator('#start-button').click()
    ]);
    assert.equal(response.status(), 202);
    await page.waitForURL(url => url.origin === origin && /^\/experiments\/[0-9a-f-]{36}$/.test(url.pathname));
    const id = new URL(page.url()).pathname.split('/').at(-1);
    // The UI navigates immediately on success, which can discard the POST body.
    const record = await page.request.get(`${origin}/api/experiments/${id}`);
    assert.ok(record.ok(), `Runner returned ${record.status()} for ${id}`);
    const experiment = await record.json();
    assert.equal(experiment.id, id);
    console.log(JSON.stringify(experiment));
  } finally {
    try { await browser?.close(); }
    finally { server?.close(); }
  }
}

main().catch(error => { console.error(error); process.exitCode = 1; });
