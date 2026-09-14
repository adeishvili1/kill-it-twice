// Chaos service: lets the UI's Simulation screen stop and start the sink containers.
// It talks to the Docker Engine API through the mounted socket. This is a DEMO convenience —
// a container holding the Docker socket is root on the host and would never ship in production.
// verify.sh does not use it; it runs `docker stop/start` on the host directly.
import http from 'node:http';

const PORT = Number(process.env.PORT ?? 3002);
const PROJECT = process.env.COMPOSE_PROJECT ?? 'kit';
const ALLOWED = new Set(['elasticsearch', 'rabbitmq']);
const SOCKET = '/var/run/docker.sock';

function docker(method, path, body) {
  return new Promise((resolve, reject) => {
    const req = http.request({ socketPath: SOCKET, path: `/v1.43${path}`, method, headers: { 'content-type': 'application/json' } }, (res) => {
      let data = '';
      res.on('data', (c) => (data += c));
      res.on('end', () => resolve({ status: res.statusCode, body: data ? safeJson(data) : null }));
    });
    req.on('error', reject);
    if (body) req.write(JSON.stringify(body));
    req.end();
  });
}
const safeJson = (s) => { try { return JSON.parse(s); } catch { return { raw: s }; } };

async function listServices() {
  const filters = encodeURIComponent(JSON.stringify({ label: [`com.docker.compose.project=${PROJECT}`] }));
  const { status, body } = await docker('GET', `/containers/json?all=true&filters=${filters}`);
  if (status !== 200) throw new Error(`docker list failed: ${status}`);
  return body
    .map((c) => ({ service: c.Labels['com.docker.compose.service'], container: c.Names?.[0]?.replace(/^\//, ''), id: c.Id, state: c.State, status: c.Status }))
    .filter((c) => ALLOWED.has(c.service));
}

async function act(action, service) {
  if (!ALLOWED.has(service)) return { status: 400, body: { error: `service must be one of ${[...ALLOWED].join(', ')}` } };
  const target = (await listServices()).find((s) => s.service === service);
  if (!target) return { status: 404, body: { error: `no container for service ${service}` } };
  const r = await docker('POST', action === 'stop' ? `/containers/${target.id}/stop?t=5` : `/containers/${target.id}/start`);
  const ok = r.status === 204 || r.status === 304;
  return { status: ok ? 200 : 502, body: { service, action, ok, docker_status: r.status, detail: r.body } };
}

const server = http.createServer(async (req, res) => {
  const started = Date.now();
  const send = (status, body) => {
    res.writeHead(status, { 'content-type': 'application/json', 'access-control-allow-origin': '*', 'access-control-allow-headers': 'content-type', 'access-control-allow-methods': 'GET,POST,OPTIONS' });
    res.end(JSON.stringify(body));
    console.log(JSON.stringify({ app: 'chaos', method: req.method, path: req.url, status, ms: Date.now() - started }));
  };
  try {
    if (req.method === 'OPTIONS') return send(204, {});
    const url = new URL(req.url, 'http://x');
    if (req.method === 'GET' && url.pathname === '/health') {
      const ping = await docker('GET', '/_ping').catch(() => ({ status: 0 }));
      return send(200, { status: 'ok', docker: ping.status === 200 });
    }
    if (req.method === 'GET' && url.pathname === '/services') return send(200, await listServices());
    if (req.method === 'POST' && (url.pathname === '/stop' || url.pathname === '/start')) {
      let raw = '';
      for await (const chunk of req) raw += chunk;
      const { service } = safeJson(raw || '{}');
      const r = await act(url.pathname.slice(1), service);
      return send(r.status, r.body);
    }
    send(404, { error: 'not found' });
  } catch (e) {
    send(500, { error: e.message });
  }
});
server.listen(PORT, () => console.log(JSON.stringify({ app: 'chaos', event: 'listening', port: PORT, project: PROJECT })));
