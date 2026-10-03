// Carga sustentada de Pix nos dois sentidos (A → B e B → A), com falhas
// opcionais nos simuladores. Cada iteração faz um Pix e acompanha o estado até
// o fim, medindo a experiência do pagador (do pedido ao estado final).
//
// Parâmetros (variáveis de ambiente):
//   RATE        Pix por segundo, somando os dois sentidos (padrão 10)
//   DURATION    duração da carga (padrão 60s)
//   DIRECTION   both (padrão), a_to_b ou b_to_a
//   FAULTS_SPI, FAULTS_DICT, FAULTS_BACEN
//               JSON de falhas (ver docs: /admin/faults) aplicado no início e
//               desligado no fim, ex.: '{"errorRate":0.3}'
//
// Use `make load`; com falhas ativas os limites abaixo podem falhar de
// propósito: é o que o cenário quer mostrar.
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const PSP_A = __ENV.PSP_A_URL || 'http://psp-a-api:8080';
const PSP_B = __ENV.PSP_B_URL || 'http://psp-b-api:8080';
const SIMS = {
  spi: [__ENV.SPI_URL || 'http://spi-sim:8080', __ENV.FAULTS_SPI],
  dict: [__ENV.DICT_URL || 'http://dict-sim:8080', __ENV.FAULTS_DICT],
  bacen: [__ENV.BACEN_URL || 'http://bacen-sim:8080', __ENV.FAULTS_BACEN],
};

// Contas e chaves da carga inicial (deploy/sims/dict-seed.json). Várias chaves
// por conta, de tipos diferentes, para exercitar o DICT além do cache.
const A = {
  base: PSP_A,
  accounts: ['1000001', '1000002'],
  keys: ['alice@example.com', 'carol@example.com', '+5511990000001', '+5511990000002', '12345678909', '98765432100'],
};
const B = {
  base: PSP_B,
  accounts: ['2000001', '2000002'],
  keys: ['bob@example.com', 'dave@example.com', '+5521990000001', '+5521990000002', '11144477735', '39053344705'],
};

const rate = Number(__ENV.RATE || 10);
const duration = __ENV.DURATION || '60s';

// DIRECTION: both (padrão), a_to_b ou b_to_a. Um sentido só esvazia a conta PI
// de quem paga (cenário de liquidez).
const direction = __ENV.DIRECTION || 'both';
const scenarios = {};
if (direction !== 'b_to_a') {
  scenarios.a_para_b = scenario('a_para_b', direction === 'both' ? rate / 2 : rate, 'A', 'B');
}
if (direction !== 'a_to_b') {
  scenarios.b_para_a = scenario('b_para_a', direction === 'both' ? rate / 2 : rate, 'B', 'A');
}

export const options = {
  scenarios,
  thresholds: {
    // Iniciação (IGA): 95% dos pedidos aceitos em até 1,5s.
    'http_req_duration{name:create}': ['p(95)<1500'],
    // Experiência do pagador: 99% dos Pix chegam ao estado final em até 10s.
    pix_end_to_end_seconds: ['p(99)<10000'],
    // Todo Pix aceito precisa terminar (nada fica preso).
    pix_unfinished: ['count==0'],
  },
};

function scenario(name, perSecond, from, to) {
  return {
    executor: 'constant-arrival-rate',
    rate: Math.max(perSecond, 0.1),
    timeUnit: '1s',
    duration,
    preAllocatedVUs: 50,
    maxVUs: 500,
    exec: 'pix',
    env: { FROM: from, TO: to },
    tags: { direction: name },
  };
}

const endToEnd = new Trend('pix_end_to_end_seconds', true);
const finalState = new Counter('pix_final_total');
const unfinished = new Counter('pix_unfinished');
const syncErrors = new Counter('pix_sync_errors_total');

const JSON_HEADERS = { 'Content-Type': 'application/json' };
const TERMINAL = new Set(['CONFIRMED', 'REJECTED']);

function pick(list) {
  return list[Math.floor(Math.random() * list.length)];
}

export function pix() {
  const payer = __ENV.FROM === 'A' ? A : B;
  const payee = __ENV.FROM === 'A' ? B : A;
  // Valores pequenos e simétricos entre os sentidos, para as contas não secarem.
  const amount = (Math.floor(Math.random() * 100) + 1) / 100;

  const started = Date.now();
  const res = http.post(
    `${payer.base}/v1/pix/payments`,
    JSON.stringify({
      debtorAccount: { branch: '0001', number: pick(payer.accounts) },
      key: pick(payee.keys),
      amount,
    }),
    {
      headers: { ...JSON_HEADERS, 'Idempotency-Key': `${__VU}-${__ITER}-${started}` },
      tags: { name: 'create' },
    },
  );
  const ok = check(res, { 'pedido aceito (201)': (r) => r.status === 201 });
  if (!ok) {
    // Recusas síncronas (saldo, DICT fora, limite de consultas) são resultado,
    // não erro do teste: ficam contadas por código.
    let code = `http_${res.status}`;
    try {
      code = res.json('code') || code;
    } catch (_) {}
    syncErrors.add(1, { code });
    return;
  }

  const id = res.json('endToEndId');
  const deadline = started + 30000;
  for (let poll = 0; Date.now() < deadline; poll++) {
    // Consulta logo no começo, quando o normal é terminar em ~300ms, e depois
    // espaça: sob pressão, sondar a cada 250ms só somaria carga.
    sleep(poll < 4 ? 0.25 : 1);
    const st = http.get(`${payer.base}/v1/pix/payments/${id}`, { tags: { name: 'get' } });
    const state = st.status === 200 ? st.json('state') : '';
    if (TERMINAL.has(state)) {
      endToEnd.add(Date.now() - started);
      finalState.add(1, { state, reason: st.json('reason') || 'none' });
      return;
    }
  }
  unfinished.add(1);
}

function admin(url, method, body) {
  const r = http.request(method, `${url}/admin/faults`, body, { headers: JSON_HEADERS, tags: { name: 'admin' } });
  if (r.status !== 200) {
    throw new Error(`${method} ${url}/admin/faults: status ${r.status} ${r.body}`);
  }
}

export function setup() {
  for (const [name, [url, faults]] of Object.entries(SIMS)) {
    if (faults) {
      console.log(`falhas em ${name}: ${faults}`);
      admin(url, 'PUT', faults);
    }
  }
  const a = http.get(`${PSP_A}/v1/accounts/0001/1000001`);
  const b = http.get(`${PSP_B}/v1/accounts/0001/2000001`);
  if (a.status !== 200 || b.status !== 200) {
    throw new Error(`PSPs fora do ar: A=${a.status} B=${b.status}`);
  }
  console.log(`saldos iniciais: Alice ${a.json('balance.available')}, Bob ${b.json('balance.available')}`);
}

export function teardown() {
  for (const [name, [url, faults]] of Object.entries(SIMS)) {
    if (faults) {
      console.log(`desligando falhas em ${name}`);
      admin(url, 'DELETE');
    }
  }
}
