<script setup>
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { api } from '../api'

const status = ref(null)
const prev = ref(null)
const loading = ref(false)
const error = ref('')
const lastLoaded = ref(null)
const auto = ref(true)
const sending = ref(false)
const log = ref([])
let timer = null

async function load() {
  loading.value = true
  error.value = ''
  try {
    const next = await api.pipelineStatus()
    diff(status.value, next)
    prev.value = status.value
    status.value = next
    lastLoaded.value = new Date()
  } catch (e) {
    error.value = e.message
  } finally {
    loading.value = false
  }
}

// 이전 조회와 비교해 바뀐 숫자를 로그로 남긴다 — 이벤트가 단계별로 옮겨 가는 것을 보여 주기 위해
function diff(a, b) {
  if (!a || !b) return
  const t = new Date().toLocaleTimeString('ko-KR')
  const push = (m) => log.value.unshift({ t, m })
  if (a.app?.ok && b.app?.ok) {
    if (b.app.orders !== a.app.orders) push(`① 앱 H2 주문 ${a.app.orders} → ${b.app.orders}`)
    if (b.app.payments !== a.app.payments) push(`① 앱 H2 결제 ${a.app.payments} → ${b.app.payments}`)
  }
  if (a.kafka?.ok && b.kafka?.ok) {
    b.kafka.topics.forEach((tp, i) => {
      const before = a.kafka.topics[i]?.messages
      if (before !== undefined && tp.messages !== before) push(`② Kafka ${tp.topic} 메시지 ${before} → ${tp.messages}`)
    })
    b.kafka.groups.forEach((g, i) => {
      const before = a.kafka.groups[i]?.lag
      if (before !== undefined && g.lag !== before) push(`② ${g.group} lag ${before} → ${g.lag}`)
    })
  }
  if (a.iceberg?.ok && b.iceberg?.ok) {
    b.iceberg.tables.forEach((tb, i) => {
      const before = a.iceberg.tables[i]
      if (before && tb.snapshots !== before.snapshots) push(`④ Iceberg ${tb.table} 커밋 (스냅샷 ${before.snapshots} → ${tb.snapshots}, 파일 ${before.files} → ${tb.files})`)
    })
  }
  if (a.trino?.ok && b.trino?.ok) {
    b.trino.tables.forEach((tb, i) => {
      const before = a.trino.tables[i]?.rows
      if (before !== undefined && tb.rows !== before) push(`⑤ Trino ${tb.t} 행 ${before} → ${tb.rows}`)
    })
  }
  log.value = log.value.slice(0, 40)
}

async function sendTest() {
  sending.value = true
  error.value = ''
  try {
    const amount = 10000 + Math.floor(Math.random() * 9) * 5000
    const o = await api.createOrder('customer-demo', '파이프라인 테스트', amount)
    await api.pay(o.id, ['CARD', 'BANK', 'POINT'][Math.floor(Math.random() * 3)], false)
    log.value.unshift({ t: new Date().toLocaleTimeString('ko-KR'), m: `▶ 테스트 주문 ${o.id} 생성 + 결제 → 이벤트 3건 발행 (ORDER_CREATED, PAYMENT_REQUESTED, PAYMENT_COMPLETED)` })
    await load()
  } catch (e) {
    error.value = e.message
  } finally {
    sending.value = false
  }
}

const totalLag = computed(() => status.value?.kafka?.ok ? status.value.kafka.groups.reduce((s, g) => s + Number(g.lag), 0) : null)
const kb = (b) => (b == null ? '' : (Number(b) / 1024).toFixed(1) + ' KB')
const hhmmss = (s) => (s ? String(s).slice(11, 19) : '–')

function schedule() {
  clearInterval(timer)
  if (auto.value) timer = setInterval(load, 5000)
}
onMounted(() => { load(); schedule() })
onBeforeUnmount(() => clearInterval(timer))
</script>

<template>
  <main class="page">
    <section class="card">
      <div class="row" style="justify-content: space-between">
        <div>
          <h2>파이프라인 <span class="muted">앱 → Kafka → Kafka Connect → Iceberg / MinIO → Trino</span></h2>
          <p class="desc" style="margin: 0">각 단계의 현재 상태를 5초마다 읽습니다. 테스트 결제를 보내고 숫자가 오른쪽으로 옮겨 가는 것을 보세요. Kafka lag 이 0 이 되고 Iceberg 스냅샷이 늘어난 뒤에야 Trino 행 수가 바뀝니다.</p>
        </div>
        <div class="row">
          <button class="primary" type="button" :disabled="sending" @click="sendTest">{{ sending ? '발행 중…' : '▶ 테스트 결제 1건 발행' }}</button>
          <label class="muted"><input v-model="auto" type="checkbox" @change="schedule" /> 5초 자동 갱신</label>
          <button type="button" :disabled="loading" @click="load">{{ loading ? '조회 중…' : '지금 조회' }}</button>
        </div>
      </div>
      <div v-if="error" class="note bad" style="margin-top: 10px">{{ error }}</div>
      <div v-else-if="lastLoaded" class="muted" style="margin-top: 8px; font-size: 12px">마지막 조회 {{ lastLoaded.toLocaleTimeString('ko-KR') }}</div>
    </section>

    <div v-if="status" class="flow">
      <!-- ① 앱 -->
      <section class="stage" :class="{ down: !status.app.ok }">
        <div class="stage-head"><span class="no">①</span><span class="name">앱 · H2</span><span class="role">운영 DB 역할 · 이벤트 발행</span></div>
        <template v-if="status.app.ok">
          <div class="metric"><span class="k">주문</span><span class="v">{{ status.app.orders }}</span></div>
          <div class="metric"><span class="k">결제</span><span class="v">{{ status.app.payments }}</span></div>
          <div class="sub">→ {{ status.app.topics.join(', ') }}</div>
        </template>
        <div v-else class="err">{{ status.app.error }}</div>
      </section>
      <div class="arrow"><span>send()</span></div>

      <!-- ② Kafka -->
      <section class="stage" :class="{ down: !status.kafka.ok }">
        <div class="stage-head"><span class="no">②</span><span class="name">Kafka</span><span class="role">토픽 · 파티션 · 오프셋</span></div>
        <template v-if="status.kafka.ok">
          <div v-for="tp in status.kafka.topics" :key="tp.topic" class="metric">
            <span class="k mono">{{ tp.topic }}</span>
            <span class="v">{{ tp.messages }}<small class="muted"> msg · {{ tp.partitions.map((p) => p.endOffset).join('/') }}</small></span>
          </div>
          <div class="sub" :class="totalLag > 0 ? 'warn' : 'good'">커넥터 lag 합계 {{ totalLag }} <small>(0 이면 모두 적재됨)</small></div>
          <div v-for="g in status.kafka.groups" :key="g.group" class="sub mono">{{ g.group.replace('connect-', '') }} · committed {{ g.committed }} · lag {{ g.lag }}</div>
        </template>
        <div v-else class="err">{{ status.kafka.error }}</div>
      </section>
      <div class="arrow"><span>poll</span></div>

      <!-- ③ Connect -->
      <section class="stage" :class="{ down: !status.connect.ok }">
        <div class="stage-head"><span class="no">③</span><span class="name">Kafka Connect</span><span class="role">Iceberg Sink · 30초 커밋</span></div>
        <template v-if="status.connect.ok">
          <div v-for="c in status.connect.connectors" :key="c.name" class="metric">
            <span class="k mono">{{ c.name }}</span>
            <span class="v"><span class="pill" :class="c.state === 'RUNNING' && c.tasks?.every((t) => t === 'RUNNING') ? 'COMPLETED' : 'FAILED'">{{ c.state }}<template v-if="c.tasks"> / task {{ c.tasks.join(',') }}</template></span></span>
          </div>
          <div class="sub">이벤트를 모았다가 Parquet 로 쓰고 스냅샷 커밋</div>
        </template>
        <div v-else class="err">{{ status.connect.error }}</div>
      </section>
      <div class="arrow"><span>파일 + 커밋</span></div>

      <!-- ④ Iceberg / MinIO -->
      <section class="stage wide" :class="{ down: !status.iceberg.ok }">
        <div class="stage-head"><span class="no">④</span><span class="name">Iceberg / MinIO</span><span class="role">Parquet + 메타데이터 + Catalog</span></div>
        <template v-if="status.iceberg.ok">
          <div v-for="tb in status.iceberg.tables" :key="tb.table" class="tbl">
            <div class="metric"><span class="k mono">{{ tb.table }}</span><span class="v">{{ tb.records }}<small class="muted"> rec · 파일 {{ tb.files }} · 스냅샷 {{ tb.snapshots }} · {{ kb(tb.bytes) }}</small></span></div>
            <div class="sub">마지막 커밋 {{ hhmmss(tb.lastCommittedAt) }} <template v-if="tb.lastAddedRecords">(+{{ tb.lastAddedRecords }}건)</template></div>
            <div class="sub mono" :title="tb.kafkaOffsets">스냅샷에 기록된 Connect 제어 오프셋 {{ tb.kafkaOffsets }}</div>
            <div class="sub mono" :title="tb.location">{{ tb.location }}</div>
          </div>
        </template>
        <div v-else class="err">{{ status.iceberg.error }}</div>
      </section>
      <div class="arrow"><span>Catalog → 파일 읽기</span></div>

      <!-- ⑤ Trino -->
      <section class="stage" :class="{ down: !status.trino.ok }">
        <div class="stage-head"><span class="no">⑤</span><span class="name">Trino</span><span class="role">SQL 조회</span></div>
        <template v-if="status.trino.ok">
          <div v-for="tb in status.trino.tables" :key="tb.t" class="metric"><span class="k mono">{{ tb.t.replace('commerce.', '') }}</span><span class="v">{{ tb.rows }}<small class="muted"> rows</small></span></div>
          <div class="sub">→ <router-link to="/analytics">Lakehouse 조회 화면</router-link></div>
        </template>
        <div v-else class="err">{{ status.trino.error }}</div>
      </section>
    </div>

    <section class="card">
      <h2>변화 로그 <span class="muted">이전 조회와 달라진 숫자</span></h2>
      <p class="desc">순서대로 나타나야 합니다: ① 앱 → ② Kafka 메시지 증가·lag 증가 → (최대 30초) → ② lag 0 → ④ 스냅샷·파일 증가 → ⑤ 행 증가.</p>
      <div class="tablewrap">
        <table>
          <tbody>
            <tr v-for="(l, i) in log" :key="i"><td class="muted" style="width: 90px">{{ l.t }}</td><td>{{ l.m }}</td></tr>
            <tr v-if="log.length === 0"><td colspan="2" class="muted">아직 변화가 없습니다. "테스트 결제 1건 발행"을 눌러 보세요.</td></tr>
          </tbody>
        </table>
      </div>
    </section>
  </main>
</template>

<style scoped>
.flow { display: grid; grid-template-columns: 1fr auto 1fr auto 1fr auto 1.5fr auto 1fr; gap: 6px; align-items: stretch; }
@media (max-width: 1100px) { .flow { grid-template-columns: 1fr; } .arrow { transform: rotate(90deg); height: 30px; } }
.stage { background: var(--surface); border: 1px solid var(--line); border-radius: 10px; padding: 12px 14px; display: flex; flex-direction: column; gap: 6px; min-width: 0; }
.stage.down { border-color: var(--bad); background: var(--bad-soft); }
.stage-head { display: flex; align-items: baseline; gap: 8px; flex-wrap: wrap; margin-bottom: 4px; }
.stage-head .no { font-family: var(--mono, monospace); color: var(--accent); font-weight: 700; }
.stage-head .name { font-weight: 700; font-size: 15px; }
.stage-head .role { color: var(--muted); font-size: 11.5px; }
.metric { display: flex; justify-content: space-between; align-items: baseline; gap: 8px; }
.metric .k { color: var(--muted); font-size: 12px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.metric .v { font-size: 18px; font-weight: 700; font-variant-numeric: tabular-nums; white-space: nowrap; }
.metric .v small { font-size: 11px; font-weight: 400; }
.sub { font-size: 12px; color: var(--muted); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.sub.good { color: var(--good); font-weight: 600; }
.sub.warn { color: var(--warn); font-weight: 600; }
.tbl { border-top: 1px dashed var(--line); padding-top: 6px; }
.tbl:first-of-type { border-top: 0; padding-top: 0; }
.err { color: var(--bad); font-size: 12.5px; word-break: break-all; }
.arrow { display: flex; flex-direction: column; align-items: center; justify-content: center; color: var(--muted); font-size: 11px; }
.arrow::before { content: "→"; font-size: 22px; line-height: 1; color: var(--accent); }
</style>
