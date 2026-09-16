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
        <div class="who"><span class="no">①</span><div><div class="name">앱 · H2</div><div class="role">운영 DB 역할. 주문·결제를 저장하고 이벤트를 발행</div></div></div>
        <div class="what">
          <table v-if="status.app.ok">
            <tbody>
              <tr><th>주문 (H2)</th><td class="num big">{{ status.app.orders }}</td><td class="muted">건</td></tr>
              <tr><th>결제 (H2)</th><td class="num big">{{ status.app.payments }}</td><td class="muted">건</td></tr>
              <tr><th>발행 토픽</th><td colspan="2" class="mono">{{ status.app.topics.join('  ·  ') }}</td></tr>
            </tbody>
          </table>
          <div v-else class="err">{{ status.app.error }}</div>
        </div>
      </section>
      <div class="link">↓ 결제할 때마다 send() 로 이벤트 발행 (ORDER_CREATED, PAYMENT_REQUESTED, PAYMENT_COMPLETED …)</div>

      <!-- ② Kafka -->
      <section class="stage" :class="{ down: !status.kafka.ok }">
        <div class="who"><span class="no">②</span><div><div class="name">Kafka</div><div class="role">토픽에 이벤트를 순서대로 보관. 커넥터가 어디까지 읽었는지(lag)가 여기서 보임</div></div></div>
        <div class="what">
          <template v-if="status.kafka.ok">
            <table>
              <thead><tr><th>토픽</th><th class="num">메시지 수</th><th>파티션별 끝 오프셋</th><th>커넥터 그룹</th><th class="num">커밋 오프셋</th><th class="num">lag</th></tr></thead>
              <tbody>
                <tr v-for="(tp, i) in status.kafka.topics" :key="tp.topic">
                  <td class="mono">{{ tp.topic }}</td>
                  <td class="num big">{{ tp.messages }}</td>
                  <td class="mono">{{ tp.partitions.map((p) => 'P' + p.partition + '=' + p.endOffset).join('  ') }}</td>
                  <td class="mono">{{ status.kafka.groups[i]?.group }}</td>
                  <td class="num">{{ status.kafka.groups[i]?.committed }}</td>
                  <td class="num"><span class="pill" :class="Number(status.kafka.groups[i]?.lag) > 0 ? 'CANCELLED' : 'COMPLETED'">{{ status.kafka.groups[i]?.lag }}</span></td>
                </tr>
              </tbody>
            </table>
            <p class="hint">lag = 끝 오프셋 − 커넥터가 커밋한 오프셋. <strong :class="totalLag > 0 ? 'warn' : 'good'">{{ totalLag > 0 ? `아직 ${totalLag}건이 Iceberg 로 넘어가지 않았습니다` : '모든 메시지가 Iceberg 에 적재됐습니다' }}</strong></p>
          </template>
          <div v-else class="err">{{ status.kafka.error }}</div>
        </div>
      </section>
      <div class="link">↓ Iceberg Sink 커넥터가 poll 로 읽어 30초 동안 모음</div>

      <!-- ③ Connect -->
      <section class="stage" :class="{ down: !status.connect.ok }">
        <div class="who"><span class="no">③</span><div><div class="name">Kafka Connect</div><div class="role">Iceberg Sink. 모은 이벤트를 Parquet 파일로 쓰고 30초마다 스냅샷 커밋</div></div></div>
        <div class="what">
          <table v-if="status.connect.ok">
            <thead><tr><th>커넥터</th><th>상태</th><th>태스크</th><th>커밋 주기</th></tr></thead>
            <tbody>
              <tr v-for="c in status.connect.connectors" :key="c.name">
                <td class="mono">{{ c.name }}</td>
                <td><span class="pill" :class="c.state === 'RUNNING' ? 'COMPLETED' : 'FAILED'">{{ c.state }}</span></td>
                <td><span v-for="(t, i) in c.tasks || []" :key="i" class="pill" :class="t === 'RUNNING' ? 'COMPLETED' : 'FAILED'" style="margin-right: 4px">{{ t }}</span><span v-if="c.error" class="err">{{ c.error }}</span></td>
                <td>{{ (c.commitIntervalMs || 0) / 1000 }}초</td>
              </tr>
            </tbody>
          </table>
          <div v-else class="err">{{ status.connect.error }}</div>
        </div>
      </section>
      <div class="link">↓ ④ 파일 업로드 → ⑤ 스냅샷 커밋 (Catalog 의 "현재 목차" 교체)</div>

      <!-- ④ Iceberg / MinIO -->
      <section class="stage" :class="{ down: !status.iceberg.ok }">
        <div class="who"><span class="no">④</span><div><div class="name">Iceberg / MinIO</div><div class="role">Parquet 데이터 파일 + 메타데이터. 커밋마다 스냅샷·파일이 하나씩 늘어남</div></div></div>
        <div class="what">
          <template v-if="status.iceberg.ok">
            <table>
              <thead><tr><th>테이블</th><th class="num">레코드</th><th class="num">파일</th><th class="num">스냅샷</th><th class="num">크기</th><th>마지막 커밋</th><th>MinIO 경로</th></tr></thead>
              <tbody>
                <tr v-for="tb in status.iceberg.tables" :key="tb.table">
                  <td class="mono">{{ tb.table }}</td>
                  <td class="num big">{{ tb.records }}</td>
                  <td class="num">{{ tb.files }}</td>
                  <td class="num">{{ tb.snapshots }}</td>
                  <td class="num">{{ kb(tb.bytes) }}</td>
                  <td>{{ hhmmss(tb.lastCommittedAt) }}<span v-if="tb.lastAddedRecords" class="muted"> (+{{ tb.lastAddedRecords }}건)</span></td>
                  <td class="mono wrap">{{ tb.location }}</td>
                </tr>
              </tbody>
            </table>
            <p class="hint">레코드 수 = Parquet 파일에 들어 있는 행의 합. ② 의 커밋 오프셋 합계와 같아야 정상입니다.</p>
          </template>
          <div v-else class="err">{{ status.iceberg.error }}</div>
        </div>
      </section>
      <div class="link">↓ Trino 가 Catalog 에서 현재 메타데이터를 찾아 Parquet 를 읽음</div>

      <!-- ⑤ Trino -->
      <section class="stage" :class="{ down: !status.trino.ok }">
        <div class="who"><span class="no">⑤</span><div><div class="name">Trino</div><div class="role">SQL 조회. 조회 화면이 보는 숫자</div></div></div>
        <div class="what">
          <table v-if="status.trino.ok">
            <tbody>
              <tr v-for="tb in status.trino.tables" :key="tb.t"><th class="mono">{{ tb.t }}</th><td class="num big">{{ tb.rows }}</td><td class="muted">rows</td></tr>
              <tr><th></th><td colspan="2"><router-link to="/analytics">→ Lakehouse 조회 화면에서 집계 보기</router-link></td></tr>
            </tbody>
          </table>
          <div v-else class="err">{{ status.trino.error }}</div>
        </div>
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
.flow { display: grid; gap: 0; }
.stage {
  display: grid; grid-template-columns: 240px minmax(0, 1fr); gap: 16px; align-items: start;
  background: var(--surface); border: 1px solid var(--line); border-radius: 10px; padding: 14px 18px;
}
.stage.down { border-color: var(--bad); background: var(--bad-soft); }
@media (max-width: 800px) { .stage { grid-template-columns: 1fr; } }
.who { display: flex; gap: 10px; align-items: flex-start; }
.who .no { font-family: var(--mono, monospace); color: var(--accent); font-weight: 700; font-size: 20px; line-height: 1.1; }
.who .name { font-weight: 700; font-size: 16px; }
.who .role { color: var(--muted); font-size: 12.5px; margin-top: 2px; line-height: 1.45; }
.what { min-width: 0; }
.what table { width: auto; min-width: 60%; }
.what th { white-space: nowrap; }
.what td, .what th { padding: 6px 12px 6px 0; vertical-align: middle; }
.what tbody th { text-align: left; color: var(--muted); font-weight: 500; background: none; }
.what td.big { font-size: 20px; font-weight: 700; }
.what td.wrap { white-space: normal; word-break: break-all; font-size: 11.5px; color: var(--muted); }
.hint { margin: 8px 0 0; font-size: 12.5px; color: var(--muted); }
.hint .good { color: var(--good); }
.hint .warn { color: var(--warn); }
.err { color: var(--bad); font-size: 12.5px; word-break: break-all; }
.link { color: var(--muted); font-size: 12.5px; padding: 6px 0 6px 26px; }
</style>
