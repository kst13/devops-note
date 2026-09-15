<script setup>
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { api, pct, won } from '../api'

const days = ref(7)
const grace = ref(120)
const auto = ref(true)

const rate = ref([])
const failures = ref([])
const cancels = ref([])
const mismatches = ref([])
const freshness = ref([])
const timelineOrder = ref('')
const timeline = ref([])

const error = ref('')
const loading = ref(false)
const lastLoaded = ref(null)
let timer = null

const totals = computed(() => {
  const t = { requested: 0, completed: 0, failed: 0 }
  for (const r of rate.value) {
    t.requested += Number(r.requested || 0)
    t.completed += Number(r.completed || 0)
    t.failed += Number(r.failed || 0)
  }
  t.pct = t.requested ? Math.round((1000 * t.completed) / t.requested) / 10 : null
  return t
})

const avgLag = computed(() => {
  if (!freshness.value.length) return null
  const s = freshness.value.reduce((a, r) => a + Number(r.lag_seconds || 0), 0)
  return Math.round(s / freshness.value.length)
})

async function load() {
  loading.value = true
  error.value = ''
  try {
    const [r, f, c, m, fr] = await Promise.all([
      api.successRate(days.value),
      api.failures(days.value),
      api.cancellations(days.value),
      api.mismatches(grace.value),
      api.freshness(20),
    ])
    rate.value = r
    failures.value = f
    cancels.value = c
    mismatches.value = m
    freshness.value = fr
    lastLoaded.value = new Date()
  } catch (e) {
    error.value = e.status === 503 ? `Lakehouse 조회 불가 — ${e.message}` : e.message
  } finally {
    loading.value = false
  }
}

async function loadTimeline() {
  if (!timelineOrder.value) return
  try {
    timeline.value = await api.timeline(timelineOrder.value.trim())
  } catch (e) {
    error.value = e.message
  }
}

function schedule() {
  clearInterval(timer)
  if (auto.value) timer = setInterval(load, 15000)
}

onMounted(() => { load(); schedule() })
onBeforeUnmount(() => clearInterval(timer))
</script>

<template>
  <main class="page">
    <section class="card">
      <div class="row" style="justify-content: space-between">
        <div>
          <h2>Lakehouse 조회 <span class="muted">(Trino → Iceberg)</span></h2>
          <p class="desc" style="margin: 0">
            운영 DB 가 아니라 Kafka → Iceberg Sink 로 적재된 이벤트 이력을 SQL 로 조회합니다. 커밋 주기(30초)만큼 늦게 나타납니다.
          </p>
        </div>
        <div class="row">
          <label class="muted">기간 <select v-model.number="days" @change="load"><option :value="1">1일</option><option :value="7">7일</option><option :value="30">30일</option></select></label>
          <label class="muted">불일치 유예 <select v-model.number="grace" @change="load"><option :value="0">0초</option><option :value="60">60초</option><option :value="120">120초</option><option :value="300">300초</option></select></label>
          <label class="muted"><input v-model="auto" type="checkbox" @change="schedule" /> 15초 자동 갱신</label>
          <button type="button" :disabled="loading" @click="load">{{ loading ? '조회 중…' : '지금 조회' }}</button>
        </div>
      </div>
      <div v-if="error" class="note bad" style="margin-top: 10px">{{ error }}</div>
      <div v-else-if="lastLoaded" class="muted" style="margin-top: 8px; font-size: 12px">마지막 조회 {{ lastLoaded.toLocaleTimeString('ko-KR') }}</div>
    </section>

    <div class="stats">
      <div class="stat"><div class="k">결제 시도</div><div class="v">{{ totals.requested }}</div></div>
      <div class="stat"><div class="k">결제 완료</div><div class="v good">{{ totals.completed }}</div></div>
      <div class="stat"><div class="k">결제 실패</div><div class="v bad">{{ totals.failed }}</div></div>
      <div class="stat"><div class="k">성공률</div><div class="v">{{ totals.pct == null ? '–' : totals.pct + '%' }}</div></div>
      <div class="stat"><div class="k">불일치 후보</div><div class="v" :class="mismatches.length ? 'bad' : 'good'">{{ mismatches.length }}</div></div>
      <div class="stat"><div class="k">평균 반영 지연</div><div class="v">{{ avgLag == null ? '–' : avgLag + '초' }}</div></div>
    </div>

    <div class="grid2" style="grid-template-columns: 1fr 1fr">
      <section class="card">
        <h2>① 결제 성공률 <span class="muted">일별 · 결제수단별</span></h2>
        <p class="desc">PAYMENT_REQUESTED 대비 PAYMENT_COMPLETED 비율. 실패 이벤트가 있어야 100% 가 아닌 숫자가 나옵니다.</p>
        <div class="tablewrap">
          <table>
            <thead><tr><th>일자</th><th>수단</th><th class="num">시도</th><th class="num">완료</th><th class="num">실패</th><th>성공률</th></tr></thead>
            <tbody>
              <tr v-for="r in rate" :key="r.day + r.method">
                <td>{{ r.day }}</td><td>{{ r.method }}</td>
                <td class="num">{{ r.requested }}</td><td class="num">{{ r.completed }}</td><td class="num">{{ r.failed }}</td>
                <td style="min-width: 120px">
                  <div class="row"><span style="width: 52px">{{ pct(r.success_rate_pct) }}</span><div class="bar" style="flex: 1"><div :style="{ width: (r.success_rate_pct || 0) + '%' }"></div></div></div>
                </td>
              </tr>
              <tr v-if="rate.length === 0"><td colspan="6" class="muted">데이터가 없습니다. 주문·결제 화면에서 결제를 실행한 뒤 30초 후 다시 조회하세요.</td></tr>
            </tbody>
          </table>
        </div>
        <h2 style="margin-top: 14px">실패 사유</h2>
        <div class="tablewrap">
          <table>
            <thead><tr><th>사유</th><th>수단</th><th class="num">건수</th><th class="num">금액</th></tr></thead>
            <tbody>
              <tr v-for="f in failures" :key="f.failure_reason + f.method"><td>{{ f.failure_reason }}</td><td>{{ f.method }}</td><td class="num">{{ f.cnt }}</td><td class="num">{{ won(f.amount) }}</td></tr>
              <tr v-if="failures.length === 0"><td colspan="4" class="muted">실패 없음</td></tr>
            </tbody>
          </table>
        </div>
      </section>

      <section class="card">
        <h2>② 취소·환불 현황</h2>
        <p class="desc">ORDER_CREATED 대비 ORDER_CANCELLED, 그리고 PAYMENT_REFUNDED 금액.</p>
        <div class="tablewrap">
          <table>
            <thead><tr><th>일자</th><th class="num">주문</th><th class="num">취소</th><th class="num">환불 건</th><th class="num">환불 금액</th><th class="num">취소율</th></tr></thead>
            <tbody>
              <tr v-for="c in cancels" :key="c.day">
                <td>{{ c.day }}</td><td class="num">{{ c.created }}</td><td class="num">{{ c.cancelled }}</td>
                <td class="num">{{ c.refunds }}</td><td class="num">{{ won(c.refund_amount) }}</td><td class="num">{{ pct(c.cancel_rate_pct) }}</td>
              </tr>
              <tr v-if="cancels.length === 0"><td colspan="6" class="muted">데이터가 없습니다.</td></tr>
            </tbody>
          </table>
        </div>

        <h2 style="margin-top: 14px">③ 주문·결제 불일치 후보</h2>
        <p class="desc">주문은 생성됐는데 {{ grace }}초가 지나도 결제 이벤트가 하나도 없는 주문. "결제 이벤트 발행 생략"으로 만든 건이 여기 잡힙니다.</p>
        <div v-if="mismatches.length" class="note warn" style="margin-bottom: 8px">
          반영 지연 때문에 일시적으로 잡힐 수 있습니다. 운영에서는 유예 시간을 두고 원본 DB 에서 재확인한 뒤 조치합니다.
        </div>
        <div class="tablewrap">
          <table>
            <thead><tr><th>주문</th><th>고객</th><th>상품</th><th class="num">금액</th><th class="num">경과</th></tr></thead>
            <tbody>
              <tr v-for="m in mismatches" :key="m.order_id" class="clickable" @click="timelineOrder = m.order_id; loadTimeline()">
                <td class="mono">{{ m.order_id }}</td><td>{{ m.customer_id }}</td><td>{{ m.item_name }}</td>
                <td class="num">{{ won(m.amount) }}</td><td class="num">{{ Math.round(Number(m.age_seconds) / 60) }}분</td>
              </tr>
              <tr v-if="mismatches.length === 0"><td colspan="5" class="muted">불일치 없음</td></tr>
            </tbody>
          </table>
        </div>
      </section>
    </div>

    <div class="grid2" style="grid-template-columns: 1fr 1fr">
      <section class="card">
        <h2>④ 반영 지연 <span class="muted">최근 결제 이벤트 20건</span></h2>
        <p class="desc">occurred_at(앱 발행 시각) 과 Iceberg 스냅샷 committed_at 의 차이. Sink 커밋 주기(30초) 안이면 정상.</p>
        <div class="tablewrap">
          <table>
            <thead><tr><th>이벤트</th><th>주문</th><th>수단</th><th class="num">금액</th><th>발행</th><th>커밋</th><th class="num">지연</th></tr></thead>
            <tbody>
              <tr v-for="f in freshness" :key="f.event_id">
                <td><span class="pill" :class="f.event_type.replace('PAYMENT_', '')">{{ f.event_type.replace('PAYMENT_', '') }}</span></td>
                <td class="mono">{{ f.order_id }}</td><td>{{ f.method }}</td><td class="num">{{ won(f.amount) }}</td>
                <td class="muted">{{ String(f.occurred_at).slice(11, 19) }}</td>
                <td class="muted">{{ String(f.committed_at).slice(11, 19) }}</td>
                <td class="num">{{ f.lag_seconds }}초</td>
              </tr>
              <tr v-if="freshness.length === 0"><td colspan="7" class="muted">아직 적재된 결제 이벤트가 없습니다.</td></tr>
            </tbody>
          </table>
        </div>
      </section>

      <section class="card">
        <h2>주문 타임라인 <span class="muted">Lakehouse 이력</span></h2>
        <p class="desc">주문 ID 로 order.events 와 payment.events 를 시간순으로 합쳐 봅니다.</p>
        <div class="row" style="margin-bottom: 10px">
          <input v-model="timelineOrder" placeholder="order-xxxxxxxx" class="mono" style="padding: 7px 10px; border: 1px solid var(--line); border-radius: 6px; flex: 1" @keyup.enter="loadTimeline" />
          <button type="button" @click="loadTimeline">조회</button>
        </div>
        <div class="tablewrap">
          <table>
            <thead><tr><th>시각</th><th>이벤트</th><th>결제</th><th>수단</th><th class="num">금액</th><th>사유</th></tr></thead>
            <tbody>
              <tr v-for="(t, i) in timeline" :key="i">
                <td class="muted">{{ String(t.occurred_at).slice(11, 19) }}</td>
                <td>{{ t.event_type }}</td><td class="mono">{{ t.payment_id || '' }}</td><td>{{ t.method || '' }}</td>
                <td class="num">{{ won(t.amount) }}</td><td>{{ t.failure_reason || '' }}</td>
              </tr>
              <tr v-if="timeline.length === 0"><td colspan="6" class="muted">조회할 주문 ID 를 입력하세요.</td></tr>
            </tbody>
          </table>
        </div>
      </section>
    </div>
  </main>
</template>
