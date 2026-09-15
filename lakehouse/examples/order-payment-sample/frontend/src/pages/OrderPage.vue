<script setup>
import { onMounted, ref } from 'vue'
import { api, won } from '../api'

const orders = ref([])
const selected = ref(null)
const payments = ref([])
const error = ref('')
const notice = ref('')

// 주문 생성 폼
const customerId = ref('customer-3001')
const itemName = ref('무선 키보드')
const amount = ref(35000)

// 결제 폼
const method = ref('CARD')
const skipEvent = ref(false)

const presets = [
  { label: '정상 결제 (35,000)', item: '무선 키보드', amount: 35000 },
  { label: '한도 초과 실패 (1,200,000)', item: '노트북', amount: 1200000 },
  { label: '포인트 부족 실패 (POINT 80,000)', item: '헤드셋', amount: 80000, method: 'POINT' },
]

async function load() {
  error.value = ''
  try {
    orders.value = await api.orders()
    if (selected.value) {
      const fresh = orders.value.find((o) => o.id === selected.value.id)
      selected.value = fresh || null
      payments.value = fresh ? await api.paymentsByOrder(fresh.id) : []
    }
  } catch (e) {
    error.value = e.message
  }
}

async function select(order) {
  selected.value = order
  payments.value = await api.paymentsByOrder(order.id)
}

function applyPreset(p) {
  itemName.value = p.item
  amount.value = p.amount
  if (p.method) method.value = p.method
}

async function createOrder() {
  error.value = ''
  notice.value = ''
  try {
    const o = await api.createOrder(customerId.value, itemName.value, Number(amount.value))
    notice.value = `주문 ${o.id} 생성 → order.events 에 ORDER_CREATED 발행`
    await load()
    await select(orders.value.find((x) => x.id === o.id))
  } catch (e) {
    error.value = e.message
  }
}

async function pay() {
  if (!selected.value) return
  error.value = ''
  notice.value = ''
  try {
    const p = await api.pay(selected.value.id, method.value, skipEvent.value)
    if (p.eventSkipped) {
      notice.value = `결제 ${p.id} ${p.status} — 결제 이벤트 발행 생략 (불일치 후보 시연)`
    } else if (p.status === 'COMPLETED') {
      notice.value = `결제 ${p.id} 완료 → payment.events 에 PAYMENT_REQUESTED, PAYMENT_COMPLETED 발행`
    } else {
      notice.value = `결제 ${p.id} 실패 (${p.failureReason}) → PAYMENT_REQUESTED, PAYMENT_FAILED 발행`
    }
    await load()
  } catch (e) {
    error.value = e.message
  }
}

async function cancel() {
  if (!selected.value) return
  error.value = ''
  try {
    await api.cancelOrder(selected.value.id)
    notice.value = `주문 ${selected.value.id} 취소 → ORDER_CANCELLED, PAYMENT_REFUNDED 발행`
    await load()
  } catch (e) {
    error.value = e.message
  }
}

onMounted(load)
</script>

<template>
  <main class="page">
    <div v-if="error" class="note bad">{{ error }}</div>
    <div v-else-if="notice" class="note">{{ notice }}</div>

    <div class="grid2">
      <section class="card">
        <h2>1. 주문 생성</h2>
        <p class="desc">주문을 만들면 <span class="mono">order.events</span> 토픽에 ORDER_CREATED 가 발행됩니다.</p>
        <div class="form">
          <label>고객 ID <input v-model="customerId" /></label>
          <label>상품명 <input v-model="itemName" /></label>
          <label>금액 (원) <input v-model="amount" type="number" min="1" step="1000" /></label>
          <div class="row presets">
            <button v-for="p in presets" :key="p.label" class="small" type="button" @click="applyPreset(p)">{{ p.label }}</button>
          </div>
          <button class="primary" type="button" @click="createOrder">주문 생성</button>
        </div>

        <h2 style="margin-top: 20px">2. 결제</h2>
        <p class="desc">
          선택한 주문을 결제합니다. 가짜 PG 규칙: 1,000,000원 이상 → LIMIT_EXCEEDED, POINT 50,000원 초과 → INSUFFICIENT_POINT.
        </p>
        <div class="form">
          <label>대상 주문
            <input :value="selected ? `${selected.id} · ${won(selected.amount)} · ${selected.status}` : '오른쪽 목록에서 선택'" disabled />
          </label>
          <label>결제수단
            <select v-model="method">
              <option value="CARD">CARD</option>
              <option value="BANK">BANK</option>
              <option value="POINT">POINT</option>
            </select>
          </label>
          <label class="row" style="display: flex">
            <input v-model="skipEvent" type="checkbox" style="width: auto" />
            결제 이벤트 발행 생략 (DB에만 저장 → 3단계 "주문·결제 불일치 후보"로 잡힘)
          </label>
          <div class="row">
            <button class="primary" type="button" :disabled="!selected || selected.status === 'PAID' || selected.status === 'CANCELLED'" @click="pay">결제</button>
            <button class="danger" type="button" :disabled="!selected || selected.status !== 'PAID'" @click="cancel">주문 취소 (환불)</button>
          </div>
        </div>
      </section>

      <section class="card">
        <h2>주문 목록 <span class="muted">(운영 DB · H2)</span></h2>
        <p class="desc">행을 누르면 결제 대상으로 선택됩니다. 여기 상태가 업무의 기준이고, Lakehouse 는 이벤트 이력입니다.</p>
        <div class="tablewrap">
          <table>
            <thead>
              <tr><th>주문</th><th>고객</th><th>상품</th><th class="num">금액</th><th>상태</th><th>생성</th></tr>
            </thead>
            <tbody>
              <tr v-for="o in orders" :key="o.id" class="clickable" :class="{ selected: selected && selected.id === o.id }" @click="select(o)">
                <td class="mono">{{ o.id }}</td>
                <td>{{ o.customerId }}</td>
                <td>{{ o.itemName }}</td>
                <td class="num">{{ won(o.amount) }}</td>
                <td><span class="pill" :class="o.status">{{ o.status }}</span></td>
                <td class="muted">{{ o.createdAt.slice(11, 19) }}</td>
              </tr>
              <tr v-if="orders.length === 0"><td colspan="6" class="muted">아직 주문이 없습니다.</td></tr>
            </tbody>
          </table>
        </div>

        <template v-if="selected">
          <h2 style="margin-top: 18px">결제 이력 · {{ selected.id }}</h2>
          <div class="tablewrap">
            <table>
              <thead><tr><th>결제</th><th>수단</th><th class="num">금액</th><th>상태</th><th>실패 사유</th><th>이벤트</th></tr></thead>
              <tbody>
                <tr v-for="p in payments" :key="p.id">
                  <td class="mono">{{ p.id }}</td>
                  <td>{{ p.method }}</td>
                  <td class="num">{{ won(p.amount) }}</td>
                  <td><span class="pill" :class="p.status">{{ p.status }}</span></td>
                  <td>{{ p.failureReason || '' }}</td>
                  <td><span v-if="p.eventSkipped" class="pill skip">발행 생략</span><span v-else class="muted">발행됨</span></td>
                </tr>
                <tr v-if="payments.length === 0"><td colspan="6" class="muted">결제 이력이 없습니다.</td></tr>
              </tbody>
            </table>
          </div>
        </template>
      </section>
    </div>
  </main>
</template>
