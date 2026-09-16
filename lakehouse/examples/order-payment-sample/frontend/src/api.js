// 백엔드 호출 한 곳. 오류는 {error} 본문을 메시지로 던진다.
async function request(path, options = {}) {
  const res = await fetch(path, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  })
  const text = await res.text()
  const body = text ? JSON.parse(text) : null
  if (!res.ok) {
    const msg = body && body.error ? body.error : `${res.status} ${res.statusText}`
    const err = new Error(msg)
    err.status = res.status
    throw err
  }
  return body
}

export const api = {
  orders: () => request('/api/orders'),
  createOrder: (customerId, itemName, amount) =>
    request('/api/orders', { method: 'POST', body: JSON.stringify({ customerId, itemName, amount }) }),
  cancelOrder: (orderId) => request(`/api/orders/${orderId}/cancel`, { method: 'POST' }),
  pay: (orderId, method, skipEvent) =>
    request('/api/payments', { method: 'POST', body: JSON.stringify({ orderId, method, skipEvent }) }),
  paymentsByOrder: (orderId) => request(`/api/payments/by-order/${orderId}`),

  successRate: (days) => request(`/api/analytics/payment-success-rate?days=${days}`),
  failures: (days) => request(`/api/analytics/payment-failures?days=${days}`),
  cancellations: (days) => request(`/api/analytics/cancellations?days=${days}`),
  mismatches: (graceSeconds) => request(`/api/analytics/mismatches?graceSeconds=${graceSeconds}`),
  freshness: (limit) => request(`/api/analytics/freshness?limit=${limit}`),
  timeline: (orderId) => request(`/api/analytics/order-timeline?orderId=${encodeURIComponent(orderId)}`),

  pipelineStatus: () => request('/api/pipeline/status'),
}

export const won = (n) => (n == null ? '' : Number(n).toLocaleString('ko-KR') + '원')
// Trino 의 decimal 은 문자열('50.0000000000000000')로 오므로 표시용으로 자른다
export const pct = (n) => (n == null || n === '' ? '–' : Number(n).toFixed(1) + '%')
