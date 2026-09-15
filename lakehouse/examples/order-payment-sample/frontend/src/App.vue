<template>
  <header class="topbar">
    <div class="brand">주문·결제 PoC <small>Kafka → Iceberg → Trino</small></div>
    <nav class="row">
      <router-link to="/order">주문·결제</router-link>
      <router-link to="/analytics">Lakehouse 조회</router-link>
    </nav>
  </header>
  <router-view />
</template>
