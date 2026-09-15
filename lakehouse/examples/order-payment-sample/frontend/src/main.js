import { createApp } from 'vue'
import { createRouter, createWebHistory } from 'vue-router'
import App from './App.vue'
import OrderPage from './pages/OrderPage.vue'
import AnalyticsPage from './pages/AnalyticsPage.vue'
import './style.css'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', redirect: '/order' },
    { path: '/order', component: OrderPage },
    { path: '/analytics', component: AnalyticsPage },
  ],
})

createApp(App).use(router).mount('#app')
