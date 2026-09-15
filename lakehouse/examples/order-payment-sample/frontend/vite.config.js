import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// /api 는 Spring Boot(:8090)로 프록시한다. 프런트는 Trino 를 직접 호출하지 않는다.
export default defineConfig({
  plugins: [vue()],
  server: {
    port: 5173,
    proxy: {
      '/api': 'http://localhost:8090',
    },
  },
})
