import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  // GitHub Pages phục vụ dưới subpath /<repo>/. Override khi build qua BASE_PATH;
  // mặc định '/' để dev local và Docker/nginx (root) vẫn chạy bình thường.
  base: process.env.BASE_PATH || '/',
  plugins: [react()],
  server: {
    proxy: {
      '/api': {
        target: 'http://localhost:8001',
        changeOrigin: true,
      }
    }
  }
})
