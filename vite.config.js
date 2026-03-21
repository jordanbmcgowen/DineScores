import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  publicDir: 'static', // Avoid conflict with Firebase's public/ output dir
  build: {
    outDir: 'public',
    emptyOutDir: false, // Don't delete data.js and other pipeline-generated files
  },
  server: {
    port: 3000,
    host: '0.0.0.0',
  },
});
