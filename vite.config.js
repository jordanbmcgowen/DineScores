import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import fs from 'node:fs';
import path from 'node:path';

// Dev-only middleware: serve the pipeline-generated data.js (which lives in
// the build output dir public/, not in publicDir) so `npm run dev` has data.
function serveDataJs() {
  return {
    name: 'serve-data-js',
    configureServer(server) {
      server.middlewares.use((req, res, next) => {
        if (req.url && req.url.split('?')[0] === '/data.js') {
          const file = path.resolve(__dirname, 'public/data.js');
          if (fs.existsSync(file)) {
            res.setHeader('Content-Type', 'text/javascript');
            fs.createReadStream(file).pipe(res);
            return;
          }
        }
        next();
      });
    },
  };
}

export default defineConfig({
  plugins: [react(), serveDataJs()],
  publicDir: 'static', // public/ is the build OUTPUT dir, so it can't also be the static-copy dir
  build: {
    outDir: 'public',
    emptyOutDir: false, // Don't delete data.js and other pipeline-generated files
  },
  server: {
    port: 3000,
    host: '0.0.0.0',
  },
});
