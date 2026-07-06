import React from 'react';
import { createRoot } from 'react-dom/client';
import '@fontsource-variable/inter';
import 'maplibre-gl/dist/maplibre-gl.css';
import './index.css';
import App from './App.jsx';

createRoot(document.getElementById('root')).render(<App />);
