#!/bin/bash
set -e  # Para o script se qualquer comando falhar

echo "🔧 Instalando navegador Chromium..."
playwright install chromium

echo "🚀 Iniciando servidor Uvicorn..."
exec uvicorn main:app --host 0.0.0.0 --port 10000
