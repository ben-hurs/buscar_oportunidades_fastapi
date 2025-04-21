from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import asyncio
import os
from busca_link_processos import buscar_processos_por_nome
from busca_detalhes_processos import coletar_detalhes_concorrente
from playwright.async_api import async_playwright

app = FastAPI()
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/buscar", response_class=HTMLResponse)
async def buscar(request: Request, nome_empresa: str = Form(...)):
    resultados = []

    try:
        links = await buscar_processos_por_nome(nome_empresa)
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
            context = await browser.new_context(locale="pt-BR", timezone_id="America/Sao_Paulo")
            resultados = await coletar_detalhes_concorrente(context, links, limite_concorrencia=5)
            await browser.close()
    except Exception as e:
        return templates.TemplateResponse("index.html", {"request": request, "erro": str(e)})

    return templates.TemplateResponse("index.html", {"request": request, "resultados": resultados, "nome": nome_empresa})
