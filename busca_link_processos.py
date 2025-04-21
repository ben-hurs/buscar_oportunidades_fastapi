# busca_por_link.py - Versão Otimizada com Exportação CSV
import asyncio
import os
import csv
import logging
import random
import pandas as pd
from playwright.async_api import async_playwright

# Configurações
TRIBUNAIS = [
    "https://esaj.tjsp.jus.br",
    "https://www2.tjal.jus.br",
    "https://esaj.tjce.jus.br"
]

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(DATA_DIR, 'busca_links.log')),
        logging.StreamHandler()
    ]
)

# Parâmetros de performance
DEFAULT_MAX_BROWSERS = 5
REQUEST_TIMEOUT = 5000
DELAY_RANGE = (0.05, 0.2)

def random_delay():
    return random.uniform(*DELAY_RANGE)

def exportar_para_csv(nome_empresa, dados):
    linhas = []
    for processo in dados:
        numero = processo["numero"]
        link = processo["link"]
        tribunal = processo["tribunal"]
        for parte in processo["partes"]:
            linhas.append({
                "numero": numero,
                "link": link,
                "tribunal": tribunal,
                "tipo_participacao": parte["tipo"],
                "nome_parte": parte["nome"]
            })

    df = pd.DataFrame(linhas)
    caminho = os.path.join(DATA_DIR, f"links_{nome_empresa.lower().replace(' ', '_')}.csv")
    df.to_csv(caminho, index=False, encoding="utf-8-sig")
    logging.info(f"CSV salvo em {caminho}")

async def extrair_processo(bloco, BASE_URL):
    try:
        link_tag = await bloco.query_selector("a.linkProcesso")
        if not link_tag:
            return None

        numero = (await link_tag.inner_text()).strip()
        href = await link_tag.get_attribute("href")
        link_absoluto = href if href.startswith("http") else BASE_URL + href

        divs = await bloco.query_selector_all("div.col-md-3")
        partes_tasks = [_extrair_parte(div) for div in divs]
        partes = await asyncio.gather(*partes_tasks)
        partes = [p for p in partes if p is not None]

        return {
            "numero": numero,
            "link": link_absoluto,
            "tribunal": BASE_URL,
            "partes": partes
        }
    except Exception as e:
        logging.debug(f"Erro ao extrair processo: {str(e)}")
        return None

async def _extrair_parte(div):
    try:
        tipo_el, nome_el = await asyncio.gather(
            div.query_selector(".tipoDeParticipacao"),
            div.query_selector(".nomeParte")
        )
        if tipo_el and nome_el:
            tipo = (await tipo_el.inner_text()).strip().replace(":", "")
            nome = (await nome_el.inner_text()).strip()
            return {"tipo": tipo, "nome": nome}
    except:
        return None

async def extrair_links(page, BASE_URL):
    try:
        await page.wait_for_selector("a.linkProcesso", timeout=15000, state="attached")
    except:
        logging.warning(f"Nenhum processo encontrado em {BASE_URL}")
        return []

    blocos = await page.query_selector_all(".home__lista-de-processos")
    tasks = [extrair_processo(bloco, BASE_URL) for bloco in blocos]
    resultados = await asyncio.gather(*tasks)
    return [r for r in resultados if r is not None]

async def navegar_paginas(page, BASE_URL, nome_empresa):
    try:
        await page.goto(BASE_URL + "/cpopg/open.do", timeout=REQUEST_TIMEOUT)
        await asyncio.sleep(random_delay())

        await asyncio.gather(
            page.select_option("#cbPesquisa", "NMPARTE"),
            page.fill("#campo_NMPARTE", nome_empresa)
        )
        await page.press("#campo_NMPARTE", "Enter")
        await asyncio.sleep(random_delay())

        erro_div = await page.query_selector("div.alert-danger")
        tem_resultado = await page.query_selector("a.linkProcesso")
        if erro_div or not tem_resultado:
            logging.warning(f"Nenhum resultado em {BASE_URL} para \"{nome_empresa}\"")
            return []

        todos_links = []
        while True:
            links = await extrair_links(page, BASE_URL)
            todos_links.extend(links)
            logging.info(f"Encontrados {len(links)} processos na página atual de {BASE_URL}")

            proxima = await page.query_selector("a.unj-pagination__next")
            if not proxima:
                break
            classe = await proxima.get_attribute("class")
            if classe and "disabled" in classe:
                break
            href = await proxima.get_attribute("href")
            if not href:
                break
            next_url = href if href.startswith("http") else BASE_URL + href
            await page.goto(next_url)
            await asyncio.sleep(random_delay())

        return todos_links
    except Exception as e:
        logging.error(f"Erro ao navegar em {BASE_URL}: {str(e)}")
        return []

async def processar_tribunal(browser, BASE_URL, nome_empresa):
    context = await browser.new_context(
        locale='pt-BR',
        timezone_id='America/Sao_Paulo',
        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
    )

    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        window.navigator.chrome = { runtime: {} };
        Object.defineProperty(navigator, 'languages', { get: () => ['pt-BR', 'pt', 'en-US', 'en'] });
    """)

    page = await context.new_page()
    try:
        return await navegar_paginas(page, BASE_URL, nome_empresa)
    finally:
        await page.close()
        await context.close()

async def buscar_processos_por_nome(nome_empresa, max_browsers=DEFAULT_MAX_BROWSERS):
    if not nome_empresa or len(nome_empresa.strip()) < 3:
        raise ValueError("Nome da parte deve ter pelo menos 3 caracteres")

    async with async_playwright() as p:
        browsers = [
            await p.chromium.launch(
                headless=True,
                timeout=60000,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-accelerated-2d-canvas',
                    '--no-first-run',
                    '--no-zygote',
                    '--disable-gpu',
                    '--disable-infobars',
                    '--window-size=1366,768'
                ]
            ) for _ in range(max_browsers)
        ]

        tasks = []
        for i, tribunal in enumerate(TRIBUNAIS):
            browser = browsers[i % len(browsers)]
            tasks.append(processar_tribunal(browser, tribunal, nome_empresa))

        resultados = await asyncio.gather(*tasks)
        todos_links = [link for sublist in resultados for link in sublist]

        for browser in browsers:
            await browser.close()

        if not todos_links:
            logging.warning(f"Nenhum processo encontrado para {nome_empresa}")
        else:
            logging.info(f"Total de {len(todos_links)} processos encontrados")
            exportar_para_csv(nome_empresa, todos_links)

        return todos_links

#asyncio.run(buscar_processos_por_nome("Coca Cola"))
