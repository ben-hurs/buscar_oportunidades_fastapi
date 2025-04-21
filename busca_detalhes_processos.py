import pandas as pd
import time
import asyncio
import json
import logging
from playwright.async_api import async_playwright
from busca_link_processos import buscar_processos_por_nome

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Função para extrair detalhes do processo por link
async def buscar_detalhes_por_link(page, link):
    dados_processo = {
        "link": link,
        "classe": "Não disponível",
        "assunto": "Não disponível",
        "foro": "Não disponível",
        "vara": "Não disponível",
        "juiz": "Não disponível",
        "data_distribuicao": "Não disponível",
        "numero_controle": "Não disponível",
        "area": "Não disponível",
        "valor_acao": "Não disponível",
        "partes": [],
        "movimentacoes": []
    }

    try:
        await page.goto(link, wait_until="domcontentloaded")

        botao_mais = await page.query_selector("#botaoExpandirDadosSecundarios")
        if botao_mais:
            await botao_mais.click()

        botao_partes = await page.query_selector("#linkpartes")
        if botao_partes:
            await botao_partes.click()
            seletor_partes = "table#tableTodasPartes tr"
        else:
            seletor_partes = "table#tablePartesPrincipais tr"

        async def get_texto_elemento(selector):
            el = await page.query_selector(selector)
            return (await el.inner_text()).strip() if el else "Não disponível"

        dados_processo.update({
            "classe": await get_texto_elemento("#classeProcesso"),
            "assunto": await get_texto_elemento("#assuntoProcesso"),
            "foro": await get_texto_elemento("#foroProcesso"),
            "vara": await get_texto_elemento("#varaProcesso"),
            "juiz": await get_texto_elemento("#juizProcesso"),
            "data_distribuicao": await get_texto_elemento("#dataHoraDistribuicaoProcesso"),
            "numero_controle": await get_texto_elemento("#numeroControleProcesso"),
            "area": await get_texto_elemento("#areaProcesso"),
            "valor_acao": await get_texto_elemento("#valorAcaoProcesso"),
        })

        partes = await page.query_selector_all(seletor_partes)
        for parte in partes:
            tipo = await parte.query_selector(".tipoDeParticipacao")
            nome = await parte.query_selector(".nomeParteEAdvogado")
            if tipo and nome:
                dados_processo["partes"].append({
                    "tipo": (await tipo.inner_text()).strip(),
                    "nome": (await nome.inner_text()).strip()
                })

        movimentacoes = await page.query_selector_all("tbody#tabelaUltimasMovimentacoes tr")
        for mov in movimentacoes:
            data = await mov.query_selector(".dataMovimentacao")
            descricao = await mov.query_selector(".descricaoMovimentacao")
            if data and descricao:
                dados_processo["movimentacoes"].append({
                    "data": (await data.inner_text()).strip(),
                    "descricao": (await descricao.inner_text()).strip()
                })

    except Exception as e:
        logging.warning(f"❌ Erro ao acessar {link}: {e}")

    return dados_processo

# Gerencia múltiplas coletas simultâneas
async def coletar_detalhes_concorrente(context, processos, limite_concorrencia=5):
    semaforo = asyncio.Semaphore(limite_concorrencia)
    resultados = []

    async def coletar(processo, index):
        async with semaforo:
            page = await context.new_page()
            link = processo.get("link")
            detalhes = await buscar_detalhes_por_link(page, link)
            await page.close()

            detalhes.update({
                "numero_processo": processo.get("numero", ""),
                "tribunal": processo.get("tribunal", ""),
                "partes_iniciais": processo.get("partes", []),
                "partes": json.dumps(detalhes["partes"], ensure_ascii=False),
                "movimentacoes": json.dumps(detalhes["movimentacoes"], ensure_ascii=False)
            })
            logging.info(f"[{index+1}/{len(processos)}] Processado")
            return detalhes

    tarefas = [coletar(proc, i) for i, proc in enumerate(processos)]
    resultados = await asyncio.gather(*tarefas)
    return resultados

# Função principal
async def main():
    nome_empresa = "Coca Cola"
    logging.info(f"🔍 Buscando processos para: {nome_empresa}")
    processos = await buscar_processos_por_nome(nome_empresa)

    if not processos:
        logging.warning("⚠️ Nenhum processo encontrado.")
        return

    inicio = time.time()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(locale="pt-BR", timezone_id="America/Sao_Paulo")

        resultados = await coletar_detalhes_concorrente(context, processos, limite_concorrencia=5)

        await browser.close()

    # Criar DataFrame e salvar CSV único
    df = pd.DataFrame(resultados)
    df.to_csv(f"data/detalhes_completos_{nome_empresa.lower().replace(' ', '_')}.csv", index=False, encoding="utf-8-sig")

    logging.info(f"✅ Coleta finalizada em {time.time() - inicio:.2f} segundos")

if __name__ == "__main__":
    asyncio.run(main())
