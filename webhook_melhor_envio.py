from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import httpx
import os
from dotenv import load_dotenv


load_dotenv()

app_servidor_web = FastAPI()

TOKEN_SHOPIFY = os.getenv("TOKEN_SHOPIFY")
NOME_DA_LOJA_SHOPIFY = os.getenv("NOME_DA_LOJA_SHOPIFY")
TOKEN_API_MELHOR_ENVIO = os.getenv("TOKEN_API_MELHOR_ENVIO")

class DadosWebhookMelhorEnvio(BaseModel):
    id: Optional[str] = None
    status: Optional[str] = None
    tracking: Optional[str] = None

async def obter_token_acesso_shopify_assincrono(nome_da_loja_shopify: str, api_key_do_app: str, api_secret_key: str) -> str:
    """Realiza a autenticação via OAuth e retorna o access_token de forma assíncrona."""
    url_de_autenticacao = f"https://{nome_da_loja_shopify}.myshopify.com/admin/oauth/access_token"
    credenciais = {
        "client_id": api_key_do_app,
        "client_secret": api_secret_key,
        "grant_type": "client_credentials"
    }
    async with httpx.AsyncClient() as cliente_http_assincrono:
        resposta_autenticacao = await cliente_http_assincrono.post(url_de_autenticacao, data=credenciais)
        resposta_autenticacao.raise_for_status()
        return resposta_autenticacao.json().get("access_token")

async def buscar_id_interno_shopify_por_numero_assincrono(numero_visual_do_pedido: str, token_acesso: str, nome_loja: str) -> str:
    """Traduz o número visual (#1024) para o ID interno da Shopify de forma assíncrona."""
    url_busca = f"https://{nome_loja}.myshopify.com/admin/api/2024-01/orders.json?name={numero_visual_do_pedido}&status=any"
    cabecalhos_requisicao = {"X-Shopify-Access-Token": token_acesso, "Content-Type": "application/json"}
    
    async with httpx.AsyncClient() as cliente_http_assincrono:
        resposta_busca = await cliente_http_assincrono.get(url_busca, headers=cabecalhos_requisicao)
        if resposta_busca.status_code == 200:
            lista_de_pedidos = resposta_busca.json().get("orders", [])
            return str(lista_de_pedidos[0].get("id")) if lista_de_pedidos else None
    return None

async def injetar_codigo_de_rastreio_na_shopify_assincrono(id_interno_pedido: str, codigo_rastreio: str, nome_transportadora: str, token_acesso: str, nome_loja: str) -> bool:
    """Injeta o código de rastreio no pedido da Shopify de forma assíncrona."""
    url_atualizacao = f"https://{nome_loja}.myshopify.com/admin/api/2024-01/orders/{id_interno_pedido}/fulfillments.json"
    cabecalhos_requisicao = {"X-Shopify-Access-Token": token_acesso, "Content-Type": "application/json"}
    corpo_da_requisicao = {
        "fulfillment": {
            "tracking_info": {
                "number": codigo_rastreio,
                "company": nome_transportadora
            }
        }
    }
    async with httpx.AsyncClient() as cliente_http_assincrono:
        resposta_atualizacao = await cliente_http_assincrono.post(url_atualizacao, json=corpo_da_requisicao, headers=cabecalhos_requisicao)
        return resposta_atualizacao.status_code in [200, 201]

@app_servidor_web.get("/")
async def rota_raiz():
    """Rota raiz para o Melhor Envio validar o domínio principal (Site da plataforma)."""
    return {"status": "sucesso", "mensagem": "Servidor da YK SoftwareHouse operante e aguardando eventos!"}

@app_servidor_web.get("/webhook/melhor-envio/atualizacao")
async def responder_ping_validacao_melhor_envio():
    """Rota GET exclusiva para responder ao ping de teste do Melhor Envio."""
    return {"status": "sucesso", "mensagem": "Endpoint ativo e validado!"}

@app_servidor_web.post("/webhook/melhor-envio/atualizacao")
async def processar_evento_de_rastreio(dados_do_webhook: DadosWebhookMelhorEnvio):
    """Rota POST que recebe as atualizações reais de pacotes."""
    
    if dados_do_webhook.status is None or not dados_do_webhook.id:
        return {"status": "sucesso", "mensagem": "Ping de teste vazio validado!"}

    status_que_importam = ["released", "posted"]
    if dados_do_webhook.status not in status_que_importam:
        return {"status": "ignorado", "mensagem": f"Status '{dados_do_webhook.status}' ignorado."}

    cabecalhos_melhor_envio = {"Authorization": f"Bearer {TOKEN_API_MELHOR_ENVIO}", "Accept": "application/json"}
    url_detalhes_frete = f"https://www.melhorenvio.com.br/api/v2/me/orders/{dados_do_webhook.id}"
    
    async with httpx.AsyncClient() as cliente_http_assincrono:
        resposta_frete = await cliente_http_assincrono.get(url_detalhes_frete, headers=cabecalhos_melhor_envio)
        
    if resposta_frete.status_code != 200:
        return {"status": "erro_ignorado", "mensagem": f"Erro no Melhor Envio: {resposta_frete.text}"}
    
    dados_completos_etiqueta = resposta_frete.json()
    nome_da_transportadora = dados_completos_etiqueta.get("service", {}).get("company", {}).get("name", "Correios")
    
    tags_do_pedido = dados_completos_etiqueta.get("tags", [])
    numero_do_pedido = tags_do_pedido[0].get("tag") if tags_do_pedido else dados_completos_etiqueta.get("non_commercial", {}).get("content")

    if not numero_do_pedido:
        return {"status": "ignorado", "erro": "Número do pedido não encontrado na etiqueta."}

    id_interno_do_pedido = await buscar_id_interno_shopify_por_numero_assincrono(numero_do_pedido, TOKEN_SHOPIFY, NOME_DA_LOJA_SHOPIFY)
    
    if not id_interno_do_pedido:
        return {"status": "ignorado", "mensagem": "Pedido não encontrado na Shopify. (Pode ser o disparo de teste do M.E.)"}

    sucesso_na_injecao = await injetar_codigo_de_rastreio_na_shopify_assincrono(id_interno_do_pedido, dados_do_webhook.tracking, nome_da_transportadora, TOKEN_SHOPIFY, NOME_DA_LOJA_SHOPIFY)
    
    if sucesso_na_injecao:
        return {"status": "sucesso", "pedido": numero_do_pedido}
    
    return {"status": "erro_ignorado", "mensagem": "Falha ao injetar rastreio na Shopify."}