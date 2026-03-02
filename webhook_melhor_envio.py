from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any
import httpx
import os
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta
from fastapi.responses import HTMLResponse
from typing import List

load_dotenv()
DB_VENDAS_KITS = []
app_servidor_web = FastAPI()

ALLOWED_ORIGINS = os.getenv("CORS_ALLOW_ORIGINS", "*").split(",")  

class ItemKit(BaseModel):
    unidade: str
    tamanho: str
    cor: str

class VendaPayload(BaseModel):
    kit_id: str
    nome_produto: str
    quantidade_itens: int
    detalhes: List[ItemKit]
    data_hora: str

@app_servidor_web.post("/Venda")
async def registrar_venda_kit(payload: VendaPayload):
    """
    Recebe os dados do carrinho do frontend da Shopify antes de ir para a Yampi.
    """
    DB_VENDAS_KITS.append(payload.dict())
    return {"status": "sucesso", "mensagem": "Kit salvo na base de dados paralela", "kit_id": payload.kit_id}

@app_servidor_web.get("/admin/vendas", response_class=HTMLResponse)
async def painel_admin_vendas():
    html_content = """
    <html>
        <head>
            <title>Admin - Separação de Kits</title>
            <style>
                body { font-family: Arial, sans-serif; background-color: #f4f4f9; padding: 20px; }
                h1 { color: #333; text-align: center; }
                table { width: 100%; border-collapse: collapse; background: #fff; box-shadow: 0 2px 5px rgba(0,0,0,0.1); border-radius: 8px; overflow: hidden; }
                th, td { padding: 15px; text-align: left; border-bottom: 1px solid #ddd; }
                th { background-color: #000; color: #fff; text-transform: uppercase; font-size: 14px; }
                tr:hover { background-color: #f1f1f1; }
                .badge { background: #2b589c; color: white; padding: 4px 8px; border-radius: 4px; font-weight: bold; font-size: 12px; }
                .roupas-list { margin: 0; padding-left: 15px; }
            </style>
        </head>
        <body>
            <h1>📦 Painel de Separação de Kits</h1>
            <table>
                <tr>
                    <th>Data/Hora</th>
                    <th>ID do Kit (Ponte Yampi)</th>
                    <th>Produto</th>
                    <th>Detalhes para Separação</th>
                </tr>
    """
    
    for venda in reversed(DB_VENDAS_KITS):
        linhas_roupas = "".join([f"<li><b>{item['unidade']}</b>: Tamanho {item['tamanho']} | Cor {item['cor']}</li>" for item in venda['detalhes']])
        
        html_content += f"""
                <tr>
                    <td>{venda['data_hora']}</td>
                    <td><span class="badge">{venda['kit_id']}</span></td>
                    <td>{venda['nome_produto']} ({venda['quantidade_itens']} peças)</td>
                    <td><ul class="roupas-list">{linhas_roupas}</ul></td>
                </tr>
        """
        
    html_content += """
            </table>
        </body>
    </html>
    """
    return HTMLResponse(content=html_content)

app_servidor_web.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

TOKEN_SHOPIFY = os.getenv("TOKEN_SHOPIFY")
NOME_DA_LOJA_SHOPIFY = os.getenv("NOME_DA_LOJA_SHOPIFY")
TOKEN_API_MELHOR_ENVIO = os.getenv("TOKEN_API_MELHOR_ENVIO")

# --- CACHE SIMPLES (em memória). Em produção troque por Redis.
CACHE: Dict[str, Dict[str, Any]] = {}
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "30"))

def now_iso():
    return datetime.utcnow().isoformat() + "Z"

def cache_set(key: str, payload: Dict[str, Any], ttl_seconds: int = CACHE_TTL_SECONDS):
    CACHE[key] = {
        "payload": payload,
        "expires_at": datetime.utcnow() + timedelta(seconds=ttl_seconds)
    }

def cache_get(key: str) -> Optional[Dict[str, Any]]:
    entry = CACHE.get(key)
    if not entry:
        return None
    if entry["expires_at"] < datetime.utcnow():
        try:
            del CACHE[key]
        except KeyError:
            pass
        return None
    return entry["payload"]

class DadosWebhookMelhorEnvio(BaseModel):
    id: Optional[str] = None
    status: Optional[str] = None
    tracking: Optional[str] = None

# ---------------------------
# Helper functions (async)
# ---------------------------
async def fetch_melhor_envio_order_details(order_id: str) -> Optional[dict]:
    """Consulta o endpoint do Melhor Envio que devolve detalhes da etiqueta/pedido."""
    headers = {"Authorization": f"Bearer {TOKEN_API_MELHOR_ENVIO}", "Accept": "application/json"}
    url = f"https://www.melhorenvio.com.br/api/v2/me/orders/{order_id}"
    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.get(url, headers=headers)
    if r.status_code == 200:
        return r.json()
    return None

async def fetch_melhor_envio_tracking_events(tracking_number: str) -> Optional[dict]:
    """
    Tenta buscar eventos diretamente por tracking.
    O endpoint exato pode variar — ajuste conforme a doc da Melhor Envio se existir um endpoint específico.
    Aqui tentamos um endpoint hipotético e também retornamos None se não funcionar.
    """
    headers = {"Authorization": f"Bearer {TOKEN_API_MELHOR_ENVIO}", "Accept": "application/json"}
    # Exemplo hipotético (verificar doc do Melhor Envio e ajustar)
    url = f"https://www.melhorenvio.com.br/api/v2/me/shipments/{tracking_number}/events"
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get(url, headers=headers)
    if r.status_code == 200:
        return r.json()
    return None

async def obter_token_acesso_shopify_assincrono(nome_da_loja_shopify: str, api_key_do_app: str, api_secret_key: str) -> str:
    """(mantive seu helper) — se você usa client credentials, mantenha; caso use App instalado com access token, talvez não precise."""
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

async def buscar_id_interno_shopify_por_numero_assincrono(numero_visual_do_pedido: str, token_acesso: str, nome_loja: str) -> Optional[str]:
    url_busca = f"https://{nome_loja}.myshopify.com/admin/api/2024-01/orders.json?name={numero_visual_do_pedido}&status=any"
    cabecalhos_requisicao = {"X-Shopify-Access-Token": token_acesso, "Content-Type": "application/json"}
    async with httpx.AsyncClient() as cliente_http_assincrono:
        resposta_busca = await cliente_http_assincrono.get(url_busca, headers=cabecalhos_requisicao)
        if resposta_busca.status_code == 200:
            lista_de_pedidos = resposta_busca.json().get("orders", [])
            return str(lista_de_pedidos[0].get("id")) if lista_de_pedidos else None
    return None

async def injetar_codigo_de_rastreio_na_shopify_assincrono(id_interno_pedido: str, codigo_rastreio: str, nome_transportadora: str, token_acesso: str, nome_loja: str) -> bool:
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

# ---------------------------
# Rotas públicas
# ---------------------------

@app_servidor_web.get("/")
async def rota_raiz():
    return {"status": "sucesso", "mensagem": "Servidor da YK SoftwareHouse operante e aguardando eventos!"}

@app_servidor_web.get("/webhook/melhor-envio/atualizacao")
async def responder_ping_validacao_melhor_envio():
    return {"status": "sucesso", "mensagem": "Endpoint ativo e validado!"}

@app_servidor_web.post("/webhook/melhor-envio/atualizacao")
async def processar_evento_de_rastreio(dados_do_webhook: DadosWebhookMelhorEnvio):
    """
    Recebe eventos do Melhor Envio, injeta na Shopify e atualiza o cache local do tracking
    para que a página pública de rastreio tenha informação imediata.
    """
    # validação básica
    if dados_do_webhook.status is None or not dados_do_webhook.id:
        return {"status": "sucesso", "mensagem": "Ping de teste vazio validado!"}

    status_que_importam = ["released", "posted"]
    if dados_do_webhook.status not in status_que_importam:
        return {"status": "ignorado", "mensagem": f"Status '{dados_do_webhook.status}' ignorado."}

    # pega detalhes da etiqueta/pedido no Melhor Envio
    detalhes = await fetch_melhor_envio_order_details(dados_do_webhook.id)
    if not detalhes:
        return {"status": "erro_ignorado", "mensagem": "Erro ao consultar detalhes no Melhor Envio."}

    # extrai transportadora e possíveis tags/número do pedido
    nome_da_transportadora = detalhes.get("service", {}).get("company", {}).get("name", "Correios")
    tags_do_pedido = detalhes.get("tags", [])
    numero_do_pedido = None
    if tags_do_pedido:
        # tags pode ser lista de dicts; adapta conforme payload real
        first_tag = tags_do_pedido[0]
        numero_do_pedido = first_tag.get("tag") if isinstance(first_tag, dict) else None

    # fallback: olha em non_commercial ou campos alternativos
    if not numero_do_pedido:
        numero_do_pedido = detalhes.get("non_commercial", {}).get("content")

    if not numero_do_pedido:
        # ainda tenta salvar cache do tracking mesmo sem pedido vinculado
        # mas como não sabemos o order shopify, só retornamos ignorado
        return {"status": "ignorado", "erro": "Número do pedido não encontrado na etiqueta."}

    # Busca ID interno da Shopify
    id_interno_do_pedido = await buscar_id_interno_shopify_por_numero_assincrono(numero_do_pedido, TOKEN_SHOPIFY, NOME_DA_LOJA_SHOPIFY)
    if not id_interno_do_pedido:
        return {"status": "ignorado", "mensagem": "Pedido não encontrado na Shopify. (Pode ser o disparo de teste do M.E.)"}

    # Injeta o código de rastreio na Shopify
    sucesso_na_injecao = await injetar_codigo_de_rastreio_na_shopify_assincrono(id_interno_do_pedido, dados_do_webhook.tracking, nome_da_transportadora, TOKEN_SHOPIFY, NOME_DA_LOJA_SHOPIFY)

    # --- Monta o payload do tracking para o cache (timeline simplificada)
    # Tente extrair eventos diretos do detalhes (se existirem), caso contrário, busque por tracking events
    timeline_events = []
    if detalhes.get("events"):
        # caso o ME já envie eventos no payload
        timeline_events = detalhes.get("events", [])
    else:
        # tenta buscar por endpoint específico (se existir)
        evs = await fetch_melhor_envio_tracking_events(dados_do_webhook.tracking)
        if evs and isinstance(evs, dict) and evs.get("events"):
            timeline_events = evs.get("events")

    payload_cache = {
        "tracking": dados_do_webhook.tracking,
        "updated_at": now_iso(),
        "eta": detalhes.get("estimated_delivery") or None,
        "events": timeline_events,
        "source_order_number": numero_do_pedido,
        "shopify_order_id": id_interno_do_pedido,
    }

    # salva em cache para resposta imediata na GET /tracking/{tracking}
    cache_set(dados_do_webhook.tracking, payload_cache)

    if sucesso_na_injecao:
        return {"status": "sucesso", "pedido": numero_do_pedido}

    return {"status": "erro_ignorado", "mensagem": "Falha ao injetar rastreio na Shopify."}

@app_servidor_web.get("/tracking/{tracking}")
async def get_tracking(tracking: str):
    """
    Endpoint público consumível pela página /pages/rastreio do Shopify.
    1) Tenta responder do cache
    2) se não houver, consulta a Melhor Envio e monta resposta
    """
    # 1) cache
    cached = cache_get(tracking)
    if cached:
        return cached

    # 2) tenta buscar no Melhor Envio (por tracking)
    evs = await fetch_melhor_envio_tracking_events(tracking)
    if evs and isinstance(evs, dict):
        # normalize minimalamente
        payload = {
            "tracking": tracking,
            "updated_at": now_iso(),
            "eta": evs.get("eta") or None,
            "events": evs.get("events") or [],
            "source_order_number": evs.get("order_number") or None,
            "shopify_order_id": None,
        }
        cache_set(tracking, payload)
        return payload

    # 3) se nenhum dado encontrado, devolve objeto vazio (com status 404 optional)
    raise HTTPException(status_code=404, detail="Rastreamento não encontrado.")