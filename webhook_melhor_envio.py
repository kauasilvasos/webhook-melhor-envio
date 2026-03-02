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
from urllib.parse import urlparse, parse_qs

def extrair_utm_campaign(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    try:
        qs = parse_qs(urlparse(url).query)
        v = qs.get("utm_campaign", [None])[0]
        return v
    except Exception:
        return None
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

@app_servidor_web.post("/webhook-shopify/pedido-criado")
async def webhook_shopify_pedido_criado(data: Dict[str, Any]):
    # Shopify manda o pedido inteiro
    numero = data.get("name")  # "#1234"
    order_id = data.get("id")
    customer = data.get("customer") or {}
    email = data.get("email") or customer.get("email")
    nome = None
    if customer:
        nome = (customer.get("first_name") or "") + " " + (customer.get("last_name") or "")
        nome = nome.strip() or customer.get("name")

    # Onde normalmente fica a URL com utms
    landing_site = data.get("landing_site") or ""
    kit_id = extrair_utm_campaign(landing_site)

    if not kit_id:
        return {"status": "ignorado", "motivo": "sem utm_campaign no landing_site"}

    for venda in DB_VENDAS_KITS:
        if venda.get("kit_id") == kit_id:
            venda["status_pedido"] = "PEDIDO_CRIADO"
            venda["numero_pedido_shopify"] = numero
            venda["id_pedido_shopify"] = str(order_id) if order_id else None
            if email:
                venda["email_cliente"] = email
            if nome and (venda.get("nome_cliente") in [None, "", "Aguardando Pagamento..."]):
                venda["nome_cliente"] = nome
            return {"status": "ok", "kit_id": kit_id, "pedido": numero}

    return {"status": "nao_encontrado", "kit_id": kit_id}


@app_servidor_web.post("/webhook-shopify/pedido-pago")
async def webhook_shopify_pedido_pago(data: Dict[str, Any]):
    numero = data.get("name")  # "#1234"
    order_id = data.get("id")
    landing_site = data.get("landing_site") or ""
    kit_id = extrair_utm_campaign(landing_site)

    for venda in DB_VENDAS_KITS:
        if (kit_id and venda.get("kit_id") == kit_id) or (numero and venda.get("numero_pedido_shopify") == numero) or (order_id and str(order_id) == str(venda.get("id_pedido_shopify"))):
            venda["status_pedido"] = "PEDIDO_APROVADO"
            venda["status_pagamento"] = "PAGO"
            return {"status": "ok", "pedido": numero}

    return {"status": "nao_encontrado", "pedido": numero}

@app_servidor_web.post("/Venda")
async def registrar_venda_kit(payload: VendaPayload):
    nova_venda = payload.dict()
    nova_venda["ja_foi_separado"] = False
    nova_venda["nome_cliente"] = "Aguardando Pagamento..."
    nova_venda["status_pedido"] = "AGUARDANDO_PEDIDO_SHOPIFY"
    nova_venda["numero_pedido_shopify"] = None
    nova_venda["id_pedido_shopify"] = None
    nova_venda["status_pagamento"] = "PENDENTE"
    nova_venda["email_cliente"] = None
    nova_venda["enviado"] = False

    DB_VENDAS_KITS.append(nova_venda)
    return {"status": "sucesso", "kit_id": payload.kit_id}

# 2. WEBHOOK DA YAMPI (PARA ATUALIZAR O NOME DO CLIENTE QUANDO PAGAR)
@app_servidor_web.post("/webhook-yampi")
async def webhook_yampi(data: Dict[str, Any]):
    cart_data = data.get("resource", {})
    customer_data = cart_data.get("customer", {})
    nome_real = customer_data.get("name", "Cliente Yampi")
    email_real = customer_data.get("email")

    
    # Busca o ID do Kit nas UTMs enviadas pela Yampi
    utm_campaign = cart_data.get("utm_campaign") or cart_data.get("tracking", {}).get("utm_campaign")
    
    if utm_campaign:
        for venda in DB_VENDAS_KITS:
            if venda["kit_id"] == utm_campaign:
                venda["nome_cliente"] = nome_real
                if email_real:
                    venda["email_cliente"] = email_real
                venda["status_pagamento"] = "PAGO"
                break
                
    return {"status": "recebido"}

@app_servidor_web.post("/admin/vendas/alternar-enviado/{kit_id}")
async def alternar_status_enviado(kit_id: str):
    for venda in DB_VENDAS_KITS:
        if venda["kit_id"] == kit_id:
            venda["enviado"] = not venda.get("enviado", False)
            return {"sucesso": True, "novo_status": venda["enviado"]}
    raise HTTPException(status_code=404, detail="Kit não encontrado")

@app_servidor_web.post("/admin/vendas/alternar-status/{kit_id}")
async def alternar_status_separacao(kit_id: str):
    for venda in DB_VENDAS_KITS:
        if venda["kit_id"] == kit_id:
            venda["ja_foi_separado"] = not venda.get("ja_foi_separado", False)
            return {"sucesso": True, "novo_status": venda["ja_foi_separado"]}
    raise HTTPException(status_code=404, detail="Kit não encontrado")

# 4. TELA DE ADMIN COM CHECKLIST
@app_servidor_web.get("/admin/vendas", response_class=HTMLResponse)
async def painel_admin_vendas():
    html_content = """
    <html>
        <head>
            <title>Painel de Separação</title>
            <style>
                body { font-family: 'Segoe UI', sans-serif; background: #f0f2f5; padding: 30px; }
                .card { background: white; border-radius: 12px; box-shadow: 0 4px 15px rgba(0,0,0,0.1); padding: 20px; margin-bottom: 20px; border-left: 8px solid #2b589c; transition: 0.3s; }
                .separado { border-left-color: #28a745; opacity: 0.6; background: #f8fff9; }
                .separado h3 { text-decoration: line-through; }
                .header { display: flex; justify-content: space-between; align-items: center; }
                .detalhes { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; margin-top: 15px; background: #f9f9f9; padding: 10px; border-radius: 8px; }
                .item-badge { background: #eee; padding: 5px 10px; border-radius: 4px; font-size: 0.9em; }
                .checkbox-custom { width: 25px; height: 25px; cursor: pointer; }
            </style>
            <script>
                async function marcar(kitId) {
                    const res = await fetch('/admin/vendas/alternar-status/' + kitId, {method: 'POST'});
                    const data = await res.json();
                    if(data.sucesso) {
                    document.getElementById('card-' + kitId).classList.toggle('separado');
                    }
                }
                async function marcarEnviado(kitId) {
                    const res = await fetch('/admin/vendas/alternar-enviado/' + kitId, {method: 'POST'});
                    const data = await res.json();
                    if(data.sucesso) {
                    document.getElementById('card-' + kitId).classList.toggle('enviado');
                    }
                }
                </script>
        </head>
        <body>
            <h1>📋 Fila de Separação de Kits</h1>
    """
    
    for v in reversed(DB_VENDAS_KITS):
        status_css = "separado" if v.get("ja_foi_separado") else ""
        check_attr = "checked" if v.get("ja_foi_separado") else ""
        enviado_css = "enviado" if v.get("enviado") else ""
        check_enviado = "checked" if v.get("enviado") else ""
        status_pedido = v.get("status_pedido", "—")
        status_pagamento = v.get("status_pagamento", "—")
        pedido_num = v.get("numero_pedido_shopify", "—")
        email_cli = v.get("email_cliente", "—")
        detalhes_html = "".join([f"<div class='item-badge'><b>{i['unidade']}:</b> {i['tamanho']} - {i['cor']}</div>" for i in v['detalhes']])
        
        html_content += f"""
            <div class="card {status_css} {enviado_css}" id="card-{v['kit_id']}">
                <div class="header">
                <div>
                    <small>
                    {v.get('data_hora','—')} | ID: {v['kit_id']} | Pedido: {pedido_num}
                    </small>

                    <div style="margin-top:6px; font-size:14px;">
                    <b>Status pedido:</b> {status_pedido} &nbsp; | &nbsp;
                    <b>Pagamento:</b> {status_pagamento} &nbsp; | &nbsp;
                    <b>Email:</b> {email_cli}
                    </div>

                    <div style="margin-top:10px;">
                    <label style="margin-left:12px;">Enviado</label>
                    <input type="checkbox" class="checkbox-custom" {check_enviado} onclick="marcarEnviado('{v['kit_id']}')">
                    </div>
                </div>
                </div>

                <div class="detalhes">
                {detalhes_html}
                </div>
            </div>
            """
    
    html_content += "</body></html>"
    return html_content

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