import logging
import httpx
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta
from fastapi.responses import HTMLResponse
from typing import List
from urllib.parse import urlparse, parse_qs
from datetime import datetime
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger_telemetria = logging.getLogger("middleware_rastreio")

URL_PROJETO_SUPABASE = os.getenv("SUPABASE_URL")
CHAVE_PUBLICA_SUPABASE = os.getenv("SUPABASE_KEY")

ME_SYNC_STATUSES = os.getenv(
    "ME_SYNC_STATUSES",
    "pending,released,posted,delivered"
)
ME_SYNC_LIMIT = int(os.getenv("ME_SYNC_LIMIT", "100"))
ME_SYNC_MAX_PAGES = int(os.getenv("ME_SYNC_MAX_PAGES", "20"))

cliente_banco_supabase: Optional[Client] = None
if URL_PROJETO_SUPABASE and CHAVE_PUBLICA_SUPABASE:
    cliente_banco_supabase = create_client(URL_PROJETO_SUPABASE, CHAVE_PUBLICA_SUPABASE)
    def _safe_float(v):
        try:
            if v is None or v == "":
                return None
            return float(v)
        except Exception:
            return None

    def _safe_int(v):
        try:
            if v is None or v == "":
                return None
            return int(v)
        except Exception:
            return None

    def _upper(v: str | None):
        return (v or "").upper() or None

    def _normalize_order_name(name: str | None) -> str | None:
        # Shopify manda "#1554"
        if not name:
            return None
        return name.strip()

    def supa_upsert_venda_from_shopify(order: dict):
        """
        Cria/atualiza 1 linha em public.vendas usando shopify_order_id como chave.
        """
        if not cliente_banco_supabase:
            return None

        order_id = str(order.get("id")) if order.get("id") else None
        if not order_id:
            return None

        customer = order.get("customer") or {}
        email = order.get("email") or customer.get("email")
        nome = (customer.get("first_name","") + " " + customer.get("last_name","")).strip() or customer.get("name")

        landing_site = order.get("landing_site") or ""
        kit_id = extrair_utm_campaign(landing_site)

        total_price = _safe_float(order.get("total_price"))
        subtotal_price = _safe_float(order.get("subtotal_price"))
        discounts = _safe_float(order.get("total_discounts"))

        financial_status = _upper(order.get("financial_status"))
        fulfillment_status = _upper(order.get("fulfillment_status"))

        pedido_gratuito = (total_price == 0) if total_price is not None else False
        status_pagamento = "GRÁTIS" if pedido_gratuito else ("PAGO" if financial_status in ["PAID", "AUTHORIZED"] else "PENDENTE")

        payload = {
            "shopify_order_id": order_id,
            "shopify_order_name": _normalize_order_name(order.get("name")),
            "shopify_created_at": order.get("created_at"),
            "financial_status": financial_status,
            "fulfillment_status": fulfillment_status,
            "customer_shopify_id": str(customer.get("id")) if customer.get("id") else None,
            "nome_cliente": nome,
            "email_cliente": email,
            "total_price": total_price,
            "subtotal_price": subtotal_price,
            "total_discounts": discounts,
            "pedido_gratuito": pedido_gratuito,
            "status_pagamento": status_pagamento,
            "kit_id": kit_id,
            "landing_site": landing_site,
            "payload_shopify": order,  # opcional: snapshot
        }

        res = cliente_banco_supabase.table("vendas").upsert(payload, on_conflict="shopify_order_id").execute()
        # retorna a linha inserida/atualizada (depende do client), mas normalmente vem em res.data
        if getattr(res, "data", None):
            return res.data[0]
        return None

    def supa_upsert_itens_from_shopify(venda_id: str, order: dict):
        """
        Sobe line_items em public.venda_itens (evita duplicar via uniq index).
        """
        if not cliente_banco_supabase or not venda_id:
            return

        itens = order.get("line_items") or []
        rows = []
        for li in itens:
            rows.append({
                "venda_id": venda_id,
                "shopify_line_item_id": str(li.get("id")) if li.get("id") else None,
                "product_id": str(li.get("product_id")) if li.get("product_id") else None,
                "variant_id": str(li.get("variant_id")) if li.get("variant_id") else None,
                "title": li.get("title") or li.get("name"),
                "quantity": _safe_int(li.get("quantity")),
                "price": _safe_float(li.get("price")),
                "properties": li.get("properties") or None,
            })

        if rows:
            # on_conflict aqui depende do seu unique index: (venda_id, shopify_line_item_id)
            cliente_banco_supabase.table("venda_itens").upsert(
                rows, on_conflict="venda_id,shopify_line_item_id"
            ).execute()

    def supa_update_venda_from_yampi(kit_id: str, nome: str | None, email: str | None, payload_yampi: dict):
        """
        Atualiza venda via kit_id (utm_campaign) enquanto o Shopify ainda não completou tudo.
        """
        if not cliente_banco_supabase or not kit_id:
            return

        update = {
            "nome_cliente": nome,
            "email_cliente": email,
            "status_pagamento": "PAGO",     # se a yampi marcou como pago
            "payload_yampi": payload_yampi,
            "fonte_criacao": "yampi",
        }

        # update por kit_id (pode afetar mais de uma se reutilizar kit_id; em geral é 1)
        cliente_banco_supabase.table("vendas").update(update).eq("kit_id", kit_id).execute()

    def supa_upsert_envio(
        melhor_envio_etiqueta_id: str,
        status_envio: str | None,
        codigo_rastreio: str | None,
        transportadora: str | None,
        previsao_entrega: str | None,
        eventos: list,
        numero_pedido_shopify: str | None,
        payload_me: dict | None,
        shopify_order_id: str | None
    ):
        """
        Cria/atualiza 1 envio em public.envios. Linka com venda via shopify_order_id.
        """
        if not cliente_banco_supabase or not melhor_envio_etiqueta_id:
            return

        venda_id = None
        if shopify_order_id:
            v = cliente_banco_supabase.table("vendas").select("id").eq("shopify_order_id", str(shopify_order_id)).limit(1).execute()
            if getattr(v, "data", None):
                venda_id = v.data[0]["id"]

        payload = {
            "venda_id": venda_id,
            "melhor_envio_etiqueta_id": melhor_envio_etiqueta_id,
            "status_envio": status_envio,
            "codigo_rastreio": codigo_rastreio,
            "transportadora": transportadora,
            "previsao_entrega": previsao_entrega,
            "eventos_json": eventos or [],
            "numero_pedido_shopify": numero_pedido_shopify,
            "payload_melhor_envio": payload_me,
        }

        cliente_banco_supabase.table("envios").upsert(payload, on_conflict="melhor_envio_etiqueta_id").execute()
    
    
    logger_telemetria.info("Conexão com banco de dados Supabase inicializada com sucesso.")
else:
    logger_telemetria.warning("Chaves do Supabase ausentes no .env. Banco de dados inativo.")

async def buscar_detalhes_completos_do_pedido_shopify_por_numero(numero_visual_do_pedido: str, token_acesso: str, nome_loja: str) -> Optional[dict]:
    """Fase 1 e 2: Resolve o identificador público para o ID interno e extrai os itens da linha e Fulfillments."""
    logger_telemetria.info(f"[Fase 1] Iniciando resolução de identidade na Shopify para o pedido visual: {numero_visual_do_pedido}")
    
    url_busca_pedido_completo = f"https://{nome_loja}.myshopify.com/admin/api/2023-07/orders.json?name={numero_visual_do_pedido}&status=any"
    cabecalhos_autenticacao_shopify = {"X-Shopify-Access-Token": token_acesso, "Content-Type": "application/json"}
    
    async with httpx.AsyncClient() as cliente_http_assincrono:
        resposta_api_shopify = await cliente_http_assincrono.get(url_busca_pedido_completo, headers=cabecalhos_autenticacao_shopify)
        
        if resposta_api_shopify.status_code == 200:
            lista_de_pedidos_encontrados = resposta_api_shopify.json().get("orders", [])
            if lista_de_pedidos_encontrados:
                pedido_alvo_encontrado = lista_de_pedidos_encontrados[0]
                logger_telemetria.info(f"[Fase 1] Sucesso! Pedido {numero_visual_do_pedido} traduzido para ID interno: {pedido_alvo_encontrado.get('id')}")
                return pedido_alvo_encontrado
            else:
                logger_telemetria.warning(f"[Fase 1] Falha na resolução: Nenhum pedido corresponde à string '{numero_visual_do_pedido}'.")
        else:
            logger_telemetria.error(f"[Fase 1] Erro crítico de comunicação com Shopify. HTTP Status: {resposta_api_shopify.status_code}")
            
    return None

async def buscar_eventos_de_rastreio_em_lote_no_melhor_envio(lista_de_codigos_rastreio: List[str]) -> dict:
    """Fase 4: Consome a API v2 do Melhor Envio via POST para obter a cronologia exata dos pacotes."""
    logger_telemetria.info(f"[Fase 4] Iniciando enriquecimento logístico. Códigos solicitados: {lista_de_codigos_rastreio}")
    
    url_rastreamento_lote_melhor_envio = "https://www.melhorenvio.com.br/api/v2/me/shipment/tracking"
    
    cabecalhos_estritos_melhor_envio = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN_API_MELHOR_ENVIO}",
        "User-Agent": "IntegracaoYkSoftwareHouse/1.0 (seu_email_admin@dominio.com)" 
    }
    
    corpo_da_requisicao_lote = {"orders": lista_de_codigos_rastreio}

    async with httpx.AsyncClient(timeout=20.0) as cliente_http_assincrono:
        resposta_api_rastreio = await cliente_http_assincrono.post(
            url_rastreamento_lote_melhor_envio, 
            headers=cabecalhos_estritos_melhor_envio, 
            json=corpo_da_requisicao_lote
        )
        
        if resposta_api_rastreio.status_code == 200:
            logger_telemetria.info(f"[Fase 4] Telemetria logística recuperada com sucesso do servidor do Melhor Envio.")
            return resposta_api_rastreio.json()
        else:
            logger_telemetria.error(f"[Fase 4] Falha ao consumir Melhor Envio. Código {resposta_api_rastreio.status_code}. Motivo: {resposta_api_rastreio.text}")
            return {}

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

def supa_salvar_selecao_kit(payload: dict):
    """
    Salva/atualiza a seleção do kit (cores e tamanhos por unidade) no Supabase.
    Tabela: kit_selecoes  (kit_id TEXT UNIQUE, nome_produto TEXT,
                           quantidade_itens INT, detalhes JSONB,
                           data_hora TIMESTAMPTZ, status_pagamento TEXT,
                           numero_pedido_shopify TEXT)
    Crie a tabela com:
        CREATE TABLE kit_selecoes (
            id              BIGSERIAL PRIMARY KEY,
            kit_id          TEXT UNIQUE NOT NULL,
            nome_produto    TEXT,
            quantidade_itens INT,
            detalhes        JSONB,
            data_hora       TIMESTAMPTZ,
            status_pagamento TEXT DEFAULT 'PENDENTE',
            numero_pedido_shopify TEXT,
            created_at      TIMESTAMPTZ DEFAULT now()
        );
    """
    if not cliente_banco_supabase:
        return
    try:
        row = {
            "kit_id":           payload["kit_id"],
            "nome_produto":     payload.get("nome_produto"),
            "quantidade_itens": payload.get("quantidade_itens"),
            "detalhes":         payload.get("detalhes"),
            "data_hora":        payload.get("data_hora"),
            "status_pagamento": "PENDENTE",
        }
        cliente_banco_supabase.table("kit_selecoes").upsert(
            row, on_conflict="kit_id"
        ).execute()
    except Exception as e:
        logger_telemetria.error(f"[kit_selecoes] Erro ao salvar no Supabase: {e}")


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
    supa_salvar_selecao_kit(nova_venda)
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

@app_servidor_web.post("/webhook-shopify/pedido-atualizado")
async def webhook_shopify_pedido_atualizado(data: Dict[str, Any]):
    kit_id = None
    for li in data.get("line_items", []) or []:
        props = li.get("properties") or []
        if isinstance(props, list):
            for p in props:
                if p.get("name") in ["_kit_id", "kit_id"] and p.get("value"):
                    kit_id = p["value"]
                    break
        if kit_id:
            break

    if not kit_id:
        return {"status": "ignorado", "motivo": "sem kit_id nas properties"}

    email = data.get("email")
    customer = data.get("customer") or {}
    nome = (customer.get("first_name","") + " " + customer.get("last_name","")).strip() or customer.get("name")

    financial_status = (data.get("financial_status") or "").upper()  # paid, pending, authorized...
    numero = data.get("name")
    order_id = data.get("id")
    total_price = float(data.get("total_price") or 0)
    subtotal_price = float(data.get("subtotal_price") or 0)
    discounts = float(data.get("total_discounts") or 0)
    financial_status = (data.get("financial_status") or "").upper()

    for venda in DB_VENDAS_KITS:
        if venda.get("kit_id") == kit_id:
            venda["status_pedido"] = "PEDIDO_CRIADO"
            venda["numero_pedido_shopify"] = numero
            venda["id_pedido_shopify"] = str(order_id) if order_id else None
            venda["total_price"] = total_price
            venda["subtotal_price"] = subtotal_price
            venda["total_discounts"] = discounts
            venda["financial_status"] = financial_status

            # marcar como gratuito
            if total_price == 0:
                venda["pedido_gratuito"] = True
                venda["status_pagamento"] = "GRÁTIS"
            else:
                venda["pedido_gratuito"] = False
            if email:
                venda["email_cliente"] = email
            if nome and (venda.get("nome_cliente") in [None, "", "Aguardando Pagamento..."]):
                venda["nome_cliente"] = nome

            if financial_status in ["PAID", "AUTHORIZED"]:
                venda["status_pagamento"] = "PAGO"
                venda["status_pedido"] = "PEDIDO_APROVADO"

            return {"status": "ok", "kit_id": kit_id, "financial_status": financial_status}

    return {"status": "nao_encontrado", "kit_id": kit_id}

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
        pedido_gratis = v.get("pedido_gratuito", False)
        total = v.get("total_price", 0)
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

                    {"<div style='margin-top:8px; color:#6c5ce7; font-weight:600;'>💸 Pedido com custo ZERO (Cupom aplicado)</div>" if pedido_gratis else f"<div style='margin-top:8px; color:#28a745; font-weight:600;'>💰 Total: R$ {total}</div>"}

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

    if cliente_banco_supabase:
        dados_para_salvar_no_banco = {
            "codigo_rastreio": dados_do_webhook.tracking,
            "id_etiqueta_melhor_envio": dados_do_webhook.id,
            "numero_pedido_shopify": numero_do_pedido,
            "eventos_json": timeline_events,
            "previsao_entrega": detalhes.get("estimated_delivery")
        }
        try:
            # O comando 'upsert' é inteligente: se o rastreio já existir, ele atualiza a linha. Se não, ele cria.
            cliente_banco_supabase.table("rastreios_logistica").upsert(dados_para_salvar_no_banco).execute()
            logger_telemetria.info(f"O pacote {dados_do_webhook.tracking} se moveu! Dados salvos no Supabase.")
        except Exception as erro_gravacao_banco:
            logger_telemetria.error(f"Falha ao gravar histórico no Supabase: {erro_gravacao_banco}")
    else:
        # Fallback de segurança para a memória RAM caso o Supabase não esteja ligado
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

@app_servidor_web.get("/api/rastreio/pedido/{numero_do_pedido_cliente}")
async def expor_contrato_de_rastreio_para_frontend(numero_do_pedido_cliente: str):
    logger_telemetria.info(f"--- [INICIO] Processando requisição de tela de rastreio para o pedido: {numero_do_pedido_cliente} ---")

    # FASES 1 e 2: Busca dados na Shopify (Identidade, Cliente, Itens)
    dicionario_pedido_shopify = await buscar_detalhes_completos_do_pedido_shopify_por_numero(
        numero_do_pedido_cliente, TOKEN_SHOPIFY, NOME_DA_LOJA_SHOPIFY
    )

    if not dicionario_pedido_shopify:
        logger_telemetria.warning(f"Fluxo abortado. O identificador {numero_do_pedido_cliente} não existe na plataforma base.")
        raise HTTPException(status_code=404, detail="Pedido não encontrado.")

    identificador_interno_shopify = dicionario_pedido_shopify.get("id")
    dados_do_consumidor = dicionario_pedido_shopify.get("customer", {})
    
    # Extração defensiva de informações do cliente
    nome_completo_consumidor = f"{dados_do_consumidor.get('first_name', '')} {dados_do_consumidor.get('last_name', '')}".strip()
    if not nome_completo_consumidor:
        nome_completo_consumidor = dados_do_consumidor.get("name", "Cliente não identificado")

    # FASE 2: Iteração dos Line Items
    logger_telemetria.info("[Fase 2] Mapeando matriz de produtos (Line Items)...")
    matriz_de_produtos_faturados = []
    
    for item_faturado in dicionario_pedido_shopify.get("line_items", []):
        matriz_de_produtos_faturados.append({
            "id": item_faturado.get("id"),
            "title": item_faturado.get("title") or item_faturado.get("name"),
            "quantity": item_faturado.get("quantity"),
            "price": item_faturado.get("price"),
            "product_id": item_faturado.get("product_id"),
            "variant_id": item_faturado.get("variant_id")
        })
    logger_telemetria.info(f"[Fase 2] Foram extraídos {len(matriz_de_produtos_faturados)} produtos faturados para este pedido.")

    # FASE 3: Navegação por Fulfillment Orders e coleta de códigos
    logger_telemetria.info("[Fase 3] Analisando Ordens de Cumprimento (Fulfillments)...")
    matriz_de_cumprimentos_shopify = dicionario_pedido_shopify.get("fulfillments", [])
    lista_plana_codigos_rastreio = []
    matriz_de_volumes_logisticos = []

    for objeto_cumprimento in matriz_de_cumprimentos_shopify:
        codigo_de_rastreio_unico = objeto_cumprimento.get("tracking_number")
        empresa_transportadora = objeto_cumprimento.get("tracking_company")

        if codigo_de_rastreio_unico:
            lista_plana_codigos_rastreio.append(codigo_de_rastreio_unico)
            matriz_de_volumes_logisticos.append({
                "tracking_number": codigo_de_rastreio_unico,
                "tracking_company": empresa_transportadora,
                "status_interno_shopify": objeto_cumprimento.get("shipment_status") or "pending",
                "events": []
            })
            logger_telemetria.info(f"[Fase 3] Identificado pacote despachado via {empresa_transportadora}. Código: {codigo_de_rastreio_unico}")

    # FASE 4: Enriquecimento via Melhor Envio
    if lista_plana_codigos_rastreio:
        logger_telemetria.info("[Fase 4] Buscando histórico logístico direto no nosso banco de dados Supabase...")
        
        if cliente_banco_supabase:
            try:
                # Busca todos os códigos do pedido no banco de uma só vez
                resposta_do_banco = cliente_banco_supabase.table("rastreios_logistica").select("*").in_("codigo_rastreio", lista_plana_codigos_rastreio).execute()
                
                # Transforma a lista numa espécie de dicionário/mapa para achar rápido pelo código
                mapa_rastreios_encontrados_no_banco = {linha["codigo_rastreio"]: linha for linha in resposta_do_banco.data}
                
                for volume_logistico in matriz_de_volumes_logisticos:
                    codigo_atual = volume_logistico["tracking_number"]
                    dados_salvos_no_banco = mapa_rastreios_encontrados_no_banco.get(codigo_atual)
                    
                    if dados_salvos_no_banco:
                        eventos_cronologicos = dados_salvos_no_banco.get("eventos_json", [])
                        volume_logistico["events"] = eventos_cronologicos
                        volume_logistico["eta"] = dados_salvos_no_banco.get("previsao_entrega")
                        logger_telemetria.info(f"[Fase 4] Sucesso! {len(eventos_cronologicos)} eventos lidos da memória do banco para a etiqueta {codigo_atual}.")
                    else:
                        logger_telemetria.info(f"[Fase 4] A encomenda {codigo_atual} ainda não recebeu Webhooks de movimentação. Linha do tempo vazia.")
            
            except Exception as erro_leitura_banco:
                logger_telemetria.error(f"[Fase 4] Erro interno ao ler do banco de dados: {erro_leitura_banco}")
        else:
            logger_telemetria.warning("[Fase 4] Supabase desconectado. Tela será montada sem o histórico da linha do tempo.")
    else:
        logger_telemetria.warning("[Fase 3/4] Pedido não possui objetos de cumprimento ativos. A mercadoria não foi despachada.")

    # FASE 5: Estruturação, Limpeza e Retorno do JSON (Contrato)
    logger_telemetria.info("[Fase 5] Consolidando contrato JSON limpo e estruturado para o frontend.")
    
    contrato_dados_frontend = {
        "status": "success",
        "data": {
            "order": {
                "customer_order_id": dicionario_pedido_shopify.get("name"),
                "platform_internal_id": str(identificador_interno_shopify),
                "created_at": dicionario_pedido_shopify.get("created_at"),
                "financial_status": dicionario_pedido_shopify.get("financial_status"),
                "fulfillment_status": dicionario_pedido_shopify.get("fulfillment_status"),
                "customer": {
                    "name": nome_completo_consumidor,
                    "email": dados_do_consumidor.get("email")
                }
            },
            "purchased_items": matriz_de_produtos_faturados,
            "logistics": matriz_de_volumes_logisticos
        }
    }

    logger_telemetria.info(f"--- [FIM] Fluxo de rastreio finalizado com sucesso para: {numero_do_pedido_cliente} ---")
    return contrato_dados_frontend

@app_servidor_web.get("/auth/callback")
async def capturar_token_oauth(shop: str, code: str):
    """Rota temporária para capturar o token shpat_ via OAuth da Shopify 2026"""
    client_id = os.getenv("SHOPIFY_CLIENT_ID")
    client_secret = os.getenv("SHOPIFY_CLIENT_SECRET")
    
    if not client_id or not client_secret:
        return {"erro": "Faltam as chaves SHOPIFY_CLIENT_ID ou SHOPIFY_CLIENT_SECRET no Render"}

    url_troca = f"https://{shop}/admin/oauth/access_token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "code": code
    }
    
    async with httpx.AsyncClient() as client:
        resposta = await client.post(url_troca, json=payload)
        dados = resposta.json()
        
        token = dados.get("access_token")
        
        if token:
            logger_telemetria.info("=" * 60)
            logger_telemetria.info(f"🔑 TOKEN CAPTURADO COM SUCESSO: {token}")
            logger_telemetria.info("=" * 60)
            return {"status": "sucesso", "mensagem": "Vá olhar os logs do Render agora! O token shpat_ está lá."}
        else:
            logger_telemetria.error(f"Erro ao capturar token: {dados}")
            return {"status": "erro", "detalhes": dados}


@app_servidor_web.get("/admin/sincronizar-historico")
async def retroalimentar_banco_de_dados_supabase():
    """Rota temporária para popular o banco com etiquetas antigas (inclui pending)."""
    if not cliente_banco_supabase:
        return {"erro": "Conexão com Supabase não estabelecida. Verifique as chaves no Render."}

    logger_telemetria.info("Iniciando varredura retroativa no Melhor Envio (incluindo pending)...")

    # Configuráveis via env (opcional)
    statuses = os.getenv("ME_SYNC_STATUSES", "pending,released,posted,delivered")
    limit = int(os.getenv("ME_SYNC_LIMIT", "100"))
    max_pages = int(os.getenv("ME_SYNC_MAX_PAGES", "20"))

    cabecalhos_acesso = {
        "Authorization": f"Bearer {TOKEN_API_MELHOR_ENVIO}",
        "Accept": "application/json",
        "User-Agent": "IntegracaoYkSoftwareHouse/1.0"
    }

    # 1) Pagina e junta tudo
    etiquetas = []
    async with httpx.AsyncClient(timeout=30.0) as cliente_http:
        for page in range(1, max_pages + 1):
            url = (
                "https://www.melhorenvio.com.br/api/v2/me/orders"
                f"?status={statuses}&limit={limit}&page={page}"
            )
            resp = await cliente_http.get(url, headers=cabecalhos_acesso)

            if resp.status_code != 200:
                return {"erro": "Falha ao listar pedidos", "detalhes": resp.text, "url": url}

            payload = resp.json()
            page_data = payload.get("data", []) if isinstance(payload, dict) else payload

            if not page_data:
                break

            etiquetas.extend(page_data)
            logger_telemetria.info(f"[ME] Página {page}: +{len(page_data)} (total {len(etiquetas)})")

    if not etiquetas:
        return {"mensagem": "Nenhuma etiqueta encontrada para sincronizar."}

    # IDs das etiquetas
    lista_de_ids_secretos = [e.get("id") for e in etiquetas if e.get("id")]
    logger_telemetria.info(f"Encontradas {len(lista_de_ids_secretos)} etiquetas. Buscando eventos em lote...")

    # 2) Busca eventos/tracking em lote
    dicionario_eventos_historicos = await buscar_eventos_de_rastreio_em_lote_no_melhor_envio(lista_de_ids_secretos)

    quantidade_salva_no_banco = 0
    quantidade_sem_tracking = 0

    # 3) Salva no Supabase (somente quando tiver tracking + número)
    for id_etiqueta, info_rastreio in (dicionario_eventos_historicos or {}).items():
        etiqueta_original = next((e for e in etiquetas if e.get("id") == id_etiqueta), {}) or {}

        # número do pedido (tag)
        numero_pedido_vinculado = None
        tags = etiqueta_original.get("tags", [])
        if tags:
            primeira = tags[0]
            numero_pedido_vinculado = primeira.get("tag") if isinstance(primeira, dict) else primeira
        if not numero_pedido_vinculado:
            numero_pedido_vinculado = (etiqueta_original.get("non_commercial") or {}).get("content")

        codigo_correios = (info_rastreio or {}).get("tracking")

        # pending costuma cair aqui (sem tracking)
        if not codigo_correios:
            quantidade_sem_tracking += 1
            continue

        if codigo_correios and numero_pedido_vinculado:
            dados_para_supabase = {
                "codigo_rastreio": codigo_correios,
                "id_etiqueta_melhor_envio": id_etiqueta,
                "numero_pedido_shopify": str(numero_pedido_vinculado),
                "eventos_json": (info_rastreio or {}).get("events", []),
                "previsao_entrega": etiqueta_original.get("estimated_delivery"),
                "atualizado_em": datetime.utcnow().isoformat() + "Z",
            }
            try:
                cliente_banco_supabase.table("rastreios_logistica").upsert(dados_para_supabase).execute()
                quantidade_salva_no_banco += 1
            except Exception as erro_banco:
                logger_telemetria.error(f"Erro ao salvar pacote {codigo_correios}: {erro_banco}")

    logger_telemetria.info(
        f"Sincronização concluída! salvos={quantidade_salva_no_banco} | sem_tracking={quantidade_sem_tracking} | total_etiquetas={len(lista_de_ids_secretos)}"
    )

    return {
        "status": "sucesso",
        "etiquetas_encontradas": len(lista_de_ids_secretos),
        "salvas_no_supabase": quantidade_salva_no_banco,
        "sem_tracking": quantidade_sem_tracking,
        "mensagem": "Sincronização concluída (pending incluído na busca; só salva quando existir tracking)."
    }