import asyncio
import json
import logging
import uuid
import httpx
import os
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Request
from pydantic import BaseModel
from typing import Optional, Dict, Any
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta
from fastapi.responses import HTMLResponse, JSONResponse
from typing import List
from urllib.parse import urlparse, parse_qs
from datetime import datetime
from supabase import create_client, Client
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

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

# ── Yampi sync ────────────────────────────────────────────────────────────────

YAMPI_USER_TOKEN  = os.getenv("YAMPI_USER_TOKEN", "")
YAMPI_USER_SECRET = os.getenv("YAMPI_USER_SECRET", "")
YAMPI_ALIAS       = os.getenv("YAMPI_ALIAS", "")
_logger_yampi     = logging.getLogger("yampi_sync")

def sync_yampi_orders(dias: int = 15) -> dict:
    """Busca pedidos da API Yampi e faz upsert em pedidos_yampi no Supabase."""
    if not (YAMPI_USER_TOKEN and YAMPI_USER_SECRET and YAMPI_ALIAS):
        _logger_yampi.warning("Credenciais Yampi não configuradas — pulando sync")
        return {"status": "skip", "motivo": "credenciais ausentes"}
    if not cliente_banco_supabase:
        _logger_yampi.warning("Supabase não configurado — pulando sync")
        return {"status": "skip", "motivo": "supabase ausente"}

    headers = {
        "User-Token": YAMPI_USER_TOKEN,
        "User-Secret-key": YAMPI_USER_SECRET,
        "Content-Type": "application/json",
    }
    data_limite = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")
    url_base    = f"https://api.dooki.com.br/v2/{YAMPI_ALIAS}/orders"

    all_rows: list[dict] = []
    page = 1

    while True:
        try:
            resp = httpx.get(f"{url_base}?page={page}", headers=headers, timeout=30)
            resp.raise_for_status()
            json_data = resp.json()
        except Exception as e:
            _logger_yampi.error("Erro na página %d: %s", page, e)
            break

        orders = json_data.get("data", [])
        if not orders:
            break

        for idx, order in enumerate(orders):
            data_criacao = (order.get("created_at") or {}).get("date", "")
            if data_criacao and str(data_criacao)[:10] < data_limite:
                continue

            for i, item in enumerate(order.get("items", {}).get("data", [])):
                sku_raw  = item.get("sku") or {}
                sku_data = sku_raw.get("data", {}) if isinstance(sku_raw, dict) else {}
                sku_id   = sku_data.get("id")

                variacoes = " | ".join(
                    v.get("value", "") for v in sku_data.get("variations", []) if v.get("value")
                )
                nome_produto = item.get("product_name", "")
                produto_fmt  = f"{nome_produto} ({variacoes})" if variacoes else nome_produto

                cupom = ""
                promo = order.get("promocode")
                if isinstance(promo, dict):
                    info = promo.get("data")
                    if isinstance(info, dict):
                        cupom = info.get("code", "")
                    elif isinstance(info, list) and info:
                        cupom = info[0].get("code", "")

                all_rows.append({
                    "id_supabase":  f"{order.get('id')}_{sku_id}_{i}" if sku_id else f"{order.get('id')}_item_{i}",
                    "id":           order.get("id"),
                    "data":         data_criacao,
                    "data_pagamento": (order.get("paid_at") or {}).get("date"),
                    "numero_pedido": order.get("number"),
                    "cliente":      (order.get("customer") or {}).get("data", {}).get("name"),
                    "cliente_email": (order.get("customer") or {}).get("data", {}).get("email"),
                    "status":       (order.get("status") or {}).get("data", {}).get("name"),
                    "produto":      produto_fmt,
                    "sku":          sku_data.get("sku"),
                    "quantidade":   item.get("quantity"),
                    "total":        order.get("total"),
                    "total_item":   item.get("price"),
                    "total_frete":  order.get("total_shipping"),
                    "total_desconto": order.get("total_discount"),
                    "cupom":        cupom,
                    "codigo_rastreamento": order.get("tracking_code"),
                    "url_rastreamento":    order.get("tracking_url"),
                    "entrega_cidade": (order.get("shipping_address") or {}).get("data", {}).get("city"),
                    "entrega_estado": (order.get("shipping_address") or {}).get("data", {}).get("state"),
                    "entrega_cep":    (order.get("shipping_address") or {}).get("data", {}).get("zipcode"),
                    "utm_source":   order.get("utm_source"),
                    "utm_campaign": order.get("utm_campaign"),
                })

        pagination = json_data.get("meta", {}).get("pagination", {})
        if page >= pagination.get("total_pages", 1):
            break
        page += 1
        time.sleep(0.5)

    if not all_rows:
        _logger_yampi.info("Nenhum pedido encontrado nos últimos %d dias", dias)
        return {"status": "ok", "total": 0}

    # Deduplica por id_supabase
    unique = list({r["id_supabase"]: r for r in all_rows}.values())

    try:
        cliente_banco_supabase.table("pedidos_yampi").upsert(
            unique, on_conflict="id_supabase"
        ).execute()
        _logger_yampi.info("✅ %d registros sincronizados (Yampi → Supabase)", len(unique))
        return {"status": "ok", "total": len(unique)}
    except Exception as e:
        _logger_yampi.error("Erro no upsert Supabase: %s", e)
        return {"status": "erro", "motivo": str(e)}


# ── Scheduler (APScheduler) ───────────────────────────────────────────────────

_scheduler = AsyncIOScheduler(timezone="America/Sao_Paulo")

def _job_sync_yampi():
    _logger_yampi.info("⏰ Scheduler: iniciando sync Yampi")
    result = sync_yampi_orders(dias=15)
    _logger_yampi.info("⏰ Scheduler: sync finalizado — %s", result)


@asynccontextmanager
async def lifespan(app):
    # Horário comercial: 8h, 11h, 14h, 17h, 20h (BRT)
    for hora in [8, 11, 14, 17, 20]:
        _scheduler.add_job(_job_sync_yampi, CronTrigger(hour=hora, minute=0, timezone="America/Sao_Paulo"))
    # Madrugada: 1h e 6h (BRT)
    for hora in [1, 6]:
        _scheduler.add_job(_job_sync_yampi, CronTrigger(hour=hora, minute=0, timezone="America/Sao_Paulo"))

    _scheduler.start()
    _logger_yampi.info("🕐 Yampi scheduler iniciado (8h,11h,14h,17h,20h + 1h,6h BRT)")
    yield
    _scheduler.shutdown()


app_servidor_web = FastAPI(lifespan=lifespan)

ALLOWED_ORIGINS = [o.strip() for o in os.getenv("CORS_ALLOW_ORIGINS", "*").split(",")]

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
    Salva a seleção inicial do kit (cores e tamanhos por unidade).
    Crie a tabela com:
        CREATE TABLE kit_selecoes (
            id               BIGSERIAL PRIMARY KEY,
            kit_id           TEXT UNIQUE NOT NULL,
            nome_produto     TEXT,
            quantidade_itens INT,
            detalhes         JSONB,
            data_hora        TIMESTAMPTZ,
            status_pagamento TEXT DEFAULT 'PENDENTE',
            nome_cliente     TEXT,
            email_cliente    TEXT,
            telefone_cliente TEXT,
            cpf_cliente      TEXT,
            payload_yampi    JSONB,
            created_at       TIMESTAMPTZ DEFAULT now()
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


def supa_atualizar_kit_com_yampi(kit_id: str, nome: str | None, email: str | None, telefone: str | None, cpf: str | None, payload_yampi: dict):
    """Atualiza kit_selecoes com dados do cliente após pagamento na Yampi."""
    if not cliente_banco_supabase or not kit_id:
        return
    try:
        update = {
            "status_pagamento": "PAGO",
            "nome_cliente":     nome,
            "email_cliente":    email,
            "telefone_cliente": telefone,
            "cpf_cliente":      cpf,
            "payload_yampi":    payload_yampi,
        }
        cliente_banco_supabase.table("kit_selecoes").update(update).eq("kit_id", kit_id).execute()
    except Exception as e:
        logger_telemetria.error(f"[kit_selecoes] Erro ao atualizar com Yampi: {e}")


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
    cart_data     = data.get("resource", {})
    customer_data = cart_data.get("customer", {})

    nome_real     = customer_data.get("name") or (
        (customer_data.get("first_name") or "") + " " + (customer_data.get("last_name") or "")
    ).strip() or "Cliente Yampi"
    email_real    = customer_data.get("email")
    telefone_real = customer_data.get("phone") or customer_data.get("mobile_phone")
    cpf_real      = customer_data.get("cpf") or customer_data.get("document")

    # Estratégia 1: busca _kit_id nas properties dos itens do pedido
    # (é o cart token do Shopify, gravado como properties[_kit_id] no momento do add-to-cart)
    kit_id = None
    for item in (cart_data.get("items") or cart_data.get("line_items") or []):
        props = item.get("properties") or []
        if isinstance(props, list):
            for p in props:
                if p.get("name") == "_kit_id" and p.get("value"):
                    kit_id = p["value"]
                    break
        elif isinstance(props, dict):
            kit_id = props.get("_kit_id")
        if kit_id:
            break

    # Estratégia 2: fallback por utm_campaign (caso Yampi passe o token via UTM)
    if not kit_id:
        kit_id = (
            cart_data.get("utm_campaign")
            or (cart_data.get("tracking") or {}).get("utm_campaign")
        )

    if kit_id:
        for venda in DB_VENDAS_KITS:
            if venda["kit_id"] == kit_id:
                venda["nome_cliente"]     = nome_real
                venda["email_cliente"]    = email_real
                venda["status_pagamento"] = "PAGO"
                break

        supa_atualizar_kit_com_yampi(kit_id, nome_real, email_real, telefone_real, cpf_real, data)

    return {"status": "recebido", "kit_id_encontrado": kit_id}

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


@app_servidor_web.get("/admin/pedidos", response_class=HTMLResponse)
async def painel_admin_pedidos():
    """Tabela de pedidos com cliente + itens do kit, alimentada pelo Supabase."""
    rows = []
    erro_supabase = None

    if cliente_banco_supabase:
        try:
            res = (
                cliente_banco_supabase
                .table("kit_selecoes")
                .select("*")
                .order("created_at", desc=True)
                .limit(200)
                .execute()
            )
            rows = res.data or []
        except Exception as e:
            erro_supabase = str(e)
    else:
        erro_supabase = "Supabase não configurado (variáveis SUPABASE_URL / SUPABASE_KEY ausentes)."

    def badge_status(s):
        s = (s or "PENDENTE").upper()
        cor = {"PAGO": "#22c55e", "GRÁTIS": "#a855f7"}.get(s, "#f59e0b")
        return f'<span style="background:{cor};color:#fff;padding:2px 10px;border-radius:20px;font-size:11px;font-weight:600;">{s}</span>'

    def itens_html(detalhes):
        if not detalhes:
            return "<em style='color:#999'>—</em>"
        partes = []
        for item in detalhes:
            u = item.get("unidade", "")
            c = item.get("cor", "—")
            t = item.get("tamanho", "—")
            partes.append(
                f'<div style="margin-bottom:4px;">'
                f'<span style="font-size:10px;color:#6b7280;text-transform:uppercase;">{u}</span><br>'
                f'<b>{c}</b> &nbsp;/&nbsp; {t}'
                f'</div>'
            )
        return "".join(partes)

    def fmt_data(d):
        if not d:
            return "—"
        try:
            from datetime import datetime as dt
            return dt.fromisoformat(d.replace("Z", "+00:00")).strftime("%d/%m/%Y %H:%M")
        except Exception:
            return d[:16]

    linhas_html = ""
    if not rows:
        linhas_html = '<tr><td colspan="6" style="text-align:center;padding:40px;color:#999;">Nenhum pedido encontrado.</td></tr>'
    else:
        for r in rows:
            linhas_html += f"""
            <tr>
              <td>{fmt_data(r.get('created_at'))}</td>
              <td>
                <div style="font-weight:600">{r.get('nome_cliente') or '<em style="color:#999">Aguardando</em>'}</div>
                <div style="font-size:12px;color:#6b7280">{r.get('email_cliente') or ''}</div>
                <div style="font-size:12px;color:#6b7280">{r.get('telefone_cliente') or ''}</div>
              </td>
              <td>
                <div style="font-weight:600">{r.get('nome_produto') or '—'}</div>
                <div style="font-size:12px;color:#6b7280">{r.get('quantidade_itens') or 0} unidades</div>
              </td>
              <td>{itens_html(r.get('detalhes'))}</td>
              <td>{badge_status(r.get('status_pagamento'))}</td>
              <td style="font-size:11px;color:#9ca3af;font-family:monospace">{r.get('kit_id','')[:12]}…</td>
            </tr>"""

    aviso_erro = f'<div style="background:#fef2f2;border:1px solid #fca5a5;padding:12px 16px;border-radius:8px;margin-bottom:20px;color:#dc2626;">⚠️ {erro_supabase}</div>' if erro_supabase else ""

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <title>Pedidos de Kit</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; }}
    body {{ font-family: 'Segoe UI', sans-serif; background: #f8fafc; padding: 32px; color: #1e293b; }}
    h1   {{ font-size: 22px; font-weight: 700; margin-bottom: 4px; }}
    p.sub {{ color: #64748b; font-size: 13px; margin-bottom: 24px; }}
    .card {{ background: #fff; border-radius: 12px; box-shadow: 0 1px 4px rgba(0,0,0,.08); overflow: hidden; }}
    table {{ width: 100%; border-collapse: collapse; }}
    thead tr {{ background: #f1f5f9; }}
    th {{ text-align: left; padding: 12px 16px; font-size: 11px; text-transform: uppercase;
          letter-spacing: .06em; color: #64748b; font-weight: 600; white-space: nowrap; }}
    td {{ padding: 14px 16px; border-top: 1px solid #f1f5f9; vertical-align: top; font-size: 13px; }}
    tr:hover td {{ background: #f8fafc; }}
    a.nav {{ display:inline-block; margin-bottom:16px; font-size:13px; color:#3b82f6; text-decoration:none; }}
    a.nav:hover {{ text-decoration:underline; }}
  </style>
</head>
<body>
  <a class="nav" href="/admin/vendas">← Fila de separação</a>
  <h1>Pedidos de Kit</h1>
  <p class="sub">Itens escolhidos pelo cliente, salvos no momento de adicionar ao carrinho.</p>
  {aviso_erro}
  <div class="card">
    <table>
      <thead>
        <tr>
          <th>Data</th>
          <th>Cliente</th>
          <th>Produto</th>
          <th>Itens escolhidos</th>
          <th>Status</th>
          <th>ID</th>
        </tr>
      </thead>
      <tbody>
        {linhas_html}
      </tbody>
    </table>
  </div>
</body>
</html>"""
    return html


app_servidor_web.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

TOKEN_SHOPIFY = os.getenv("TOKEN_SHOPIFY")
NOME_DA_LOJA_SHOPIFY = os.getenv("NOME_DA_LOJA_SHOPIFY")

# Cache do token Shopify com renovação automática a cada 12h
_shopify_token_cache: Dict[str, Any] = {
    "token": TOKEN_SHOPIFY,
    "fetched_at": None,  # None = usa o TOKEN_SHOPIFY do env sem validade
}
_SHOPIFY_TOKEN_TTL_HORAS = 12

async def _get_shopify_token() -> Optional[str]:
    """Retorna um token válido. Renova via client_credentials se expirado (>12h) ou ausente."""
    agora = datetime.utcnow()
    cache = _shopify_token_cache

    # Ainda válido?
    if cache["token"] and cache["fetched_at"]:
        idade = (agora - cache["fetched_at"]).total_seconds() / 3600
        if idade < _SHOPIFY_TOKEN_TTL_HORAS:
            return cache["token"]

    # TOKEN_SHOPIFY do env sem data de busca — usa sem forçar renovação
    if cache["token"] and cache["fetched_at"] is None:
        return cache["token"]

    return await _renovar_token_shopify()

async def _renovar_token_shopify() -> Optional[str]:
    """Busca novo access_token via client_credentials e atualiza o cache."""
    client_id = os.getenv("SHOPIFY_CLIENT_ID")
    client_secret = os.getenv("SHOPIFY_CLIENT_SECRET")
    shop = NOME_DA_LOJA_SHOPIFY

    if not client_id or not client_secret or not shop:
        logger_telemetria.warning("[Token] SHOPIFY_CLIENT_ID/SECRET ou NOME_DA_LOJA_SHOPIFY não configurados.")
        return _shopify_token_cache.get("token")

    url = f"https://{shop}.myshopify.com/admin/oauth/access_token"
    payload = {"client_id": client_id, "client_secret": client_secret, "grant_type": "client_credentials"}

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(url, json=payload)
        dados = resp.json()
        token = dados.get("access_token")
        if token:
            _shopify_token_cache["token"] = token
            _shopify_token_cache["fetched_at"] = datetime.utcnow()
            logger_telemetria.info("[Token] Token Shopify renovado com sucesso.")
            return token
        else:
            logger_telemetria.error(f"[Token] Falha ao renovar token: {dados}")
    except Exception as e:
        logger_telemetria.error(f"[Token] Exceção ao renovar token: {e}")

    return _shopify_token_cache.get("token")
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


@app_servidor_web.post("/admin/sync-yampi")
async def admin_sync_yampi(dias: int = 15):
    """Dispara manualmente a sincronização de pedidos Yampi → Supabase."""
    result = await asyncio.get_event_loop().run_in_executor(None, lambda: sync_yampi_orders(dias=dias))
    return result


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


# ---------------------------
# INVENTÁRIO / ESTOQUE
# ---------------------------

async def _buscar_produtos_shopify_paginado() -> List[dict]:
    """Busca todos os produtos com variants e inventory_quantity via paginação de cursor.
    Renova o token automaticamente em caso de 401 ou lista vazia."""
    for tentativa in range(2):
        token = await _get_shopify_token()
        if not token:
            logger_telemetria.error("[Estoque] Token Shopify indisponível.")
            return []

        headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
        produtos = []
        url = f"https://{NOME_DA_LOJA_SHOPIFY}.myshopify.com/admin/api/2024-01/products.json?limit=250&fields=id,title,variants,status,image"
        erro_401 = False

        async with httpx.AsyncClient(timeout=30.0) as client:
            while url:
                resp = await client.get(url, headers=headers)
                if resp.status_code == 401:
                    logger_telemetria.warning(f"[Estoque] 401 na tentativa {tentativa + 1}. Renovando token...")
                    erro_401 = True
                    break
                if resp.status_code != 200:
                    logger_telemetria.error(f"[Estoque] Erro ao buscar produtos: {resp.status_code} {resp.text}")
                    break
                data = resp.json()
                produtos.extend(data.get("products", []))

                # paginação via Link header
                link_header = resp.headers.get("Link", "")
                next_url = None
                for part in link_header.split(","):
                    if 'rel="next"' in part:
                        next_url = part.strip().split(";")[0].strip().strip("<>")
                        break
                url = next_url

        if erro_401 or (tentativa == 0 and len(produtos) == 0):
            # Força renovação antes da próxima tentativa
            await _renovar_token_shopify()
            continue

        return produtos

    return []


def _montar_estoque_de_produtos(produtos: List[dict]) -> List[dict]:
    """Transforma a lista de produtos em uma view plana de estoque por variant."""
    resultado = []
    for produto in produtos:
        for variant in produto.get("variants", []):
            resultado.append({
                "produto_id": str(produto.get("id")),
                "produto_titulo": produto.get("title"),
                "variant_id": str(variant.get("id")),
                "variant_titulo": variant.get("title"),
                "sku": variant.get("sku"),
                "inventory_item_id": str(variant.get("inventory_item_id")),
                "inventory_quantity": variant.get("inventory_quantity", 0),
                "status_produto": produto.get("status"),
                "imagem_url": (produto.get("image") or {}).get("src"),
            })
    return resultado


@app_servidor_web.get("/api/estoque")
async def consultar_estoque():
    """Retorna o estoque atual de todos os produtos/variants da Shopify."""
    if not NOME_DA_LOJA_SHOPIFY:
        raise HTTPException(status_code=503, detail="NOME_DA_LOJA_SHOPIFY não configurado.")
    token = await _get_shopify_token()
    if not token:
        raise HTTPException(status_code=503, detail="Token Shopify indisponível — verifique SHOPIFY_CLIENT_ID/SECRET.")

    produtos = await _buscar_produtos_shopify_paginado()
    estoque = _montar_estoque_de_produtos(produtos)

    total_skus = len(estoque)
    total_unidades = sum(i["inventory_quantity"] for i in estoque)

    logger_telemetria.info(f"[Estoque] Consulta concluída: {total_skus} SKUs, {total_unidades} unidades totais.")
    return {
        "status": "sucesso",
        "total_skus": total_skus,
        "total_unidades": total_unidades,
        "itens": estoque,
    }


class MovimentarEstoqueRequest(BaseModel):
    inventory_item_id: str
    tipo: str  # 'ajuste' | 'entrada'
    quantidade: int
    observacao: Optional[str] = None

@app_servidor_web.post("/api/estoque/movimentar")
async def movimentar_estoque(body: MovimentarEstoqueRequest):
    """Ajusta (total absoluto) ou dá entrada (adiciona) no estoque de um item Shopify."""
    if not NOME_DA_LOJA_SHOPIFY:
        raise HTTPException(status_code=503, detail="NOME_DA_LOJA_SHOPIFY não configurado.")

    token = await _get_shopify_token()
    if not token:
        raise HTTPException(status_code=503, detail="Token Shopify indisponível.")

    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    base_url = f"https://{NOME_DA_LOJA_SHOPIFY}.myshopify.com/admin/api/2024-01"

    # 1) Busca location_id via inventory_levels do próprio item (não requer read_locations)
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp_loc = await client.get(
            f"{base_url}/inventory_levels.json?inventory_item_ids={body.inventory_item_id}",
            headers=headers,
        )

    if resp_loc.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Erro ao buscar inventory_levels: {resp_loc.text}")

    levels = resp_loc.json().get("inventory_levels", [])
    if not levels:
        raise HTTPException(status_code=502, detail="Nenhuma location encontrada para este item.")

    location_id = levels[0]["location_id"]

    # 2) Chama o endpoint correto da Shopify
    async with httpx.AsyncClient(timeout=15.0) as client:
        if body.tipo == "ajuste":
            resp = await client.post(
                f"{base_url}/inventory_levels/set.json",
                headers=headers,
                json={"location_id": location_id, "inventory_item_id": body.inventory_item_id, "available": body.quantidade},
            )
        else:  # entrada
            resp = await client.post(
                f"{base_url}/inventory_levels/adjust.json",
                headers=headers,
                json={"location_id": location_id, "inventory_item_id": body.inventory_item_id, "available_adjustment": body.quantidade},
            )

    if resp.status_code not in (200, 201):
        raise HTTPException(status_code=resp.status_code, detail=f"Shopify: {resp.text}")

    novo_estoque = resp.json().get("inventory_level", {}).get("available", body.quantidade)
    logger_telemetria.info(f"[Estoque] {body.tipo} item={body.inventory_item_id} qty={body.quantidade} → {novo_estoque}")

    # Salva movimentação no Supabase (usando service key do backend)
    if cliente_banco_supabase:
        try:
            cliente_banco_supabase.table("movimentacoes_estoque").insert({
                "produto_id": body.inventory_item_id,
                "tipo": body.tipo,
                "quantidade": body.quantidade,
                "observacao": body.observacao,
            }).execute()
        except Exception as e:
            logger_telemetria.error(f"[Estoque] Erro ao salvar movimentação no Supabase: {e}")

    return {"novoEstoque": novo_estoque}


@app_servidor_web.post("/webhook-shopify/estoque-atualizado")
async def webhook_shopify_estoque_atualizado(data: Dict[str, Any]):
    """
    Recebe o webhook inventory_levels/update da Shopify.
    Payload esperado: { inventory_item_id, location_id, available, updated_at }
    """
    inventory_item_id = str(data.get("inventory_item_id") or "")
    location_id = str(data.get("location_id") or "")
    available = data.get("available")
    updated_at = data.get("updated_at")

    if not inventory_item_id:
        return {"status": "ignorado", "motivo": "inventory_item_id ausente"}

    logger_telemetria.info(
        f"[Estoque] Webhook recebido — item={inventory_item_id} location={location_id} disponivel={available}"
    )

    if cliente_banco_supabase:
        try:
            row = {
                "inventory_item_id": inventory_item_id,
                "location_id": location_id,
                "available": available,
                "shopify_updated_at": updated_at,
                "payload_shopify": data,
            }
            cliente_banco_supabase.table("estoque_shopify").upsert(
                row, on_conflict="inventory_item_id,location_id"
            ).execute()
            logger_telemetria.info(f"[Estoque] Salvo no Supabase: item={inventory_item_id}")
        except Exception as e:
            logger_telemetria.error(f"[Estoque] Erro ao salvar no Supabase: {e}")

    return {"status": "ok", "inventory_item_id": inventory_item_id, "available": available}


# ---------------------------
# PEDIDO: CANCELAMENTO
# ---------------------------

@app_servidor_web.post("/webhook-shopify/pedido-cancelado")
async def webhook_shopify_pedido_cancelado(data: Dict[str, Any]):
    """Recebe orders/cancelled da Shopify."""
    order_id = str(data.get("id") or "")
    numero = data.get("name")
    cancel_reason = data.get("cancel_reason")

    logger_telemetria.info(f"[Pedido] Cancelado: {numero} (id={order_id}) motivo={cancel_reason}")

    if cliente_banco_supabase and order_id:
        try:
            cliente_banco_supabase.table("vendas").update({
                "financial_status": "CANCELLED",
                "fulfillment_status": "CANCELLED",
                "status_pagamento": "CANCELADO",
            }).eq("shopify_order_id", order_id).execute()
        except Exception as e:
            logger_telemetria.error(f"[Pedido] Erro ao cancelar no Supabase: {e}")

    return {"status": "ok", "pedido": numero, "motivo": cancel_reason}


# ---------------------------
# REEMBOLSO
# ---------------------------

@app_servidor_web.post("/webhook-shopify/reembolso-criado")
async def webhook_shopify_reembolso_criado(data: Dict[str, Any]):
    """Recebe refunds/create da Shopify."""
    order_id = str(data.get("order_id") or "")
    refund_id = str(data.get("id") or "")
    created_at = data.get("created_at")

    logger_telemetria.info(f"[Reembolso] Criado: refund_id={refund_id} order_id={order_id}")

    if cliente_banco_supabase and order_id:
        try:
            cliente_banco_supabase.table("reembolsos").upsert({
                "shopify_refund_id": refund_id,
                "shopify_order_id": order_id,
                "created_at": created_at,
                "payload_shopify": data,
            }, on_conflict="shopify_refund_id").execute()
        except Exception as e:
            logger_telemetria.error(f"[Reembolso] Erro ao salvar no Supabase: {e}")

    return {"status": "ok", "refund_id": refund_id, "order_id": order_id}


# ---------------------------
# PRODUTOS
# ---------------------------

def _extrair_produto(data: Dict[str, Any]) -> dict:
    return {
        "shopify_product_id": str(data.get("id") or ""),
        "titulo": data.get("title"),
        "status": data.get("status"),
        "vendor": data.get("vendor"),
        "product_type": data.get("product_type"),
        "shopify_updated_at": data.get("updated_at"),
        "payload_shopify": data,
    }


@app_servidor_web.post("/webhook-shopify/produto-criado")
async def webhook_shopify_produto_criado(data: Dict[str, Any]):
    """Recebe products/create da Shopify."""
    product_id = str(data.get("id") or "")
    logger_telemetria.info(f"[Produto] Criado: {data.get('title')} (id={product_id})")

    if cliente_banco_supabase and product_id:
        try:
            cliente_banco_supabase.table("produtos_shopify").upsert(
                _extrair_produto(data), on_conflict="shopify_product_id"
            ).execute()
        except Exception as e:
            logger_telemetria.error(f"[Produto] Erro ao salvar no Supabase: {e}")

    return {"status": "ok", "product_id": product_id}


@app_servidor_web.post("/webhook-shopify/produto-atualizado")
async def webhook_shopify_produto_atualizado(data: Dict[str, Any]):
    """Recebe products/update da Shopify."""
    product_id = str(data.get("id") or "")
    logger_telemetria.info(f"[Produto] Atualizado: {data.get('title')} (id={product_id})")

    if cliente_banco_supabase and product_id:
        try:
            cliente_banco_supabase.table("produtos_shopify").upsert(
                _extrair_produto(data), on_conflict="shopify_product_id"
            ).execute()
        except Exception as e:
            logger_telemetria.error(f"[Produto] Erro ao salvar no Supabase: {e}")

    return {"status": "ok", "product_id": product_id}


@app_servidor_web.get("/admin/sincronizar-produtos-estoque")
async def sincronizar_produtos_e_estoque():
    """
    Importação inicial: busca todos os produtos da Shopify, salva em produtos_shopify
    e busca os níveis de estoque, salvando em estoque_shopify.
    Chame este endpoint uma vez para popular as tabelas.
    """
    if not TOKEN_SHOPIFY or not NOME_DA_LOJA_SHOPIFY:
        raise HTTPException(status_code=503, detail="Credenciais Shopify não configuradas.")
    if not cliente_banco_supabase:
        raise HTTPException(status_code=503, detail="Supabase não configurado.")

    headers = {"X-Shopify-Access-Token": TOKEN_SHOPIFY, "Content-Type": "application/json"}

    # 1) Busca todos os produtos e salva em produtos_shopify
    produtos = await _buscar_produtos_shopify_paginado()
    logger_telemetria.info(f"[Sync] {len(produtos)} produtos encontrados na Shopify.")

    rows_produtos = [_extrair_produto(p) for p in produtos if p.get("id")]
    if rows_produtos:
        cliente_banco_supabase.table("produtos_shopify").upsert(
            rows_produtos, on_conflict="shopify_product_id"
        ).execute()
    logger_telemetria.info(f"[Sync] {len(rows_produtos)} produtos salvos em produtos_shopify.")

    # 2) Coleta todos os inventory_item_ids dos variants
    all_item_ids = []
    for produto in produtos:
        for variant in produto.get("variants", []):
            iid = variant.get("inventory_item_id")
            if iid:
                all_item_ids.append(str(iid))

    # 3) Busca inventory_levels em lotes de 50 (limite da API Shopify)
    rows_estoque = []
    async with httpx.AsyncClient(timeout=30.0) as client:
        for i in range(0, len(all_item_ids), 50):
            batch = all_item_ids[i:i + 50]
            ids_param = ",".join(batch)
            url = (
                f"https://{NOME_DA_LOJA_SHOPIFY}.myshopify.com"
                f"/admin/api/2024-01/inventory_levels.json"
                f"?inventory_item_ids={ids_param}&limit=250"
            )
            resp = await client.get(url, headers=headers)
            if resp.status_code != 200:
                logger_telemetria.error(f"[Sync] Erro inventory_levels: {resp.status_code} {resp.text}")
                continue
            levels = resp.json().get("inventory_levels", [])
            for lv in levels:
                rows_estoque.append({
                    "inventory_item_id": str(lv.get("inventory_item_id")),
                    "location_id": str(lv.get("location_id")),
                    "available": lv.get("available"),
                    "shopify_updated_at": lv.get("updated_at"),
                    "payload_shopify": lv,
                })

    if rows_estoque:
        cliente_banco_supabase.table("estoque_shopify").upsert(
            rows_estoque, on_conflict="inventory_item_id,location_id"
        ).execute()
    logger_telemetria.info(f"[Sync] {len(rows_estoque)} registros de estoque salvos em estoque_shopify.")

    return {
        "status": "sucesso",
        "produtos_salvos": len(rows_produtos),
        "estoque_salvo": len(rows_estoque),
    }


@app_servidor_web.post("/webhook-shopify/produto-deletado")
async def webhook_shopify_produto_deletado(data: Dict[str, Any]):
    """Recebe products/delete da Shopify."""
    product_id = str(data.get("id") or "")
    logger_telemetria.info(f"[Produto] Deletado: id={product_id}")

    if cliente_banco_supabase and product_id:
        try:
            cliente_banco_supabase.table("produtos_shopify").delete().eq(
                "shopify_product_id", product_id
            ).execute()
        except Exception as e:
            logger_telemetria.error(f"[Produto] Erro ao deletar no Supabase: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# LLM RELAY — túnel WebSocket para a máquina local com OpenLLM
# ─────────────────────────────────────────────────────────────────────────────

_llm_local_ws: Optional[WebSocket] = None
_llm_pending: dict[str, asyncio.Future] = {}


@app_servidor_web.websocket("/ws/local")
async def llm_local_tunnel(websocket: WebSocket):
    """Máquina local conecta aqui e mantém o túnel aberto."""
    global _llm_local_ws
    await websocket.accept()
    _llm_local_ws = websocket
    logger_telemetria.info("[LLM-tunnel] Máquina local conectada ao relay")
    try:
        while True:
            raw = await websocket.receive_text()
            msg = json.loads(raw)
            req_id = msg.get("request_id")
            logger_telemetria.debug("[LLM-tunnel] Resposta recebida da máquina local | request_id=%s | keys=%s", req_id, list(msg.keys()))
            if req_id and req_id in _llm_pending:
                fut = _llm_pending[req_id]
                if not fut.done():
                    logger_telemetria.debug("[LLM-tunnel] Future resolvida | request_id=%s", req_id)
                    fut.set_result(msg)
            else:
                logger_telemetria.warning("[LLM-tunnel] request_id=%s não encontrado em pending (total pending=%d)", req_id, len(_llm_pending))
    except WebSocketDisconnect:
        _llm_local_ws = None
        logger_telemetria.info("[LLM-tunnel] Máquina local desconectada")


async def _llm_call(method: str, path: str, body: dict | None = None, timeout: float = 90.0) -> dict:
    logger_telemetria.info("[LLM-relay] _llm_call | method=%s path=%s local_conectado=%s pending_count=%d", method, path, _llm_local_ws is not None, len(_llm_pending))

    if _llm_local_ws is None:
        logger_telemetria.error("[LLM-relay] Máquina local NÃO conectada — retornando 503")
        raise HTTPException(503, detail="Máquina local não conectada")

    req_id = str(uuid.uuid4())
    loop = asyncio.get_event_loop()
    fut: asyncio.Future = loop.create_future()
    _llm_pending[req_id] = fut

    payload = {"request_id": req_id, "method": method, "path": path, "body": body or {}}
    logger_telemetria.debug("[LLM-relay] Enviando para local | request_id=%s | body_keys=%s", req_id, list((body or {}).keys()))
    await _llm_local_ws.send_text(json.dumps(payload))

    try:
        result = await asyncio.wait_for(fut, timeout=timeout)
        data = result.get("data", {})
        logger_telemetria.info("[LLM-relay] Resposta recebida | request_id=%s | data_type=%s | data_keys=%s", req_id, type(data).__name__, list(data.keys()) if isinstance(data, dict) else "—")
        return data
    except asyncio.TimeoutError:
        logger_telemetria.error("[LLM-relay] TIMEOUT aguardando local | request_id=%s | timeout=%.1fs", req_id, timeout)
        raise HTTPException(504, detail="Máquina local não respondeu a tempo")
    finally:
        _llm_pending.pop(req_id, None)


# ── Endpoints públicos do LLM ─────────────────────────────────────────────────

@app_servidor_web.get("/llm/health")
async def llm_health():
    return {"relay": "ok", "local_conectado": _llm_local_ws is not None}


@app_servidor_web.get("/llm/status")
async def llm_status():
    return await _llm_call("GET", "/status")


@app_servidor_web.get("/llm/models")
async def llm_list_models():
    return await _llm_call("GET", "/models")


@app_servidor_web.get("/llm/models/active")
async def llm_active_model():
    return await _llm_call("GET", "/models/active")


@app_servidor_web.post("/llm/models/{model_id}/start")
async def llm_start_model(model_id: str, request: Request):
    body = await request.json() if request.headers.get("content-length", "0") != "0" else {}
    return await _llm_call("POST", f"/models/{model_id}/start", body, timeout=120.0)


@app_servidor_web.post("/llm/models/{model_id}/stop")
async def llm_stop_model(model_id: str):
    return await _llm_call("POST", f"/models/{model_id}/stop")


@app_servidor_web.post("/llm/models/switch")
async def llm_switch_model(request: Request):
    body = await request.json()
    return await _llm_call("POST", "/models/switch", body, timeout=120.0)


@app_servidor_web.get("/llm/logs")
async def llm_logs(lines: int = 50):
    return await _llm_call("GET", f"/logs?lines={lines}")


@app_servidor_web.post("/llm/chat/completions")
async def llm_chat_completions(request: Request):
    body = await request.json()
    logger_telemetria.info("[LLM-chat] POST /llm/chat/completions | model=%s | messages_count=%d", body.get("model"), len(body.get("messages", [])))
    result = await _llm_call("POST", "/chat/completions", body, timeout=120.0)
    logger_telemetria.info("[LLM-chat] Resultado | keys=%s", list(result.keys()) if isinstance(result, dict) else type(result).__name__)
    return result


@app_servidor_web.api_route("/llm/v1/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def llm_v1_proxy(path: str, request: Request):
    """
    Proxy transparente para o LiteLLM na máquina local.
    Permite usar ANTHROPIC_BASE_URL=https://webhook-melhor-envio.onrender.com/llm
    no Claude Code ou qualquer app OpenAI-compatible.
    """
    body: dict = {}
    if request.method in ("POST", "PUT"):
        try:
            body = await request.json()
        except Exception:
            body = {}

    logger_telemetria.info("[LLM-v1] %s /llm/v1/%s | model=%s | messages_count=%d | stream=%s",
                           request.method, path, body.get("model"), len(body.get("messages", [])), body.get("stream"))

    # Força max_tokens alto e desativa streaming (relay WebSocket não suporta SSE)
    if request.method == "POST" and isinstance(body, dict):
        body["max_tokens"] = 200000
        body["stream"] = False
        logger_telemetria.debug("[LLM-v1] stream forçado para False, max_tokens=200000")

    result = await _llm_call(request.method, f"/v1/{path}", body, timeout=120.0)

    logger_telemetria.info("[LLM-v1] Resposta | path=%s | result_type=%s | keys=%s",
                           path, type(result).__name__, list(result.keys()) if isinstance(result, dict) else "—")

    # Garante que usage.input_tokens e usage.output_tokens existam
    if isinstance(result, dict):
        usage = result.get("usage") or {}
        if "input_tokens" not in usage or "output_tokens" not in usage:
            usage.setdefault("input_tokens", 0)
            usage.setdefault("output_tokens", 0)
            result["usage"] = usage
            logger_telemetria.debug("[LLM-v1] usage preenchido com zeros: %s", result["usage"])

    if isinstance(result, dict) and "error" in result:
        logger_telemetria.error("[LLM-v1] Erro na resposta: %s", result["error"])

    from fastapi.responses import JSONResponse
    return JSONResponse(result)