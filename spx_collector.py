import json
import os
import time
import logging
import requests
from datetime import datetime, timedelta
import pytz

try:
    from dotenv import load_dotenv
    if load_dotenv(override=False):
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
        logging.info("🔧 Ambiente LOCAL detectado — variáveis carregadas do .env")
except ImportError:
    pass

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# =================================================================
# CONFIGURAÇÃO GERAL
# =================================================================

PROD_OUTBOUND_SPREADSHEET_ID = "1-etBpNtYbvYvpQ5e8NLKxlJXBq5Wp4SmBfhylLY6QP8"
YMS_SPREADSHEET_ID           = "1Ro56eetkC_IS4JUtLium5oA8Ty6XZ5-noQxO44VlYfc"

CONFIG_SHEET_NAME            = "Configuracoes"
PRODUTIVIDADE_SHEET_NAME     = "raw_spx_workstation"
OUTBOUND_SHEET_NAME          = "raw_spx_packing_formated"
OUTBOUND_ORIGINAL_SHEET_NAME = "raw_spx_packing"
YMS_SHEET_NAME               = "yms_ontime"

PRODUCTIVITY_API_URL  = "https://spx.shopee.com.br/api/wfm/admin/workstation/productivity/productivity_individual_list"
OUTBOUND_API_URL      = "https://spx.shopee.com.br/api/wfm/admin/dashboard/list"
LINEHAUL_API_BASE     = "https://spx.shopee.com.br/api/admin/transportation/trip/list_v2"
LINEHAUL_REFERER      = "https://spx.shopee.com.br/#/hubLinehaulTrips/trip"

LINEHAUL_STATION_TYPES = "2,3,7,12,14,16,18"
LINEHAUL_TAB_TYPES     = [1, 2, 3]
LINEHAUL_TAB_LABEL     = {1: "Pending", 2: "Handover", 3: "Ended"}
LINEHAUL_PAGE_SIZE     = 50
LINEHAUL_DISPLAY_DAYS  = 7

FMS_LOGIN_PAGE_URL = (
    "https://fms.business.accounts.shopee.com.br/authenticate/login/"
    "?client_id=25"
    "&next=https%3A%2F%2Fspx.shopee.com.br%2Fapi%2Fadmin%2Fbasicserver%2Fops_tob_login"
    "%3Frefer%3Dhttps%3A%2F%2Fspx.shopee.com.br%2F%23%2F"
)
SPX_LOGIN_API_URL = "https://shopee.com.br/api/v4/account/business/login"
SPX_TOB_LOGIN_URL = (
    "https://spx.shopee.com.br/api/admin/basicserver/ops_tob_login"
    "?refer=https://spx.shopee.com.br/%23/"
)

SPX_DEVICE_FINGERPRINT     = os.environ.get("SPX_DEVICE_FINGERPRINT", "")
EXECUTION_INTERVAL_SECONDS = int(os.environ.get("EXECUTION_INTERVAL_SECONDS", "60"))
TIMEZONE = "America/Sao_Paulo"
SCOPES   = ["https://www.googleapis.com/auth/spreadsheets"]
COLLECTOR_MODE = os.environ.get("COLLECTOR_MODE", "spx")

SPX_USERNAME = os.environ.get("SPX_USERNAME", "")
SPX_PASSWORD = os.environ.get("SPX_PASSWORD", "")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# =================================================================
# SESSÃO HTTP
# =================================================================

def criar_sessao() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Origin":          "https://spx.shopee.com.br",
    })
    return session


def get_cookie_safe(session, name):
    """
    Pega cookie pelo nome sem explodir com CookieConflictError.
    Prefere domínio exato 'spx.shopee.com.br' sobre '.spx.shopee.com.br'.
    """
    for cookie in session.cookies:
        if cookie.name == name and cookie.domain == "spx.shopee.com.br":
            return cookie.value
    for cookie in session.cookies:
        if cookie.name == name and cookie.domain == ".spx.shopee.com.br":
            return cookie.value
    for cookie in session.cookies:
        if cookie.name == name:
            return cookie.value
    return ""


def _md5(texto: str) -> str:
    import hashlib
    return hashlib.md5(texto.encode()).hexdigest()


def fazer_login(session: requests.Session) -> bool:
    if not SPX_USERNAME or not SPX_PASSWORD:
        logging.critical("SPX_USERNAME e/ou SPX_PASSWORD não definidos nos Secrets.")
        return False

    try:
        logging.info("Login — Passo 1: carregando página FMS...")
        session.headers.update({"Origin": "https://fms.business.accounts.shopee.com.br"})
        resp_page = session.get(FMS_LOGIN_PAGE_URL, timeout=30, allow_redirects=True)
        resp_page.raise_for_status()
        logging.info(f"Login — Passo 1 OK. Cookies obtidos: {len(session.cookies)}")

        logging.info("Login — Passo 2: enviando credenciais...")
        payload = {
            "username":                    SPX_USERNAME,
            "password":                    _md5(SPX_PASSWORD),
            "captcha_signature":           "",
            "security_device_fingerprint": SPX_DEVICE_FINGERPRINT,
        }
        login_headers = {
            "Referer":      "https://fms.business.accounts.shopee.com.br/",
            "Origin":       "https://fms.business.accounts.shopee.com.br",
            "Content-Type": "application/json",
            "x-app-type":   "27",
        }
        resp_login = session.post(
            SPX_LOGIN_API_URL,
            json=payload,
            headers=login_headers,
            timeout=30,
            allow_redirects=False,
        )
        resp_login.raise_for_status()

        data = resp_login.json()
        logging.info(f"Login — Passo 2 resposta: error={data.get('error')} | data keys={list(data.get('data', {}).keys())}")

        if data.get("error") != 0:
            logging.error(f"Login falhou. Resposta completa: {data}")
            return False

        token = (
            data.get("data", {}).get("nonce")
            or data.get("data", {}).get("token")
            or data.get("data", {}).get("access_token")
            or ""
        )
        logging.info(f"Login — Passo 3: nonce obtido: {'OK' if token else 'VAZIO'}")

        tob_url = SPX_TOB_LOGIN_URL
        if token:
            tob_url = (
                f"https://spx.shopee.com.br/api/admin/basicserver/ops_tob_login"
                f"?code={token}&refer=https://spx.shopee.com.br/%23/"
            )

        logging.info("Login — Passo 3: finalizando sessão SPX...")
        session.headers.update({"Origin": "https://spx.shopee.com.br"})
        resp_tob = session.get(tob_url, timeout=30, allow_redirects=True)
        resp_tob.raise_for_status()

        # FIX: usa get_cookie_safe para evitar CookieConflictError com cookies duplicados
        csrf    = get_cookie_safe(session, "csrftoken")
        spx_cid = get_cookie_safe(session, "spx_cid")
        spx_uk  = get_cookie_safe(session, "spx_uk")

        if csrf:
            session.headers.update({"x-csrftoken": csrf})

        # FIX: aceita sessão se tiver spx_uk ou spx_cid, mesmo sem csrftoken
        if csrf or spx_cid or spx_uk:
            logging.info("✅ Login SPX completo! Sessão estabelecida.")
            return True

        logging.warning(
            "Login: cookies spx_cid/csrftoken/spx_uk não encontrados, "
            "mas nenhum erro ocorreu. Tentando continuar..."
        )
        return True

    except ConnectionAbortedError:
        raise
    except Exception as exc:
        logging.error(f"Exceção durante login SPX: {exc}", exc_info=True)
        return False


def executar_chamada_api(
    session: requests.Session,
    method: str,
    url: str,
    referer: str,
    payload: dict | None = None
) -> dict | None:
    try:
        # FIX: usa get_cookie_safe para evitar CookieConflictError
        csrf = get_cookie_safe(session, "csrftoken")
        headers = {"Referer": referer}
        if csrf:
            headers["x-csrftoken"] = csrf
            session.headers.update({"x-csrftoken": csrf})

        if method.upper() == "POST":
            resp = session.post(url, json=payload, headers=headers, timeout=30)
        else:
            resp = session.get(url, headers=headers, timeout=30)

        if resp.status_code == 403:
            logging.debug(f"API '{url}' retornou 403 (sem permissão) — ignorado.")
            return None

        resp.raise_for_status()
        json_response = resp.json()

        retcode = json_response.get("retcode")

        if retcode != 0:
            msg = json_response.get("message", "sem mensagem")
            if retcode in (401, 403) or "cookie" in msg.lower() or "login" in msg.lower():
                raise ConnectionAbortedError("Sessão expirada detectada pela API.")
            logging.debug(f"API '{url}' retornou retcode={retcode}: {msg} — ignorado.")
            return None

        return json_response.get("data")

    except ConnectionAbortedError:
        raise
    except Exception as exc:
        if "403" in str(exc) or "401" in str(exc):
            logging.debug(f"API '{url}' sem permissão — ignorado.")
            return None
        logging.error(f"Falha na chamada API '{url}': {exc}")
        return None

# =================================================================
# FUNÇÕES AUXILIARES
# =================================================================

def mapear_status_doca(status_id):
    return {1: "Pending", 2: "Assigned", 3: "Occupied", 4: "Ended", 5: "On Hold"}.get(
        status_id, f"Desconhecido ({status_id})"
    )

def mapear_tipo_chegada(tipo_id):
    return {1: "Line Haul", 7: "First Mile", 3: "Returns"}.get(
        tipo_id, f"ID Desconhecido ({tipo_id})"
    )

def formatar_tempo_de_espera(minutos):
    if not isinstance(minutos, (int, float)) or minutos <= 0:
        return "00:00"
    h, m = int(minutos // 60), int(minutos % 60)
    return f"{h:02d}:{m:02d}"

def calcular_periodos_coleta():
    tz    = pytz.timezone(TIMEZONE)
    agora = datetime.now(tz)
    dia_trabalho = agora if agora.hour >= 6 else agora - timedelta(days=1)
    inicio = dia_trabalho.replace(hour=6, minute=0, second=0, microsecond=0)
    fim    = (agora + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    periodos, hora_iter = [], inicio
    while hora_iter < fim:
        fim_iter = hora_iter + timedelta(hours=1)
        dia_iter = hora_iter if hora_iter.hour >= 6 else hora_iter - timedelta(days=1)
        periodos.append({
            "data_calendario": hora_iter.strftime("%Y-%m-%d"),
            "data_trabalho":   dia_iter.strftime("%Y-%m-%d"),
            "hora_inicio":     hora_iter.strftime("%H:%M"),
            "hora_fim":        fim_iter.strftime("%H:%M"),
            "periodo_str":     f"{hora_iter.hour}-{fim_iter.hour}",
        })
        hora_iter = fim_iter
    logging.info(f"Períodos a coletar: {len(periodos)}")
    return periodos

# =================================================================
# FUNÇÕES DE COLETA
# =================================================================

def coletar_dados_produtividade(session):
    logging.info("--- Coletando Produtividade (Workstation) ---")
    tz = pytz.timezone(TIMEZONE)
    dados_finais = []

    try:
        session.get("https://spx.shopee.com.br/admin/workstation/productivity", timeout=15)
        time.sleep(2)
    except Exception:
        pass

    for periodo in calcular_periodos_coleta():
        start_dt = tz.localize(
            datetime.strptime(f"{periodo['data_calendario']} {periodo['hora_inicio']}", "%Y-%m-%d %H:%M")
        )
        end_dt = tz.localize(
            datetime.strptime(f"{periodo['data_calendario']} {periodo['hora_fim']}", "%Y-%m-%d %H:%M")
        )
        if start_dt.time() > end_dt.time():
            end_dt += timedelta(days=1)

        url = (
            f"{PRODUCTIVITY_API_URL}"
            f"?pageno=1&count=500"
            f"&start_time={int(start_dt.timestamp())}"
            f"&end_time={int(end_dt.timestamp())}"
            f"&activity_type=12"
        )

        data = None
        for tentativa in range(1, 3):
            data = executar_chamada_api(
                session, "GET", url,
                "https://spx.shopee.com.br/admin/workstation/productivity"
            )
            if data is not None:
                break
            logging.warning(f"Produtividade {periodo['periodo_str']} tentativa {tentativa}/2 — aguardando 5s...")
            time.sleep(5)

        if data and data.get("list"):
            for item in data["list"]:
                ops_id, ops_name = "", ""
                if item.get("ops"):
                    parts = item["ops"].split("]")
                    if len(parts) > 1:
                        ops_id   = parts[0].replace("[", "").strip()
                        ops_name = parts[1].strip()
                dados_finais.append([
                    ops_id, ops_name,
                    item.get("workstation", ""),
                    item.get("activity_type", ""),
                    item.get("working_hours", 0),
                    item.get("total_throughput", 0),
                    item.get("check_in_time", ""),
                    item.get("check_out_time", ""),
                    "", "",
                    periodo["periodo_str"].split("-")[0],
                    periodo["data_trabalho"],
                ])
    return dados_finais


def coletar_dados_outbound(session):
    logging.info("--- Coletando Outbound (Packing) ---")
    payload = {
        "unit_type": 1, "process_type": 2, "period_type": 1,
        "pageno": 1, "count": 500, "productivity": 1,
        "order_by_total": 100, "event_id_list": [],
    }

    MAX_TENTATIVAS = 3
    ESPERA = 10

    for tentativa in range(1, MAX_TENTATIVAS + 1):
        data = executar_chamada_api(
            session, "POST", OUTBOUND_API_URL,
            "https://spx.shopee.com.br/dashboard/overview",
            payload
        )

        if not data:
            logging.warning(f"Outbound tentativa {tentativa}/{MAX_TENTATIVAS}: API retornou None.")
        else:
            efficiency_list = data.get("efficiency_list", [])
            if efficiency_list:
                hora_atual = datetime.now(pytz.timezone(TIMEZONE)).hour
                originais, formatados = [], []
                for item in efficiency_list:
                    eff    = item.get("efficiency", [])
                    padded = eff + [0] * (12 - len(eff))
                    originais.append([item.get("operator", ""), item.get("efficiency_total", 0)] + padded)
                    for i in range(12):
                        hora_eff = (hora_atual - i + 24) % 24
                        formatados.append([item.get("operator", ""), item.get("efficiency_total", 0), hora_eff, padded[i]])
                logging.info(f"Sucesso! {len(originais)} registros de Outbound na tentativa {tentativa}.")
                return originais, formatados
            else:
                logging.warning(f"Outbound tentativa {tentativa}/{MAX_TENTATIVAS}: efficiency_list vazio.")

        if tentativa < MAX_TENTATIVAS:
            logging.info(f"Aguardando {ESPERA}s antes de tentar novamente...")
            time.sleep(ESPERA)

    logging.warning("Outbound: todas as tentativas falharam — mantendo dados anteriores no Sheets.")
    return [], []


def calcular_display_range():
    tz    = pytz.timezone(TIMEZONE)
    agora = datetime.now(tz)
    fim   = agora.replace(hour=23, minute=59, second=59, microsecond=0)
    ini   = (agora - timedelta(days=LINEHAUL_DISPLAY_DAYS)).replace(
                hour=0, minute=0, second=0, microsecond=0)
    return f"{int(ini.timestamp())},{int(fim.timestamp())}"


def ts_to_str(ts):
    if not ts or ts == 0:
        return "-"
    try:
        ts = int(ts)
        if ts > 1e12:
            ts = ts // 1000
        tz = pytz.timezone(TIMEZONE)
        return datetime.fromtimestamp(ts, tz=tz).strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return str(ts)


def safe(val, default="-"):
    if val is None or val == "":
        return default
    return val


STATUS_MAP = {
    1: "Pending", 2: "Assigned", 3: "Departed",
    4: "Arrived", 5: "Completed", 6: "Cancelled",
    7: "Loading", 8: "Loaded",
}
ON_TIME_MAP = {
    0: "-",            1: "On Time",
    2: "Late Arrival", 3: "Early Arrival",
    4: "Late Departure", 5: "Early Departure",
}
CIOT_STATUS_MAP = {1: "Created", 2: "Pending Create"}
TOLL_STATUS_MAP = {1: "Fail", 2: "NA", 3: "Paid", 4: "Pending Payment"}
MDFE_STATUS_MAP = {
    1: "Cancel",          2: "Closed",
    3: "Created",         4: "Failed Creation",
    5: "No MDFe issued",  6: "Waiting tax document",
    7: "Webhook Failure",
}


def processar_trip(t, tab_label):
    try:
        trip_stations = t.get("trip_station") or []
        trip_stations_sorted = sorted(trip_stations, key=lambda x: x.get("sequence_number", 0))

        origem = next(
            (s for s in trip_stations_sorted if s.get("station_operation_type") == 0),
            trip_stations_sorted[0] if trip_stations_sorted else None
        )
        destino = next(
            (s for s in reversed(trip_stations_sorted) if s.get("station_operation_type") == 1),
            trip_stations_sorted[-1] if len(trip_stations_sorted) > 1 else None
        )

        def ts_orig(campo): return ts_to_str(origem.get(campo) if origem else None)
        def ts_dest(campo): return ts_to_str(destino.get(campo) if destino else None)

        std = ts_orig("std")
        sta = ts_dest("sta") if (destino and destino.get("sta")) else ts_orig("sta")

        SIMOES_FILHO_ID = 8808
        estacao_simoes = next(
            (s for s in trip_stations_sorted if s.get("station") == SIMOES_FILHO_ID),
            None
        )

        if estacao_simoes:
            ata = ts_to_str(estacao_simoes.get("ata", 0))
            atd = ts_to_str(estacao_simoes.get("atd", 0))
        else:
            ata_orig_val = origem.get("ata",  0) if origem  else 0
            ata_dest_val = destino.get("ata", 0) if destino else 0
            atd_orig_val = origem.get("atd",  0) if origem  else 0
            atd_dest_val = destino.get("atd", 0) if destino else 0
            ata = ts_to_str(ata_dest_val if ata_dest_val else ata_orig_val)
            atd = ts_to_str(atd_dest_val if atd_dest_val else atd_orig_val)

        eta = ts_dest("eta")
        etd = ts_orig("etd")

        loading_time = ts_orig("loading_time")
        seal_time    = ts_orig("seal_time")
        load_qty     = sum(s.get("load_quantity",   0) for s in trip_stations)
        unload_qty   = sum(s.get("unload_quantity", 0) for s in trip_stations)

        stations = t.get("station_list") or t.get("stations") or []
        if stations:
            station_str = " → ".join(s.get("name") or s.get("station_name") or "?" for s in stations)
        elif trip_stations:
            station_str = " → ".join(
                s.get("station_name") or "?"
                for s in sorted(trip_stations, key=lambda x: x.get("sequence_number", 0))
            )
        else:
            station_str = safe(t.get("station") or t.get("station_name"))

        on_time_raw = next(
            (s.get("on_time_indicator_value") for s in trip_stations
             if s.get("on_time_indicator_value")), 0
        )
        on_time_str = ON_TIME_MAP.get(on_time_raw, str(on_time_raw) if on_time_raw else "-")

        veh_plate = t.get("vehicle_number") or t.get("plate_number") or "-"
        if isinstance(veh_plate, list):
            veh_plate = ", ".join(veh_plate)

        status_str = STATUS_MAP.get(t.get("trip_status") or t.get("status"), str(safe(t.get("trip_status"))))
        ciot_str   = CIOT_STATUS_MAP.get(t.get("ciot_status"), str(safe(t.get("ciot_status"))))
        toll_str   = TOLL_STATUS_MAP.get(t.get("toll_status"), str(safe(t.get("toll_status"))))
        mdfe_str   = MDFE_STATUS_MAP.get(t.get("mdfe_status"), str(safe(t.get("mdfe_status"))))

        return [
            tab_label,
            safe(t.get("trip_number")   or t.get("lh_trip_number")),
            safe(t.get("trip_name")     or t.get("lh_trip_name")),
            status_str, station_str,
            ts_to_str(t.get("last_location_update_time")),
            on_time_str, safe(t.get("vehicle_type")),
            f"{sta} / {std}", f"{ata} / {atd}", f"{eta} / {etd}",
            loading_time, seal_time, load_qty, unload_qty,
            veh_plate,
            safe(t.get("driver_name")        or t.get("driver")),
            safe(t.get("second_driver_name") or t.get("second_driver")),
            ciot_str, safe(t.get("ciot_err") or t.get("ciot_error")),
            toll_str, safe(t.get("toll_err") or t.get("toll_error")),
            mdfe_str,
            safe(t.get("trip_source")), safe(t.get("trip_type")),
            safe(t.get("cost_type")),
            safe(t.get("agency_name") or t.get("agency")),
            ts_to_str(t.get("mtime") or t.get("update_time")),
            safe(t.get("operator")),
            ts_to_str(t.get("assigned_time") or t.get("assign_time")),
            safe(t.get("to_inbound_quantity"),    0),
            safe(t.get("order_inbound_quantity"), 0),
            safe(t.get("pack_type")),
            safe(t.get("order_packed_quantity"),  0),
            safe(t.get("to_packed_quantity"),     0),
            safe(t.get("to_loaded_quantity"),     0),
            safe(t.get("order_loaded_quantity"),  0),
        ]
    except Exception as e:
        logging.warning(f"Erro ao processar trip {t.get('trip_number','?')}: {e}")
        return None


def executar_chamada_linehaul(session, url):
    """Chamada GET específica para LineHaul — usa get_cookie_safe para evitar CookieConflictError."""
    try:
        # FIX: get_cookie_safe no lugar de session.cookies.get() que explode com duplicatas
        csrf = get_cookie_safe(session, "csrftoken")
        headers = {
            "Referer":     "https://spx.shopee.com.br/",
            "x-csrftoken": csrf,
        }
        resp = session.get(url, headers=headers, timeout=30)

        if resp.status_code == 403:
            logging.debug("LineHaul 403 — ignorado.")
            return None

        resp.raise_for_status()
        json_response = resp.json()


        retcode = json_response.get("retcode", json_response.get("code", -1))

        if retcode != 0:
            msg = json_response.get("message", "")
            if "cookie" in msg.lower() or "login" in msg.lower():
                raise ConnectionAbortedError("Sessão expirada.")
            logging.debug(f"LineHaul retcode={retcode}: {msg}")
            return None

        return json_response.get("data", json_response)

    except ConnectionAbortedError:
        raise
    except Exception as exc:
        logging.error(f"Falha LineHaul [{url[:80]}]: {exc}")
        return None


def coletar_linehaul_trips(session):
    logging.info("--- Coletando LineHaul Trips ---")

    try:
        session.get("https://spx.shopee.com.br/", timeout=15)
        time.sleep(1)
        session.get("https://spx.shopee.com.br/admin/transportation/trip", timeout=15)
        time.sleep(1)
    except Exception:
        pass

    display_range = calcular_display_range()
    todas = []

    for tab in LINEHAUL_TAB_TYPES:
        label     = LINEHAUL_TAB_LABEL[tab]
        pageno    = 1
        coletados = 0
        logging.info(f"  [{label}]")
        time.sleep(3)

        while True:
            url = (
                f"{LINEHAUL_API_BASE}"
                f"?station_type={LINEHAUL_STATION_TYPES}"
                f"&pageno={pageno}&count={LINEHAUL_PAGE_SIZE}"
                f"&query_type=1&tab_type={tab}"
                f"&display_range={display_range}"
            )

            data = None
            for tentativa in range(1, 4):
                data = executar_chamada_linehaul(session, url)
                if data is not None:
                    break
                logging.warning(f"  [{label}] p.{pageno} tentativa {tentativa}/3: None — aguardando 10s...")
                time.sleep(10)

            if not data:
                logging.warning(f"  [{label}] p.{pageno}: todas as tentativas falharam.")
                break

            lista     = data.get("list") or data.get("trip_list") or data.get("trips") or []
            total_api = int(data.get("total", data.get("count", 0)))

            if not lista:
                logging.warning(f"  [{label}] p.{pageno} lista vazia. Chaves: {list(data.keys())}")
                break

            logging.info(f"    p.{pageno}: {len(lista)} registros (total={total_api})")

            for item in lista:
                row = processar_trip(item, label)
                if row:
                    todas.append(row)

            coletados += len(lista)
            if coletados >= total_api or len(lista) < LINEHAUL_PAGE_SIZE:
                logging.info(f"    [{label}] concluído: {coletados}/{total_api}")
                break

            pageno += 1
            time.sleep(5)

    logging.info(f"LineHaul TOTAL antes dedup: {len(todas)} registros.")

    STATUS_PRIORIDADE = {"Ended": 3, "Handover": 2, "Pending": 1}
    dedup = {}

    for row in todas:
        trip_num   = row[1]
        tab        = row[0]
        prioridade = STATUS_PRIORIDADE.get(tab, 0)

        if trip_num not in dedup:
            dedup[trip_num] = (prioridade, row)
        else:
            prioridade_atual, row_atual = dedup[trip_num]
            campos_preenchidos_novo  = sum(1 for c in row     if c and c != "-")
            campos_preenchidos_atual = sum(1 for c in row_atual if c and c != "-")
            if prioridade > prioridade_atual or (
                prioridade == prioridade_atual and
                campos_preenchidos_novo > campos_preenchidos_atual
            ):
                dedup[trip_num] = (prioridade, row)

    todas_dedup = [v[1] for v in dedup.values()]
    logging.info(f"LineHaul TOTAL após dedup: {len(todas_dedup)} registros.")
    return todas_dedup

# =================================================================
# GOOGLE SHEETS
# =================================================================

def get_sheets_service():
    creds = None
    token_json = os.environ.get("GOOGLE_TOKEN_JSON", "")
    if token_json:
        import tempfile
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        tmp.write(token_json)
        tmp.close()
        creds = Credentials.from_authorized_user_file(tmp.name, SCOPES)
    elif os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as f:
            f.write(creds.to_json())

    return build("sheets", "v4", credentials=creds)


def write_to_sheet(service, spreadsheet_id, sheet_name, data):
    try:
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'"
        ).execute()
        if data and any(data):
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"'{sheet_name}'!A1",
                valueInputOption="USER_ENTERED",
                body={"values": data}
            ).execute()
            logging.info(f"✅ {len(data)} linhas escritas em '{sheet_name}'.")
        else:
            logging.warning(f"Nenhum dado para '{sheet_name}' — aba limpa.")
    except HttpError as err:
        logging.error(f"Erro ao escrever em '{sheet_name}': {err}")


def append_timestamp(service, spreadsheet_id, sheet_name, ts):
    try:
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'!A:B",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [["Última Atualização:", ts]]}
        ).execute()
    except HttpError as err:
        logging.error(f"Erro ao adicionar timestamp em '{sheet_name}': {err}")


def salvar_configs_sessao(session: requests.Session, service, spreadsheet_id, sheet_name):
    logging.info(f"--- Salvando configs de sessão em '{sheet_name}' ---")
    try:
        cookies_str = "; ".join(f"{c.name}={c.value}" for c in session.cookies)
        csrf  = get_cookie_safe(session, "csrftoken")
        dados = [
            ["Chave de Configuração", "Valor"],
            ["Data/Hora da Extração", datetime.now(pytz.timezone(TIMEZONE)).strftime("%Y-%m-%d %H:%M:%S")],
            ["Cookie", cookies_str],
            ["x-csrftoken", csrf or "N/A"],
            ["User-Agent", session.headers.get("User-Agent", "N/A")],
        ]
        write_to_sheet(service, spreadsheet_id, sheet_name, dados)
    except Exception as exc:
        logging.error(f"Erro ao salvar configs de sessão: {exc}")

# =================================================================
# ORQUESTRADOR PRINCIPAL
# =================================================================

def main():
    try:
        sheets_service = get_sheets_service()
    except Exception as exc:
        logging.critical(f"Falha ao autenticar Google Sheets: {exc}")
        return

    logging.info(f"🚀 Iniciando em modo: {COLLECTOR_MODE.upper()} | Intervalo: {EXECUTION_INTERVAL_SECONDS}s")

    MAX_RETRIES_LOGIN = 5
    session = None
    ultimo_ciclo_prod = None
    INTERVALO_PROD    = 600

    while True:
        if session is None:
            for tentativa in range(1, MAX_RETRIES_LOGIN + 1):
                logging.info(f"Tentativa de login {tentativa}/{MAX_RETRIES_LOGIN}…")
                session = criar_sessao()
                if fazer_login(session):
                    break
                session = None
                time.sleep(10 * tentativa)
            else:
                logging.critical("Todas as tentativas de login falharam. Abortando.")
                return

        logging.info("### INICIANDO NOVO CICLO ###")
        try:
            ts    = datetime.now(pytz.timezone(TIMEZONE)).strftime("%Y-%m-%d %H:%M:%S")
            agora = time.time()

            if COLLECTOR_MODE == "spx":
                salvar_configs_sessao(session, sheets_service, PROD_OUTBOUND_SPREADSHEET_ID, CONFIG_SHEET_NAME)

                if ultimo_ciclo_prod is None or (agora - ultimo_ciclo_prod) >= INTERVALO_PROD:
                    header_prod = [
                        "ID do Operador", "Nome do Operador", "Estação de Trabalho",
                        "Tipo de Atividade", "Horas Trabalhadas", "QUANTO O COLABORADOR FEZ",
                        "Check-in", "Check-out", "Vazia 1", "Vazia 2", "Hora", "Data",
                    ]
                    dados_prod = coletar_dados_produtividade(session)
                    write_to_sheet(sheets_service, PROD_OUTBOUND_SPREADSHEET_ID, PRODUTIVIDADE_SHEET_NAME, [header_prod] + dados_prod)
                    if dados_prod:
                        append_timestamp(sheets_service, PROD_OUTBOUND_SPREADSHEET_ID, PRODUTIVIDADE_SHEET_NAME, ts)
                    ultimo_ciclo_prod = agora
                    logging.info("Produtividade atualizada. Próxima em 10 minutos.")
                else:
                    restante = int(INTERVALO_PROD - (agora - ultimo_ciclo_prod))
                    logging.info(f"Produtividade: aguardando {restante}s para próxima atualização.")

                originais, formatados = coletar_dados_outbound(session)

                header_orig = ["Operador", "Total", "H-0","H-1","H-2","H-3","H-4","H-5","H-6","H-7","H-8","H-9","H-10","H-11"]
                if originais:
                    write_to_sheet(sheets_service, PROD_OUTBOUND_SPREADSHEET_ID, OUTBOUND_ORIGINAL_SHEET_NAME, [header_orig] + originais)
                    append_timestamp(sheets_service, PROD_OUTBOUND_SPREADSHEET_ID, OUTBOUND_ORIGINAL_SHEET_NAME, ts)
                else:
                    logging.warning("Outbound original vazio — mantendo dados anteriores.")

                header_fmt = ["Operador", "Total", "Hora", "Eficiência"]
                if formatados:
                    write_to_sheet(sheets_service, PROD_OUTBOUND_SPREADSHEET_ID, OUTBOUND_SHEET_NAME, [header_fmt] + formatados)
                    append_timestamp(sheets_service, PROD_OUTBOUND_SPREADSHEET_ID, OUTBOUND_SHEET_NAME, ts)
                else:
                    logging.warning("Outbound formatado vazio — mantendo dados anteriores.")

            elif COLLECTOR_MODE == "linehaul":
                header_yms = [
                    "Tab", "LH Trip Number", "LH Trip Name", "Status",
                    "Station (Origem → Destino)", "Last Location Update Time",
                    "On Time Indicator", "Vehicle Type",
                    "STA / STD", "ATA / ATD", "ETA / ETD",
                    "Loading Time", "Seal Time",
                    "Inbound Qty", "Outbound Qty",
                    "Vehicle Plate Number", "Driver", "Second Driver",
                    "CIOT Status", "CIOT Error",
                    "Toll Status", "Toll Error",
                    "MDFe Status", "Trip Source", "Trip Type", "Cost Type",
                    "Agency", "Time Update", "Operator", "Assign Time",
                    "Pending Inbound TO", "Pending Inbound Order",
                    "Pending Inbound TO Pack Type",
                    "Order Packed", "TO Packed", "TO Loaded", "Order Loaded",
                ]
                dados_yms = coletar_linehaul_trips(session)
                if dados_yms:
                    write_to_sheet(sheets_service, YMS_SPREADSHEET_ID, YMS_SHEET_NAME, [header_yms] + dados_yms)
                    append_timestamp(sheets_service, YMS_SPREADSHEET_ID, YMS_SHEET_NAME, ts)
                else:
                    logging.warning("LineHaul vazio — mantendo dados anteriores.")

            else:
                logging.error(f"COLLECTOR_MODE inválido: '{COLLECTOR_MODE}'. Use 'spx' ou 'linehaul'.")

        except ConnectionAbortedError:
            logging.warning("Sessão expirada — forçando novo login no próximo ciclo.")
            session = None
            continue

        except Exception as exc:
            logging.error(f"Erro inesperado no ciclo: {exc}", exc_info=True)

        logging.info(f"### CICLO CONCLUÍDO. Aguardando {EXECUTION_INTERVAL_SECONDS}s… ###")
        time.sleep(EXECUTION_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()