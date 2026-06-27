"""
renovar_cookies.py
==================
Script de renovação semi-automática dos cookies SPX.

Como usar:
1. Execute: python renovar_cookies.py
2. O Chrome abrirá automaticamente
3. Faça login no SPX com sua conta Google @shopee.com normalmente
4. O script detecta os cookies automaticamente e atualiza o GitHub
5. O collector volta a funcionar no próximo ciclo

Requisitos:
    pip install selenium webdriver-manager requests python-dotenv
"""

import os
import sys
import time
import json
import base64
import requests
import logging

try:
    from dotenv import load_dotenv
    load_dotenv(override=False)
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# =================================================================
# CONFIGURAÇÃO
# =================================================================

GITHUB_OWNER      = "iagofroes"
GITHUB_REPO       = "spx-collector"
GITHUB_TOKEN      = os.environ.get("GH_PAT", "")
SEATALK_WEBHOOK   = "https://openapi.seatalk.io/webhook/group/ff7KsJx3QeKIJ1oEhNdKWw"
SPX_URL           = "https://spx.shopee.com.br/#/"
COOKIES_ESPERADOS = ["spx_uk", "spx_cid", "spx_st", "spx_uid"]
TIMEOUT_LOGIN     = 120  # segundos para aguardar login


def enviar_seatalk(mensagem: str):
    try:
        requests.post(
            SEATALK_WEBHOOK,
            json={"tag": "text", "text": {"content": mensagem}},
            timeout=10
        )
    except Exception as e:
        logging.warning(f"Falha ao enviar SeaTalk: {e}")


def atualizar_secret_github(nome: str, valor: str) -> bool:
    """Atualiza uma Secret no GitHub via API."""
    if not GITHUB_TOKEN:
        logging.error("GH_PAT não definido no .env")
        return False

    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    # Pega a chave pública do repositório para criptografar a secret
    url_key = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/actions/secrets/public-key"
    resp = requests.get(url_key, headers=headers, timeout=15)
    if resp.status_code != 200:
        logging.error(f"Falha ao obter chave pública GitHub: {resp.status_code} {resp.text}")
        return False

    key_data = resp.json()
    key_id   = key_data["key_id"]
    pub_key  = key_data["key"]

    # Criptografa o valor com a chave pública (libsodium)
    try:
        from nacl import encoding, public
        public_key = public.PublicKey(pub_key.encode("utf-8"), encoding.Base64Encoder())
        sealed_box = public.SealedBox(public_key)
        encrypted  = base64.b64encode(sealed_box.encrypt(valor.encode("utf-8"))).decode("utf-8")
    except ImportError:
        logging.error("Instale PyNaCl: pip install PyNaCl")
        return False

    # Atualiza a secret
    url_secret = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/actions/secrets/{nome}"
    resp2 = requests.put(
        url_secret,
        headers=headers,
        json={"encrypted_value": encrypted, "key_id": key_id},
        timeout=15
    )

    if resp2.status_code in (201, 204):
        logging.info(f"✅ Secret {nome} atualizada no GitHub.")
        return True
    else:
        logging.error(f"Falha ao atualizar {nome}: {resp2.status_code} {resp2.text}")
        return False


def disparar_workflow_github() -> bool:
    """Dispara o workflow SPX Collector manualmente."""
    if not GITHUB_TOKEN:
        return False

    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/actions/workflows/spx_collector.yml/dispatches"
    resp = requests.post(url, headers=headers, json={"ref": "main"}, timeout=15)

    if resp.status_code == 204:
        logging.info("✅ Workflow SPX Collector disparado.")
        return True
    else:
        logging.warning(f"Falha ao disparar workflow: {resp.status_code}")
        return False


def extrair_cookies_spx() -> dict | None:
    """Abre o Chrome, aguarda login e extrai os cookies SPX."""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.chrome.options import Options
        from webdriver_manager.chrome import ChromeDriverManager
    except ImportError:
        logging.error("Instale as dependências: pip install selenium webdriver-manager")
        return None

    logging.info("🌐 Abrindo Chrome... Faça login no SPX com sua conta Google.")

    options = Options()
    options.add_argument("--start-maximized")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--disable-blink-features=AutomationControlled")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    try:
        driver.get(SPX_URL)
        logging.info(f"⏳ Aguardando login... (timeout: {TIMEOUT_LOGIN}s)")
        logging.info("👉 Faça login com sua conta Google @shopee.com normalmente.")

        inicio = time.time()
        cookies_encontrados = {}

        while time.time() - inicio < TIMEOUT_LOGIN:
            cookies_raw = driver.get_cookies()
            cookies_dict = {c["name"]: c["value"] for c in cookies_raw}

            encontrados = {k: cookies_dict[k] for k in COOKIES_ESPERADOS if k in cookies_dict}

            if len(encontrados) >= 3:
                logging.info(f"✅ Cookies detectados: {list(encontrados.keys())}")
                cookies_encontrados = encontrados
                break

            time.sleep(2)

        if not cookies_encontrados:
            logging.error("❌ Timeout — cookies não encontrados. Tente novamente.")
            return None

        return cookies_encontrados

    finally:
        time.sleep(3)
        driver.quit()


def main():
    logging.info("=" * 50)
    logging.info("🔄 Renovador de Cookies SPX")
    logging.info("=" * 50)

    if not GITHUB_TOKEN:
        logging.error("❌ GH_PAT não encontrado no .env. Adicione: GH_PAT=seu_token")
        sys.exit(1)

    # Extrai cookies do browser
    cookies = extrair_cookies_spx()
    if not cookies:
        enviar_seatalk("❌ *Renovação de cookies falhou* — cookies não detectados. Tente novamente.")
        sys.exit(1)

    logging.info("📤 Atualizando Secrets no GitHub...")

    mapa_secrets = {
        "SPX_UK":  cookies.get("spx_uk", ""),
        "SPX_CID": cookies.get("spx_cid", "BR"),
        "SPX_ST":  cookies.get("spx_st",  "1"),
        "SPX_UID": cookies.get("spx_uid", ""),
    }

    sucesso = True
    for nome, valor in mapa_secrets.items():
        if valor:
            ok = atualizar_secret_github(nome, valor)
            if not ok:
                sucesso = False
        else:
            logging.warning(f"⚠️ {nome} vazio — pulando.")

    if sucesso:
        logging.info("🚀 Disparando workflow no GitHub Actions...")
        disparar_workflow_github()

        enviar_seatalk(
            "✅ *Cookies SPX renovados com sucesso!*\n"
            "🔄 Workflow SPX Collector reiniciado automaticamente.\n"
            f"🔑 spx_uk: {mapa_secrets.get('SPX_UK', '')[:15]}..."
        )
        logging.info("✅ Tudo pronto! O collector vai voltar no próximo ciclo.")
    else:
        enviar_seatalk("⚠️ *Renovação parcialmente falhou* — verifique os logs.")
        logging.error("❌ Algumas secrets não foram atualizadas.")


if __name__ == "__main__":
    main()