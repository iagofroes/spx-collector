import os
import time
import requests

from base64 import b64encode
from urllib.parse import quote

from nacl import encoding, public

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ============================================================
# FUNÇÕES PARA ATUALIZAR OS SECRETS NO GITHUB
# ============================================================

def encrypt(public_key: str, secret_value: str) -> str:
    """
    Criptografa o secret usando a chave pública
    do Environment do GitHub via Libsodium (PyNaCl).
    """

    public_bytes = public.PublicKey(
        public_key.encode("utf-8"),
        encoding.Base64Encoder()
    )

    sealed_box = public.SealedBox(public_bytes)

    encrypted = sealed_box.encrypt(
        secret_value.encode("utf-8")
    )

    return b64encode(encrypted).decode("utf-8")


def mudar_secret_ambiente_github(
    owner: str,
    repo: str,
    environment: str,
    secret_name: str,
    secret_value: str,
    token: str
):
    """
    Cria ou atualiza um secret dentro de um Environment
    específico no GitHub.
    """

    # Não envia secret vazio
    if not secret_value:
        print(
            f"⚠️ Ignorando atualização de '{secret_name}' "
            "porque o valor extraído está vazio."
        )
        return False

    if not token:
        print("❌ GITHUB_PAT não foi informado.")
        return False

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28"
    }

    # Protege nomes com caracteres especiais
    environment_encoded = quote(environment, safe="")
    secret_name_encoded = quote(secret_name, safe="")

    try:

        # --------------------------------------------------------
        # 1. BUSCAR A CHAVE PÚBLICA DO ENVIRONMENT
        # --------------------------------------------------------

        url_key = (
            f"https://api.github.com/repos/"
            f"{owner}/{repo}/environments/"
            f"{environment_encoded}/secrets/public-key"
        )

        print(
            f"🔑 Buscando chave pública do ambiente "
            f"'{environment}'..."
        )

        key_response = requests.get(
            url_key,
            headers=headers,
            timeout=30
        )

        key_response.raise_for_status()

        key_data = key_response.json()

        public_key = key_data.get("key")
        key_id = key_data.get("key_id")

        if not public_key or not key_id:
            raise Exception(
                "A resposta do GitHub não contém 'key' ou 'key_id'."
            )

        print("✅ Chave pública obtida com sucesso.")

        # --------------------------------------------------------
        # 2. CRIPTOGRAFAR O VALOR
        # --------------------------------------------------------

        encrypted_value = encrypt(
            public_key,
            secret_value
        )

        # --------------------------------------------------------
        # 3. ENVIAR O SECRET PARA O ENVIRONMENT
        # --------------------------------------------------------

        url_secret = (
            f"https://api.github.com/repos/"
            f"{owner}/{repo}/environments/"
            f"{environment_encoded}/secrets/"
            f"{secret_name_encoded}"
        )

        payload = {
            "encrypted_value": encrypted_value,
            "key_id": key_id
        }

        print(
            f"🔐 Enviando secret '{secret_name}' "
            f"para o GitHub..."
        )

        put_response = requests.put(
            url_secret,
            json=payload,
            headers=headers,
            timeout=30
        )

        # 201 = criado
        # 204 = atualizado
        if put_response.status_code in (201, 204):

            print(
                f"✅ Secret '{secret_name}' atualizado "
                f"com sucesso no ambiente '{environment}'!"
            )

            return True

        else:

            print(
                f"❌ Erro ao atualizar secret "
                f"'{secret_name}': "
                f"{put_response.status_code} - "
                f"{put_response.text}"
            )

            return False

    except requests.exceptions.HTTPError as api_err:

        print(
            f"❌ Erro HTTP na API do GitHub "
            f"para o secret '{secret_name}': "
            f"{api_err}"
        )

        if 'key_response' in locals():
            print(
                f"Resposta do GitHub: "
                f"{key_response.text}"
            )

        return False

    except requests.exceptions.RequestException as api_err:

        print(
            f"❌ Erro de comunicação com a API do GitHub "
            f"para o secret '{secret_name}': "
            f"{api_err}"
        )

        return False

    except Exception as api_err:

        print(
            f"❌ Falha ao processar o secret "
            f"'{secret_name}': "
            f"{api_err}"
        )

        return False


# ============================================================
# FUNÇÃO PARA ENVIAR MENSAGEM AO SEATALK
# ============================================================

def enviar_seatalk(mensagem: str, webhook: str) -> bool:

    if not webhook:
        print("⚠️ SEATALK_WEBHOOK não foi informado.")
        return False

    payload = {
        "tag": "text",
        "text": {
            "content": mensagem
        }
    }

    try:
        response = requests.post(
            webhook,
            json=payload,
            timeout=30
        )

        if response.ok:
            print("✅ Mensagem enviada para o SeaTalk.")
            return True

        print(
            f"❌ Erro ao enviar mensagem para o SeaTalk: "
            f"{response.status_code} - {response.text}"
        )
        return False

    except requests.exceptions.RequestException as seatalk_error:
        print(
            f"❌ Erro de comunicação com o SeaTalk: "
            f"{seatalk_error}"
        )
        return False


def enviar_screenshot_seatalk(driver, webhook: str) -> bool:

    if not webhook:
        print("⚠️ SEATALK_WEBHOOK não foi informado.")
        return False

    try:

        screenshot_bytes = driver.get_screenshot_as_png()
        screenshot_base64 = b64encode(
            screenshot_bytes
        ).decode("utf-8")

        if len(screenshot_base64) > 5 * 1024 * 1024:
            print(
                "⚠️ Screenshot excede o limite de 5 MB "
                "do SeaTalk."
            )
            return False

        payload = {
            "tag": "image",
            "image_base64": {
                "content": screenshot_base64
            }
        }

        response = requests.post(
            webhook,
            json=payload,
            timeout=30
        )

        if response.ok:
            print("📸 Screenshot enviado para o SeaTalk.")
            return True

        print(
            f"❌ Erro ao enviar screenshot para o SeaTalk: "
            f"{response.status_code} - {response.text}"
        )
        return False

    except requests.exceptions.RequestException as screenshot_error:
        print(
            f"❌ Erro de comunicação com o SeaTalk "
            f"ao enviar screenshot: {screenshot_error}"
        )
        return False

    except Exception as screenshot_error:
        print(
            f"⚠️ Não foi possível capturar/enviar "
            f"o screenshot: {screenshot_error}"
        )
        return False


# ============================================================
# CONFIGURAÇÃO DO SELENIUM / CHROME
# ============================================================

chrome_options = Options()

# Headless para GitHub Actions
chrome_options.add_argument("--headless=new")

# Necessário em muitos ambientes Linux/CI
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# Tamanho da janela
chrome_options.add_argument("--window-size=1920,1080")

# User-Agent
chrome_options.add_argument(
    "--user-agent=Mozilla/5.0 "
    "(Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 "
    "(KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


# ============================================================
# INICIALIZAÇÃO DO DRIVER
# ============================================================

driver = webdriver.Chrome(
    options=chrome_options
)

wait = WebDriverWait(driver, 40)


try:

    # ========================================================
    # 1. ABRIR SHOPEE XPRESS
    # ========================================================

    print("🌐 Navegando para Shopee Xpress SPX...")

    driver.get(
        "https://spx.shopee.com.br"
    )

    print("⏳ Aguardando página de login...")

    # Aguarda o campo de usuário aparecer
    campo_usuario = wait.until(
        EC.presence_of_element_located(
            (
                By.CSS_SELECTOR,
                'input[placeholder="Ops ID"]'
            )
        )
    )

    print("✅ Página de login carregada.")


    # ========================================================
    # 2. LER VARIÁVEIS DE AMBIENTE
    # ========================================================

    SPX_USERNAME = os.environ.get(
        "SPX_USERNAME",
        ""
    )

    SPX_PASSWORD = os.environ.get(
        "SPX_PASSWORD",
        ""
    )

    GITHUB_PAT = os.environ.get(
        "GITHUB_PAT",
        ""
    )

    SEATALK_WEBHOOK = os.environ.get(
        "SEATALK_WEBHOOK",
        ""
    )


    # ========================================================
    # 3. VALIDAR CREDENCIAIS
    # ========================================================

    if not SPX_USERNAME:
        raise Exception(
            "A variável SPX_USERNAME está vazia."
        )

    if not SPX_PASSWORD:
        raise Exception(
            "A variável SPX_PASSWORD está vazia."
        )

    if not GITHUB_PAT:
        raise Exception(
            "A variável GITHUB_PAT está vazia."
        )


    print("✅ Variáveis de ambiente encontradas.")


    # ========================================================
    # 4. PREENCHER LOGIN
    # ========================================================

    print("🔐 Preenchendo usuário...")

    campo_usuario.clear()
    campo_usuario.send_keys(
        SPX_USERNAME
    )


    print("🔐 Procurando campo de senha...")

    campo_senha = wait.until(
        EC.presence_of_element_located(
            (
                By.CSS_SELECTOR,
                'input[placeholder="Senha"]'
            )
        )
    )

    campo_senha.clear()
    campo_senha.send_keys(
        SPX_PASSWORD
    )


    # ========================================================
    # 5. ENVIAR LOGIN
    # ========================================================
    time.sleep(5)
    print("🔘 Procurando botão de login...")

    botao_login = wait.until(
        EC.element_to_be_clickable(
            (
                By.CSS_SELECTOR,
                "button[type='submit']"
            )
        )
    )

    print("🚀 Enviando login...")

    botao_login.click()


    # ========================================================
    # 6. AGUARDAR LOGIN
    # ========================================================

    print(
        "⏳ Aguardando conclusão do login..."
    )

    time.sleep(15)


    # ========================================================
    # 7. AGUARDAR COOKIES
    # ========================================================

    print(
        "🍪 Aguardando cookies de autenticação..."
    )

    def cookies_disponiveis(driver):

        uid = driver.get_cookie("spx_uid")
        uk = driver.get_cookie("spx_uk")

        return uid is not None and uk is not None


    try:

        wait.until(
            cookies_disponiveis
        )

        print(
            "✅ Cookies encontrados."
        )

    except Exception:

        print(
            "⚠️ Os cookies não apareceram "
            "dentro do tempo esperado."
        )


    # ========================================================
    # 8. EXTRAIR COOKIES
    # ========================================================

    cookie_uid_data = driver.get_cookie(
        "spx_uid"
    )

    cookie_uk_data = driver.get_cookie(
        "spx_uk"
    )


    spx_uid_value = (
        cookie_uid_data["value"]
        if cookie_uid_data
        else None
    )

    spx_uk_value = (
        cookie_uk_data["value"]
        if cookie_uk_data
        else None
    )


    print(
        "📋 Resultado da extração:"
    )

    print(
        f"   spx_uid encontrado: "
        f"{spx_uid_value is not None}"
    )

    print(
        f"   spx_uk encontrado: "
        f"{spx_uk_value is not None}"
    )


    # ========================================================
    # 9. VALIDAR COOKIES
    # ========================================================

    if not spx_uid_value:

        raise Exception(
            "Cookie spx_uid não foi encontrado "
            "após o login."
        )

    if not spx_uk_value:

        raise Exception(
            "Cookie spx_uk não foi encontrado "
            "após o login."
        )


    # ========================================================
    # 10. ATUALIZAR SPX_UK NO GITHUB
    # ========================================================

    print(
        "\n🔐 Iniciando atualização dos "
        "secrets no GitHub..."
    )

    spx_uk_atualizado = mudar_secret_ambiente_github(

        owner="iagofroes",

        repo="spx-collector",

        secret_name="SPX_UK",

        secret_value=spx_uk_value,

        token=GITHUB_PAT
    )


    # ========================================================
    # 11. ATUALIZAR SPX_UID NO GITHUB
    # ========================================================

    spx_uid_atualizado = mudar_secret_ambiente_github(

        owner="iagofroes",

        repo="spx-collector",

        secret_name="SPX_UID",

        secret_value=spx_uid_value,

        token=GITHUB_PAT
    )


    # ========================================================
    # FINAL
    # ========================================================

    processo_concluido = (
        spx_uid_atualizado
        and spx_uk_atualizado
    )

    msg = (
        "🍪 ATUALIZAÇÃO DOS COOKIES SPX\n\n"
        f"SPX_UID: {'✅ Encontrado' if spx_uid_value else '❌ Não encontrado'}\n"
        f"SPX_UK: {'✅ Encontrado' if spx_uk_value else '❌ Não encontrado'}\n\n"
        "ATUALIZAÇÃO NO GITHUB\n\n"
        f"SPX_UID: {'✅ Atualizado' if spx_uid_atualizado else '❌ Falhou'}\n"
        f"SPX_UK: {'✅ Atualizado' if spx_uk_atualizado else '❌ Falhou'}\n\n"
        f"RESULTADO: {'✅ PROCESSO CONCLUÍDO' if processo_concluido else '❌ PROCESSO COM ERRO'}"
    )

    print(
        "\n" + msg
    )

    enviar_seatalk(
        msg,
        SEATALK_WEBHOOK
    )


except Exception as e:

    print(
        f"\n❌ Ocorreu um erro durante a automação:"
    )

    print(
        f"   {str(e)}"
    )


    enviar_seatalk(
        "🚨 ERRO NA AUTOMAÇÃO SPX\n\n"
        f"❌ {str(e)}\n\n"
        "Resultado: ❌ PROCESSO NÃO CONCLUÍDO",
        os.environ.get("SEATALK_WEBHOOK", "")
    )


    # ========================================================
    # SCREENSHOT PARA DEBUG
    # ========================================================

    enviar_screenshot_seatalk(
        driver,
        os.environ.get("SEATALK_WEBHOOK", "")
    )


finally:

    # ========================================================
    # ENCERRAR SELENIUM
    # ========================================================

    try:

        driver.quit()

        print(
            "🛑 Chrome encerrado."
        )

    except Exception:

        pass