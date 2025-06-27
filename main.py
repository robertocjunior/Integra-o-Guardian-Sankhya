import requests
import os
from dotenv import load_dotenv
import json
import sys

def load_credentials():
    """
    Carrega e valida as credenciais do arquivo .env.
    Retorna um dicionário com as credenciais ou encerra o script se alguma estiver faltando.
    """
    # Usamos override=True para garantir que as variáveis do .env
    # substituam quaisquer variáveis de ambiente do sistema com o mesmo nome (como USERNAME).
    load_dotenv(override=True)
    
    required_vars = ["TOKEN", "APPKEY", "USERNAME", "PASSWORD"]
    credentials = {}
    
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            print(f"Erro Crítico: A variável de ambiente '{var}' não foi encontrada ou está vazia no arquivo .env.")
            sys.exit(1) # Encerra o script com um código de erro
        # O .strip() é crucial para remover espaços em branco acidentais
        credentials[var.lower()] = value.strip()
        
    return credentials

def perform_login(credentials):
    """
    Executa a requisição de login para a API Sankhya e trata a resposta.
    """
    url = "https://api.sankhya.com.br/login"

    # Headers da requisição, usando as credenciais validadas
    headers = {
        "token": credentials["token"],
        "appkey": credentials["appkey"],
        "username": credentials["username"],
        "password": credentials["password"]
    }

    print("Tentando realizar o login...")
    try:
        response = requests.post(url, headers=headers)
        response.raise_for_status() # Lança uma exceção para respostas com códigos de erro (4xx ou 5xx)

        response_data = response.json()
        bearer_token = response_data.get("bearerToken")
        print("\n✅ Login realizado com sucesso!")
        return bearer_token

    except requests.exceptions.HTTPError as http_err:
        print(f"\n❌ Ocorreu um erro HTTP: {http_err}")
        print(f"Status Code: {http_err.response.status_code}")
        try:
            # Tenta formatar a resposta de erro como JSON para melhor leitura
            error_details = http_err.response.json()
            print("Detalhes do erro:")
            print(json.dumps(error_details, indent=4, ensure_ascii=False))
        except json.JSONDecodeError:
            # Se a resposta de erro não for JSON, imprime como texto
            print(f"Resposta do servidor (não-JSON): {http_err.response.text}")
    except requests.exceptions.RequestException as e:
        print(f"\n❌ Ocorreu um erro de conexão: {e}")
    
    return None

def get_guardian_partners(token):
    """
    Busca os dados da view 'VIEW_PARCEIROS_GUARDIAN' usando o serviço CRUDServiceProvider.
    """
    # URL para o serviço de consulta de views
    url = "https://api.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=CRUDServiceProvider.loadView&outputType=json"
    
    print(f"\n--- Buscando parceiros da view 'VIEW_PARCEIROS_GUARDIAN' ---")
    
    # Para requisições autenticadas, o header padrão é "Authorization: Bearer <seu_token>"
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    # Corpo (payload) da requisição para carregar a view específica
    payload = {
        "serviceName": "CRUDServiceProvider.loadView",
        "requestBody": {
            "query": {
                "viewName": "VIEW_PARCEIROS_GUARDIAN",
                "fields": {
                    "field": {
                        "$": "SKN_CODIGO,SKN_DECRICAO,SKN_RAZAOSOCIAL,SKN_CNPJ,SKN_INSCRICAO_ESTADUAL,SKN_ENDERECO,SKN_COMPLEMENTO,SKN_MUNICIPIO,SNK_EST_CODIGO,SNK_CEP,SNK_TELEFONE"
                    }
                },
                "offsetPage": "0",
                "maxRows": "500"
            }
        }
    }

    try:
        # Note que a requisição para este serviço também é um POST
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        
        print("✅ Dados recebidos com sucesso!")
        print("Resposta da API:")
        print(json.dumps(response.json(), indent=4, ensure_ascii=False))
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro na requisição de dados: {e}")
        if e.response is not None:
            print(f"Status Code: {e.response.status_code}")
            print(f"Resposta do servidor: {e.response.text}")

def perform_logout(token, appkey):
    """
    Encerra a sessão na API Sankhya (logout).
    """
    url = "https://api.sankhya.com.br/gateway/v1/mge/service.sbr?serviceName=MobileLoginSP.logout&outputType=json"
    
    print("\n--- Encerrando a sessão (logout) ---")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "appkey": appkey,
        "Content-Type": "application/json"
    }
    
    try:
        # A requisição de logout é um GET
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        response_data = response.json()
        if response_data.get("status") == "1":
            print("✅ Sessão encerrada com sucesso!")
        else:
            print("⚠️ Logout realizado, mas o status da resposta não foi o esperado.")
            print(json.dumps(response_data, indent=4, ensure_ascii=False))
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao tentar encerrar a sessão: {e}")
        if e.response is not None:
            print(f"Status Code: {e.response.status_code}")
            print(f"Resposta do servidor: {e.response.text}")

if __name__ == "__main__":
    credentials = load_credentials()
    bearer_token = perform_login(credentials)

    if bearer_token:
        try:
            print(f"\n🔑 Bearer Token armazenado com sucesso.")
            get_guardian_partners(bearer_token)
        finally:
            # Garante que o logout seja sempre tentado se o login foi bem-sucedido
            perform_logout(bearer_token, credentials['appkey'])
    else:
        print("\nFalha ao obter o Bearer Token. As próximas requisições não serão executadas.")