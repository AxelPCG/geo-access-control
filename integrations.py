# FUNÇÕES DE INTEGRAÇÃO COM SERVIÇOS EXTERNOS

# %%
#BIBLIOTECAS
import pandas as pd
import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import geoip2.database
import geoip2.webservice
from google.oauth2 import service_account
import gspread


# %%
# FUNÇÔES RELACIONADAS A API DO SLACK
def send_slack_message(text):
    """Envia uma mensagem para um canal do Slack."""
    client = WebClient(os.getenv("SLACK_TOKEN"))
    try:
        client.chat_postMessage(channel="C04JR8Z8XHN", text=text)
    except SlackApiError as e:
        print(f"Erro ao enviar mensagem para o Slack: {e.response['error']}")


# %%
# FUNÇÕES RELACIONADAS AO GEOIP (MAXMIND) - DATABASE LOCAL 
def get_location(ip_address):
    """Obtém a localização de um IP usando um banco de dados local."""
    try:
        geoloc = os.getenv("GEOIP_LOCAL_DB")
        with geoip2.database.Reader(geoloc) as reader:
            response = reader.city(ip_address)
            return {
                "cidade": response.city.name,
                "pais": response.country.name,
                "latitude": response.location.latitude,
                "longitude": response.location.longitude
                
            }
    except Exception as e:
        return f"Erro ao buscar o IP: {e}"


# %%
# FUNÇÕES RELACIONADAS AO GEOIP (MAXMIND) - DATABASE API
def get_insights(ip_address):
    """Obtém insights sobre um IP usando o GeoIP Webservice."""
    try:
        with geoip2.webservice.Client(949830, os.getenv("GEOIP_WEB_KEY")) as client:
            response = client.insights(ip_address)
            insights_en = {
                'ip_address': ip_address,
                'continent_name': response.continent.names.get('en', 'Not Available'),
                'continent_code': response.continent.code,
                'country_name': response.country.names.get('en', 'Not Available'),
                'country_iso_code': response.country.iso_code,
                'city': response.city.names.get('en', 'Not Available') if response.city else None,
                'subdivisions': response.subdivisions.most_specific.iso_code,
                'postal': response.postal.code,
                'location': response.location,
                'country_confidence': response.country.confidence,
                'subdivision_confidence': response.subdivisions.most_specific.confidence,
                'city_confidence': response.city.confidence,
                'postal_confidence': response.postal.confidence,
                'registered_country': response.registered_country.names.get('en', 'Not Available'),
                'traits': response.traits
            }
            return insights_en
    except Exception as e:
        return f"Erro ao buscar IP: {e}"


# %%
# FUNÇÕES RELACIONADAS AO GOOGLE SHEETS
def login():
    """Realiza login no Google Sheets usando credenciais do arquivo JSON."""
    credentials = service_account.Credentials.from_service_account_file(os.getenv("GOOGLE_SHEETS_CREDENTIALS"))
    scoped_credentials = credentials.with_scopes(["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    gc = gspread.authorize(scoped_credentials)
    return gc

class GoogleSheets:
    def leitor(pagina, planilha, dtype=None):
        """Lê dados de uma planilha do Google Sheets e retorna um DataFrame."""
        gc=login()
        planilha = gc.open(planilha)
        aba = planilha.worksheet(pagina)
        dados = aba.get_all_records(numericise_ignore=['all'])
        return pd.DataFrame(dados, dtype=dtype)

    def escritor(dataframe, pagina, planilha="Ranking Compartilhamento de Acesso"):
        """Escreve um DataFrame em uma planilha do Google Sheets."""
        gc = login()
        planilha = gc.open(planilha)
        aba = planilha.worksheet(pagina)
        aba.clear()
        aba.update([dataframe.columns.values.tolist()] + dataframe.values.tolist())