# %% [markdown]
# # Relatório Recorrente de Liberações de Dispositivos Temporários a Longas Distâncias

# %% [markdown]
# ### CRITÉRIOS:

# %% [markdown]
# #### 1°: Liberações de acessos temporários Mobile x Windowns (100 Km ou mais)
# #### 2°: Liberações de acessos temporários Mobile x Mobile (100 metros ou menos)

# %%
# BIBLIOTECAS
import os
import sys
import warnings
from time import sleep
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from db_connections import get_postgres_engine, get_mariadb_engine, get_mongo_client, test_database_SQL_connection
from data_processing import (
    corrigir_codificacao, filtrar_clientes_ativos, tratar_dados_temp,
    normalizar_dados, ajustar_colunas, converter_colunas_para_str, convert_to_tuple, tratar_dados_desktop, 
    tratar_dados_mobile, ajustar_coordenadas_apos_uniao, ajustar_coordenadas_apos_uniao_2, remove_fuzo, verificar_acessos_dual, determine_priority, 
    atualizar_ranking, gerar_mapa
)
from integrations import send_slack_message, GoogleSheets, get_location
from utils import same_network, info_ips, calcular_distancia, func_bloqueio
warnings.filterwarnings("ignore")

# %% 
# DEFINIÇÕES DE DIRETÓRIOS
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DIR_ARQUIVOS = os.path.join(BASE_DIR, "ARQUIVOS")
DIR_RESULTADOS = os.path.join(BASE_DIR, "RESULTADOS")
DIR_RANKING = os.path.join(BASE_DIR, "RANKING")

# %%
# PARÂMETROS DE DISTÂNCIA EM KM
DIST_MAX_WINDOWS = 50
MARGEM_ERRO_WINDOWS = 0
DIST_MAX_MOBILE = 0.1

# %%
# FUNÇÃO DE LOG COM ENVIO PARA O SLACK
def log_message(message):
    """Print and send a log message to Slack."""
    print(message)
    send_slack_message(message)


# %%
def main():
    # %%
    # INÍCIO DO SCRIPT
    chamada_inicio_slack = 'Iniciando execução do relatório de compartilhamentos:\n*Máquina virtual:* WIN10 - Autorum, *Script:* automacao_compartilhamentos.py*'
    log_message(chamada_inicio_slack)

    # %%
    # CONECTANDO AOS BANCOS DE DADOS
    print('Conectando aos bancos de dados...')

    # %%
    pg_engine = get_postgres_engine()
    mariadb_engine = get_mariadb_engine()

    # %%
    # TESTE DE CONEXÃO
    queries_testes = [
        ("Postgres", pg_engine, "SELECT cliente_id FROM clientes.dispositivos"),
        ("MariaDB", mariadb_engine, "SELECT id FROM subscriber")
    ]

    # %%
    CONEXÕES_OK = True
    POSTGRES_OK = False
    MARIADB_OK = False

    for nome_bd, engine, query in queries_testes:
        if test_database_SQL_connection(engine, query) is None:
            log_message(f'{nome_bd} offline! Contatar a equipe de software.')
            CONEXÕES_OK = False
            if nome_bd == "Postgres":
                sleep(10)
                sys.exit(1)
            else:
                sleep(5)
        else:
            if nome_bd == "Postgres":
                POSTGRES_OK = True
            elif nome_bd == "MariaDB":
                MARIADB_OK = True

    if CONEXÕES_OK:
        log_message('MariaDB e Postgres funcionando normalmente.')
    elif MARIADB_OK and not POSTGRES_OK:
        log_message('Apenas MariaDB em funcionamento! Contatar a equipe de software para backup do Postgres.')
    
    # %%
    # DEFININDO O PERÍODO DE COLETA DE DADOS
    hoje = datetime.today()
    hoje_inicio = hoje.replace(hour=0, minute=0, second=0, microsecond=0)
    ontem = hoje_inicio - timedelta(days=3 if hoje.weekday() == 0 else 1)

    # %%
    # FUNÇÃO PARA BUSCAR AÇÃO NO MONGODB    
    def busca_acao(codigo):
        mongo_client = get_mongo_client()
        filter = {'cod': int(codigo), 'h_utc': {'$gt': ontem, '$lte': hoje}}
        return mongo_client['api']['logs'].find(filter=filter)

    # %%
    # COLETANDO LIBERAÇÃO DE TEMPORÁRIOS E LOGINS (CÓDIGOS 1041 E 1040)
    try:
        lib_temp = pd.DataFrame(busca_acao(1041))
        temp = pd.DataFrame(busca_acao(1040))
        log_message('MongoDB funcionando normalmente.')
    except:
        log_message('MongoDB offline! Verificar.')
        sleep(10)
        sys.exit(1)

    # %%
    # VERIFICAÇÃO BÁSICA DE DADOS
    if lib_temp.empty or temp.empty:
        log_message('Nenhum dado encontrado no período definido! Fechando programa.')
        sleep(3)
        sys.exit(0)

    # %%
    # VERIFICAÇÃO BÁSICA DE DADOS
    if lib_temp.empty or temp.empty:
        log_message('Nenhum dado encontrado no período definido! Fechando programa.')
        sleep(3)
        sys.exit(0)

    # %%
    # COLETA DE INFORMAÇÕES DOS CLIENTES NO BANCO DE DADOS
    query = "SELECT id, name, state, city, account_type, status FROM subscriber WHERE account_type in (3, 5)"
    subscriber = pd.read_sql(query, mariadb_engine).replace('', np.nan).dropna()
    subscriber['city'] = subscriber['city'].apply(lambda x: corrigir_codificacao(x) if isinstance(x, str) else x)
    subscriber = subscriber.rename(columns={'id': 'u_id'})

    # %%
    # PROCESSAMENTO DOS DADOS DOS CLIENTES
    clientes_ativos = filtrar_clientes_ativos(subscriber)
    temp_filtrado = tratar_dados_temp(temp, clientes_ativos).reset_index(drop=True)
    dispositivos = normalizar_dados(temp_filtrado)
    dispositivos = pd.concat([temp_filtrado, dispositivos], axis=1).drop(columns='dados')

    # %%
    # DROPANDO COLUNAS INDESEJADAS E SELECIONANDO APENAS OS DISPOSITIVOS PRINCIPAIS
    try:
        dispositivos = dispositivos[dispositivos['metodo']=='ASSINATURA_PRINCIPAL']
        colunas_desejadas = ['u_id', 'h_tz', 'hw', 'metodo', 'hw_info.brand', 'hw_info.device', 'hw_info.model', 'hw_info.utsname.machine', 'hw_info.utsname.nodename']
        dispositivos = dispositivos[colunas_desejadas]
        dispositivos = ajustar_colunas(dispositivos)
    except:
        pass

    # %%
    # TRANSFORMANDO COLUNAS EM STRING
    dispositivos = converter_colunas_para_str(dispositivos, ['marca', 'device'])

    # %%
    # DIVISÃO DE DADOS ENTRE DESKTOP E MOBILE
    temp_filtrado_desk = temp_filtrado.query("p == 'windows' and t == 'T'").reset_index(drop=True)
    temp_filtrado_mobile = temp_filtrado.query("p != 'windows' and t == 'T'").reset_index(drop=True)

    # %%
    # TRATAMENTO DOS DADOS DO DESKTOP
    temp_dados = normalizar_dados(temp_filtrado_desk).drop(columns=['metodo', 'token', 'local', 'local_accuracy', 'hw_info.platform', 
                                                                     'hw_info.numberOfCores', 'hw_info.systemMemoryInMegabytes', 'hw_info.locale', 
                                                                     'hw_info.wmic_os_get_osarchitecture'], errors='ignore').reset_index(drop=True)
    temp_final_desk = pd.concat([temp_filtrado_desk, temp_dados], axis=1).drop(columns=['dados', 't', 'hw'])

    # %%
    # REMOVER TESTE DRIVE E RENOMEAR COLUNAS
    temp_final_desk = temp_final_desk[temp_final_desk['plano'] != 'trial'].reset_index(drop=True)
    temp_final_desk = temp_final_desk.rename(columns={'gsi_login_token_sk': 'tipo_login', 'hw_info.deviceId': 'informacoes_desktop', 
                                                      'hw_info.computerName': 'nome_desktop', 'hw_info.version': 'sistema_operacional', 
                                                      'hw_info.wmic_bios_list_brief': 'bios'})

    # %%
    # CRIAÇÃO DE COLUNAS AUXILIARES
    temp_final_desk['Ips_diferentes'] = temp_final_desk.groupby('u_id')['ip'].transform('count')

    # %%
    # PROCESSO SIMILAR PARA DADOS MOBILE
    temp_dados_mobile = normalizar_dados(temp_filtrado_mobile).reindex(columns=['local_accuracy', 'local.coordinates', 'plano'])
    temp_final_mobile = pd.concat([temp_filtrado_mobile, temp_dados_mobile], axis=1)
    temp_final_mobile = temp_final_mobile[temp_final_mobile['plano'] != 'trial'].reset_index(drop=True)
    temp_final_mobile = temp_final_mobile.drop(columns=['dados', 't', 'plano']).dropna().reset_index(drop=True)
    temp_final_mobile['Ips_diferentes'] = temp_final_mobile.groupby('u_id')['ip'].transform('count')
    temp_final_mobile = temp_final_mobile.rename(columns={'hw': 'hw_temporario', 'local.coordinates': 'coordenadas_temporario', 'local_accuracy': 'raio(m)_temporario'})

    # %%
    # TRATAMENTO DOS DADOS DE ACESSO TEMPORÁRIO
    lib_temp_filtrado = tratar_dados_temp(lib_temp, clientes_ativos).reset_index(drop=True)
    lib_temp_dados = normalizar_dados(lib_temp_filtrado).drop(columns=['local.type', 'local_is_mock', 'local'], errors='ignore').reset_index(drop=True)
    lib_temp_final = pd.concat([lib_temp_filtrado, lib_temp_dados], axis=1).drop(columns='dados')

    # %%
    # RENOMEAÇÃO DE COLUNAS PARA UNIFORMIDADE
    lib_temp_final = lib_temp_final.rename(columns={'h_tz': 'h_tz_principal', 'ip': 'ip_principal', 'local_accuracy': 'raio(m)_principal', 
                                                    'local.coordinates': 'coordenadas_principal'})

    # %%
    # COMPARAÇÃO DE DISPOSITIVOS ENTRE REGISTROS E LIB_TEMP
    for index, row in lib_temp_final.iterrows():
        hw = row['hw']
        matching_rows = dispositivos[dispositivos['hw'] == hw]
        if not matching_rows.empty:
            for col in ['marca', 'device', 'modelo', 'modelo_ios', 'nome_dispositivo']:
                lib_temp_final.at[index, col] = matching_rows.iloc[0].get(col, None)
    lib_temp_final.drop(columns='hw', inplace=True)

    # %%
    # TRATANDO DADOS DADOS DE DISPOSITIVOS DESKTOPS TEMPORÁRIOS
    temp_final_desk = tratar_dados_desktop(temp_final_desk, lib_temp_final)

    # %%
    # LISTA PARA ARMAZENAR OS U_ID ONDE 'IP' E 'IP_PRINCIPAL' SÃO DIFERENTES
    u_ids_diferentes = []

    # %%
    # ITERAR SOBRE AS LINHAS DO DATAFRAME
    for index, row in temp_final_desk.iterrows():
        if row['ip'] != row['ip_principal']:
            u_ids_diferentes.append(row['u_id'])

    # %%
    # FILTRAR O DATAFRAME PARA MANTER APENAS AS LINHAS COM U_ID NA LISTA
    df_desk_ips_diferentes = temp_final_desk[temp_final_desk['u_id'].isin(u_ids_diferentes)].copy()
    df_desk_ips_diferentes.reset_index(drop=True, inplace=True)

    # %%
    # APLICAR A FUNÇÃO AO DATAFRAME
    df_desk_ips_diferentes.loc[:, 'same_network'] = df_desk_ips_diferentes.apply(lambda row: same_network(row['ip'], row['ip_principal']), axis=1)

    # %%
    # LISTA PARA ARMAZENAR OS U_ID ONDE 'IP' E 'IP_PRINCIPAL' NÃO ESTÃO NA MESMA REDE
    redes_diferentes = []

    # %%
    # ITERAR SOBRE AS LINHAS DO DATAFRAME
    for index, row in df_desk_ips_diferentes.iterrows():
        if row['same_network'] != True:
            redes_diferentes.append(row['u_id'])

    # %%
    # FILTRAR O DATAFRAME PARA MANTER APENAS AS LINHAS COM U_ID NA LISTA
    df_desk_ips_diferentes = df_desk_ips_diferentes[df_desk_ips_diferentes['u_id'].isin(redes_diferentes)].copy()
    df_desk_ips_diferentes.reset_index(drop=True, inplace=True)

    # %%
    # ADICIONANDO NOVOS DADOS DA TABELA SUBSCRIBER E RENOMEANDO COLUNAS
    df_desk_ips_diferentes = df_desk_ips_diferentes.merge(subscriber, how='left',on='u_id')
    df_desk_ips_diferentes = df_desk_ips_diferentes.rename(columns={'city':'cidade_cadastro', 'state': 'estado_cadastro'})

    # %%
    # VARIÁVEL QUE VERIFICA A QUANTIDADE DE COORDENADAS VAZIAS
    sem_coordenadas = df_desk_ips_diferentes['coordenadas_principal'].isnull().sum()

    # %%
    # FUNÇÃO QUE VERIFICA E PREENCHE COORDENADAS VAZIAS DO DISPOSITIVO PRINCIPAL (INSIGTHS)
    if sem_coordenadas != 0:
        try:
            print('Coletando registros de coordenadas a partir dos ips registrados (insights)')
             # ITERAR SOBRE AS LINHAS COM COORDENADAS VAZIAS
            for index, row in df_desk_ips_diferentes[df_desk_ips_diferentes['coordenadas_principal'].isnull()].iterrows():
                ip_principal = row['ip_principal']
                location_info = info_ips(ip_principal)
                # VERIFIQUE SE A LOCALIZAÇÃO FOI OBTIDA COM SUCESSO
                if 'location_latitude' in location_info and 'location_longitude' in location_info:
                    latitude = location_info['location_latitude'][0]
                    longitude = location_info['location_longitude'][0]
                    # ATUALIZAR A COLUNA 'COORDENADAS_PRINCIPAL' COM AS COORDENADAS NO FORMATO DESEJADO
                    df_desk_ips_diferentes.at[index, 'coordenadas_principal'] = [longitude, latitude]
        except:
            # FUNÇÃO QUE VERIFICA E PREENCHE COORDENADAS VAZIAS DO DISPOSITIVO PRINCIPAL (BANCO LOCAL)
            print('Coletando registros de coordenadas a partir dos ips registrados (banco de dados local)')
            # ITERAR SOBRE AS LINHAS COM COORDENADAS VAZIAS
            for index, row in df_desk_ips_diferentes[df_desk_ips_diferentes['coordenadas_principal'].isnull()].iterrows():
                ip_principal = row['ip_principal']
                location_info = get_location(ip_principal)
                # VERIFIQUE SE A LOCALIZAÇÃO FOI OBTIDA COM SUCESSO
                if 'latitude' in location_info and 'longitude' in location_info:
                    latitude = location_info['latitude']
                    longitude = location_info['longitude']
                    # ATUALIZAR A COLUNA 'COORDENADAS_PRINCIPAL' COM AS COORDENADAS NO FORMATO DESEJADO
                    df_desk_ips_diferentes.at[index, 'coordenadas_principal'] = [longitude, latitude]
    else:
        print('Nenhuma coordenada vazia!')

    # %%
    # FUNÇÃO QUE VERIFICA E PREENCHE COORDENADAS VAZIAS DO DISPOSITIVO TEMPORÁRIO (INSIGHTS)
    try:
        print('Coletando coordenadas (insights)')
        # Dicionário para armazenar informações dos IPs
        ip_info_cache = {}
        # Inicialização das colunas no DataFrame
        df_desk_ips_diferentes['pais_temporario'] = pd.NA
        df_desk_ips_diferentes['cidade_temporario'] = pd.NA
        df_desk_ips_diferentes['coordenadas_temporario'] = pd.NA
        df_desk_ips_diferentes['is_anonymous'] = pd.NA
        df_desk_ips_diferentes['is_anonymous_vpn'] = pd.NA

        # Iteração sobre as linhas do DataFrame
        for index, row in df_desk_ips_diferentes.iterrows():
            ip = row['ip']

            # Verifica se o IP já foi consultado antes
            if ip in ip_info_cache:
                location_info = ip_info_cache[ip]
            else:
                # Chamada à API para novos IPs
                location_info = info_ips(ip)
                # Armazenando o resultado no cache
                ip_info_cache[ip] = location_info

            # Verifica se a localização foi obtida com sucesso
            if 'location_latitude' in location_info and 'location_longitude' in location_info and 'city' in location_info and 'country_name' in location_info:
                cidade = location_info['city']
                if isinstance(cidade, pd.Series):
                    cidade = cidade.iloc[0]
                pais = location_info['country_name']
                if isinstance(pais, pd.Series):
                    pais = pais.iloc[0]
                latitude = location_info['location_latitude'][0]
                longitude = location_info['location_longitude'][0]
                is_anonymous = location_info['traits_is_anonymous'][0]
                is_anonymous_vpn = location_info['traits_is_anonymous_vpn'][0]

                # Atualizar as colunas com as informações de localização
                df_desk_ips_diferentes.at[index, 'pais_temporario'] = pais
                df_desk_ips_diferentes.at[index, 'cidade_temporario'] = cidade
                df_desk_ips_diferentes.at[index, 'coordenadas_temporario'] = [longitude, latitude]
                df_desk_ips_diferentes.at[index, 'is_anonymous'] = is_anonymous
                df_desk_ips_diferentes.at[index, 'is_anonymous_vpn'] = is_anonymous_vpn
    except:
        print('Coletando coordenadas (banco local)')
        # FUNÇÃO QUE VERIFICA E PREENCHE COORDENADAS VAZIAS DO DISPOSITIVO TEMPORÁRIO (BANCO LOCAL)
        df_desk_ips_diferentes['pais_temporario'] = pd.NA
        df_desk_ips_diferentes['cidade_temporario'] = pd.NA
        df_desk_ips_diferentes['coordenadas_temporario'] = pd.NA
        for index, row in df_desk_ips_diferentes.iterrows():
           ip = row['ip']
           location_info = get_location(ip)

           # Verifique se a localização foi obtida com sucesso
           if 'latitude' in location_info and 'longitude' in location_info and 'cidade' in location_info and 'pais' in location_info:
               cidade = location_info['cidade']
               pais = location_info['pais']
               latitude = location_info['latitude']
               longitude = location_info['longitude']

               # Atualizar a coluna 'coordenadas_principal' com as coordenadas no formato desejado
               df_desk_ips_diferentes.at[index, 'pais_temporario'] = pais
               df_desk_ips_diferentes.at[index, 'cidade_temporario'] = cidade
               df_desk_ips_diferentes.at[index, 'coordenadas_temporario'] = [longitude, latitude]

    # %%
    # REORDENANDO COLUNAS E CÁLCULO DE DISTÂNCIAS
    df_desk_ips_diferentes = df_desk_ips_diferentes.reindex(columns=[
        'u_id', 'name', 'h_tz', 'cidade_cadastro', 'estado_cadastro', 'account_type', 'status', 'ip', 'is_anonymous', 
        'is_anonymous_vpn', 'coordenadas_temporario', 'cidade_temporario', 'pais_temporario', 'p', 'plano', 
        'informacoes_desktop', 'nome_desktop', 'sistema_operacional', 'bios', 'Ips_diferentes', 'same_network', 
        'h_tz_principal', 'ip_principal', 'raio(m)_principal', 'coordenadas_principal', 'marca', 'device', 
        'modelo', 'modelo_ios', 'nome_dispositivo'
    ])

    df_desk_ips_diferentes = calcular_distancia(df_desk_ips_diferentes, 'coordenadas_temporario', 'coordenadas_principal')
    df_desk_ips_diferentes['distancia'] = df_desk_ips_diferentes.apply(lambda row: 0 if row['same_network'] else row['distancia'], axis=1)
    df_desk_ips_diferentes['distancia'] += MARGEM_ERRO_WINDOWS

    # %%
    # FILTRANDO PARA DISTÂNCIAS SUPERIORES AO LIMITE
    lista_temp_w = df_desk_ips_diferentes[df_desk_ips_diferentes['distancia'] >= (DIST_MAX_WINDOWS + MARGEM_ERRO_WINDOWS)].reset_index(drop=True)

    # %%
    # TRANSFORMANDO COLUNA EM DATETIME
    lib_temp_final.loc[lib_temp_final.index, 'h_tz_principal'] = pd.to_datetime(lib_temp_final['h_tz_principal'])
    temp_final_mobile.loc[temp_final_mobile.index, 'h_tz'] = pd.to_datetime(temp_final_mobile['h_tz'])

    # %%
    # APLICAR A FUNÇÃO DE VERIFICAÇÃO DE NETWORK AO DATAFRAME E SE É NECESSÁRIO O BLOQUEIO
    temp_final_mobile = tratar_dados_mobile(temp_final_mobile, lib_temp_final)
    temp_final_mobile.loc[:, 'same_network'] = temp_final_mobile.apply(lambda row: same_network(row['ip'], row['ip_principal']), axis=1)
    temp_final_mobile = calcular_distancia(temp_final_mobile, 'coordenadas_temporario', 'coordenadas_principal')
    temp_final_mobile.loc[:, 'bloqueio'] = temp_final_mobile.apply(lambda row: func_bloqueio(row['raio(m)_temporario'], row['raio(m)_principal'], row['distancia']), axis=1)
    lista_temp_m = temp_final_mobile[temp_final_mobile['bloqueio']].reset_index(drop=True)

    # %%
    # ADICIONANDO NOVOS DADOS DA TABELA SUBSCRIBER E RENOMEANDO COLUNAS
    lista_temp_m = lista_temp_m.merge(subscriber, how='left', on='u_id')
    lista_temp_m = lista_temp_m.rename(columns={'city': 'cidade_cadastro', 'state': 'estado_cadastro'})

    # %%
    # VERIFICAÇÕES FINAIS E GERAÇÃO DE RESULTADOS
    soma_clientes_w = lista_temp_w['u_id'].nunique()
    soma_clientes_m = lista_temp_m['u_id'].nunique()

    if soma_clientes_w == 0 and soma_clientes_m == 0:
        print('Nenhum cliente compartilhou nas distâncias fornecidas ontem!')
    elif soma_clientes_w > 0:
        print('Número de clientes que fizeram logins na distância de ' + str(DIST_MAX_WINDOWS) + ' Km no dispositivo temporário windows:', soma_clientes_w)
        sleep(5)
        
        if soma_clientes_m > 0:
            print('Número de clientes que fizeram logins na distância de ' + str(DIST_MAX_MOBILE) + ' Km no dispositivo temporário mobile:', soma_clientes_m)
            sleep(5)

    # %%
    # CONVERTER 'H_TZ' E 'H_TZ_PRINCIPAL' PARA DATETIME64[NS] EM AMBOS OS DATAFRAMES
    lista_temp_w['h_tz'] = pd.to_datetime(lista_temp_m['h_tz'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    lista_temp_w['h_tz_principal'] = pd.to_datetime(lista_temp_m['h_tz_principal'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

    lista_temp_m['h_tz'] = pd.to_datetime(lista_temp_m['h_tz'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    lista_temp_m['h_tz_principal'] = pd.to_datetime(lista_temp_m['h_tz_principal'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

    # %%
    # REMOVENDO FUZOS
    lista_temp_m = remove_fuzo(lista_temp_m)
    lista_temp_w = remove_fuzo(lista_temp_w)

    # %%
    # CONVERTENDO DISTÂNCIAS
    lista_temp_w['distancia'] = lista_temp_w['distancia'].astype(float)
    lista_temp_m['distancia'] = lista_temp_m['distancia'].astype(float)

    # %%
    # CONVERTENDO RAIOS
    lista_temp_w['raio(m)_principal'] = pd.to_numeric(lista_temp_w['raio(m)_principal'], errors='coerce')
    lista_temp_m['raio(m)_principal'] = pd.to_numeric(lista_temp_w['raio(m)_principal'], errors='coerce')

    # %%
    # TRANSFORMANDO COLUNAS EM TUPLAS
    lista_temp_w = ajustar_coordenadas_apos_uniao(lista_temp_w)
    lista_temp_m = ajustar_coordenadas_apos_uniao(lista_temp_m)

    # %%
    # VERIFICAÇÕES PARA DECIDIR QUAL CONJUNTO DE DADOS UNIR
    current_date = ontem.strftime("%d-%m-%Y")
    if soma_clientes_w > 0 and soma_clientes_m > 0:
        resultado = pd.merge(lista_temp_w, lista_temp_m, on=[
            'u_id', 'name', 'h_tz_principal', 'cidade_cadastro', 'estado_cadastro', 'status', 'account_type', 'p',
            'ip_principal', 'raio(m)_principal', 'coordenadas_principal', 'h_tz', 'ip', 'coordenadas_temporario', 
            'Ips_diferentes', 'same_network', 'distancia', 'marca', 'device', 'modelo', 'modelo_ios', 'nome_dispositivo'
        ], how='outer')
        log = 3
    elif (soma_clientes_m > 0) & (soma_clientes_w == 0):
        resultado = lista_temp_m.copy()
        resultado['Ambos Dispositivos (W+M)'] = np.nan
        log = 2
    elif (soma_clientes_w > 0) & (soma_clientes_m == 0):
        resultado = lista_temp_w.copy()
        log = 1
    else:
        filename = f"{current_date}.txt"
        log = 0

    # %%
    # AJUSTES FINAIS E GERAÇÃO DE ARQUIVO EXCEL E MAPA
    if log > 0:
        resultado = convert_to_tuple(resultado, ['coordenadas_temporario', 'coordenadas_principal'])
        resultado = ajustar_coordenadas_apos_uniao_2(resultado)
        ranking_sheets = GoogleSheets.leitor("Ranking", "Ranking Compartilhamento de Acesso")
        ranking_sheets = ranking_sheets.astype({
            'Quantidade de Acessos (Windows)': int, 'Quantidade de Acessos (Mobile)': int, 'Ips Diferentes': int
        }).fillna(0)

        if 'bloqueio' in resultado.columns:
            resultado = verificar_acessos_dual(resultado, ranking_sheets)
        else:
            resultado['Ambos Dispositivos (W+M)'] = 'Não'
            resultado['bloqueio'] = ''

        resultado['Prioridade'] = resultado['u_id'].map(lambda uid: determine_priority(resultado, uid))

        print('Gerando arquivo .xlsx dos resultados')
        with pd.ExcelWriter(os.path.join(DIR_RESULTADOS, f'resultados_{current_date}.xlsx'), engine='xlsxwriter') as writer:
            resultado.to_excel(writer, sheet_name='Visão Geral Acessos', index=False)

        print('Gerando mapa')
        gerar_mapa(resultado, DIR_RESULTADOS, f'mapa_acessos_ilegitimos_{current_date}')
        print('Mapa Gerado')

    else:
        with open(DIR_RESULTADOS + '\\' + filename, 'w') as file:
            file.write("Nenhum cliente acessou a distância em " + current_date)

    # %%
    # ALTERANDO TIPO DE DADOS DE RANKING
    ranking_sheets.loc[ranking_sheets.index, 'u_id'] = ranking_sheets['u_id'].astype(int)
    ranking_sheets.loc[ranking_sheets.index, 'Quantidade de Dispositivos'] = ranking_sheets['Quantidade de Dispositivos'].astype(int)

    ranking_sheets['Ips Diferentes'] = ranking_sheets['Ips Diferentes'].replace('', 0)
    ranking_sheets.loc[ranking_sheets.index, 'Ips Diferentes'] = ranking_sheets['Ips Diferentes'].astype(int)

    ranking_sheets['Quantidade de Acessos (Windows)'] = ranking_sheets['Quantidade de Acessos (Windows)'].replace('', 0)
    ranking_sheets.loc[ranking_sheets.index, 'Quantidade de Acessos (Windows)'] = ranking_sheets['Quantidade de Acessos (Windows)'].astype(int)

    ranking_sheets['Quantidade de Acessos (Mobile)'] = ranking_sheets['Quantidade de Acessos (Mobile)'].replace('', 0)
    ranking_sheets.loc[ranking_sheets.index, 'Quantidade de Acessos (Mobile)'] = ranking_sheets['Quantidade de Acessos (Mobile)'].astype(int)

    # %%
    # ATUALIZADOR DE RANKING
    ranking_novo = atualizar_ranking(resultado, ranking_sheets)
    ranking_novo.to_excel(DIR_RANKING + '\\ranking.xlsx', index=False)

    # %%
    # ATUALIZAÇÃO DO RANKING
    dispositivos_totais = pd.read_sql(
        "SELECT cliente_id AS u_id, COUNT(assinatura_de_hardware) AS Quantidade_de_Dispositivos FROM clientes.dispositivos WHERE tipo_de_acesso='T' GROUP BY cliente_id",
        pg_engine
    )
    ranking_novo = ranking_sheets.merge(dispositivos_totais, on='u_id', how='left')
    ranking_novo = ranking_novo.sort_values(by=['Ambos Dispositivos (W+M)', 'Quantidade de Acessos (Mobile)'], ascending=[False, False]).reset_index(drop=True)
    ranking_novo = ranking_novo.drop(columns='Quantidade de Dispositivos')
    ranking_novo = ranking_novo.rename(columns={'quantidade_de_dispositivos':'Quantidade de Dispositivos'})
    ranking_novo.to_excel(os.path.join(DIR_RANKING, 'ranking.xlsx'), index=False)

    if len(ranking_novo) > 1000:
        GoogleSheets.escritor(ranking_novo, 'Ranking')

# %%
if __name__ == "__main__":
    main()