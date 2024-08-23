##FUNÇÕES DE MANIPULAÇÃO DE DATAFRAMES

# %% 
# BIBLIOTECAS
import pandas as pd
import folium
import random
import ast
									 

# %% 
# FUNÇÃO PARA CORRIGIR A CODIFICAÇÃO DE STRINGS											   
def corrigir_codificacao(s):
    """Corrige a codificação de strings para UTF-8."""
    try:
        return s.encode('latin-1').decode('utf-8')
    except:
        return s     


# %% 
# FUNÇÃO PARA LEITURA E FILTRAGEM DE DADOS DE UM DATAFRAME														
def filtrar_clientes_ativos(subscriber_df):
    """Filtra clientes ativos a partir de um DataFrame de assinantes."""
    print('Iniciando filtro dos dados')
    clientes = subscriber_df['u_id']
    clientes.drop_duplicates(inplace=True)
    return clientes.reset_index(drop=True)


# %% 
# FUNÇÃO PARA TRATAMENTO DE DADOS DA TABELA 'TEMP'
def tratar_dados_temp(temp_df, clientes):
    """Filtra e trata dados da tabela 'temp' para clientes ativos."""
    try:
        selecao = temp_df['u_id'].isin(clientes)
        temp_filtrado = temp_df[selecao]
    except ValueError:
        print('O(s) cliente(s) informado(s) não estão no banco com account_type = 3 ou 5 (busca banco cod: 1040)')
    temp_filtrado = temp_filtrado.drop(columns={'_id', 'u_tipo', 'cod', 'v', 'h_utc'})
    return temp_filtrado


# %% 
# FUNÇÃO PARA NORMALIZAR DADOS JSON EM DATAFRAMES
def normalizar_dados(temp_filtrado):
    """Normaliza e limpa dados JSON de uma coluna 'dados'."""
    dispositivos_dados = pd.DataFrame()
    dispositivos_dados['dados'] = temp_filtrado['dados'].astype(str)
    dispositivos_dados['dados'] = dispositivos_dados['dados'].apply(eval)
    return pd.json_normalize(dispositivos_dados['dados'])


# %% 
# FUNÇÃO PARA RENOMEAR E REORGANIZAR COLUNAS APÓS NORMALIZAÇÃO
def ajustar_colunas(dispositivos):
    """Renomeia colunas e ajusta o DataFrame após normalização."""
    dispositivos = dispositivos.rename(columns={
        'hw_info.brand': 'marca',
        'hw_info.device': 'device',
        'hw_info.model': 'modelo',
        'hw_info.utsname.machine': 'modelo_ios',
        'hw_info.utsname.nodename': 'nome_dispositivo'
    })
    return dispositivos.reset_index(drop=True)


# %% 
# FUNÇÃO PARA CONVERTER COLUNAS EM STRINGS												 										
def converter_colunas_para_str(df, colunas):
    """Converte colunas específicas de um DataFrame para strings."""
    for coluna in colunas:
        try:
            df[coluna] = df[coluna].astype(str)
        except:
            pass
    return df


# %% 
# FUNÇÃO PARA CONVERTER STRING EM TUPLA
def convert_to_tuple(df, colunas):
    for coluna in colunas:
        if isinstance(df[coluna], str):  # Se o item for uma string
            try:
                return ast.literal_eval(df[coluna])  # Tenta converter a string para uma tupla
            except (ValueError, SyntaxError):
                return None  # Retorna None ou alguma outra ação se a conversão falhar
    return df  # Retorna o item como está se não for uma string


# %% 
# FUNÇÃO PARA TRATAR DADOS DE DISPOSITIVOS DESKTOPS TEMPORÁRIOS
def tratar_dados_desktop(temp_final_desk, lib_temp_final):
    # PREPARANDO NOVAS COLUNAS PARA AS INFORMAÇÕES DESEJADAS
    temp_final_desk['h_tz_principal'] = None
    temp_final_desk['ip_principal'] = pd.Series(dtype='object')
    temp_final_desk['raio(m)_principal'] = pd.Series(dtype='object')
    temp_final_desk['coordenadas_principal'] = pd.Series(dtype='object')
    temp_final_desk['marca'] = pd.Series(dtype='object')
    temp_final_desk['device'] = pd.Series(dtype='object')
    temp_final_desk['modelo'] = pd.Series(dtype='object')
    temp_final_desk['modelo_ios'] = pd.Series(dtype='str')
    temp_final_desk['nome_dispositivo'] = pd.Series(dtype='str')

    # %%
    # TRANSFORMANDO COLUNA EM DATETIME
    temp_final_desk['h_tz'] = pd.to_datetime(temp_final_desk['h_tz'])
    lib_temp_final['h_tz_principal'] = pd.to_datetime(lib_temp_final['h_tz_principal'])

    # %%
    # ITERAR SOBRE CADA LINHA DA TABELA TEMP_FINAL
    for index, row in temp_final_desk.iterrows():
        u_id = row['u_id']
        h_tz = row['h_tz']
        # ENCONTRAR TODAS AS LINHAS EM LIB_TEMP_FINAL COM O MESMO U_ID
        matching_rows = lib_temp_final[lib_temp_final['u_id'] == u_id]
        # VERIFICAR A CONDIÇÃO DE DIFERENÇA DE TEMPO E ADICIONAR AS COLUNAS
        for _, match in matching_rows.iterrows():
            if abs(match['h_tz_principal'] - h_tz) <= pd.Timedelta(minutes=1):
                temp_final_desk.at[index, 'h_tz_principal'] = match['h_tz_principal']
                temp_final_desk.at[index, 'ip_principal'] = match['ip_principal']
                temp_final_desk.at[index, 'raio(m)_principal'] = match['raio(m)_principal']
                temp_final_desk.at[index, 'coordenadas_principal'] = match['coordenadas_principal']
                try:
                    temp_final_desk.at[index, 'marca'] = match['marca']
                except:
                    pass
                try:
                    temp_final_desk.at[index, 'device'] = match['device']
                except:
                    pass
                try:
                    temp_final_desk.at[index, 'modelo'] = match['modelo']
                except:
                    pass
                try:
                    temp_final_desk.at[index, 'modelo_ios'] = match['modelo_ios']
                except:
                    pass
                try:
                    temp_final_desk.at[index, 'nome_dispositivo'] = match['nome_dispositivo']
                except:
                    pass
                break  # Adiciona apenas a primeira correspondência válida
            
    return temp_final_desk

# %% 
# FUNÇÃO PARA TRATAR DADOS DE DISPOSITIVOS MÓVEIS TEMPORÁRIOS															 
def tratar_dados_mobile(temp_final_mobile, lib_temp_final):
    """Trata dados de dispositivos móveis temporários."""
    temp_final_mobile['coordenadas_principal'] = None
    temp_final_mobile['raio(m)_principal'] = None
    temp_final_mobile['h_tz_principal'] = None
    temp_final_mobile['ip_principal'] = None
    temp_final_mobile['marca'] = None
    temp_final_mobile['device'] = None
    temp_final_mobile['modelo'] = None
    temp_final_mobile['modelo_ios'] = None
    temp_final_mobile['nome_dispositivo'] = None

    for index, row in temp_final_mobile.iterrows():
        u_id = row['u_id']
        h_tz = row['h_tz']

        matching_rows = lib_temp_final[lib_temp_final['u_id'] == u_id]

        for _, match in matching_rows.iterrows():
            if abs(match['h_tz_principal'] - h_tz) <= pd.Timedelta(minutes=1):
                temp_final_mobile.at[index, 'h_tz_principal'] = match['h_tz_principal']
                temp_final_mobile.at[index, 'raio(m)_principal'] = match['raio(m)_principal']
                temp_final_mobile.at[index, 'coordenadas_principal'] = match['coordenadas_principal']
                temp_final_mobile.at[index, 'ip_principal'] = match['ip_principal']
                temp_final_mobile.at[index, 'marca'] = match['marca']
                temp_final_mobile.at[index, 'device'] = match['device']
                temp_final_mobile.at[index, 'modelo'] = match['modelo']
                temp_final_mobile.at[index, 'modelo_ios'] = match['modelo_ios']
                temp_final_mobile.at[index, 'nome_dispositivo'] = match['nome_dispositivo']
                break

    return temp_final_mobile


# %% 
# FUNÇÃO REMOVE FUZO
def remove_fuzo(dataframe):
    """Remove informações de fuso horário das colunas de data e hora de um DataFrame."""
    lista_temp_copy = pd.DataFrame()
    lista_temp_copy = dataframe.copy()
    for col in dataframe.columns:
        if pd.api.types.is_datetime64_any_dtype(lista_temp_copy[col]):
            lista_temp_copy[col] = lista_temp_copy[col].apply(lambda x: x.replace(tzinfo=None) if pd.notnull(x) else x)
    return lista_temp_copy


# %% 
# FUNÇÃO PARA AJUSTAR COORDENADAS APÓS UNIÃO DE DATAFRAMES
def ajustar_coordenadas_apos_uniao(df):
    """Ajusta as coordenadas e converte para formato correto após união de DataFrames."""
    df['coordenadas_temporario'] = df['coordenadas_temporario'].apply(
        lambda x: tuple(x) if isinstance(x, list) else x)
    df['coordenadas_principal'] = df['coordenadas_principal'].apply(
        lambda x: tuple(x) if isinstance(x, list) else x)
    return df


# %% 
# FUNÇÃO PARA INVERTER COORDENADAS APÓS UNIÃO DE DATAFRAMES
def ajustar_coordenadas_apos_uniao_2(df):
    """Inverte as coordenadas (latitude e longitude) após a união de DataFrames."""
    df['coordenadas_temporario'] = df['coordenadas_temporario'].apply(
        lambda x: [x[1], x[0]] if x else x)
    df['coordenadas_principal'] = df['coordenadas_principal'].apply(
        lambda x: [x[1], x[0]] if x else x)
    return df


# %% 
# FUNÇÃO QUE VERIFICA SE O COMPARTILHAMENTO FOI FEITO POR WINDOWS E MOBILE																  
def verificar_acessos_dual(resultado, ranking_sheets):
    """Verifica se os acessos foram feitos tanto por dispositivos Windows quanto Mobile."""
    # Cálculo dos UIDs únicos para Windows e Mobile
    acessos_ranking_windows = ranking_sheets[ranking_sheets['Quantidade de Acessos (Windows)'] >= 1]
    unique_uids_windows = set(acessos_ranking_windows['u_id'].unique())
    
    acessos_ranking_mobile = ranking_sheets[ranking_sheets['Quantidade de Acessos (Mobile)'] >= 1]
    unique_uids_mobile = set(acessos_ranking_mobile['u_id'].unique())
    
    # Função auxiliar para verificar o tipo de dispositivo
    def dispositivo(uid, bloqueio):
        uid_str = str(uid)
        if pd.isna(bloqueio): # Acesso Windows
            return uid_str in unique_uids_mobile
        else: # Acesso Mobile
            return uid_str in unique_uids_windows

    # Aplicando a função auxiliar para cada linha do dataframe 'resultado'
    resultado['Ambos Dispositivos (W+M)'] = resultado.apply(lambda row: 'Sim' if dispositivo(row['u_id'], row['bloqueio']) else 'Não', axis=1)
    
    return resultado


# %% 
# FUNÇÃO PARA DETERMINAR PRIORIDADE COM BASE EM DISPOSITIVOS USADOS
def determine_priority(resultado, uid):
    """Determina a prioridade do cliente com base nos dispositivos usados (Windows e Mobile)."""
    # Extrair todos os valores de 'Ambos Dispositivos (W+M)' para o dado 'u_id'
    values = resultado[resultado['u_id'] == uid]['Ambos Dispositivos (W+M)']
    values_2 = resultado[resultado['u_id'] == uid]['bloqueio']
    
    # Definir a prioridade (1 para 'Sim', 2 para NaN, 3 para 'Não')
    if 'Sim' in values.values:
        return 1
    elif ~values_2.isnull().any():
        return 2
    else:
        return 3
	

# %% 
# FUNÇÃO PARA GERAR E SALVAR MAPA COM FOLIUM										  
def gerar_mapa(df, caminho, nome_arquivo):
    """Gera e salva um mapa com base nos dados de coordenadas em um DataFrame."""

    def random_color():
        return '#' + ''.join([random.choice('0123456789ABCDEF') for j in range(6)])

    color_map = {u_id: random_color() for u_id in df['u_id'].unique()}
    mapa = folium.Map(location=[-15.788497, -47.879873], zoom_start=5)

    for _, row in df.iterrows():
        temp_coord = row['coordenadas_temporario']
        main_coord = row['coordenadas_principal']

        # ADICIONAR MARCADOR TEMPORÁRIO
        try:
            folium.Marker(
                temp_coord,
                popup=f"<b>Temporário</b><br>u_id: {row['u_id']}<br>IP: {row['ip']}",
                icon=folium.Icon(color="red")
            ).add_to(mapa)
        except:
            folium.Marker(
                temp_coord,
                popup=f"<b>Temporário</b><br>u_id: {row['u_id']}",
                icon=folium.Icon(color="red")
            ).add_to(mapa)
            
        # ADICIONAR MARCADOR PRINCIPAL
        try:
            folium.Marker(
                main_coord,
                popup=f"<b>Principal</b><br>u_id: {row['u_id']}<br>IP Principal: {row['ip_principal']}",
                icon=folium.Icon(color="blue")
            ).add_to(mapa)
        except:
            folium.Marker(
                main_coord,
                popup=f"<b>Principal</b><br>u_id: {row['u_id']}",
                icon=folium.Icon(color="blue")
            ).add_to(mapa)
        
        # LINHA CONECTANDO OS MARCADORES COM COR BASEADA NO 'u_id'
        line_color = color_map[row['u_id']]
        folium.PolyLine([temp_coord, main_coord], color=line_color).add_to(mapa)

    mapa.save(f"{caminho}/{nome_arquivo}.html")


# %% 
# FUNÇÃO DE ATUALIZAÇÃO DE RANKING	
def atualizar_ranking(resultado, ranking):
    """Atualiza o ranking de usuários com base nos resultados de acessos recentes."""
    novas_linhas = []
    ranking_novo = ranking.copy()
    for _, row in resultado.iterrows():
        u_id = row['u_id']
        if u_id in ranking_novo['u_id'].values:
            # Certifique-se de trabalhar com uma cópia para evitar SettingWithCopyWarning
            ranking_row = ranking_novo.loc[ranking_novo['u_id'] == u_id].copy()

            # CONVERTA E ATUALIZE IPS DIFERENTES (WINDOWS)
            if 'Ips_diferentes' in resultado.columns and pd.notnull(row['Ips_diferentes']) and row['Ips_diferentes'] != 0:
                ranking_novo.loc[ranking_novo['u_id'] == u_id, 'Ips Diferentes'] = ranking_row['Ips Diferentes'].fillna(0).astype(int) + 1

            # ATUALIZAR AMBOS DISPOSITIVOS (W+M)
            if ranking_row['Ambos Dispositivos (W+M)'].iloc[0] == 'Não' and row['Ambos Dispositivos (W+M)'] == 'Sim':
                ranking_novo.loc[ranking_novo['u_id'] == u_id, 'Ambos Dispositivos (W+M)'] = 'Sim'
            
            # ATUALIZA QUANTIDADE DE ACESSOS (WINDOWS)
            if 'bloqueio' in resultado.columns and pd.isnull(row['bloqueio']):
                ranking_novo.loc[ranking_novo['u_id'] == u_id, 'Quantidade de Acessos (Windows)'] = ranking_row['Quantidade de Acessos (Windows)'].fillna(0).astype(int) + 1
                
            # ATUALIZA QUANTIDADE DE ACESSOS (MOBILE)
            if 'bloqueio' in resultado.columns and pd.notna(row['bloqueio']):
                ranking_novo.loc[ranking_novo['u_id'] == u_id,'Quantidade de Acessos (Mobile)'] = ranking_row['Quantidade de Acessos (Mobile)'].fillna(0).astype(int) + 1
            
        else:
            nova_linha = {
                'u_id': u_id,
                'Nome': row['name'],
                'Ips Diferentes': int(row['Ips_diferentes']) if 'Ips_diferentes' in resultado.columns and pd.notnull(row['Ips_diferentes']) else 0,
                'Quantidade de Acessos (Windows)': 1 if pd.isnull(row['bloqueio']) else 0,
                'Quantidade de Acessos (Mobile)': 1 if pd.notna(row['bloqueio']) else 0,
                'Ambos Dispositivos (W+M)': row['Ambos Dispositivos (W+M)'],
                'Quantidade de Dispositivos': int(row['Quantidade de Dispositivos']) if 'Quantidade de Dispositivos' in resultado.columns and pd.notnull(row['Quantidade de Dispositivos']) else 2,
            }
            novas_linhas.append(nova_linha)
    
    if novas_linhas:
        novas_linhas_df = pd.DataFrame(novas_linhas)
        ranking_novo = pd.concat([ranking_novo, novas_linhas_df], ignore_index=True)

    return ranking_novo