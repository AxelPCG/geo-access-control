# FUNÇÕES DE UTILIDADES

# %%
#BIBLIOTECAS
import numpy as np
import ipaddress
from integrations import get_insights
import pandas as pd
dist_max_mobile = 0.1


# %%
# FUNÇÃO PARA CALCULAR A DISTÂNCIA ENTRE DUAS COORDENADAS
def haversine(coord1, coord2):
    """Calcula a distância em quilômetros entre duas coordenadas geográficas."""
    lon1, lat1 = map(np.radians, coord1)
    lon2, lat2 = map(np.radians, coord2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    km = 6371 * c  # Raio da Terra em km
    return km


# %%
# FUNÇÃO AUXILIAR PARA CALCULAR A DISTÂNCIA ENTRE DUAS COORDENADAS 
def calcular_distancia(df, col_coord1, col_coord2):
    """Calcula a distância entre duas colunas de coordenadas em um DataFrame."""
    df['distancia'] = df.apply(
        lambda row: haversine(row[col_coord1], row[col_coord2])
        if isinstance(row[col_coord1], list) and len(row[col_coord1]) == 2
        and isinstance(row[col_coord2], list) and len(row[col_coord2]) == 2
        else np.nan, axis=1)
    return df


# %%
# FUNÇÃO QUE EXTRAI O CONTEÚDO CORRETAMENTE DOS DADOS DO GEOIP
def info_ips(ip):
    try:
        # Lista para armazenar os resultados
        resultados_ips = []

        # Iterar sobre cada IP e coletar informações
        insight = get_insights(ip)
        if isinstance(insight, dict):  # Verificar se a resposta é um dicionário (sem erro)
            # Extração de campos de objetos complexos
            location_data = {f'location_{key}': value for key, value in insight['location'].__dict__.items() if key != '_extensions'}
            traits_data = {f'traits_{key}': value for key, value in insight['traits'].__dict__.items()}
            # Removendo os objetos complexos do dicionário original
            insight.pop('location')
            insight.pop('traits')
            # Atualizando o dicionário com os novos campos
            insight.update(location_data)
            insight.update(traits_data)
            # Adicionando ao resultado
            resultados_ips.append(insight)
            # Criar DataFrame a partir da lista de resultados
            df_resultado_ips = pd.DataFrame(resultados_ips)
        else:
            df_resultado_ips = insight
    except:
        df_resultado_ips = get_insights('')
    return df_resultado_ips


# %%
# FUNÇÃO PARA CALCULAR A CLASSE DO IP (IPv4)
def ip_class(ip):
    first_octet = int(ip.split('.')[0])
    if first_octet < 128:
        return 'A'
    elif first_octet < 192:
        return 'B'
    elif first_octet < 224:
        return 'C'
    else:
        return 'Outra'


# %%
# FUNÇÃO PARA COMPARAR IPs COM BASE NA CLASSE (IPv4)
def same_network_class_based(ip1, ip2):
    class_ip1 = ip_class(ip1)
    class_ip2 = ip_class(ip2)

    if class_ip1 != class_ip2:
        return False  # Diferentes classes

    ip1_parts = ip1.split('.')
    ip2_parts = ip2.split('.')

    if class_ip1 == 'A':
        return ip1_parts[0] == ip2_parts[0]
    elif class_ip1 == 'B':
        return ip1_parts[:2] == ip2_parts[:2]
    elif class_ip1 == 'C':
        return ip1_parts[:3] == ip2_parts[:3]
    else:
        return False  # Não cobrimos classes além de A, B, C


# %%
# FUNÇÃO PARA VERIFICAR SE DOIS IPs ESTÃO NA MESMA REDE
def same_network(ip1, ip2):
    """Verifica se dois IPs pertencem à mesma rede."""
    try:
        # Determinar se os IPs são IPv4 ou IPv6
        ip1_type = ipaddress.ip_address(ip1).version
        ip2_type = ipaddress.ip_address(ip2).version

        if ip1_type == 4 and ip2_type == 4:
            return same_network_class_based(ip1, ip2)
        elif ip1_type == 6 and ip2_type == 6:
            return ip1.split(':')[:4] == ip2.split(':')[:4]
        else:
            return False
    except ValueError:
        return False


# %%
# FUNÇÃO PARA VERIFICAR SE DUAS REDES SÃO IGUAIS
def verificar_redes_iguais(df, col_ip1, col_ip2):
    """Verifica se dois IPs pertencem à mesma rede."""
    df['same_network'] = df.apply(lambda row: same_network(row[col_ip1], row[col_ip2]), axis=1)
    return df


# %%
# FUNÇÃO QUE VERIFICA SE O DISPOSITIVO DEVE SER BLOQUEADO SE O ACESSO FOR CONSIDERADO COMPARTILHAMENTO
def func_bloqueio(raio_p, raio_t, dist):
    try:
        soma = (raio_p +raio_t+(dist_max_mobile*1000))/1000
        dist = dist
        if dist > soma:
            return True
        else:
            return False
    except ValueError:
        # Retorna False em caso de erro na conversão dos IPs
        return False