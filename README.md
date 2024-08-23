# Controle de Acesso Geográfico

## Descrição do Projeto

Este projeto implementa um sistema de controle de acesso geográfico, monitorando e registrando tentativas de acesso a partir de diferentes dispositivos (Windows e Mobile). O objetivo principal é identificar e analisar acessos temporários realizados a partir de locais geograficamente distantes ou próximos, aplicando regras de bloqueio conforme critérios estabelecidos. O sistema gera relatórios recorrentes e mapas de acessos, além de enviar notificações sobre eventos importantes.

## Estrutura de Arquivos

### `main.py`
- **Descrição**: Script principal que orquestra a execução do sistema.
- **Funções**:
  - Conexão com bancos de dados (PostgreSQL, MariaDB e MongoDB).
  - Coleta e processamento de dados de acessos temporários.
  - Verificação de conformidade com regras de distância.
  - Geração de relatórios e mapas.
  - Integração com o Slack para envio de notificações.

### `data_processing.py`
- **Descrição**: Conjunto de funções utilitárias para manipulação e processamento de dados.
- **Funções**:
  - Correção de codificação de strings.
  - Filtragem de clientes ativos.
  - Normalização de dados JSON.
  - Tratamento de dados de dispositivos temporários (desktop e mobile).
  - Geração de mapas com coordenadas geográficas.

### `db_connections.py`
- **Descrição**: Funções responsáveis por estabelecer conexões com bancos de dados.
- **Funções**:
  - Criação de conexões com bancos de dados PostgreSQL, MariaDB, e MongoDB.
  - Teste de conexão com bancos de dados.

### `integrations.py`
- **Descrição**: Gerencia integrações com serviços externos.
- **Funções**:
  - Envio de mensagens para o Slack.
  - Consulta de localização geográfica via GeoIP (MaxMind).
  - Leitura e escrita de dados em planilhas do Google Sheets.

### `utils.py`
- **Descrição**: Funções auxiliares para cálculos e verificações.
- **Funções**:
  - Cálculo de distâncias geográficas entre coordenadas.
  - Verificação se dois IPs pertencem à mesma rede.
  - Decisão de bloqueio de dispositivos com base na distância calculada.

### `requirements.txt`
- **Descrição**: Arquivo de dependências do projeto.
- **Função**: Lista pacotes Python necessários para rodar o projeto, como `pandas`, `sqlalchemy`, `pymongo`, `geoip2`, entre outros.

---

Este projeto utiliza uma abordagem de monitoramento baseada em localização para garantir a segurança de acessos temporários a partir de dispositivos móveis e desktops. A combinação de verificações de distância e integração com serviços externos como o Slack e o GeoIP permite uma análise precisa e uma resposta automatizada a acessos suspeitos.

