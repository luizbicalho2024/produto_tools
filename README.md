# Produto Tools 3.0 Professional

Plataforma Streamlit para modelagem, simulação, governança e documentação de processos. Utiliza os mesmos usuários do **Simulador-Telemetria** e grava fluxos, versões, rascunhos, comentários, aprovações, templates e presença no MongoDB Atlas.

## Principais recursos

### Modelagem e navegação

- Canvas amplo, grade, zoom, minimapa, tela cheia, modo claro e escuro.
- Raias redimensionáveis, recolhíveis e reproduzíveis.
- Elementos de início, fim, atividade, decisão, subprocesso, evento, espera, documento, API e observação.
- Decisões com no mínimo duas saídas independentes e condições nomeadas.
- Subprocessos vinculados a outros fluxos; duplo clique abre o detalhe e mantém navegação de retorno.
- Busca global por nome, descrição, responsável, tag, categoria, ID ou raia.
- Visões completa, executiva, operacional, técnica, exceções e raia selecionada.
- Filtros de linhas: todas, relacionadas à seleção, somente entre raias ou ocultas.

### Layout e fluxos extensos

- Mundo e zoom dinâmicos para processos com milhares de pixels.
- Auto-organização por camadas e raias.
- Modos compacto, legível e preservação das posições.
- Ordenação por vizinhança para reduzir cruzamentos.
- Roteamento ortogonal em corredores, com separação das entradas e saídas.
- Importação do fluxo SIGYO Modular com mais de cem elementos usada como regressão automatizada.

### Rotas e simulação

- Destaque e Play a partir de qualquer card, conexão ou raia.
- Explorador de rotas com origem, destino e estratégia:
  - principal;
  - mais curta;
  - mais longa;
  - todos os caminhos entre origem e destino;
  - comparação das ramificações de uma decisão;
  - todas as etapas anteriores;
  - todas as etapas posteriores;
  - exceções posteriores.
- Simulação interativa: ao chegar a uma decisão, o Play pausa e solicita a escolha da condição.
- Pausar, continuar, parar, alterar velocidade e centralizar automaticamente a etapa atual.

### Governança e colaboração

- Estados: rascunho, em revisão, aprovado, publicado e arquivado.
- Perfis por fluxo: visualizador, editor, revisor e aprovador.
- Visibilidade privada ou para toda a organização.
- Histórico das transições e comentários da aprovação.
- Comentários em fluxo, card, conexão e raia, com menções e resolução.
- Presença temporária dos usuários que estão visualizando o processo.
- Controle de concorrência por revisão otimista; conflitos podem ser recarregados, salvos como cópia ou sobrescritos pelo proprietário.
- Rascunho automático no MongoDB sem criar uma versão formal para cada movimento.

### Versionamento, qualidade e relatórios

- Versões imutáveis e restauração como nova versão.
- Comparação entre versões com elementos, linhas e raias adicionados, removidos ou alterados.
- Índice de qualidade para estrutura, documentação, responsáveis, SLA e subprocessos.
- Indicadores de decisões, integrações, exceções, ciclos, transições entre raias e caminho mais longo.
- Exportações:
  - JSON;
  - SVG;
  - PNG;
  - PDF;
  - HTML;
  - CSV de etapas;
  - matriz RACI em CSV.
- Biblioteca de templates incorporados e templates personalizados.
- Central de Processos com portfólio, filtros, qualidade e pendências.
- Gestão de Acesso com diretório, métricas e auditoria.

## Coleções no MongoDB

```text
simulador_db
├── users                               compartilhada com Simulador-Telemetria
├── activity_logs                       auditoria compartilhada
├── produto_tools_flowcharts            estado atual dos processos
├── produto_tools_flowchart_versions    versões formais
├── produto_tools_flowchart_drafts      autosave por usuário
├── produto_tools_flowchart_comments    comentários por objeto
├── produto_tools_flowchart_approvals   histórico de governança
├── produto_tools_flowchart_templates   biblioteca personalizada
└── produto_tools_flowchart_presence    presença com expiração automática
```

Os documentos antigos da versão 1.0 são normalizados automaticamente para o schema 2.0 ao serem carregados e salvos. As coleções e os índices são criados automaticamente; não é necessário executar script de migração manual.

## Estrutura

```text
produto_tools/
├── login_app.py
├── database.py
├── requirements.txt
├── pages/
│   ├── 1_Gestão_de_Acesso.py
│   ├── 2_Central_de_Processos.py
│   └── 5_Editor_de_Fluxos.py
├── core/
├── schemas/
├── services/
│   ├── flow_analytics.py
│   ├── flow_diff.py
│   ├── flow_permissions.py
│   ├── flowchart_repository.py
│   ├── report_export.py
│   └── template_library.py
├── components/flow_editor/frontend/
├── tests/
└── .github/workflows/ci.yml
```

## Streamlit Cloud

Arquivo principal:

```text
login_app.py
```

Secrets necessários:

```toml
MONGO_CONNECTION_STRING = "mongodb+srv://USUARIO:SENHA@cluster.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
MONGO_DB_NAME = "simulador_db"

AUTH_COOKIE_NAME = "simulador_telemetria_auth"
AUTH_COOKIE_KEY = "A_MESMA_CHAVE_SEGURA_USADA_NO_SIMULADOR"
AUTH_COOKIE_EXPIRY_DAYS = 30
```

Não envie `.streamlit/secrets.toml`, `.env` ou credenciais ao GitHub.

## Execução local

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
streamlit run login_app.py
```

## Testes

```powershell
pip install -r requirements-dev.txt
python -m compileall -q .
node --check components/flow_editor/frontend/main.js
pytest -q
```

O workflow `.github/workflows/ci.yml` executa essas verificações em pushes e pull requests.

## Publicação

O arquivo `atualizar_produto_tools_v3_main.ps1`:

1. valida se o pacote está na raiz correta;
2. executa compilação, JavaScript e testes;
3. clona a `main` em uma pasta temporária;
4. preserva a pasta `.git`;
5. bloqueia Secrets, bancos e caches;
6. cria o commit;
7. envia para o GitHub;
8. interrompe imediatamente em caso de falha.
