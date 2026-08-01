# Produto Tools 3.1 Professional

Aplicação Streamlit para modelagem, governança e publicação de processos, com autenticação compartilhada, persistência no MongoDB Atlas e suporte a **projetos compostos por vários fluxos vinculados**.

## Gestão de projetos

Um projeto pode reunir uma visão executiva, uma visão operacional e diversos fluxos auxiliares. Cada fluxo continua independente, com seu próprio rascunho, revisão, versão, comentários e governança.

Principais recursos:

- mapa visual das dependências entre fluxos;
- abertura de subprocessos por duplo clique;
- navegação de volta ao fluxo pai;
- até oito fluxos abertos como abas internas do projeto;
- busca global por cards, responsáveis, tags, raias e IDs;
- destaque automático do resultado dentro do fluxo correto;
- execução guiada entre fluxos;
- análise de impacto de alterações em fluxos auxiliares;
- detecção de vínculos quebrados, entradas ou saídas inexistentes, ciclos e fluxos órfãos;
- release consolidada, fixando a versão e revisão de cada fluxo;
- importação e exportação em pacote `project.zip`;
- importação simultânea de vários JSONs como um novo projeto.

## Estrutura de um pacote de projeto

```text
sigyo_modular_project.zip
├── project.json
└── flows/
    ├── flow_sigyo_modular_simplificado_aprovacao.json
    ├── flow_sigyo_modular_completo_aprovacao_comercial.json
    ├── flow_sigyo_aux_proposta_aprovacao.json
    ├── flow_sigyo_aux_assinatura_onboarding.json
    ├── flow_sigyo_aux_provisionamento_modular.json
    ├── flow_sigyo_aux_faturamento_empenho.json
    └── flow_sigyo_aux_ciclo_vida_contratual.json
```

O projeto inclui `examples/sigyo_modular_project.zip`, pronto para importação pela tela **Gestão de Projetos**.

## Vínculo entre fluxos

Cards do tipo subprocesso utilizam:

```json
{
  "linkedFlowId": "flow_sigyo_aux_proposta_aprovacao",
  "linkedFlowEntryNodeId": "p_start",
  "linkedFlowExitNodeId": "p_end_aceita"
}
```

O editor abre o fluxo vinculado, centraliza o card de entrada e mantém a pilha de navegação para retornar ao fluxo pai.

## Rascunhos isolados

O rascunho local utiliza a combinação:

```text
projeto + usuário + fluxo + revisão
```

Isso impede que alterações de abas ou fluxos diferentes sobrescrevam o rascunho atual.

## Editor visual

- raias horizontais;
- decisões com no mínimo duas saídas;
- layout automático para fluxos grandes;
- conexões ortogonais por corredores;
- busca dentro do canvas;
- filtros executivo, operacional, técnico, exceções e raia selecionada;
- Play a partir de qualquer card ou raia;
- decisões interativas durante a reprodução;
- modo claro e escuro;
- tela cheia, minimapa, zoom e enquadramento;
- exportação JSON, SVG, PNG, PDF, HTML e CSV;
- autosave local silencioso e sincronização explícita do rascunho no MongoDB.

## Governança

- rascunho;
- em revisão;
- aprovado;
- publicado;
- arquivado;
- comentários por fluxo, raia, card ou conexão;
- visualizador, editor, revisor e aprovador;
- controle otimista de concorrência por revisão;
- versões imutáveis e comparação de alterações;
- releases imutáveis do projeto.

## Coleções MongoDB

```text
simulador_db
├── users
├── activity_logs
├── produto_tools_projects
├── produto_tools_project_members
├── produto_tools_project_releases
├── produto_tools_project_release_flows
├── produto_tools_flowcharts
├── produto_tools_flowchart_versions
├── produto_tools_flowchart_drafts
├── produto_tools_flowchart_comments
├── produto_tools_flowchart_approvals
├── produto_tools_flowchart_templates
└── produto_tools_flowchart_presence
```

## Estrutura do projeto

```text
produto_tools/
├── login_app.py
├── database.py
├── requirements.txt
├── atualizar_produto_tools_v3_1_main.ps1
├── components/
│   └── flow_editor/
├── core/
├── docs/
├── examples/
│   ├── sigyo_modular_project.zip
│   └── sigyo_modular_project/
├── pages/
│   ├── 1_Gestão_de_Acesso.py
│   ├── 2_Central_de_Processos.py
│   ├── 3_Gestão_de_Projetos.py
│   └── 5_Editor_de_Fluxos.py
├── schemas/
├── services/
│   ├── flowchart_repository.py
│   └── project_repository.py
└── tests/
```

## Streamlit Cloud

Use:

```text
Branch: main
Main file path: login_app.py
```

Secrets mínimos:

```toml
MONGO_CONNECTION_STRING = "mongodb+srv://USUARIO:SENHA@cluster.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
MONGO_DB_NAME = "simulador_db"
AUTH_COOKIE_NAME = "simulador_telemetria_auth"
AUTH_COOKIE_KEY = "A_MESMA_CHAVE_USADA_NO_SIMULADOR"
AUTH_COOKIE_EXPIRY_DAYS = 30
```

## Execução local

```bash
python -m venv .venv
pip install -r requirements.txt
streamlit run login_app.py
```

Para testes:

```bash
pip install -r requirements-dev.txt
pytest -q
node --check components/flow_editor/frontend/main.js
```

## Publicação no GitHub

No Windows PowerShell 5.1:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force
Unblock-File .\atualizar_produto_tools_v3_1_main.ps1
.\atualizar_produto_tools_v3_1_main.ps1
```

Para ignorar testes locais e deixar a validação para o GitHub Actions:

```powershell
.\atualizar_produto_tools_v3_1_main.ps1 -SkipTests
```
