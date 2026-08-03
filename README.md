# Produto Tools 3.2.3 Professional

Aplicação Streamlit para modelagem, governança e publicação de processos, com autenticação compartilhada, persistência no MongoDB Atlas e projetos compostos por vários fluxos vinculados.


## Novidades da versão 3.2.3

- raias automáticas aumentam, diminuem e reorganizam verticalmente os cards para evitar sobreposição;
- seleção múltipla por Ctrl/Shift + clique, Shift + arrastar no fundo e Ctrl+A;
- movimento, alinhamento, distribuição, duplicação e exclusão de cards em grupo;
- confirmação de navegação quando há rascunho local ainda não sincronizado com o MongoDB;
- saídas positivas de decisões em verde, negativas em vermelho e não classificadas em cinza;
- cores semânticas também no SVG, PNG e Mapa de Relações;
- pan do fluxo com espaço, botão central ou botão direito do mouse;
- correção reforçada dos controles do sidebar no tema escuro;
- mapa de relações com setas explícitas de direção.

## Correção 3.2.1 — Compatibilidade do login

O seletor Claro/Escuro não depende mais do argumento `compact` nas páginas, evitando falhas quando o deploy contém uma versão anterior do módulo de estilos. A preferência continua persistida na URL e no perfil do usuário.

## Novidades da versão 3.2

- importação resiliente: decisões sem ramificações reais são convertidas em atividades, sem criar condições de negócio artificiais;
- exclusão visível de fluxos e projetos;
- login compacto, sem textos promocionais;
- tema claro/escuro persistido por usuário e também na URL;
- correção de contraste nos inputs e menus do modo escuro;
- caminhos de páginas sem acentuação para navegação estável no Streamlit Cloud;
- traçado global das conexões em curvas suaves, linhas retas, ortogonal simples ou corredores;
- nova visualização de relações inspirada no grafo do Obsidian.

## Gestão de projetos

Um projeto pode reunir uma visão executiva, uma visão operacional e diversos fluxos auxiliares. Cada fluxo continua independente, com seu próprio rascunho, revisão, versão, comentários e governança.

Recursos:

- mapa visual das dependências entre fluxos;
- abertura de subprocessos por duplo clique;
- navegação de volta ao fluxo pai;
- abas internas para alternar entre fluxos;
- busca global por cards, responsáveis, tags, raias e IDs;
- execução guiada entre fluxos;
- análise de impacto de alterações;
- detecção de vínculos quebrados, ciclos e fluxos órfãos;
- releases consolidadas, fixando a revisão de cada fluxo;
- importação e exportação em pacote `project.zip`;
- importação simultânea de vários JSONs.

## Importação resiliente

O schema continua exigindo duas ou mais saídas para uma decisão legítima. Na importação, caso um arquivo traga uma decisão com zero ou uma saída, o sistema:

1. normaliza o documento;
2. converte o elemento para atividade;
3. preserva sua conexão existente;
4. adiciona a tag `Importação corrigida`;
5. informa o reparo ao usuário.

Nenhuma saída, condição ou regra de negócio é inventada automaticamente.

## Traçado das conexões

O seletor **Traçado** no rodapé do canvas aplica o mesmo estilo a todo o fluxo:

- curvas suaves;
- linhas retas;
- ortogonal simples;
- corredores inteligentes;
- corredores simples.

## Mapa de relações

A página `Mapa de Relações` apresenta um grafo de força local no navegador, inspirado no Obsidian. Ela permite:

- visualizar somente os fluxos do projeto;
- visualizar os cards de um fluxo;
- combinar fluxos e cards;
- arrastar nós;
- mover e ampliar o canvas;
- explodir a rede;
- pausar a física;
- pesquisar elementos;
- destacar a vizinhança de um nó;
- abrir o fluxo selecionado no editor.

## Estrutura de um pacote de projeto

```text
sigyo_modular_project.zip
├── project.json
├── README.txt
└── flows/
    ├── flow_sigyo_modular_simplificado_aprovacao.json
    ├── flow_sigyo_modular_completo_aprovacao_comercial.json
    ├── flow_sigyo_aux_proposta_aprovacao.json
    ├── flow_sigyo_aux_assinatura_onboarding.json
    ├── flow_sigyo_aux_provisionamento_modular.json
    ├── flow_sigyo_aux_faturamento_empenho.json
    └── flow_sigyo_aux_ciclo_vida_contratual.json
```

O projeto inclui `examples/sigyo_modular_project.zip`, pronto para importação.

## Vínculo entre fluxos

```json
{
  "linkedFlowId": "flow_sigyo_aux_proposta_aprovacao",
  "linkedFlowEntryNodeId": "p_start",
  "linkedFlowExitNodeId": "p_end_aceita"
}
```

## Rascunhos isolados

O rascunho local considera:

```text
projeto + usuário + fluxo + revisão
```

## Editor visual

- raias horizontais;
- decisões interativas;
- layout automático para fluxos grandes;
- cinco estilos globais de conexão;
- busca dentro do canvas;
- filtros executivo, operacional, técnico, exceções e raia;
- Play a partir de qualquer card ou raia;
- modo claro e escuro persistente;
- tela cheia, minimapa, zoom e enquadramento;
- exportação JSON, SVG, PNG, PDF, HTML e CSV;
- autosave local silencioso e sincronização explícita no MongoDB.

## Exclusão

- **Fluxo:** Gestão de Projetos → Fluxos → Excluir fluxo permanentemente. O sistema pode remover referências de subprocessos antes da exclusão.
- **Projeto:** Gestão de Projetos → Configurações e exclusão → Excluir projeto. É possível preservar os fluxos como avulsos ou excluí-los também.

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

## Estrutura

```text
produto_tools/
├── login_app.py
├── database.py
├── requirements.txt
├── atualizar_produto_tools_v3_2_2_main.ps1
├── components/
│   └── flow_editor/
├── core/
├── docs/
├── examples/
├── pages/
│   ├── 1_Gestao_de_Acesso.py
│   ├── 2_Central_de_Processos.py
│   ├── 3_Gestao_de_Projetos.py
│   ├── 4_Mapa_de_Relacoes.py
│   └── 5_Editor_de_Fluxos.py
├── schemas/
├── services/
└── tests/
```

## Streamlit Cloud

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

Testes:

```bash
pip install -r requirements-dev.txt
pytest -q
node --check components/flow_editor/frontend/main.js
```

## Publicação no GitHub

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force
Unblock-File .\atualizar_produto_tools_v3_2_2_main.ps1
.\atualizar_produto_tools_v3_2_2_main.ps1
```

Sem testes locais:

```powershell
.\atualizar_produto_tools_v3_2_2_main.ps1 -SkipTests
```
