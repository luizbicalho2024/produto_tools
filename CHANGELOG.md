# Changelog

## 4.1.1

- Adiciona download de modelo de importação WBS em Excel e CSV.
- Excel inclui exemplos, instruções, valores aceitos e validação de campos.
- O mesmo modelo baixado pode ser preenchido e reenviado no importador WBS.


## 4.1.0 — WBS / EAP

- nova página exclusiva `WBS — Work Breakdown Structure`;
- CRUD completo de WBS e pacotes hierárquicos;
- vínculo da WBS aos projetos existentes;
- visão gráfica Graphviz, tabela, outline e indicadores;
- importação JSON/XML/Microsoft Project XML/Excel/CSV/TSV/TXT/Markdown/PDF;
- exportação JSON/XML/Excel/CSV/TSV/PDF/Markdown/TXT/ZIP;
- controle de revisão otimista e trilha de auditoria;
- coleção MongoDB dedicada `produto_tools_wbs`;
- testes de hierarquia, formatos, exportação e repositório.

## 4.0.0 — Enterprise Suite

- Nova Central de Melhoria: SIPOC, 5W2H, FMEA/riscos, Ishikawa, 5 Porquês, Pareto e VSM.
- Simulação What-if, custos e cockpit de SLA.
- Process Mining, variantes e conformance checking.
- Importação/exportação BPMN 2.0 e tabelas DMN.
- Form Builder, instâncias e Central de Tarefas.
- Change Requests, compliance, sistemas, capacidades, Customer Journey e evidências.
- Diff visual de versões e Portal de Processos Publicados.
- Copiloto, texto para fluxo, gerador POP/SOP, webhooks e notificações.
- MFA TOTP e autenticação OIDC opcional.
- Consultas em lote na Central e catálogo do Editor para reduzir N+1.
- Soft delete de usuários, senha mínima de 12 caracteres e segregação de edição/revisão.
- CI ampliado com cobertura, lint crítico e verificações de segurança.


## 3.2.6.1

- Corrige falso negativo do publicador no Windows PowerShell 5.1 durante a validação de remoção do autosave.
- A validação de recursos agora é executada por arquivo Python temporário, evitando perda de aspas em argumentos nativos enviados com `python -c`.
- Nenhuma regra funcional do editor 3.2.6 foi alterada nesta revisão; trata-se de hotfix do processo de publicação.

## 3.2.5 — Persistência de edição e PDF legível

- Proteção síncrona de cada alteração no armazenamento local do navegador.
- Restauração do rascunho local baseada em fluxo + usuário + projeto + revisão, sem depender de relógio entre navegador e servidor.
- Correção do problema em que um rerun do Streamlit podia remontar o editor com o último rascunho do MongoDB e desfazer a edição recém-feita.
- PDF de fluxo com mensagens/descrições dentro dos cards.
- Fluxos densos recebem páginas adicionais de detalhe por raia para leitura confortável.
- Criticidade Alta/Crítica posicionada acima do card, sem cobrir título ou descrição.

## 3.2.4 — Central de downloads e exportação PDF

- centraliza as exportações do fluxo em um único painel;
- adiciona PDF vetorial contendo somente o diagrama completo;
- adiciona PDF de documentação completa com fluxo, cards, decisões, conexões, raias, qualidade, correções, RACI e configurações;
- adiciona pacote ZIP com JSON, PDFs, HTML e CSVs;
- inclui versão e revisão nos nomes dos arquivos exportados;
- mantém SVG e PNG no menu visual do canvas, agora identificado como Exportar visual.

## 3.2.3 — Raias dinâmicas, edição em grupo e decisões semânticas

- raias aumentam ou diminuem automaticamente de acordo com os cards e evitam sobreposição vertical;
- cards desbloqueados são distribuídos em linhas internas quando ocupam a mesma faixa horizontal;
- seleção múltipla por Ctrl/Shift + clique, área de seleção e Ctrl+A;
- arraste, alinhamento, distribuição, duplicação e exclusão em grupo;
- aviso antes de sair do editor quando há alterações locais ainda não sincronizadas com o MongoDB;
- opções para continuar editando, sair mantendo o rascunho local ou salvar o rascunho no banco antes de sair;
- conexões de decisões positivas em verde, negativas em vermelho e neutras em cinza;
- semântica preservada no editor, SVG, PNG e Mapa de Relações;
- pan do canvas com espaço, botão central ou botão direito do mouse;
- correção ampliada dos inputs, selects, datas, campos desabilitados e placeholders do sidebar no tema escuro;
- mapa de relações passa a exibir setas de direção e cores semânticas das decisões.

## 3.2.2 — Legibilidade de conexões, mapa e qualidade acionável

- indicadores permanentes de entradas e saídas em cada card;
- setas das conexões posicionadas fora do card e origem marcada por terminal visual;
- downloads do canvas e relatórios agrupados em menus;
- problemas identificados com linguagem simples, impacto e orientação de correção;
- mapa de relações com tela cheia, filtros por tipo e fluxo, destaque e isolamento;
- qualidade consolidada detalhando cards afetados e o problema específico;
- Central de Processos com detalhes acionáveis de qualidade.

## 3.2.1 — Compatibilidade do seletor de tema e login

- Corrige o `TypeError` no login causado por incompatibilidade entre chamadas com `compact=True` e versões anteriores de `core.styles`.
- Remove o argumento `compact` das chamadas do login e da barra lateral.
- Mantém o parâmetro aceito na função para compatibilidade regressiva.
- Substitui o seletor por um controle simples Claro/Escuro com persistência por sessão, URL e perfil.
- Adiciona teste de regressão para impedir novas chamadas incompatíveis.

## 3.2.0 — Importação resiliente, tema persistente e mapa de relações

- Corrige a importação de decisões com zero ou uma saída sem inventar regras de negócio.
- Converte automaticamente essas decisões inconsistentes em atividades e registra aviso de reparo.
- Corrige os fluxos auxiliares SIGYO que continham decisões de saída única.
- Adiciona exclusão permanente de fluxo com limpeza opcional das referências pai-filho.
- Mantém a exclusão de projeto com opção de preservar ou excluir seus fluxos.
- Simplifica a página de login e remove os textos promocionais.
- Corrige ícones inválidos em `st.page_link` e elimina o uso desse componente no login.
- Adota nomes ASCII estáveis para todas as páginas usadas por `st.switch_page`.
- Persiste o tema claro/escuro no MongoDB e no parâmetro da URL.
- Amplia o contraste de inputs, selects, menus, modais e controles no modo escuro.
- Adiciona seletor global de traçado: suave, reto, ortogonal ou corredores.
- Adiciona a página Mapa de Relações, com grafo de força inspirado no Obsidian.
- Inclui busca, zoom, pan, arraste, explosão, pausa e destaque de vizinhança no grafo.
- Atualiza testes de importação, navegação, tema e integridade do pacote SIGYO.

## 3.1.0 — Projetos e fluxos vinculados

- Nova coleção e tela de Gestão de Projetos.
- Mapa visual de dependências entre fluxos.
- Visões executiva, operacional, auxiliar e de apoio por projeto.
- Abas internas para alternar entre fluxos do mesmo projeto.
- Navegação pai-filho por subprocessos vinculados.
- Busca global em todos os fluxos do projeto.
- Foco automático no card encontrado.
- Execução guiada entre fluxos.
- Análise de impacto de alterações.
- Validação de vínculos quebrados, entradas, saídas, ciclos e órfãos.
- Releases consolidadas com versões e revisões fixadas.
- Importação e exportação de pacote `project.zip`.
- Importação simultânea de vários JSONs.
- Pacote de exemplo do SIGYO Modular com sete fluxos.
- Rascunho local isolado por projeto, usuário, fluxo e revisão.
- Novos índices MongoDB para projetos, participantes e releases.
- Central de Processos com filtro e coluna de projeto.
- Testes automatizados para projetos, vínculos, busca, releases e pacotes.

## 3.0.3 — Estabilidade do editor

- Autosave local silencioso.
- Correções de tema escuro.
- Layout automático para fluxos grandes.
- Roteamento por corredores.
- Correções dos filtros de visão.

## 3.2.4.1

- Corrige o publicador PowerShell no Windows quando o ambiente local possui pytest, mas nao possui todas as dependencias do projeto.
- Captura e exibe a saida completa de comandos Python/Git em caso de falha.
- Testes locais passam a ser nao bloqueantes por padrao; use `-StrictTests` para bloquear o push em caso de falha da suite.
- Mantem `-SkipTests` para pular completamente as validacoes locais.
