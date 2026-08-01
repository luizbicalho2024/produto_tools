# Changelog

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
