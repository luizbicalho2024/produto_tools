# Changelog

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
