# Validação — Produto Tools 3.2.6

## Comandos

```bash
python -m compileall -q .
pytest -q
node --check components/flow_editor/frontend/main.js
```

## Casos cobertos

- criação e atualização de fluxos;
- concorrência por revisão;
- rascunhos, comentários e governança;
- criação e exclusão de projetos;
- associação, desvinculação e exclusão de fluxos;
- reparo seguro de decisões importadas com zero ou uma saída;
- importação dos sete fluxos SIGYO;
- detecção de vínculos válidos e quebrados;
- busca global e rota entre fluxos;
- criação de release e reimportação do pacote;
- persistência do tema por usuário;
- existência das páginas ASCII usadas pelo `st.switch_page`;
- seletor global de traçado;
- página de mapa de relações com física local;
- ajuste dinâmico de altura das raias;
- seleção, arraste e organização de vários cards;
- aviso de navegação com alterações pendentes;
- classificação semântica das saídas de decisões;
- pan pelo botão direito do mouse;
- contraste dos controles do sidebar no modo escuro.

## Verificação manual recomendada

1. importar `examples/sigyo_modular_project.zip`;
2. importar o JSON original de Assinatura e Onboarding e conferir os dois avisos de reparo;
3. alternar entre tema claro e escuro, sair e entrar novamente;
4. testar inputs, selects, menus e modais nos dois temas;
5. abrir Gestão de Projetos e excluir um fluxo de teste;
6. excluir um projeto de teste preservando seus fluxos;
7. abrir o mapa de relações e usar busca, explosão, zoom e arraste;
8. trocar o traçado do editor entre suave, reto, ortogonal e corredores;
9. validar os vínculos e criar uma release;
10. criar/mover cards na mesma raia e conferir expansão/redução sem reorganização global inesperada;
11. selecionar vários cards com Ctrl/Shift e arrastá-los como grupo;
12. editar nome/descrição de card e raia, usar Backspace/Delete dentro dos campos e confirmar que nenhum elemento é excluído;
13. alterar um card, forçar um rerun do Streamlit e confirmar que a edição em andamento continua na tela;
14. confirmar que nenhum rascunho antigo é carregado automaticamente e testar **Carregar rascunho** manualmente;
15. conferir saídas Sim/Não em verde/vermelho no editor, exportação e mapa;
16. mover o canvas com o botão direito do mouse;
17. baixar e reimportar o pacote.


## Validação 3.2.6

- salvamento automático removido do frontend, componente Python e fluxo da página Streamlit;
- `autosaveSeconds` removido da normalização e dos exemplos;
- rascunho MongoDB carregado apenas mediante ação explícita;
- cache de trabalho é volátil, apenas em memória da aba, e não usa `localStorage`;
- proteção de teclado considera `input`, `textarea`, `select`, `contenteditable`, `role=textbox` e `role=combobox`;
- campos textuais são atualizados por `input` sem reconstruir o painel de propriedades durante a digitação;
- abertura do fluxo não chama auto-layout nem redimensionamento automático;
- inserção, duplicação e arraste resolvem sobreposição somente nos cards afetados;
- redimensionamento de raia mantém a posição relativa das raias seguintes e é marcado como alteração;
- `python -m compileall -q .`: aprovado;
- `node --check components/flow_editor/frontend/main.js`: aprovado;
- suíte automatizada: **38 aprovados e 2 opcionais ignorados**.

A regressão visual em Chromium não pôde ser executada neste ambiente porque a política do navegador bloqueia URLs locais (`file://` e `127.0.0.1`). A validação visual final deve ser feita no Streamlit Cloud após o deploy.

## Validação 3.2.5

- persistência local imediata testada por inspeção de fonte e suíte automatizada;
- restauração do rascunho da mesma revisão não depende mais de comparação entre relógio do navegador e do servidor;
- PDF do fluxo SIGYO de 153 cards gerou 22 páginas: 1 visão geral e 21 páginas de detalhe por raia;
- renderização visual do PDF validada em PNG, com mensagens dos cards legíveis e sem sobreposição;
- badge de criticidade Alta/Crítica posicionado acima do card, fora da área de texto;
- suíte automatizada: 31 aprovados e 2 opcionais ignorados no ambiente de validação.
