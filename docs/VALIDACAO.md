# Validação — Produto Tools 3.2

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
- página de mapa de relações com física local.

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
10. baixar e reimportar o pacote.
