# Validação — Produto Tools 3.1

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
- criação de projetos;
- associação de fluxos ao projeto;
- detecção de vínculos válidos e quebrados;
- busca global;
- rota entre fluxos;
- análise consolidada;
- criação de release;
- exportação e reimportação do pacote do projeto;
- remapeamento de IDs em conflitos;
- fluxo SIGYO com visão executiva, visão completa e cinco auxiliares.

## Verificação manual recomendada

1. importar `examples/sigyo_modular_project.zip`;
2. abrir o mapa do projeto;
3. abrir um subprocesso por duplo clique;
4. voltar ao fluxo pai;
5. alternar entre abas;
6. pesquisar `empenho`;
7. iniciar execução guiada;
8. criar uma release;
9. baixar e reimportar o pacote;
10. confirmar que os vínculos continuam válidos.
