# Arquitetura — Produto Tools 3.0

## Camadas

- `login_app.py` e `pages/`: interface Streamlit e orquestração de casos de uso.
- `components/flow_editor/`: componente bidirecional V2, renderer e interação do canvas.
- `schemas/`: normalização retrocompatível e validação estrutural.
- `services/flowchart_repository.py`: persistência, revisão otimista, governança, comentários, templates e presença.
- `services/flow_analytics.py`: análise de grafo, qualidade, RACI e indicadores.
- `services/flow_diff.py`: comparação de documentos e versões.
- `services/report_export.py`: PDF, HTML e CSV.
- `database.py`: conexão compartilhada, usuários e índices do Atlas.

## Persistência

O documento atual é mantido em `produto_tools_flowcharts`. Uma versão formal é criada somente no salvamento manual, restauração ou operação equivalente. Movimentos intermediários são armazenados por usuário em `produto_tools_flowchart_drafts`.

Cada documento principal possui `revision`. O cliente envia a revisão que carregou. O update somente é aceito quando a revisão ainda coincide. Caso contrário, o editor apresenta as alternativas de recarregar, criar cópia ou sobrescrever como proprietário.

## Governança

As transições são validadas no backend. Uma versão publicada permanece identificada por `published_version`. Ao editar um processo publicado, o estado volta a rascunho, sem apagar a referência da publicação anterior.

## Compatibilidade

`normalize_document` converte documentos anteriores para schema 2.0, adicionando metadados de nível, categoria, criticidade, RACI, subprocesso vinculado e novas configurações sem remover campos desconhecidos.

## Segurança

- Secrets somente no Streamlit Cloud ou ambiente.
- Senhas bcrypt permanecem na coleção compartilhada `users`.
- Permissões são verificadas novamente no backend.
- O último administrador ativo não pode ser removido.
- Comentários, aprovações e alterações relevantes são auditados.
- Presença usa TTL e não funciona como bloqueio exclusivo.
