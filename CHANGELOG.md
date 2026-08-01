# Changelog

## 2.2.0 — 2026-08-01

- Propriedades transferidas da lateral direita para um inspetor horizontal superior.
- Canvas ampliado para utilizar toda a largura disponível da página.
- Paleta de elementos e inspetor de propriedades recolhíveis.
- Botão de tela cheia ao lado de Enquadrar.
- Tema claro e escuro para as páginas e para o editor.
- Correção do destaque colorido em nós arredondados e não retangulares.
- Destaque de caminho a partir do elemento ou conexão selecionada.
- Reprodução animada do caminho até o fim, com pausa, parada e velocidade.
- Acompanhamento automático da etapa atual durante a reprodução.

## 2.1.0 — 2026-08-01

- Migração completa do SQLite para MongoDB Atlas.
- Uso do banco `simulador_db` e da coleção compartilhada `users`.
- Compatibilidade com username, bcrypt, perfis e usuários ativos do Simulador-Telemetria.
- Autenticação com `streamlit-authenticator` e cookie compartilhado por configuração.
- Novas coleções `produto_tools_flowcharts` e `produto_tools_flowchart_versions`.
- Histórico e restauração de versões no MongoDB.
- Logs identificados com `application=produto_tools`.
- Gestão de Acesso atualizada para usuários compartilhados.
- Remoção das páginas Consulta Sigyo, Consulta Logpay e Análise de Arredondamento.

## 2.0.0 — 2026-08-01

- Nova arquitetura modular em `core`, `services`, `schemas` e `components`.
- Novo Editor de Processos com canvas interativo, raias, JSON e versões.
