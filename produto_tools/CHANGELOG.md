# Changelog

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
