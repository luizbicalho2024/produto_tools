# Arquitetura — Produto Tools 3.1

## Camadas

```text
Streamlit
├── login e autenticação compartilhada
├── Central de Processos
├── Gestão de Projetos
└── Editor Custom Component V2

Serviços
├── flowchart_repository.py
├── project_repository.py
├── flow_analytics.py
├── flow_diff.py
└── report_export.py

MongoDB
├── projetos e participantes
├── releases consolidadas
├── fluxos e versões
├── rascunhos
├── comentários e aprovações
└── presença colaborativa
```

## Projeto

O projeto é a camada agregadora. Os fluxos permanecem documentos independentes e armazenam os campos:

```json
{
  "projectId": "project_sigyo_modular",
  "projectRole": "subprocess",
  "projectGroup": "Financeiro",
  "projectOrder": 6
}
```

A coleção `produto_tools_projects` armazena metadados, proprietário, participantes, visibilidade, fluxo inicial e release atual.

## Vínculos

As dependências são derivadas dos cards com `linkedFlowId`. O sistema não duplica a lista de vínculos no projeto; o mapa é recalculado a partir dos documentos atuais, evitando inconsistência entre duas fontes.

## Releases

Uma release registra, para cada fluxo:

- ID;
- versão;
- revisão;
- hash SHA-256;
- papel, grupo e ordem no projeto.

O pacote de uma release utiliza as versões imutáveis da coleção `produto_tools_flowchart_versions`.

## Rascunhos

No navegador, a chave inclui projeto, usuário, fluxo e revisão. No MongoDB, os rascunhos continuam indexados por fluxo e usuário e registram também `project_id`.

## Importação

O importador lê `project.json`, carrega `flows/*.json`, resolve conflitos de IDs e remapeia automaticamente `linkedFlowId` quando novos IDs são gerados.
