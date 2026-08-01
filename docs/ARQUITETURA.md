# Arquitetura — Produto Tools 3.2

## Camadas

```text
Streamlit
├── login compacto e autenticação compartilhada
├── Central de Processos
├── Gestão de Projetos
├── Mapa de Relações
└── Editor Custom Component V2

Serviços
├── flowchart_repository.py
├── project_repository.py
├── flow_analytics.py
├── flow_diff.py
└── report_export.py

MongoDB
├── usuários e preferência de tema
├── projetos e participantes
├── releases consolidadas
├── fluxos e versões
├── rascunhos
├── comentários e aprovações
└── presença colaborativa
```

## Projeto

O projeto é a camada agregadora. Os fluxos permanecem documentos independentes e armazenam os campos `projectId`, `projectRole`, `projectGroup` e `projectOrder`.

## Vínculos

As dependências são derivadas dos cards com `linkedFlowId`. O sistema calcula o mapa a partir dos documentos atuais, sem duplicar a relação em outra fonte.

## Importação resiliente

O schema exige ao menos duas saídas para uma decisão. Antes da validação de um arquivo importado, `repair_import_document` normaliza inconsistências seguras. Uma decisão com zero ou uma saída é convertida em atividade, preservando a conexão e registrando o reparo em `data.importRepair`. O sistema nunca cria uma condição de negócio que não exista no arquivo.

## Tema

A preferência `ui_theme` é mantida na coleção de usuários e refletida no parâmetro `theme` da URL. O frontend do editor recebe o tema selecionado e possui tokens próprios para inputs, menus, canvas e modais.

## Traçado

O documento guarda um único `settings.edgeRouting`, aplicado na renderização de todas as conexões. Os modos disponíveis são `smooth`, `straight`, `orthogonal`, `corridor-v2` e `corridor`.

## Mapa de relações

A página gera um payload enxuto no servidor e executa a simulação de força inteiramente no navegador, em `canvas`, sem gravar as posições nos fluxos.

## Releases

Uma release registra ID, versão, revisão, hash SHA-256, papel, grupo e ordem de cada fluxo. A exportação utiliza versões imutáveis.
