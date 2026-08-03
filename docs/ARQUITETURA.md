# Arquitetura — Produto Tools 3.2.3

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

## Raias e seleção múltipla

O frontend calcula a ocupação horizontal dos cards de cada raia, distribui conflitos em linhas internas e ajusta a altura da raia. A seleção múltipla é mantida em `selectedNodeIds` e o arraste usa um snapshot das posições originais para mover o grupo de maneira consistente.

## Proteção de navegação

O autosave periódico permanece local e silencioso. Quando existe diferença ainda não sincronizada com o MongoDB, o frontend intercepta a navegação disponível no documento host e apresenta as opções de permanecer, sair mantendo o rascunho local ou emitir a sincronização explícita antes de continuar. O `beforeunload` do navegador atua como proteção adicional.

## Semântica das decisões

As conexões cuja origem é uma decisão são classificadas a partir do rótulo, condição e destino. Saídas positivas são verdes, negativas são vermelhas e saídas sem semântica reconhecível permanecem cinza. A regra é compartilhada conceitualmente pelo editor e pelo mapa de relações.
