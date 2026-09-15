# WBS / EAP no Produto Tools

O módulo **WBS — Work Breakdown Structure** adiciona ao Produto Tools uma página dedicada à Estrutura Analítica do Projeto (EAP).

## Recursos

- criação, duplicação, edição e exclusão de WBS;
- vínculo opcional com projetos já cadastrados;
- pacotes hierárquicos com código WBS automático;
- responsável, status, entregável, datas, duração, progresso, custo, marco e tags;
- visualização gráfica por árvore Graphviz;
- visualização em tabela, outline textual e indicadores consolidados;
- edição por formulário e edição tabular rápida;
- revisão otimista para evitar sobrescrita concorrente;
- auditoria das operações no log do Produto Tools.

## Importação

- JSON nativo;
- XML nativo;
- Microsoft Project XML (best-effort para WBS/OutlineNumber/Tasks);
- Excel XLS/XLSX;
- CSV e TSV;
- TXT e Markdown por outline/código WBS;
- PDF com texto extraível. A importação de PDF é best-effort e não executa OCR.

## Exportação

- JSON;
- XML;
- Excel XLSX;
- CSV;
- TSV;
- PDF;
- Markdown;
- TXT;
- ZIP com todos os formatos acima.

## Modelo de dados

Cada WBS é um documento MongoDB na coleção `produto_tools_wbs`. A hierarquia é persistida por `id`/`parent_id`; os códigos `1`, `1.1`, `1.1.1` etc. são recalculados deterministicamente conforme a ordem entre irmãos.
