# Changelog

## 3.0.3 - Estabilidade do editor

- Substitui o autosave que disparava reruns do Streamlit por salvamento local silencioso no navegador.
- Adiciona sincronizacao explicita de rascunho com o MongoDB pelo botao `Rascunho`.
- Recupera automaticamente alteracoes locais depois de reruns, comentarios, navegacao ou falhas de rede.
- Corrige contraste de botoes, selects, modais, cards, raias e campos no modo escuro.
- Corrige os filtros Completa, Executiva, Operacional, Tecnica, Excecoes e Raia selecionada.
- Faz os filtros ocultarem de fato os cards fora da visao e enquadrarem somente os resultados visiveis.
- Normaliza presets antigos, incluindo `compact-readable-v2`.
- Reescreve o Organizar para distribuir cards em colunas e linhas por raia, respeitando ciclos e fases.
- Reserva canais superiores nas raias para as conexoes e usa roteamento ortogonal por corredores.
- Procura corredores verticais livres antes de rotear entre raias, evitando atravessar cards intermediarios.
- Adiciona halo nas linhas para manter cada conexao legivel quando houver cruzamentos.
- Usa o fluxo SIGYO Modular revisado como arquivo de regressao de grande porte.

## 3.0.2 - Publicador PowerShell

- Corrige a sintaxe de aspas para Windows PowerShell 5.1.
- Salva o script com UTF-8 BOM e conteudo interno ASCII.
- Remove escapes de Bash incompatíveis com PowerShell.
- Exclui recursivamente caches, ambientes locais, bancos e Secrets durante a publicacao.
- Interrompe a publicacao em qualquer falha de Git, testes ou copia.

## 2.3.0 — 2026-08-01

- Mundo do canvas dimensionado dinamicamente para fluxos com dezenas de milhares de pixels.
- Zoom mínimo reduzido para enquadrar processos extensos.
- Importação de fluxos grandes com organização automática por raias.
- Raias contíguas e com altura expansível conforme a quantidade de elementos.
- Normalização de elementos fora dos limites de suas raias.
- Redução de sobreposição por agrupamento em colunas e linhas internas.
- Decisões com dois ou mais conectores de saída independentes.
- Validação obrigatória de no mínimo duas conexões de saída por decisão.
- Escolha explícita e persistente da ramificação padrão.
- Destaque e Play calculados como rota completa, sem priorizar o fim mais próximo.
- Seletor de ramificação exibido quando uma decisão ainda não possui saída preferida.
- Minimap e enquadramento adaptados ao tamanho real do fluxo.

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
