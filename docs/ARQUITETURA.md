# Arquitetura do Produto Tools 2.3

## Visão geral

```text
Streamlit Cloud
      │
      ├── autenticação: streamlit-authenticator
      │       └── simulador_db.users
      │
      ├── páginas Streamlit
      │       ├── Gestão de Acesso
      │       └── Editor de Processos
      │
      ├── componente visual v2
      │       └── HTML + CSS + JavaScript local
      │
      └── MongoDB Atlas
              ├── produto_tools_flowcharts
              └── produto_tools_flowchart_versions
```

## Compatibilidade com o Simulador-Telemetria

A aplicação usa o mesmo contrato de usuário:

- banco `simulador_db`;
- coleção `users`;
- login pelo campo `username` normalizado em minúsculas;
- senha no campo `hashed_password` com bcrypt;
- campos `name`, `email`, `role` e `active`;
- perfis `user`, `head_comercial` e `admin`;
- `streamlit-authenticator` e a mesma configuração de cookie.

A sessão do navegador depende do domínio do app. Portanto, as credenciais são compartilhadas e o cookie possui a mesma configuração, mas o navegador pode exigir novo login quando os dois apps estiverem em domínios diferentes.

## Persistência dos fluxos

### `produto_tools_flowcharts`

Armazena a versão atual, proprietário, metadados e documento JSON completo.

### `produto_tools_flowchart_versions`

Armazena snapshots por `flowchart_id` e `version`, permitindo restauração.

O identificador de propriedade é `owner_username`, porque o username é o identificador de autenticação usado pelos dois sistemas. O e-mail é mantido como metadado.

## Isolamento

O Produto Tools compartilha usuários e logs, mas usa coleções próprias para fluxos. Assim, não há colisão com propostas, preços, veículos ou configurações do Simulador-Telemetria.

## Segurança operacional

As credenciais devem existir somente nos Secrets do Streamlit Cloud. Nunca envie `.streamlit/secrets.toml` ao GitHub. Caso uma URI com senha seja exposta, troque a senha do usuário do MongoDB Atlas e atualize os dois aplicativos.

## Experiência visual 2.2

O editor mantém o estado do canvas no componente frontend e adiciona estados visuais transitórios que não alteram o JSON persistido:

- `focusPath`: conjunto ordenado de nós e conexões destacados para leitura de um caminho específico;
- `playback`: sequência, etapa atual, itens visitados, pausa e velocidade da reprodução;
- `paletteCollapsed` e `inspectorCollapsed`: preferências locais salvas no navegador;
- `uiTheme`: tema claro ou escuro do editor, inicializado pela preferência da sessão Streamlit.

O caminho destacado é calculado por busca em largura sobre os elementos e conexões ativos. Quando uma etapa é selecionada, o editor procura um caminho de um início até a etapa e da etapa até um fim. Quando uma conexão é selecionada, ela é obrigatoriamente incluída no caminho.

## Motor de layout para fluxos grandes 2.3

O componente calcula `worldSize` a partir das dimensões reais das raias e dos elementos, sem depender de um canvas fixo. Na importação, fluxos extensos são reorganizados por coluna lógica e por raia. O algoritmo preserva a sequência horizontal, expande a altura das raias quando há várias etapas na mesma coluna e impede que um nó seja posicionado fora de sua raia.

As decisões possuem handles `branch-0`, `branch-1` e seguintes. A preferência de rota fica em `node.data.preferredEdgeId`, permitindo que destaque e reprodução utilizem a mesma ramificação em sessões futuras. Na ausência de preferência, o editor solicita ao usuário a saída desejada.

A navegação não usa mais o menor caminho até qualquer elemento final. Ela combina o prefixo desde um início, a seleção atual e uma continuação completa, evitando ciclos já visitados e favorecendo a continuidade estrutural do processo.
