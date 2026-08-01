# Arquitetura do Produto Tools 2.1

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
