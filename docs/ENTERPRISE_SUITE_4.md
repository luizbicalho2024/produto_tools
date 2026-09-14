# Produto Tools 4.0 Enterprise Suite

A versão 4.0 amplia o Produto Tools de editor/governança de fluxos para uma suíte integrada de gestão, análise, melhoria e execução de processos.

## Módulos

- **Melhoria Contínua:** SIPOC, 5W2H, Riscos/FMEA, Ishikawa, 5 Porquês, Pareto e VSM.
- **Simulação e Custos:** Monte Carlo simples baseado em SLA, variabilidade e custo/hora; comparação de cenários e cockpit.
- **Process Mining:** descoberta por direct-follows graph, variantes, duração de casos e conformance checking.
- **BPMN e DMN:** importação/exportação BPMN 2.0 e tabelas de decisão persistidas.
- **Execução:** Form Builder, instâncias, tarefas, decisões operacionais e SLA.
- **Governança Enterprise:** Change Requests, compliance, catálogo de sistemas, capacidades L0-L4, Customer Journey, evidências e diff visual.
- **Portal de Processos:** leitura da versão publicada e download da documentação oficial.
- **IA e Automações:** copiloto heurístico, endpoint de IA compatível com Chat Completions, texto para fluxo, POP/SOP, webhooks e notificações.
- **Segurança:** MFA TOTP e modo opcional SSO/OIDC nativo do Streamlit.

## Segurança

### MFA local

Acesse **Segurança** e ative MFA TOTP. Após ativado, o login local exigirá senha e código TOTP.

### OIDC

Defina `AUTH_MODE=oidc` e configure no `.streamlit/secrets.toml`:

```toml
[auth]
redirect_uri = "https://SEU-HOST/oauth2callback"
cookie_secret = "SEGREDO_FORTE"
client_id = "CLIENT_ID"
client_secret = "CLIENT_SECRET"
server_metadata_url = "https://SEU-IDP/.well-known/openid-configuration"
```

O e-mail autenticado pelo IdP precisa existir na coleção compartilhada `users`.

## IA externa opcional

```toml
AI_API_URL = "https://seu-provedor/v1/chat/completions"
AI_API_KEY = "..."
AI_MODEL = "..."
```

Sem essas variáveis, o copiloto continua operando com análise determinística local.

## Compatibilidade

A atualização é aditiva. As coleções e fluxos 3.x permanecem válidos. As novas coleções são criadas sob demanda e usam prefixo `produto_tools_`.