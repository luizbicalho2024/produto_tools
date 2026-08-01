# Produto Tools 2.1 — MongoDB compartilhado

Aplicação Streamlit com editor visual de processos, autenticação compartilhada com o **Simulador-Telemetria** e persistência dos fluxos no MongoDB Atlas.

## O que mudou nesta versão

- O SQLite foi removido.
- O login usa a mesma coleção `simulador_db.users` do Simulador-Telemetria.
- Usuário, senha bcrypt, nome, e-mail, perfil e situação de acesso são compartilhados.
- Os fluxos são armazenados no mesmo banco MongoDB, em coleções exclusivas do Produto Tools.
- O histórico de versões também fica no MongoDB.
- As páginas Consulta Sigyo, Consulta Logpay e Análise de Arredondamento foram removidas.
- A Gestão de Acesso administra a coleção compartilhada; alterações afetam as duas aplicações.

## Coleções utilizadas

```text
simulador_db
├── users                              compartilhada com Simulador-Telemetria
├── activity_logs                      compartilhada, com application=produto_tools
├── produto_tools_flowcharts           versão atual dos fluxos
└── produto_tools_flowchart_versions   histórico de versões
```

## Estrutura

```text
produto_tools/
├── login_app.py
├── database.py
├── requirements.txt
├── core/
│   ├── auth.py
│   ├── configuration.py
│   └── styles.py
├── services/
│   └── flowchart_repository.py
├── schemas/
│   └── flowchart_schema.py
├── components/
│   └── flow_editor/
│       ├── component.py
│       └── frontend/
│           ├── index.html
│           ├── styles.css
│           └── main.js
└── pages/
    ├── 1_Gestão_de_Acesso.py
    └── 5_Editor_de_Fluxos.py
```

## Publicação no Streamlit Cloud

1. Envie todo o projeto para o GitHub.
2. Crie ou edite o app no Streamlit Community Cloud.
3. Defina o arquivo principal como `login_app.py`.
4. Em **Settings > Secrets**, configure:

```toml
MONGO_CONNECTION_STRING = "mongodb+srv://USUARIO:SENHA@cluster.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
MONGO_DB_NAME = "simulador_db"

AUTH_COOKIE_NAME = "simulador_telemetria_auth"
AUTH_COOKIE_KEY = "A_MESMA_CHAVE_USADA_NO_SIMULADOR"
AUTH_COOKIE_EXPIRY_DAYS = 30
```

Use a mesma conexão, banco, nome de cookie e chave do Simulador-Telemetria. O arquivo `.streamlit/secrets.toml.example` contém o modelo sem credenciais reais.

## MongoDB Atlas

Em **Network Access**, permita a saída do Streamlit Cloud. Para testes, pode ser usado `0.0.0.0/0`; em produção, aplique a política de rede mais restritiva disponível e mantenha usuário do banco com privilégios mínimos necessários.

## Login compartilhado

O Produto Tools lê diretamente os documentos existentes em `simulador_db.users`:

```json
{
  "username": "usuario",
  "name": "Nome do usuário",
  "email": "usuario@empresa.com",
  "hashed_password": "$2b$...",
  "role": "user",
  "active": true
}
```

Os perfis aceitos são `user`, `head_comercial` e `admin`.

## Execução local

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run login_app.py
```

No Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
streamlit run login_app.py
```

## Segurança

- A URI do MongoDB e a chave do cookie não ficam no repositório.
- `.streamlit/secrets.toml` está no `.gitignore`.
- As senhas permanecem bcrypt, no mesmo formato usado pelo simulador.
- Usuários inativos não conseguem autenticar.
- O último administrador ativo não pode ser excluído.
- O JSON do fluxo é validado novamente no backend antes da gravação.
