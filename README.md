# Produto Tools 3.0.3 Professional — Editor de Processos

Aplicação Streamlit com editor visual de processos, autenticação compartilhada com o **Simulador-Telemetria** e persistência dos fluxos no MongoDB Atlas.

## Correções de estabilidade 3.0.3

- O autosave é feito primeiro no navegador e não recarrega a página durante a edição.
- O botão **Rascunho** sincroniza explicitamente a cópia local com o MongoDB.
- Alterações locais são restauradas automaticamente após reruns do Streamlit.
- O modo escuro cobre controles, menus, modais, cards, raias e campos.
- Os filtros de visão ocultam elementos incompatíveis e enquadram apenas o conjunto filtrado.
- O botão **Organizar** redistribui cards por raia, reserva canais para conexões e reduz sobreposições.
- As linhas usam corredores ortogonais, procuram colunas livres entre raias e recebem halo visual para melhorar a leitura em processos extensos.


## Recursos visuais do editor 2.3

- Inspetor de propriedades horizontal na parte superior.
- Canvas ocupando toda a largura restante da página.
- Paleta de elementos recolhível para liberar espaço.
- Tela cheia nativa para usar o monitor inteiro.
- Modo claro e escuro.
- Nós arredondados com faixa de cor corrigida.
- Importação e organização automática de fluxos extensos, com mundo dinâmico e zoom de até 4%.
- Elementos reposicionados dentro das respectivas raias, com expansão automática da altura quando necessário.
- Decisões com no mínimo duas saídas visuais e validação estrutural no frontend e no backend.
- Seleção explícita da ramificação padrão de cada decisão.
- Destaque de uma rota completa em fluxos grandes, evitando encerrar no primeiro fim ou exceção próximos.
- Reprodução da rota escolhida com **Play**, pausa, parada, velocidade e centralização automática em cada etapa.

## Fluxos grandes e decisões

Ao importar um JSON com muitos elementos, o editor: 

1. calcula o tamanho necessário do mundo;
2. preserva as raias e ajusta suas alturas;
3. agrupa os elementos em colunas de leitura;
4. evita sobreposições;
5. enquadra o fluxo completo com zoom reduzido;
6. atribui conectores separados às saídas de decisões.

Para definir qual ramificação será usada no destaque e no Play, selecione uma decisão e escolha **Saída padrão para destaque e Play** no inspetor superior. Também é possível selecionar diretamente uma conexão de saída antes de destacar a rota.

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
