# Validação realizada

Antes da geração do pacote foram executadas as seguintes verificações:

- compilação sintática de todos os arquivos Python;
- validação sintática do JavaScript do editor com `node --check`;
- confirmação de que somente as páginas Gestão de Acesso e Editor de Processos permanecem;
- verificação de ausência de credenciais reais nos arquivos do pacote;
- validação do schema JSON do fluxo de exemplo;
- teste automatizado do repositório MongoDB com coleções em memória;
- criação, leitura, atualização e exclusão de fluxo;
- criação e consulta de versões;
- duplicação e exclusão de fluxo duplicado;
- conferência dos nomes do banco e das coleções compartilhadas;
- conferência do contrato de usuários usado pelo Simulador-Telemetria.

A conexão real com o MongoDB Atlas deve ser validada no ambiente de destino, pois depende dos Secrets do Streamlit Cloud e das regras de Network Access do cluster.

## Validações visuais 2.2

- Faixa colorida respeitando nós em formato de pílula, arredondados e assimétricos.
- Propriedades exibidas no inspetor horizontal superior.
- Paleta e inspetor recolhíveis sem perda do estado do fluxo.
- Tema claro e escuro no editor.
- Destaque de caminho com redução de opacidade dos itens externos.
- Reprodução com play, pausa, parada, velocidade e centralização da etapa atual.
- Tela cheia com API nativa do navegador e modo expandido de contingência.
