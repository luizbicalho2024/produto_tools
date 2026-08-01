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

## Validações visuais 2.3

- Faixa colorida respeitando nós em formato de pílula, arredondados e assimétricos.
- Propriedades exibidas no inspetor horizontal superior.
- Paleta e inspetor recolhíveis sem perda do estado do fluxo.
- Tema claro e escuro no editor.
- Destaque de caminho com redução de opacidade dos itens externos.
- Reprodução com play, pausa, parada, velocidade e centralização da etapa atual.
- Tela cheia com API nativa do navegador e modo expandido de contingência.

## Caso de regressão: SIGYO Modular completo

O editor foi validado com um fluxo contendo 12 raias, 134 elementos e 187 conexões. Após a importação e organização automática foram confirmados:

- nenhum elemento fora da respectiva raia;
- nenhuma sobreposição entre elementos;
- canvas dinâmico de aproximadamente 21.600 × 4.400 pixels;
- enquadramento integral com zoom de 6% no ambiente de teste;
- pelo menos dois conectores visuais em todas as decisões;
- seletor de ramificação apresentando as saídas rotuladas;
- rota principal destacada com 77 etapas no cenário de teste;
- reprodução iniciada sobre a rota completa selecionada.
