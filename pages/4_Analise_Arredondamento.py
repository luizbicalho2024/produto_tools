import streamlit as st
import pandas as pd
import json
from decimal import Decimal, ROUND_HALF_EVEN, ROUND_HALF_UP, ROUND_DOWN, ROUND_CEILING, ROUND_HALF_DOWN

# --- Configuração da Página ---
st.set_page_config(
    page_title="Simulador de Arredondamento",
    page_icon="📐",
    layout="wide"
)

st.title("📐 Simulador: NBR 5891 vs Outros Métodos")
st.markdown("""
Esta ferramenta compara o impacto financeiro da norma **ABNT NBR 5891** (Arredondamento Bancário) 
contra outros métodos de arredondamento escolhidos por você.
""")

# --- Sidebar: Configurações e Upload ---
st.sidebar.header("1. Upload de Dados")
uploaded_file = st.sidebar.file_uploader("Arquivo JSON (response.json)", type=["json"])

st.sidebar.divider()

st.sidebar.header("2. Configuração da Comparação")
st.sidebar.info("A referência fixa será sempre a **ABNT NBR 5891** (Round Half to Even). Escolha abaixo o método para duelar com ela.")

# Mapeamento de opções para constantes do Decimal
opcoes_arredondamento = {
    "Padrão Escolar (Round Half Up)": ROUND_HALF_UP,
    "Truncar / Para Baixo (Floor)": ROUND_DOWN,
    "Sempre para Cima (Ceiling)": ROUND_CEILING,
    "Padrão Inverso (Round Half Down)": ROUND_HALF_DOWN
}

metodo_escolhido_nome = st.sidebar.selectbox(
    "Escolha o Método de Simulação:",
    options=list(opcoes_arredondamento.keys()),
    index=0
)

metodo_escolhido_const = opcoes_arredondamento[metodo_escolhido_nome]

# Descrição visual do método escolhido
descricoes = {
    "Padrão Escolar (Round Half Up)": "Se for 0.005, arredonda para cima (0.01). É o mais comum no comércio.",
    "Truncar / Para Baixo (Floor)": "Simplesmente ignora as casas extras. 0.009 vira 0.00.",
    "Sempre para Cima (Ceiling)": "Qualquer fração força o valor para cima. 0.001 vira 0.01.",
    "Padrão Inverso (Round Half Down)": "Se for 0.005, arredonda para baixo. O oposto do escolar."
}
st.sidebar.caption(f"ℹ️ **Como funciona:** {descricoes[metodo_escolhido_nome]}")

st.divider()

# --- Processamento ---
if uploaded_file is not None:
    try:
        data = json.load(uploaded_file)
        
        if isinstance(data, list):
            rows = []
            
            # Precisão monetária de 2 casas
            TWO_PLACES = Decimal("0.01")
            
            for item in data:
                try:
                    # Extração segura
                    item_id = item.get("id")
                    
                    # Conversão para Decimal para precisão matemática absoluta
                    valor_total = Decimal(str(item.get("valor_total", 0)))
                    taxa_admin = Decimal(str(item.get("taxa_administrativa", 0)))
                    
                    # Usamos módulo (abs) pois o desconto é um valor monetário positivo derivado da taxa
                    taxa_abs = abs(taxa_admin)
                    
                    # 1. Cálculo RAW (Infinitas casas decimais)
                    desconto_raw = valor_total * (taxa_abs / Decimal("100"))
                    
                    # 2. Aplicar ABNT NBR 5891 (Referência Fixa)
                    val_nbr = desconto_raw.quantize(TWO_PLACES, rounding=ROUND_HALF_EVEN)
                    
                    # 3. Aplicar Método Selecionado pelo Usuário
                    val_simulado = desconto_raw.quantize(TWO_PLACES, rounding=metodo_escolhido_const)
                    
                    # Diferença
                    diff = val_simulado - val_nbr

                    rows.append({
                        "ID Transação": item_id,
                        "Valor Base": float(valor_total),
                        "Taxa (%)": float(taxa_admin),
                        "Cálculo Puro (Raw)": float(desconto_raw),
                        "ABNT NBR 5891": float(val_nbr),
                        f"Simulado ({metodo_escolhido_nome})": float(val_simulado),
                        "Diferença (R$)": float(diff),
                        "Status": "DIVERGENTE" if abs(diff) > 0 else "IGUAL"
                    })
                    
                except Exception as e:
                    # Ignora itens mal formados mas avisa no log se necessário
                    continue
            
            # Criar DataFrame
            df = pd.DataFrame(rows)
            
            if not df.empty:
                # --- KPI's Superiores ---
                total_nbr = df["ABNT NBR 5891"].sum()
                total_simulado = df[f"Simulado ({metodo_escolhido_nome})"].sum()
                total_diff = total_simulado - total_nbr
                
                c1, c2, c3 = st.columns(3)
                
                c1.metric(
                    label="Total (Norma ABNT 5891)", 
                    value=f"R$ {total_nbr:,.2f}",
                    help="Soma total aplicando arredondamento bancário (par mais próximo)."
                )
                
                c2.metric(
                    label=f"Total ({metodo_escolhido_nome})", 
                    value=f"R$ {total_simulado:,.2f}",
                    delta=f"R$ {total_diff:,.2f}",
                    delta_color="inverse", # Se aumentar o custo (positivo), fica vermelho, se economizar, verde (ou vice-versa dependendo da ótica)
                    help="Soma total aplicando o método selecionado no menu lateral."
                )
                
                qtd_divergentes = len(df[df["Status"] == "DIVERGENTE"])
                c3.metric(
                    label="Itens com Divergência", 
                    value=f"{qtd_divergentes} de {len(df)}",
                    help="Número de transações onde o arredondamento resultou em centavos diferentes."
                )
                
                st.divider()
                
                # --- Tabela Detalhada ---
                st.subheader("Detalhamento das Diferenças")
                
                filtro_divergentes = st.toggle("Ver apenas linhas com diferença de valor", value=True)
                
                df_view = df.copy()
                if filtro_divergentes:
                    df_view = df_view[df_view["Status"] == "DIVERGENTE"]
                
                # Formatação visual da tabela
                st.dataframe(
                    df_view,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Valor Base": st.column_config.NumberColumn(format="R$ %.2f"),
                        "Cálculo Puro (Raw)": st.column_config.NumberColumn(format="%.6f"), # Mostrar mais casas para ver o "quebra"
                        "ABNT NBR 5891": st.column_config.NumberColumn(format="R$ %.2f"),
                        f"Simulado ({metodo_escolhido_nome})": st.column_config.NumberColumn(format="R$ %.2f"),
                        "Diferença (R$)": st.column_config.NumberColumn(format="R$ %.2f"),
                    }
                )
                
                # Download
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Baixar Resultado da Simulação (CSV)",
                    data=csv,
                    file_name="simulacao_arredondamento.csv",
                    mime="text/csv",
                )
                
            else:
                st.warning("O arquivo JSON foi lido, mas não gerou dados válidos para cálculo.")
                
        else:
            st.error("O JSON deve ser uma lista de objetos.")
            
    except Exception as e:
        st.error(f"Erro ao processar o arquivo: {e}")
else:
    # Estado inicial (sem arquivo)
    st.info("👈 Por favor, faça o upload do arquivo JSON na barra lateral para começar a análise.")
