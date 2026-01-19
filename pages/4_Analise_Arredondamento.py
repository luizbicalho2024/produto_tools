import streamlit as st
import pandas as pd
import json
from decimal import Decimal, ROUND_HALF_EVEN, ROUND_HALF_UP, ROUND_DOWN, ROUND_CEILING, ROUND_HALF_DOWN

# --- Funções Auxiliares ---
def to_decimal(val):
    """Converte valor para Decimal de forma segura."""
    if val is None:
        return Decimal("0")
    return Decimal(str(val))

def aplicar_abnt(valor):
    """Aplica arredondamento ABNT NBR 5891 (Round Half To Even) para 2 casas."""
    return valor.quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN)

# --- Configuração da Página ---
st.set_page_config(
    page_title="Auditoria de Arredondamento",
    page_icon="⚖️",
    layout="wide"
)

st.title("⚖️ Auditoria e Comparação de Arredondamento")
st.markdown("""
Esta ferramenta confronta os dados do JSON (Sistema Atual) contra a norma **ABNT NBR 5891**.
Calculamos a diferença exata ("Gap") para identificar perdas ou sobras financeiras no montante total.
""")

# --- Sidebar ---
st.sidebar.header("📁 Upload de Dados")
uploaded_file = st.sidebar.file_uploader("Arquivo JSON (response.json)", type=["json"])

st.sidebar.divider()
st.sidebar.header("🛠️ Simulação Extra")
st.sidebar.info("Além da ABNT (Padrão), compare com outro método:")
opcoes_simulacao = {
    "Padrão Escolar (Half Up)": ROUND_HALF_UP,
    "Truncar (Floor)": ROUND_DOWN,
    "Sempre p/ Cima (Ceiling)": ROUND_CEILING
}
metodo_simulado_nome = st.sidebar.selectbox("Método Comparativo:", list(opcoes_simulacao.keys()))
metodo_simulado_const = opcoes_simulacao[metodo_simulado_nome]

# --- Processamento ---
if uploaded_file is not None:
    try:
        data = json.load(uploaded_file)
        
        if isinstance(data, list):
            rows = []
            
            for item in data:
                try:
                    # --- 1. Extração de Dados Originais (JSON) ---
                    id_transacao = item.get("id")
                    
                    # Campos base
                    qtd = to_decimal(item.get("quantidade", 0))
                    vlr_unit = to_decimal(item.get("valor_unitario", 0))
                    taxa_pct = to_decimal(item.get("taxa_administrativa", 0))
                    
                    # Valores "oficiais" do JSON (O que está no banco hoje)
                    json_total = to_decimal(item.get("valor_total", 0))
                    json_desconto = to_decimal(item.get("desconto", 0))

                    # --- 2. Recálculo ABNT NBR 5891 ---
                    
                    # A) Validação do Valor Total (Qtd * Unitário)
                    # Calculamos Qtd * Unit e arredondamos conforme ABNT para ver se bate com o Total do JSON
                    calc_total_raw = qtd * vlr_unit
                    abnt_total = aplicar_abnt(calc_total_raw)
                    
                    # Gap Total: Diferença entre o Total guardado no JSON e o Total ideal ABNT
                    gap_total = json_total - abnt_total 
                    
                    # B) Validação do Desconto (Total * Taxa)
                    # Usamos o 'json_total' como base (assumindo que ele é a verdade para o desconto)
                    taxa_abs = abs(taxa_pct)
                    calc_desconto_raw = json_total * (taxa_abs / Decimal("100"))
                    
                    abnt_desconto = aplicar_abnt(calc_desconto_raw)
                    
                    # Gap Desconto: Diferença entre o Desconto do JSON e o ideal ABNT
                    gap_desconto = json_desconto - abnt_desconto
                    
                    # --- 3. Simulação Extra (Opcional) ---
                    simulado_desconto = calc_desconto_raw.quantize(Decimal("0.01"), rounding=metodo_simulado_const)
                    gap_simulado = simulado_desconto - abnt_desconto

                    rows.append({
                        "ID": id_transacao,
                        # Dados Originais
                        "JSON Total": float(json_total),
                        "JSON Desconto": float(json_desconto),
                        
                        # Comparação ABNT - Total
                        "ABNT Total (Calc)": float(abnt_total),
                        "Dif. Total (R$)": float(gap_total), # Se positivo, JSON está maior que ABNT
                        
                        # Comparação ABNT - Desconto
                        "ABNT Desconto (Calc)": float(abnt_desconto),
                        "Dif. Desconto (R$)": float(gap_desconto), # Se positivo, JSON cobrou mais desconto que devia
                        
                        # Simulação
                        f"Simulado ({metodo_simulado_nome})": float(simulado_desconto),
                        "Dif. Simulado vs ABNT": float(gap_simulado)
                    })
                    
                except Exception as ex:
                    continue
            
            df = pd.DataFrame(rows)
            
            if not df.empty:
                # --- KPI 1: Impacto Financeiro (A "Perda" ou "Sobra") ---
                st.subheader("💰 Impacto Financeiro: JSON (Atual) vs ABNT NBR 5891")
                
                # Soma das Diferenças
                total_gap_desconto = df["Dif. Desconto (R$)"].sum()
                total_gap_total = df["Dif. Total (R$)"].sum()
                
                col1, col2, col3 = st.columns(3)
                
                # Exibição do Gap de Desconto
                cor_delta_desc = "normal" if total_gap_desconto >= 0 else "inverse"
                col1.metric(
                    label="Diferença Acumulada (Descontos)",
                    value=f"R$ {total_gap_desconto:,.4f}",
                    delta="Sobrando no JSON" if total_gap_desconto > 0 else "Faltando no JSON",
                    delta_color=cor_delta_desc,
                    help="Soma de (Desconto JSON - Desconto ABNT). Se positivo, o sistema calculou descontos maiores que a norma."
                )

                # Exibição do Gap de Valor Total
                col2.metric(
                    label="Diferença Acumulada (Valor Total)",
                    value=f"R$ {total_gap_total:,.4f}",
                    delta="Divergência" if abs(total_gap_total) > 0.01 else "Conforme",
                    help="Soma de (Valor Total JSON - Qtd*Unit ABNT). Verifica se a multiplicação base está correta."
                )
                
                # Contagem de Erros
                erros_desconto = len(df[abs(df["Dif. Desconto (R$)"]) > 0.005])
                col3.metric(
                    label="Itens com Divergência (Desconto)",
                    value=f"{erros_desconto} / {len(df)}",
                    help="Quantidade de transações onde o arredondamento do desconto não bate com a ABNT."
                )
                
                st.divider()
                
                # --- Tabela Detalhada ---
                st.subheader("📋 Detalhamento (Confronto Direto)")
                
                filtro = st.radio("Filtrar Tabela:", ["Ver Tudo", "Apenas Divergentes (Desconto)"], horizontal=True)
                
                df_show = df.copy()
                if filtro == "Apenas Divergentes (Desconto)":
                    df_show = df_show[abs(df_show["Dif. Desconto (R$)"]) > 0.001]
                
                # Formatação condicional e visualização
                st.dataframe(
                    df_show,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "ID": st.column_config.TextColumn(width="small"),
                        "JSON Desconto": st.column_config.NumberColumn("JSON (Atual)", format="%.4f"),
                        "ABNT Desconto (Calc)": st.column_config.NumberColumn("ABNT (Ideal)", format="%.2f"),
                        "Dif. Desconto (R$)": st.column_config.NumberColumn(
                            "❌ Diferença R$", 
                            format="%.4f",
                            help="Quanto o valor diverge da norma"
                        ),
                        "JSON Total": st.column_config.NumberColumn(format="R$ %.2f"),
                        "ABNT Total (Calc)": st.column_config.NumberColumn(format="R$ %.2f"),
                        f"Simulado ({metodo_simulado_nome})": st.column_config.NumberColumn(format="R$ %.2f"),
                    }
                )
                
                # Download CSV
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Baixar Relatório de Auditoria (CSV)",
                    data=csv,
                    file_name="auditoria_arredondamento_abnt.csv",
                    mime="text/csv",
                )
                
            else:
                st.warning("Não foi possível processar os dados do JSON.")
        else:
            st.error("Formato JSON inválido. Esperada uma lista de objetos.")
            
    except Exception as e:
        st.error(f"Erro ao ler arquivo: {e}")
else:
    st.info("Aguardando upload do arquivo JSON.")
