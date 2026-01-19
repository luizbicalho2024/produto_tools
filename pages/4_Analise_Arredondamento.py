import streamlit as st
import pandas as pd
import json
from decimal import Decimal, ROUND_HALF_EVEN, ROUND_HALF_UP, ROUND_DOWN, ROUND_CEILING

# --- Configuração da Página ---
st.set_page_config(
    page_title="Análise de Impacto Financeiro (ABNT)",
    page_icon="💸",
    layout="wide"
)

st.title("💸 Análise de Impacto: Repasse de Descontos")
st.markdown("""
**Contexto de Negócio:** O valor "Desconto" representa um **repasse** financeiro. 
* Se o valor do desconto **aumentar**, a empresa **perde dinheiro**.
* Se o valor do desconto **diminuir**, a empresa **economiza**.

Esta ferramenta compara o valor atualmente registrado no JSON contra o valor calculado pela norma **ABNT NBR 5891**.
""")

# --- Sidebar ---
st.sidebar.header("📁 Upload")
uploaded_file = st.sidebar.file_uploader("Carregue o JSON (response.json)", type=["json"])

st.sidebar.divider()
st.sidebar.info("ℹ️ **Regra ABNT NBR 5891:** Arredonda para o par mais próximo (Ex: 2.5 → 2 | 3.5 → 4).")

# --- Processamento ---
if uploaded_file is not None:
    try:
        data = json.load(uploaded_file)
        
        if isinstance(data, list):
            rows = []
            
            # Totais acumuladores para performance
            total_json_acumulado = Decimal("0.00")
            total_abnt_acumulado = Decimal("0.00")
            
            for item in data:
                try:
                    # 1. Dados Originais (O que está no sistema hoje)
                    # Convertendo para Decimal para não perder precisão dos centavos
                    valor_total_base = Decimal(str(item.get("valor_total", 0)))
                    taxa_admin = Decimal(str(item.get("taxa_administrativa", 0)))
                    
                    # O desconto que está salvo no JSON (Referência Atual)
                    desconto_atual_json = Decimal(str(item.get("desconto", 0)))
                    
                    # 2. Simulação ABNT NBR 5891
                    # Regra: Valor Total * Taxa (abs)
                    # Importante: A ABNT aplica o arredondamento no resultado final da conta
                    taxa_abs = abs(taxa_admin)
                    calculo_bruto = valor_total_base * (taxa_abs / Decimal("100"))
                    
                    # Aplicando arredondamento bancário (NBR 5891) para 2 casas decimais
                    desconto_simulado_abnt = calculo_bruto.quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN)
                    
                    # 3. Análise de Impacto (ABNT - JSON)
                    # Se ABNT for maior que JSON, diff > 0 (Aumento de Repasse = RUIM)
                    # Se ABNT for menor que JSON, diff < 0 (Redução de Repasse = BOM)
                    diferenca = desconto_simulado_abnt - desconto_atual_json
                    
                    # Classificação para filtro
                    if diferenca > 0:
                        status = "PREJUÍZO (Aumenta Repasse)"
                    elif diferenca < 0:
                        status = "ECONOMIA (Diminui Repasse)"
                    else:
                        status = "NEUTRO"

                    rows.append({
                        "ID": item.get("id"),
                        "Valor Base": float(valor_total_base),
                        "Taxa (%)": float(taxa_admin),
                        "Repasse Atual (JSON)": float(desconto_atual_json),
                        "Repasse ABNT (Simulado)": float(desconto_simulado_abnt),
                        "Impacto Financeiro (R$)": float(diferenca),
                        "Status": status
                    })
                    
                    total_json_acumulado += desconto_atual_json
                    total_abnt_acumulado += desconto_simulado_abnt
                    
                except Exception as e:
                    continue
            
            # --- Exibição dos Resultados ---
            df = pd.DataFrame(rows)
            
            if not df.empty:
                st.divider()
                
                # --- KPI Principal: O Veredito ---
                diff_total = total_abnt_acumulado - total_json_acumulado
                
                c1, c2, c3 = st.columns(3)
                
                c1.metric(
                    "Total Repassado HOJE (JSON)", 
                    f"R$ {total_json_acumulado:,.2f}"
                )
                
                c2.metric(
                    "Total Repassado se fosse ABNT", 
                    f"R$ {total_abnt_acumulado:,.2f}"
                )
                
                # Lógica de cor inversa: Se a diferença for positiva (Aumento de Custo), delta_color="inverse" deixa VERMELHO.
                # Se for negativa (Economia), fica VERDE.
                label_veredito = "Veredito Financeiro"
                if diff_total > 0:
                    texto_delta = f"AUMENTO DE R$ {diff_total:,.2f} (Prejuízo)"
                elif diff_total < 0:
                    texto_delta = f"ECONOMIA DE R$ {abs(diff_total):,.2f}"
                else:
                    texto_delta = "Sem alteração de custo"

                c3.metric(
                    label=label_veredito,
                    value=f"R$ {diff_total:,.2f}",
                    delta=texto_delta,
                    delta_color="inverse", # Inverte: Positivo fica Vermelho (Ruim), Negativo fica Verde (Bom)
                    help="Valor Positivo = Você vai pagar mais dinheiro ao órgão. Valor Negativo = Você vai economizar."
                )
                
                # --- Alertas Visuais ---
                if diff_total > 0:
                    st.error(f"⚠️ **ATENÇÃO:** A adoção da norma ABNT 5891 resultará em um custo extra total de **R$ {diff_total:,.2f}** neste lote de dados.")
                elif diff_total < 0:
                    st.success(f"✅ **BOA NOTÍCIA:** A adoção da norma ABNT 5891 gerará uma economia de **R$ {abs(diff_total):,.2f}** neste lote de dados.")
                else:
                    st.info("ℹ️ A alteração para a norma ABNT não impactará o montante financeiro total.")

                st.divider()

                # --- Tabela Detalhada ---
                st.subheader("🔎 Detalhes por Transação")
                
                filtro_impacto = st.radio(
                    "O que você quer visualizar?",
                    ["Tudo", "Apenas onde perco dinheiro (Prejuízo)", "Apenas onde ganho dinheiro (Economia)"],
                    horizontal=True
                )
                
                df_view = df.copy()
                if filtro_impacto == "Apenas onde perco dinheiro (Prejuízo)":
                    df_view = df_view[df_view["Impacto Financeiro (R$)"] > 0]
                elif filtro_impacto == "Apenas onde ganho dinheiro (Economia)":
                    df_view = df_view[df_view["Impacto Financeiro (R$)"] < 0]
                
                st.dataframe(
                    df_view,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "ID": st.column_config.TextColumn(width="small"),
                        "Repasse Atual (JSON)": st.column_config.NumberColumn(format="R$ %.4f"),
                        "Repasse ABNT (Simulado)": st.column_config.NumberColumn(format="R$ %.2f"),
                        "Impacto Financeiro (R$)": st.column_config.NumberColumn(
                            label="Diferença (R$)",
                            format="%.4f",
                            help="Positivo = Prejuízo | Negativo = Economia"
                        ),
                        "Status": st.column_config.TextColumn(width="medium")
                    }
                )
                
                # --- Download ---
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Baixar Relatório de Impacto Financeiro (CSV)",
                    data=csv,
                    file_name="analise_impacto_financeiro_abnt.csv",
                    mime="text/csv",
                )

            else:
                st.warning("JSON carregado, mas sem dados válidos para cálculo.")
        else:
            st.error("Formato do JSON inválido (esperado: lista de objetos).")
    except Exception as e:
        st.error(f"Erro ao processar: {e}")
else:
    st.info("Aguardando upload do arquivo JSON.")
