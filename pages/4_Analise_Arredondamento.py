import streamlit as st
import pandas as pd
import json
from decimal import Decimal, ROUND_HALF_EVEN, ROUND_HALF_UP, ROUND_DOWN, ROUND_CEILING

# --- Configuração da Página ---
st.set_page_config(
    page_title="Análise de Arredondamento",
    page_icon="📐",
    layout="wide"
)

st.title("📐 Análise de Arredondamento: NBR 5891 vs Outros")
st.markdown("""
Esta ferramenta realiza um 'De-Para' do cálculo de **Desconto** (`valor_total` * `taxa_administrativa`), 
comparando o valor exato do sistema com simulações de arredondamento em **2 casas decimais**.

**Legenda de Métodos:**
* **NBR 5891 (Bancário):** Arredonda para o par mais próximo se o dígito for 5 (Ex: 2.5 -> 2, 3.5 -> 4). Padrão Python/C#.
* **Tradicional (Half Up):** Arredonda para cima se >= 5 (Ex: 2.5 -> 3). Padrão escolar/Excel.
* **Truncar (Floor):** Simplesmente corta as casas decimais extras.
""")

st.divider()

# --- Upload do Arquivo ---
uploaded_file = st.file_uploader("Faça o upload do arquivo JSON (ex: response.json)", type=["json"])

if uploaded_file is not None:
    try:
        # Carregar JSON
        data = json.load(uploaded_file)
        
        # Se o JSON for uma lista de objetos, prosseguimos
        if isinstance(data, list):
            rows = []
            
            for item in data:
                # Extração segura dos dados
                try:
                    item_id = item.get("id")
                    valor_total = Decimal(str(item.get("valor_total", 0)))
                    # Taxa pode ser negativa, usamos abs para calcular o valor absoluto do desconto
                    taxa_admin = Decimal(str(item.get("taxa_administrativa", 0)))
                    taxa_abs = abs(taxa_admin)
                    
                    # Desconto original do JSON (Referência)
                    desconto_sistema = Decimal(str(item.get("desconto", 0)))
                    
                    # Cálculo Puro (Recalculado com precisão total)
                    # Fórmula: Valor * (Taxa / 100)
                    desconto_calculado_raw = valor_total * (taxa_abs / Decimal("100"))
                    
                    # --- APLICAÇÃO DOS ARREDONDAMENTOS (para 2 casas) ---
                    
                    # 1. NBR 5891 (Round Half To Even)
                    nbr_5891 = desconto_calculado_raw.quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN)
                    
                    # 2. Tradicional (Round Half Up)
                    tradicional = desconto_calculado_raw.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                    
                    # 3. Truncar (Floor) - arredonda para baixo
                    truncado = desconto_calculado_raw.quantize(Decimal("0.01"), rounding=ROUND_DOWN)

                    rows.append({
                        "ID": item_id,
                        "Valor Total": float(valor_total),
                        "Taxa (%)": float(taxa_admin),
                        "Desconto Sistema (Raw)": float(desconto_sistema),
                        "Desconto Recalculado (Raw)": float(desconto_calculado_raw),
                        "NBR 5891 (2 casas)": float(nbr_5891),
                        "Tradicional (2 casas)": float(tradicional),
                        "Truncado (2 casas)": float(truncado),
                        # Diferenças (Deltas)
                        "Dif. NBR vs Sistema": float(nbr_5891 - desconto_sistema),
                        "Dif. NBR vs Tradicional": float(nbr_5891 - tradicional)
                    })
                    
                except Exception as e:
                    st.warning(f"Erro ao processar item ID {item.get('id', 'desconhecido')}: {e}")
                    continue
            
            # Criar DataFrame
            df = pd.DataFrame(rows)
            
            # --- Exibição dos Dados ---
            
            if not df.empty:
                # Métricas Gerais
                st.subheader("Resumo das Diferenças (Total Monetário)")
                c1, c2, c3 = st.columns(3)
                
                total_nbr = df["NBR 5891 (2 casas)"].sum()
                total_trad = df["Tradicional (2 casas)"].sum()
                total_sys = df["Desconto Sistema (Raw)"].sum()
                
                c1.metric("Total NBR 5891", f"R$ {total_nbr:,.2f}")
                c2.metric("Total Tradicional", f"R$ {total_trad:,.2f}", delta=f"{total_trad - total_nbr:,.2f} vs NBR")
                c3.metric("Total Sistema (Raw)", f"R$ {total_sys:,.2f}", delta=f"{total_sys - total_nbr:,.2f} vs NBR")
                
                st.divider()
                
                # Filtros de Visualização
                st.subheader("Detalhamento por Transação")
                
                ver_apenas_diferencas = st.checkbox("Mostrar apenas linhas onde há divergência entre NBR 5891 e Tradicional", value=True)
                
                df_show = df.copy()
                if ver_apenas_diferencas:
                    # Filtra onde a diferença não é zero (usando pequena margem para float)
                    df_show = df_show[abs(df_show["Dif. NBR vs Tradicional"]) > 0.001]
                    st.caption(f"Exibindo {len(df_show)} registros com divergência de arredondamento.")
                else:
                    st.caption(f"Exibindo todos os {len(df_show)} registros.")

                # Estilização do Dataframe
                st.dataframe(
                    df_show,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Valor Total": st.column_config.NumberColumn(format="R$ %.2f"),
                        "Desconto Sistema (Raw)": st.column_config.NumberColumn(format="%.6f", label="Desc. Atual (JSON)"),
                        "Desconto Recalculado (Raw)": st.column_config.NumberColumn(format="%.6f"),
                        "NBR 5891 (2 casas)": st.column_config.NumberColumn(format="R$ %.2f"),
                        "Tradicional (2 casas)": st.column_config.NumberColumn(format="R$ %.2f"),
                        "Truncado (2 casas)": st.column_config.NumberColumn(format="R$ %.2f"),
                        "Dif. NBR vs Tradicional": st.column_config.NumberColumn(format="%.2f", label="Δ NBR x Trad.")
                    }
                )
                
                # Botão de Download
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Baixar Relatório Completo (CSV)",
                    data=csv,
                    file_name="analise_arredondamento_nbr5891.csv",
                    mime="text/csv",
                )
            else:
                st.info("Nenhum dado válido encontrado no JSON.")
                
        else:
            st.error("O JSON enviado não está no formato de lista esperado.")
            
    except Exception as e:
        st.error(f"Erro ao ler o arquivo: {e}")

else:
    st.info("Aguardando upload do arquivo JSON...")
