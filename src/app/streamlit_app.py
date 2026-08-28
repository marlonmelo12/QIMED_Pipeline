"""
Dashboard Executivo de Inteligencia em Saude Publica - QIMED Health Lakehouse & Data Warehouse.
Permite navegacao em tres niveis hierarquicos: Nacional (Brasil), Estadual (27 UFs) e Municipal (5.570 Cidades).
"""
import os
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

DW_PATH = "warehouse/qimed_dw.duckdb"

# --- CONFIGURACAO DA PAGINA ---
st.set_page_config(
    page_title="QIMED Health Intelligence | Lakehouse & DW",
    page_icon="H",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- FUNCAO SEGURA DE CONSULTA AO DUCKDB ---
def query_dw(sql: str) -> pd.DataFrame:
    if not os.path.exists(DW_PATH):
        return pd.DataFrame()
    try:
        conn = duckdb.connect(DW_PATH, read_only=True)
        df = conn.execute(sql).df()
        conn.close()
        return df
    except Exception as e:
        st.error(f"Erro ao consultar Data Warehouse: {e}")
        return pd.DataFrame()


# --- ESTILOS CSS ---
st.markdown("""
    <style>
    .metric-card {
        background-color: #1e293b;
        border-radius: 8px;
        padding: 16px 20px;
        border: 1px solid #334155;
        margin-bottom: 12px;
    }
    .metric-title {
        color: #94a3b8;
        font-size: 0.85rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 4px;
    }
    .metric-value {
        color: #f8fafc;
        font-size: 1.8rem;
        font-weight: 700;
        line-height: 1.2;
    }
    .metric-delta {
        color: #38bdf8;
        font-size: 0.8rem;
        margin-top: 4px;
    }
    .header-title {
        font-size: 1.9rem;
        font-weight: 700;
        color: #f8fafc;
        margin-bottom: 2px;
    }
    .header-subtitle {
        font-size: 1.0rem;
        color: #94a3b8;
        margin-bottom: 20px;
    }
    </style>
""", unsafe_allow_html=True)


# --- SIDEBAR & NAVEGACAO HIERARQUICA ---
with st.sidebar:
    st.markdown("## **QIMED Health Lakehouse**")
    st.markdown("### *Data Warehouse & BI*")
    st.markdown("---")

    visao = st.radio(
        "Nivel Hierarquico de Analise:",
        [
            "1. Visao Nacional (Brasil - 27 UFs)",
            "2. Visao Estadual (Por Estado)",
            "3. Visao Municipal (Por Municipio / ICSAP)",
            "4. Perfil Epidemiologico e Doencas (CID-10)",
            "5. Auditoria de Glosas Financeiras",
            "6. Terminal SQL Interativo",
            "7. Saude Suplementar e Ressarcimento ao SUS (ANS)",
        ]
    )

    df_nac_info = query_dw("SELECT periodo FROM dm_nacional_kpis LIMIT 1")
    periodo_label = df_nac_info.iloc[0]["periodo"] if not df_nac_info.empty else "Consolidado"

    st.markdown("---")
    st.markdown(f"**Periodo:** {periodo_label}")
    st.markdown("**Fontes:** DATASUS (SIH / SIA / CNES / SISREG)")
    st.markdown("**Engine:** DuckDB OLAP / Delta Lake")
    st.markdown("---")
    st.caption("Conformidade com a LGPD (MPI / Identificador Anonimizado).")


# ==============================================================================
# 1. VISAO NACIONAL (BRASIL - 27 UFs)
# ==============================================================================
if visao == "1. Visao Nacional (Brasil - 27 UFs)":
    st.markdown(f'<div class="header-title">Painel Executivo Nacional de Saude Publica ({periodo_label})</div>', unsafe_allow_html=True)
    st.markdown('<div class="header-subtitle">Consolidacao multi-fonte cobrindo todas as 27 Unidades da Federacao</div>', unsafe_allow_html=True)

    df_nac = query_dw("SELECT * FROM vw_kpi_nacional_sumario")
    df_est = query_dw("SELECT * FROM vw_kpi_estado_ocupacao_e_glosas")

    if not df_nac.empty:
        r = df_nac.iloc[0]
        c1, c2, c3, c4, c5 = st.columns(5)
        with c1:
            st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-title">Estados Monitorados</div>
                    <div class="metric-value">27 UFs</div>
                    <div class="metric-delta">Brasil Integral</div>
                </div>
            """, unsafe_allow_html=True)
        with c2:
            st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-title">Total de Internacoes</div>
                    <div class="metric-value">{int(r['total_internacoes']):,}</div>
                    <div class="metric-delta">Producao Julho (AIH)</div>
                </div>
            """, unsafe_allow_html=True)
        with c3:
            st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-title">Leitos Cadastrados</div>
                    <div class="metric-value">{int(r['leitos_totais_brasil']):,}</div>
                    <div class="metric-delta">Rede Hospitalar CNES</div>
                </div>
            """, unsafe_allow_html=True)
        with c4:
            st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-title">Taxa Media Ocupacao</div>
                    <div class="metric-value">{float(r['taxa_media_ocupacao_pct']):.1f}%</div>
                    <div class="metric-delta">Capacidade Operacional</div>
                </div>
            """, unsafe_allow_html=True)
        with c5:
            st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-title">Prejuizo por Glosas</div>
                    <div class="metric-value">R$ {float(r['total_glosado_brl'])/1e6:.1f}M</div>
                    <div class="metric-delta">Taxa Media: {float(r['taxa_media_glosa_pct']):.1f}%</div>
                </div>
            """, unsafe_allow_html=True)

    st.markdown("---")

    col_g1, col_g2 = st.columns(2)
    with col_g1:
        st.markdown("#### Taxa de Ocupacao de Leitos por Estado (%)")
        fig_leitos = px.bar(
            df_est.sort_values(by="taxa_ocupacao_leitos_pct", ascending=True),
            x="taxa_ocupacao_leitos_pct",
            y="estado_nome",
            orientation="h",
            color="regiao",
            labels={"taxa_ocupacao_leitos_pct": "Taxa de Ocupacao (%)", "estado_nome": "Estado", "regiao": "Regiao"}
        )
        fig_leitos.update_layout(template="plotly_dark", height=580, margin=dict(l=0, r=0, t=20, b=0))
        st.plotly_chart(fig_leitos, use_container_width=True)

    with col_g2:
        st.markdown("#### Ranking de Perdas Financeiras por Glosas (R$)")
        fig_glosas = px.bar(
            df_est.sort_values(by="total_glosado_brl", ascending=True),
            x="total_glosado_brl",
            y="estado_nome",
            orientation="h",
            color="taxa_glosa_pct",
            color_continuous_scale="Reds",
            labels={"total_glosado_brl": "Valor Glosado (R$)", "estado_nome": "Estado", "taxa_glosa_pct": "Taxa (%)"}
        )
        fig_glosas.update_layout(template="plotly_dark", height=580, margin=dict(l=0, r=0, t=20, b=0))
        st.plotly_chart(fig_glosas, use_container_width=True)


# ==============================================================================
# 2. VISAO ESTADUAL (POR ESTADO)
# ==============================================================================
elif visao == "2. Visao Estadual (Por Estado)":
    st.markdown('<div class="header-title">Diagnostico de Saude por Estado</div>', unsafe_allow_html=True)
    st.markdown('<div class="header-subtitle">Analise detalhada de capacidade de leitos, faturamento e mortalidade</div>', unsafe_allow_html=True)

    df_est = query_dw("SELECT * FROM vw_kpi_estado_ocupacao_e_glosas")
    estados_lista = sorted(df_est["estado_nome"].unique().tolist())
    
    estado_selecionado = st.selectbox("Selecione o Estado para Analise:", estados_lista, index=estados_lista.index("Ceará") if "Ceará" in estados_lista else 0)
    
    df_sel = df_est[df_est["estado_nome"] == estado_selecionado].iloc[0]

    ec1, ec2, ec3, ec4 = st.columns(4)
    with ec1:
        st.metric("Total de Internacoes (AIH)", f"{int(df_sel['total_internacoes']):,}")
    with ec2:
        st.metric("Leitos Totais / UTI", f"{int(df_sel['leitos_totais_cnes']):,} / {int(df_sel['leitos_uti_cnes']):,}")
    with ec3:
        st.metric("Taxa de Ocupacao de Leitos", f"{float(df_sel['taxa_ocupacao_leitos_pct']):.1f}%")
    with ec4:
        st.metric("Taxa de Mortalidade", f"{float(df_sel['taxa_mortalidade_pct']):.2f}%")

    st.markdown("---")
    st.markdown("#### Detalhamento Financeiro do Estado")
    st.dataframe(
        pd.DataFrame([df_sel]).rename(columns={
            "uf_sigla": "UF", "estado_nome": "Estado", "regiao": "Regiao",
            "total_internacoes": "Internacoes", "taxa_ocupacao_leitos_pct": "Ocupacao (%)",
            "custo_total_brl": "Custo Internacoes (R$)", "custo_medio_aih_brl": "Custo Medio/AIH (R$)",
            "total_faturado_brl": "Faturado Ambulatorial (R$)", "total_glosado_brl": "Glosado (R$)",
            "taxa_glosa_pct": "Taxa de Glosa (%)"
        }),
        use_container_width=True
    )


# ==============================================================================
# 3. VISAO MUNICIPAL (POR MUNICIPIO & ICSAP)
# ==============================================================================
elif visao == "3. Visao Municipal (Por Municipio / ICSAP)":
    st.markdown('<div class="header-title">Analise Municipal & Internacoes Evitaveis (ICSAP)</div>', unsafe_allow_html=True)
    st.markdown('<div class="header-subtitle">Impacto da Atencao Primaria a Saude por Cidade</div>', unsafe_allow_html=True)

    df_mun = query_dw("SELECT * FROM vw_kpi_municipio_saude_e_icsap")
    
    col_m1, col_m2 = st.columns([1.2, 1])
    with col_m1:
        st.markdown("#### Municipios com Maior Volume de Internacoes Evitaveis (ICSAP)")
        fig_mun = px.bar(
            df_mun.head(15),
            x="internacoes_icsap_evitaveis",
            y="municipality_name",
            orientation="h",
            color="taxa_icsap_pct",
            color_continuous_scale="YlOrRd",
            labels={"internacoes_icsap_evitaveis": "Internacoes Evitaveis", "municipality_name": "Municipio", "taxa_icsap_pct": "Taxa ICSAP (%)"}
        )
        fig_mun.update_layout(yaxis={'categoryorder':'total ascending'}, template="plotly_dark", height=480)
        st.plotly_chart(fig_mun, use_container_width=True)

    with col_m2:
        st.markdown("#### Indicadores Municipais Detalhados")
        st.dataframe(
            df_mun.rename(columns={
                "uf_sigla": "UF",
                "municipality_code": "Codigo IBGE",
                "municipality_name": "Municipio",
                "total_internacoes": "Total Internacoes",
                "internacoes_icsap_evitaveis": "Evitaveis (ICSAP)",
                "taxa_icsap_pct": "Taxa ICSAP (%)",
                "custo_total_brl": "Custo Total (R$)",
                "tempo_medio_permanencia_dias": "Media Permanencia (Dias)"
            }),
            use_container_width=True,
            height=480
        )


# ==============================================================================
# 4. PERFIL EPIDEMIOLOGICO E DOENCAS (CID-10)
# ==============================================================================
elif visao == "4. Perfil Epidemiologico e Doencas (CID-10)":
    st.markdown('<div class="header-title">Perfil Epidemiologico e Diagnosticos Clinicos (CID-10)</div>', unsafe_allow_html=True)
    st.markdown('<div class="header-subtitle">Distribuicao de internacoes por capitulo da CID-10, causas de obito e custos</div>', unsafe_allow_html=True)

    df_cap = query_dw("""
        SELECT 
            primary_diagnosis_chapter as especialidade,
            COUNT(*) as internacoes,
            ROUND(SUM(total_cost_brl), 2) as custo_total_brl,
            ROUND(AVG(total_cost_brl), 2) as custo_medio_brl,
            ROUND(AVG(length_of_stay_days), 1) as media_permanencia_dias,
            SUM(CASE WHEN discharge_disposition = 'expired' THEN 1 ELSE 0 END) as obitos,
            ROUND(SUM(CASE WHEN discharge_disposition = 'expired' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as taxa_mortalidade_pct
        FROM fct_encounters
        GROUP BY primary_diagnosis_chapter
        ORDER BY internacoes DESC
    """)

    col_ep1, col_ep2 = st.columns([1.1, 1])
    with col_ep1:
        st.markdown("#### Volume de Internacoes por Especialidade Medica")
        fig_cap = px.pie(
            df_cap,
            names="especialidade",
            values="internacoes",
            hole=0.4,
            template="plotly_dark"
        )
        fig_cap.update_layout(height=450, margin=dict(l=0, r=0, t=20, b=20))
        st.plotly_chart(fig_cap, use_container_width=True)

    with col_ep2:
        st.markdown("#### Custo Total por Especialidade Medica (R$)")
        fig_custo = px.bar(
            df_cap.sort_values(by="custo_total_brl", ascending=True),
            x="custo_total_brl",
            y="especialidade",
            orientation="h",
            color="custo_medio_brl",
            color_continuous_scale="Blues",
            labels={"custo_total_brl": "Custo Total (R$)", "especialidade": "Especialidade", "custo_medio_brl": "Custo Medio (R$)"}
        )
        fig_custo.update_layout(template="plotly_dark", height=450, margin=dict(l=0, r=0, t=20, b=0))
        st.plotly_chart(fig_custo, use_container_width=True)

    st.markdown("---")
    st.markdown("#### Top 15 Doencas Mais Frequentes com Nomes Traduzidos")
    df_top_diseases = query_dw("""
        SELECT 
            c.condition_code as codigo_cid10,
            c.disease_name as diagnostico_clinico,
            c.chapter_description as especialidade,
            COUNT(*) as total_casos,
            SUM(CASE WHEN e.discharge_disposition = 'expired' THEN 1 ELSE 0 END) as total_obitos,
            ROUND(SUM(CASE WHEN e.discharge_disposition = 'expired' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as letalidade_pct,
            ROUND(AVG(e.total_cost_brl), 2) as custo_medio_brl
        FROM fct_conditions c
        JOIN fct_encounters e ON c.encounter_id = e.encounter_id
        GROUP BY c.condition_code, c.disease_name, c.chapter_description
        ORDER BY total_casos DESC
        LIMIT 15
    """)
    st.dataframe(
        df_top_diseases.rename(columns={
            "codigo_cid10": "Codigo CID-10",
            "diagnostico_clinico": "Diagnostico Clinico Traduzido",
            "especialidade": "Capitulo",
            "total_casos": "Casos",
            "total_obitos": "Obitos",
            "letalidade_pct": "Letalidade (%)",
            "custo_medio_brl": "Custo Medio (R$)"
        }),
        use_container_width=True
    )


# ==============================================================================
# 5. AUDITORIA DE GLOSAS FINANCEIRAS
# ==============================================================================
elif visao == "5. Auditoria de Glosas Financeiras":
    st.markdown('<div class="header-title">Auditoria de Glosas e Faturamento SUS</div>', unsafe_allow_html=True)
    st.markdown('<div class="header-subtitle">Rastreamento de discrepancias financeiras e perdas de faturamento</div>', unsafe_allow_html=True)

    df_glo = query_dw("SELECT uf_sigla, estado_nome, regiao, total_faturado_brl, total_aprovado_brl, total_glosado_brl, taxa_glosa_pct FROM vw_kpi_estado_ocupacao_e_glosas ORDER BY total_glosado_brl DESC")
    st.dataframe(
        df_glo.rename(columns={
            "uf_sigla": "UF", "estado_nome": "Estado", "regiao": "Regiao",
            "total_faturado_brl": "Faturado (R$)", "total_aprovado_brl": "Aprovado (R$)",
            "total_glosado_brl": "Glosado (R$)", "taxa_glosa_pct": "Taxa Glosa (%)"
        }),
        use_container_width=True
    )


# ==============================================================================
# 6. TERMINAL SQL INTERATIVO
# ==============================================================================
elif visao == "6. Terminal SQL Interativo":
    st.markdown('<div class="header-title">Console SQL Analitico (DuckDB Engine)</div>', unsafe_allow_html=True)
    st.markdown('<div class="header-subtitle">Execute consultas SQL diretamente sobre as tabelas e views do Data Warehouse</div>', unsafe_allow_html=True)

    sql_exemplo = "SELECT estado_nome, taxa_ocupacao_leitos_pct, total_glosado_brl FROM vw_kpi_estado_ocupacao_e_glosas ORDER BY taxa_ocupacao_leitos_pct DESC LIMIT 5;"
    user_query = st.text_area("Consulta SQL:", value=sql_exemplo, height=100)

    if st.button("Executar Consulta SQL"):
        df_result = query_dw(user_query)
        st.dataframe(df_result, use_container_width=True)


# ==============================================================================
# 7. SAUDE SUPLEMENTAR E RESSARCIMENTO AO SUS (ANS)
# ==============================================================================
elif visao == "7. Saude Suplementar e Ressarcimento ao SUS (ANS)":
    st.markdown('<div class="header-title">Saude Suplementar e Ressarcimento ao SUS</div>', unsafe_allow_html=True)
    st.markdown('<div class="header-subtitle">Operadoras de Planos de Saude | Ressarcimento ABI/FNS | Notificacoes de Negativas (NIP)</div>', unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs([
        "Ressarcimento ao SUS (ABI / FNS)",
        "Ranking de Operadoras",
        "Negativas de Cobertura (NIP)",
    ])

    with tab1:
        st.subheader("Ressarcimento ao SUS por Operadora e UF")
        df_rss = query_dw("SELECT * FROM vw_kpi_ressarcimento_e_cobertura_ans")
        if df_rss.empty:
            st.info("Nenhum dado de ressarcimento disponivel. Execute o pipeline com a coleta ANS ativa.")
        else:
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Notificado (R$)", f"R$ {df_rss['total_notificado_brl'].sum():,.2f}")
            col2.metric("Total Recolhido ao FNS (R$)", f"R$ {df_rss['total_recolhido_brl'].sum():,.2f}")
            taxa_media = (df_rss['total_recolhido_brl'].sum() / max(df_rss['total_notificado_brl'].sum(), 1)) * 100
            col3.metric("Taxa Media de Recuperacao", f"{taxa_media:.1f}%")

            fig_rss = px.bar(
                df_rss.head(10),
                x="operadora",
                y=["total_notificado_brl", "total_recolhido_brl"],
                barmode="group",
                labels={"value": "Valor (R$)", "operadora": "Operadora", "variable": "Tipo"},
                title="Top 10 Operadoras: Notificado vs Recolhido ao FNS",
                color_discrete_map={"total_notificado_brl": "#3b82f6", "total_recolhido_brl": "#22c55e"},
            )
            fig_rss.update_layout(template="plotly_dark", xaxis_tickangle=-30)
            st.plotly_chart(fig_rss, use_container_width=True)

            st.dataframe(df_rss, use_container_width=True)

    with tab2:
        st.subheader("Cadastro Nacional de Operadoras Ativas (Cadop / ANS)")
        df_plans = query_dw("""
            SELECT cd_operadora, razao_social, modalidade, uf, cnpj, situacao
            FROM dim_health_plans
            ORDER BY razao_social
        """)
        if df_plans.empty:
            st.info("Nenhum dado de operadoras disponivel.")
        else:
            col1, col2 = st.columns(2)
            col1.metric("Operadoras Ativas", len(df_plans))

            modalidade_filtro = st.multiselect(
                "Filtrar por Modalidade:",
                options=df_plans["modalidade"].dropna().unique().tolist(),
                default=[]
            )
            df_filtrado = df_plans[df_plans["modalidade"].isin(modalidade_filtro)] if modalidade_filtro else df_plans

            if not df_filtrado.empty:
                fig_mod = px.pie(
                    df_filtrado.groupby("modalidade").size().reset_index(name="count"),
                    values="count",
                    names="modalidade",
                    title="Distribuicao por Modalidade de Operadora",
                )
                fig_mod.update_layout(template="plotly_dark")
                st.plotly_chart(fig_mod, use_container_width=True)

            st.dataframe(df_filtrado, use_container_width=True)

        st.subheader("Cobertura de Beneficiarios por Municipio")
        df_ben = query_dw("""
            SELECT cd_municipio_ibge, uf, razao_social, nr_beneficiarios_ativos, modalidade
            FROM dim_beneficiarios_municipio
            ORDER BY CAST(nr_beneficiarios_ativos AS INTEGER) DESC
        """)
        if not df_ben.empty:
            st.dataframe(df_ben, use_container_width=True)

    with tab3:
        st.subheader("Notificacoes de Negativas de Cobertura (NIP)")
        df_nip = query_dw("""
            SELECT uf, razao_social as operadora, motivo_negativa, desfecho_nip,
                   CAST(nr_notificacoes AS INTEGER) as nr_notificacoes
            FROM fct_nip_negativas
            ORDER BY CAST(nr_notificacoes AS INTEGER) DESC
        """)
        if df_nip.empty:
            st.info("Nenhum dado de NIP disponivel.")
        else:
            col1, col2 = st.columns(2)
            col1.metric("Total de Notificacoes", int(df_nip["nr_notificacoes"].sum()))
            revertidas = df_nip[df_nip["desfecho_nip"] == "REVERTIDA_PELA_OPERADORA"]["nr_notificacoes"].sum()
            col2.metric("Negativas Revertidas pelo Beneficiario", int(revertidas))

            fig_nip = px.bar(
                df_nip.groupby("motivo_negativa")["nr_notificacoes"].sum().reset_index().sort_values("nr_notificacoes", ascending=True).tail(10),
                x="nr_notificacoes",
                y="motivo_negativa",
                orientation="h",
                title="Top 10 Motivos de Negativas de Cobertura (NIP)",
                labels={"nr_notificacoes": "Notificacoes", "motivo_negativa": "Motivo"},
                color="nr_notificacoes",
                color_continuous_scale="Reds",
            )
            fig_nip.update_layout(template="plotly_dark")
            st.plotly_chart(fig_nip, use_container_width=True)

            fig_desfecho = px.pie(
                df_nip.groupby("desfecho_nip")["nr_notificacoes"].sum().reset_index(),
                values="nr_notificacoes",
                names="desfecho_nip",
                title="Desfecho das Notificacoes de Negativa",
                color_discrete_map={
                    "REVERTIDA_PELA_OPERADORA": "#22c55e",
                    "MANTIDA_PELA_OPERADORA": "#ef4444",
                    "EM_RECURSO": "#f59e0b",
                },
            )
            fig_desfecho.update_layout(template="plotly_dark")
            st.plotly_chart(fig_desfecho, use_container_width=True)

            st.dataframe(df_nip, use_container_width=True)

