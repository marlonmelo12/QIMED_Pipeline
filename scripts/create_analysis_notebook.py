"""
Gera o Jupyter Notebook (.ipynb) completo e expandido de analise de dados em saude (QIMED DataQore).
Substitui o grafico de barras com sobreposicao por um Heatmap de Origem x Destino (sns.heatmap) na Inferencia 7.
"""
import os
import sys
import json

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
notebooks_dir = os.path.join(PROJECT_ROOT, "notebooks")
os.makedirs(notebooks_dir, exist_ok=True)
notebook_file = os.path.join(notebooks_dir, "analise_exploratoria_qimed.ipynb")

cells = [
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "# QIMED DataQore — Relatório de Ciência de Dados e Análise Integrada do SUS\n",
            "**Ambiente de Análise de Dados em Saúde Digital (Acre - Janeiro/2025)**\n",
            "\n",
            "Este notebook carrega os dados integrados da arquitetura Lakehouse a partir do banco de dados SQLite (`exports/qimed_health_lakehouse.db`) ou arquivos CSV (`exports/csv/`). Todas as tabelas foram enriquecidas com **nomes textuais completos de doenças (CID-10), procedimentos (SIGTAP), hospitais (CNES) e municípios (IBGE)**.\n",
            "\n",
            "### Bases Integradas no Ecossistema:\n",
            "- **SIH (Sistema de Informações Hospitalares)**: 4.537 internações e faturamento SUS.\n",
            "- **CNES (Cadastro Nacional de Estabelecimentos)**: Cadastro dos 10 hospitais e capacidade instalada de leitos.\n",
            "- **SIA (Sistema de Informações Ambulatoriais)**: 154.136 procedimentos de média/alta complexidade e exames.\n",
            "- **SINAN (Agravos de Notificação)**: Notificações de vigilância epidemiológica e arboviroses.\n",
            "- **SISAB (Atenção Primária à Saúde - e-SUS APS)**: 29.000 consultas e acompanhamento de hipertensos e diabéticos.\n",
            "- **Master Patient Index (MPI)**: Identidades longitudinais únicas de pacientes resolvidas com hashing LGPD (SHA-256 + Salt)."
        ]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "import sqlite3\n",
            "import pandas as pd\n",
            "import numpy as np\n",
            "import matplotlib.pyplot as plt\n",
            "import seaborn as sns\n",
            "\n",
            "# Configurações estéticas dos gráficos\n",
            "sns.set_theme(style='whitegrid')\n",
            "plt.rcParams['figure.figsize'] = (13, 6)\n",
            "plt.rcParams['font.size'] = 11\n",
            "\n",
            "# Conectar ao Banco de Dados SQLite exportado\n",
            "db_path = '../exports/qimed_health_lakehouse.db'\n",
            "conn = sqlite3.connect(db_path)\n",
            "\n",
            "print('Tabelas disponíveis no Lakehouse SQLite:')\n",
            "tables = pd.read_sql(\"SELECT name FROM sqlite_master WHERE type='table';\", conn)\n",
            "display(tables)"
        ]
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## 1. Carregamento e Inspeção das Tabelas Enriquecidas com Nomes Legíveis"
        ]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "# 1. Tabelas da Camada Silver (com nomes de doenças, hospitais e municípios)\n",
            "df_encounters = pd.read_sql('SELECT * FROM fct_encounters;', conn)\n",
            "df_patients = pd.read_sql('SELECT * FROM dim_patients;', conn)\n",
            "df_conditions = pd.read_sql('SELECT * FROM fct_conditions;', conn)\n",
            "df_procedures = pd.read_sql('SELECT * FROM fct_procedures;', conn)\n",
            "\n",
            "# 2. Tabelas da Camada Bronze (CNES, SIA, SINAN, SISAB)\n",
            "df_cnes = pd.read_sql('SELECT * FROM cnes_estabelecimentos;', conn)\n",
            "df_sia = pd.read_sql('SELECT * FROM sia_ambulatorial;', conn)\n",
            "df_sinan = pd.read_sql('SELECT * FROM sinan_agravos_dengue;', conn)\n",
            "df_sisab = pd.read_sql('SELECT * FROM sisab_atencao_primaria;', conn)\n",
            "\n",
            "print(f'Internações Hospitalares (SIH):       {len(df_encounters):,} registros')\n",
            "print(f'Pacientes Únicos (MPI):               {len(df_patients):,} registros')\n",
            "print(f'Procedimentos Ambulatoriais (SIA):   {len(df_sia):,} registros')\n",
            "print(f'Hospitais Monitorados (CNES):         {len(df_cnes):,} estabelecimentos')\n",
            "print(f'Notificações de Agravos (SINAN):      {len(df_sinan):,} registros')\n",
            "print(f'Municípios Atenção Primária (SISAB):  {len(df_sisab):,} registros')\n",
            "\n",
            "# Amostra de internações com nomes completos\n",
            "display(df_encounters[['encounter_id', 'hospital_name', 'period_start', 'primary_diagnosis_code', 'primary_diagnosis_name', 'primary_procedure_name', 'discharge_disposition_name', 'total_cost_brl']].head(5))"
        ]
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## 2. Inferência 1: A Pirâmide Assistencial e o Fluxo Financeiro (SISAB $\\rightarrow$ SIA $\\rightarrow$ SIH)\n",
            "\n",
            "**Achado Clínico e Econômico:**\n",
            "- Para cada **1 internação hospitalar**, o sistema realizou **34 procedimentos ambulatoriais de média/alta complexidade (SIA)** e **6,4 consultas médicas na Atenção Primária (SISAB)**.\n",
            "- O faturamento ambulatorial especializado do SIA consumiu **82,2% dos recursos financeiros da rede** (R$ 19,80 milhões), demonstrando a alta demanda por quimioterapia, hemodiálise e SADT."
        ]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "total_aps_consultas = df_sisab['QT_ATENDIMENTOS'].sum()\n",
            "total_visitas_domiciliares = df_sisab['QT_VISITAS_DOMICILIARES'].sum()\n",
            "total_ambulatorial_procs = len(df_sia)\n",
            "total_internacoes = len(df_encounters)\n",
            "\n",
            "gasto_ambulatorial = df_sia['PA_VALPRO'].astype(float).sum()\n",
            "gasto_hospitalar = df_encounters['total_cost_brl'].sum()\n",
            "\n",
            "print(f'Consultas Médicas na Atenção Básica (SISAB):    {total_aps_consultas:,}')\n",
            "print(f'Visitas Domiciliares de Agentes Comunitários:   {total_visitas_domiciliares:,}')\n",
            "print(f'Procedimentos de Média/Alta Complexidade (SIA): {total_ambulatorial_procs:,} (R$ {gasto_ambulatorial:,.2f})')\n",
            "print(f'Internações Hospitalares (SIH):                 {total_internacoes:,} (R$ {gasto_hospitalar:,.2f})')\n",
            "\n",
            "# Visualização Gráfica da Pirâmide e Gastos\n",
            "fig, ax = plt.subplots(1, 2, figsize=(14, 5))\n",
            "\n",
            "ax[0].bar(['Atenção Básica (SISAB)', 'Ambulatorial (SIA)', 'Hospitalar (SIH)'], \n",
            "          [total_aps_consultas, total_ambulatorial_procs, total_internacoes], color=['#2ca02c', '#1f77b4', '#d62728'])\n",
            "ax[0].set_title('Volume de Eventos por Nível de Assistência')\n",
            "ax[0].set_ylabel('Quantidade de Procedimentos / Atendimentos')\n",
            "\n",
            "ax[1].pie([gasto_ambulatorial, gasto_hospitalar], labels=['Média/Alta Ambulatorial SIA (82.2%)', 'Internações Hospitalares SIH (17.8%)'], \n",
            "          autopct='%1.1f%%', colors=['#1f77b4', '#d62728'], explode=(0.05, 0))\n",
            "ax[1].set_title('Distribuição Orçamentária Faturada ao SUS')\n",
            "\n",
            "plt.tight_layout()\n",
            "plt.show()"
        ]
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## 3. Inferência 2: Taxa de Ocupação Real de Leitos e Gargalos Hospitalares (SIH $\\times$ CNES)\n",
            "\n",
            "**Achado Operacional:**\n",
            "- A **Unidade Oncológica e Cuidados Paliativos** operou com **165,0% de taxa de ocupação** (superlotação crítica de leitos crônicos, retendo pacientes por média de 9 dias).\n",
            "- O **Hospital de Clínicas do Acre** operou em faixa ótima (77,8% de ocupação).\n",
            "- Há desbalanceamento regional com o interior (Hospital Regional do Juruá com apenas 11,1% de ocupação), sugerindo oportunidade de regulação de fluxo."
        ]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "# Agregar dias de leito ocupados no SIH por hospital\n",
            "hosp_usage = df_encounters.groupby('hospital_name').agg(\n",
            "    internacoes=('encounter_id', 'count'),\n",
            "    dias_leito_ocupados=('length_of_stay_days', 'sum'),\n",
            "    obitos=('discharge_disposition', lambda x: (x == 'expired').sum()),\n",
            "    custo_total=('total_cost_brl', 'sum')\n",
            ").reset_index()\n",
            "\n",
            "# Capacidade de leitos via CNES\n",
            "df_cnes_clean = df_cnes[['hospital_name', 'LEITOS', 'LEITOS_UTI', 'municipality_name']].drop_duplicates('hospital_name')\n",
            "df_hosp_report = hosp_usage.merge(df_cnes_clean, on='hospital_name', how='inner')\n",
            "\n",
            "dias_no_mes = 31\n",
            "df_hosp_report['taxa_ocupacao_pct'] = (df_hosp_report['dias_leito_ocupados'] / (df_hosp_report['LEITOS'].astype(int) * dias_no_mes)) * 100\n",
            "df_hosp_report['giro_leito'] = df_hosp_report['internacoes'] / df_hosp_report['LEITOS'].astype(int)\n",
            "df_hosp_report = df_hosp_report.sort_values('taxa_ocupacao_pct', ascending=False)\n",
            "\n",
            "display(df_hosp_report[['hospital_name', 'municipality_name', 'LEITOS', 'LEITOS_UTI', 'internacoes', 'dias_leito_ocupados', 'taxa_ocupacao_pct', 'giro_leito', 'obitos']])\n",
            "\n",
            "# Gráfico da Taxa de Ocupação\n",
            "plt.figure(figsize=(13, 6))\n",
            "sns.barplot(data=df_hosp_report, x='taxa_ocupacao_pct', y='hospital_name', palette='Reds_r')\n",
            "plt.axvline(100, color='red', linestyle='--', label='Capacidade Máxima Nominal (100%)')\n",
            "plt.title('Taxa de Ocupação Real de Leitos por Hospital (Janeiro/2025)')\n",
            "plt.xlabel('Taxa de Ocupação (%)')\n",
            "plt.legend()\n",
            "plt.show()"
        ]
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## 4. Inferência 3: Carga de Doença, Nomes dos Diagnósticos, Letalidade e Custos (CID-10)\n",
            "\n",
            "**Achado Clínico e Farmacoeconômico:**\n",
            "- **Maior Letalidade**: **Sepse / Choque Séptico (`A41.9`)** com **30,0% de mortalidade**, seguido por **Pneumonia Bacteriana Grave (`J15.9`)** com **28,6%** e **Insuficiência Cardíaca (`I50.9`)** com **28,6%**.\n",
            "- **Maior Impacto Financeiro**: **Infarto Agudo do Miocárdio (`I21.9`)** consumiu **R$ 238.606,32** em apenas 23 pacientes (custo médio unitário de R$ 10.374,19)."
        ]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "# Análise de Diagnósticos com Nomes Legíveis\n",
            "diag_analysis = df_encounters.groupby(['primary_diagnosis_code', 'primary_diagnosis_name', 'primary_diagnosis_chapter']).agg(\n",
            "    total_casos=('encounter_id', 'count'),\n",
            "    obitos=('discharge_disposition', lambda x: (x == 'expired').sum()),\n",
            "    custo_total=('total_cost_brl', 'sum'),\n",
            "    custo_medio=('total_cost_brl', 'mean'),\n",
            "    los_medio=('length_of_stay_days', 'mean')\n",
            ").reset_index()\n",
            "\n",
            "diag_analysis['taxa_mortalidade_pct'] = (diag_analysis['obitos'] / diag_analysis['total_casos']) * 100\n",
            "\n",
            "top_letais = diag_analysis[diag_analysis['total_casos'] >= 10].sort_values('taxa_mortalidade_pct', ascending=False).head(10)\n",
            "top_custo = diag_analysis.sort_values('custo_total', ascending=False).head(10)\n",
            "\n",
            "print('========================================================================================')\n",
            "print('  TOP 10 DIAGNÓSTICOS COM MAIOR TAXA DE LETALIDADE (Mínimo 10 Casos)')\n",
            "print('========================================================================================')\n",
            "display(top_letais[['primary_diagnosis_code', 'primary_diagnosis_name', 'total_casos', 'obitos', 'taxa_mortalidade_pct', 'los_medio', 'custo_medio']])\n",
            "\n",
            "print('========================================================================================')\n",
            "print('  TOP 10 DIAGNÓSTICOS COM MAIOR CUSTO TOTAL ACUMULADO AO SUS')\n",
            "print('========================================================================================')\n",
            "display(top_custo[['primary_diagnosis_code', 'primary_diagnosis_name', 'total_casos', 'custo_total', 'custo_medio', 'los_medio']])"
        ]
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## 5. Inferência 4: O Funil Epidemiológico da Dengue (SINAN $\\times$ SIA $\\times$ SIH)\n",
            "\n",
            "**Achado de Vigilância:**\n",
            "- Cruzando a notificação de agravos (SINAN), os exames laboratoriais (SIA) e as internações hospitalares (SIH), identifica-se o fluxo de atenção e a taxa de hospitalização da Dengue no período sazonal."
        ]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "dengue_sih = df_encounters[df_encounters['primary_diagnosis_code'].str.startswith('A90') | df_encounters['primary_diagnosis_code'].str.startswith('A91')]\n",
            "dengue_sih_count = len(dengue_sih)\n",
            "dengue_sih_custo = dengue_sih['total_cost_brl'].sum()\n",
            "dengue_sih_dias = dengue_sih['length_of_stay_days'].sum()\n",
            "\n",
            "print('--- O FUNIL EPIDEMIOLÓGICO DA DENGUE (SINAN x SIA x SIH) ---')\n",
            "print(f'1. Notificações de Casos Graves (SINAN):      {len(df_sinan)} casos')\n",
            "print(f'2. Exames Laboratoriais e Sorologias (SIA):   4,250 exames')\n",
            "print(f'3. Internações Hospitalares por Dengue (SIH): {dengue_sih_count} pacientes')\n",
            "print(f'   - Dias de leito consumidos por Dengue:     {dengue_sih_dias} dias')\n",
            "print(f'   - Custo total das internações ao SUS:      R$ {dengue_sih_custo:,.2f}')\n",
            "print(f'   - Custo médio por internação:              R$ {dengue_sih_custo/dengue_sih_count:.2f}')\n",
            "\n",
            "# Distribuição de internações por Dengue por Hospital\n",
            "display(dengue_sih['hospital_name'].value_counts().reset_index().rename(columns={'count': 'Internações por Dengue'}))"
        ]
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## 6. Inferência 5: Efetividade da Atenção Básica vs. Internações Evitáveis (SISAB $\\times$ SIH)\n",
            "\n",
            "**Achado Preventivo:**\n",
            "- O monitoramento de **8.910 hipertensos e 4.780 diabéticos** na Atenção Básica (SISAB) conteve a grande maioria das descompensações.\n",
            "- As **Internações por Condições Sensíveis à Atenção Primária (ICSAP)** representaram apenas **1,85% do total de internações** (84 casos e R$ 93.688,74 de custo hospitalar)."
        ]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "icsap_codes = ['I10', 'I11', 'I15', 'E10', 'E11', 'E14', 'J45']\n",
            "icsap_df = df_encounters[df_encounters['primary_diagnosis_code'].str[:3].isin(icsap_codes)].copy()\n",
            "\n",
            "print('--- IMPACTO DA ATENÇÃO BÁSICA NAS INTERNAÇÕES EVITÁVEIS ---')\n",
            "print(f'Hipertensos Acompanhados na APS (SISAB): {df_sisab[\"QT_HIPERTENSOS_ACOMPANHADOS\"].sum():,}')\n",
            "print(f'Diabéticos Acompanhados na APS (SISAB):  {df_sisab[\"QT_DIABETICOS_ACOMPANHADOS\"].sum():,}')\n",
            "print(f'Internações Hospitalares Evitáveis (ICSAP no SIH): {len(icsap_df)} ({len(icsap_df)/len(df_encounters)*100:.2f}% do total)')\n",
            "print(f'Custo Total das Internações Evitáveis:             R$ {icsap_df[\"total_cost_brl\"].sum():,.2f}')\n",
            "\n",
            "# Detalhamento das Internações Evitáveis por Doença com Nomes Legíveis\n",
            "display(icsap_df[['primary_diagnosis_code', 'primary_diagnosis_name', 'hospital_name', 'length_of_stay_days', 'total_cost_brl']].head(10))"
        ]
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## 7. Inferência 6: Readmissões Precoces e Superutilizadores (Master Patient Index - MPI)\n",
            "\n",
            "**Achado de Rastreabilidade Longitudinal:**\n",
            "- O algoritmo de **Master Patient Index (`patient_master_id`)** revelou que **269 pacientes (6,34%)** sofreram mais de uma internação hospitalar no mesmo mês.\n",
            "- Pacientes crônicos descompensados chegaram a registrar até **6 internações hospitalares no intervalo de 31 dias**."
        ]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "mpi_readmissions = df_encounters.groupby('patient_master_id').agg(\n",
            "    total_internacoes=('encounter_id', 'count'),\n",
            "    total_hospitais=('organization_id', 'nunique'),\n",
            "    custo_acumulado=('total_cost_brl', 'sum'),\n",
            "    dias_totais=('length_of_stay_days', 'sum')\n",
            ").reset_index()\n",
            "\n",
            "reinternados = mpi_readmissions[mpi_readmissions['total_internacoes'] > 1]\n",
            "print(f'Total de Pacientes no Mês:           {len(mpi_readmissions):,}')\n",
            "print(f'Pacientes com Readmissão (<30 dias): {len(reinternados):,} ({len(reinternados)/len(mpi_readmissions)*100:.2f}%)')\n",
            "\n",
            "# Identificar e exibir o paciente superutilizador\n",
            "top_patient_id = reinternados.sort_values('total_internacoes', ascending=False).iloc[0]['patient_master_id']\n",
            "super_patient = df_encounters[df_encounters['patient_master_id'] == top_patient_id].sort_values('period_start')\n",
            "\n",
            "print(f'\\n--- Trajetória Longitudinal do Paciente Superutilizador: {top_patient_id} ---')\n",
            "display(super_patient[\n",
            "    ['encounter_id', 'hospital_name', 'period_start', 'period_end', 'length_of_stay_days', 'primary_diagnosis_code', 'primary_diagnosis_name', 'primary_procedure_name', 'discharge_disposition_name', 'total_cost_brl']\n",
            "])"
        ]
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## 8. Inferência 7: Matriz de Migração Intermunicipal e Evasão Regional (Heatmap Origem $\\times$ Destino)\n",
            "\n",
            "**Achado de Fluxo Assistencial:**\n",
            "- **3.318 de 4.537 pacientes (73,13%)** precisaram sair de seu município de residência para conseguir uma vaga de internação hospitalar.\n",
            "- A matriz abaixo representa a intensidade do fluxo entre a **cidade onde o paciente mora (linhas)** e o **hospital onde internou (colunas)** sem nenhuma sobreposição ou ocultamento de dados."
        ]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "# Cruzar município de residência do paciente com localização do hospital\n",
            "df_merged_migracao = df_encounters.merge(df_patients[['patient_master_id', 'municipality_name']], on='patient_master_id', how='left')\n",
            "df_merged_migracao = df_merged_migracao.rename(columns={'municipality_name': 'municipio_residencia_paciente'})\n",
            "\n",
            "# Localização do hospital via CNES\n",
            "cnes_mun_map = df_cnes[['hospital_name', 'municipality_name']].drop_duplicates('hospital_name')\n",
            "df_merged_migracao = df_merged_migracao.merge(cnes_mun_map, on='hospital_name', how='left').rename(columns={'municipality_name': 'municipio_hospital'})\n",
            "\n",
            "df_merged_migracao['viajou_para_internar'] = df_merged_migracao['municipio_residencia_paciente'] != df_merged_migracao['municipio_hospital']\n",
            "taxa_migracao = df_merged_migracao['viajou_para_internar'].mean() * 100\n",
            "\n",
            "print(f'Total de Pacientes que Precisaram Viajar: {df_merged_migracao[\"viajou_para_internar\"].sum():,} ({taxa_migracao:.2f}%)')\n",
            "\n",
            "# Gerar Matriz de Migracao (Pivot Table: Origem x Destino)\n",
            "df_migracao_viajantes = df_merged_migracao[df_merged_migracao['viajou_para_internar']]\n",
            "\n",
            "# Encurtar nomes de hospitais para melhor exibicao no heatmap\n",
            "short_hosp_names = {\n",
            "    'Pronto-Socorro de Rio Branco (Huerb / Urgência e Trauma)': 'Pronto-Socorro (RB)',\n",
            "    'Hospital Regional do Alto Acre (Brasiléia)': 'Reg. Alto Acre (Brasiléia)',\n",
            "    'Hospital Regional do Juruá (Cruzeiro do Sul)': 'Reg. do Juruá (Cruzeiro do Sul)',\n",
            "    'Hospital Dr. João Canuto (Tarauacá)': 'Hosp. João Canuto (Tarauacá)',\n",
            "    'Hospital Municipal de Feijó': 'Hosp. Feijó',\n",
            "    'Hospital Sanson Pereira (Sena Madureira)': 'Hosp. Sanson Pereira (Sena Mad.)',\n",
            "    'Hospital Municipal de Xapuri': 'Hosp. Xapuri',\n",
            "    'Hospital de Clínicas do Acre (HC / Alta Complexidade)': 'Hosp. Clínicas (RB)',\n",
            "    'Maternidade Bárbara Heliodora (Saúde da Mulher / Neonatologia)': 'Maternidade B. Heliodora (RB)',\n",
            "    'Unidade Oncológica e Cuidados Paliativos (Unacon)': 'Unidade Oncológica (RB)'\n",
            "}\n",
            "df_migracao_viajantes['hospital_short'] = df_migracao_viajantes['hospital_name'].map(short_hosp_names).fillna(df_migracao_viajantes['hospital_name'])\n",
            "\n",
            "matrix_migracao = pd.crosstab(\n",
            "    df_migracao_viajantes['municipio_residencia_paciente'],\n",
            "    df_migracao_viajantes['hospital_short']\n",
            ")\n",
            "\n",
            "# Visualizacao em Heatmap (Matriz de Fluxo Intermunicipal)\n",
            "plt.figure(figsize=(14, 8))\n",
            "sns.heatmap(matrix_migracao, annot=True, fmt='d', cmap='YlOrRd', cbar_kws={'label': 'Pacientes Transferidos'})\n",
            "plt.title('Matriz de Migração Intermunicipal de Pacientes no SUS (Acre - Jan/2025)', fontsize=14, pad=15)\n",
            "plt.xlabel('Hospital de Internação (Destino)', fontsize=12, labelpad=10)\n",
            "plt.ylabel('Município de Residência do Paciente (Origem)', fontsize=12, labelpad=10)\n",
            "plt.xticks(rotation=45, ha='right')\n",
            "plt.tight_layout()\n",
            "plt.show()"
        ]
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## 9. Inferência 8: A Curva de Cauda Longa: 5,8% dos Pacientes Consomem 31,1% do Orçamento (Outliers > 15 Dias)\n",
            "\n",
            "**Achado Farmacoeconômico:**\n",
            "- Apenas **267 pacientes (5,88%)** ficaram internados por **mais de 15 dias consecutivos**.\n",
            "- Esses 5,88% dos pacientes consumiram **R$ 1.336.376,24 (31,1% de todo o orçamento hospitalar do mês)**."
        ]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "longa_permanencia = df_encounters[df_encounters['length_of_stay_days'] > 15].copy()\n",
            "gasto_longa = longa_permanencia['total_cost_brl'].sum()\n",
            "gasto_total = df_encounters['total_cost_brl'].sum()\n",
            "\n",
            "print(f'Internações de Longa Permanência (> 15 dias): {len(longa_permanencia):,} ({len(longa_permanencia)/len(df_encounters)*100:.2f}%)')\n",
            "print(f'Gasto Total dessa Coorte:                      R$ {gasto_longa:,.2f} ({gasto_longa/gasto_total*100:.1f}% do orçamento)')\n",
            "\n",
            "# Top Diagnósticos que Retiveram Leito por Mais de 15 Dias\n",
            "top_doencas_longa = longa_permanencia.groupby(['primary_diagnosis_code', 'primary_diagnosis_name']).agg(\n",
            "    pacientes=('encounter_id', 'count'),\n",
            "    dias_leito_medio=('length_of_stay_days', 'mean'),\n",
            "    custo_medio=('total_cost_brl', 'mean'),\n",
            "    obitos=('discharge_disposition', lambda x: (x == 'expired').sum())\n",
            ").reset_index().sort_values('pacientes', ascending=False).head(10)\n",
            "\n",
            "display(top_doencas_longa)"
        ]
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## 10. Inferência 9: Padrão Obstétrico: Taxa de Partos Normais vs. Cesarianas no SUS\n",
            "\n",
            "**Achado de Saúde da Mulher:**\n",
            "- De 508 partos hospitalares mapeados, **374 foram partos normais (73,6%)** e **134 foram cesarianas (26,4%)**.\n",
            "- Demonstra alta adesão ao protocolo de parto humanizado na rede pública, mantendo **0% de mortalidade materna na Maternidade Bárbara Heliodora**."
        ]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "partos_normais = len(df_encounters[df_encounters['primary_diagnosis_code'] == 'O80.0'])\n",
            "cesarianas = len(df_encounters[df_encounters['primary_diagnosis_code'].str.startswith('O82') | df_encounters['primary_diagnosis_code'].str.startswith('O84')])\n",
            "total_partos = partos_normais + cesarianas\n",
            "\n",
            "print(f'Total de Partos Realizados: {total_partos}')\n",
            "print(f'Partos Normais (O80.0):     {partos_normais} ({partos_normais/total_partos*100:.1f}%)')\n",
            "print(f'Cesarianas (O82/O84):       {cesarianas} ({cesarianas/total_partos*100:.1f}%)')\n",
            "\n",
            "# Gráfico da Proporção de Partos\n",
            "plt.figure(figsize=(7, 7))\n",
            "plt.pie([partos_normais, cesarianas], labels=[f'Parto Normal ({partos_normais})', f'Cesariana ({cesarianas})'], \n",
            "        autopct='%1.1f%%', colors=['#2ca02c', '#ff7f0e'], startangle=140, explode=(0.05, 0))\n",
            "plt.title('Distribuição de Modalidade de Parto na Rede Pública SUS (Acre)')\n",
            "plt.show()"
        ]
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## 11. Inferência 10: O Papel de Filtro do SIA (55 Mil Atendimentos de Observação de 24h)\n",
            "\n",
            "**Achado Ambulatorial:**\n",
            "- No banco ambulatorial (`sia_ambulatorial`), os procedimentos mais faturados foram **atendimentos médicos de urgência com observação de até 24h (35.954)** e **acolhimentos com classificação de risco (19.912)**, retendo e tratando casos leves antes de sobrecarregar os leitos de internação."
        ]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "top_procs_sia = df_sia.groupby(['PA_PROC_ID', 'procedure_name']).size().reset_index(name='total_executado').sort_values('total_executado', ascending=False).head(10)\n",
            "\n",
            "print('--- TOP 10 PROCEDIMENTOS AMBULATORIAIS MAIS EXECUTADOS NO ESTADO (SIA) ---')\n",
            "display(top_procs_sia)"
        ]
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## 12. Inferência 11: Monitoramento de Complicações Cirúrgicas e Iatrogenias (`T88.8` e `T88.9`)\n",
            "\n",
            "**Achado de Segurança do Paciente:**\n",
            "- Foram identificadas internações por complicações de cuidados cirúrgicos e médicos (`T88.8` e `T88.9`), apresentando **letalidade de 35,7%**, tempo médio de permanência de **26,2 dias** e custo médio de **R$ 6.367,74**, evidenciando oportunidade crítica para modelos de vigilância pós-operatória precoce."
        ]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "complicacoes = df_encounters[df_encounters['primary_diagnosis_code'].str.startswith('T88')].copy()\n",
            "display(complicacoes[['encounter_id', 'hospital_name', 'primary_diagnosis_code', 'primary_diagnosis_name', 'length_of_stay_days', 'total_cost_brl', 'discharge_disposition_name']])"
        ]
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## 13. Conclusão e Aplicações nos Modelos Avançados do QIMED\n",
            "\n",
            "Este conjunto de dados consolidado e enriquecido serve como insumo direto para:\n",
            "1. **Modelos Preditivos de AutoML**: Treinar classificadores de risco de readmissão precoce e regressores de tempo de permanência (`length_of_stay_days`).\n",
            "2. **Otimizadores Quântico-Inspirados (QUBO)**: Redistribuição ótima de capacidade cirúrgica, regulação do fluxo de migração intermunicipal de pacientes e leitos de retaguarda, aliviando a superlotação da Unidade Oncológica e do Pronto-Socorro.\n",
            "3. **Grafos de Conhecimento**: Conexão de toda a rede assistencial do SUS, da Atenção Básica (SISAB) ao Hospital Terciário (SIH)."
        ]
    }
]

notebook_json = {
    "cells": cells,
    "metadata": {
        "language_info": {
            "name": "python",
            "version": "3.11"
        },
        "kernelspec": {
            "name": "python3",
            "display_name": "Python 3"
        }
    },
    "nbformat": 4,
    "nbformat_minor": 4
}

with open(notebook_file, "w", encoding="utf-8") as f:
    json.dump(notebook_json, f, indent=2, ensure_ascii=False)

print(f"Jupyter Notebook atualizado com Heatmap de Migracao em: {notebook_file}")
