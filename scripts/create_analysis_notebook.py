"""
Gera o Jupyter Notebook (.ipynb) completo de analise de dados em saude (QIMED).
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
            "# QIMED DataQore — Relatorio de Ciencia de Dados e Analise Integrada do SUS\n",
            "**Ambiente de Analise de Dados em Saude Digital (Acre - Janeiro/2025)**\n",
            "\n",
            "Este notebook carrega os dados integrados da arquitetura Lakehouse a partir do banco de dados SQLite (`exports/qimed_health_lakehouse.db`) ou arquivos CSV (`exports/csv/`), cruzando:\n",
            "- **SIH**: Internacoes Hospitalares (AIH)\n",
            "- **CNES**: Cadastro de Estabelecimentos e Leitos\n",
            "- **SIA**: Procedimentos Ambulatoriais e APAC (154 mil registros)\n",
            "- **SINAN**: Vigilancia Epidemiologica e Notificacoes de Agravos\n",
            "- **SISAB**: Producao da Atencao Primaria a Saude (e-SUS APS)\n",
            "- **Master Patient Index (MPI)**: Identidades unificadas com governanca LGPD (SHA-256 + Salt)"
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
            "# Configuracao visual\n",
            "sns.set_theme(style='whitegrid')\n",
            "plt.rcParams['figure.figsize'] = (12, 6)\n",
            "\n",
            "# Conectar ao Banco de Dados SQLite exportado\n",
            "db_path = '../exports/qimed_health_lakehouse.db'\n",
            "conn = sqlite3.connect(db_path)\n",
            "\n",
            "print('Tabelas disponiveis no banco de dados:')\n",
            "tables = pd.read_sql(\"SELECT name FROM sqlite_master WHERE type='table';\", conn)\n",
            "display(tables)"
        ]
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## 1. Carregamento dos Datasets"
        ]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "# 1. Tabelas Dimensionais e Fatos da Camada Silver\n",
            "df_encounters = pd.read_sql('SELECT * FROM fct_encounters;', conn)\n",
            "df_patients = pd.read_sql('SELECT * FROM dim_patients;', conn)\n",
            "df_conditions = pd.read_sql('SELECT * FROM fct_conditions;', conn)\n",
            "df_procedures = pd.read_sql('SELECT * FROM fct_procedures;', conn)\n",
            "\n",
            "# 2. Tabelas Complementares da Rede\n",
            "df_cnes = pd.read_sql('SELECT * FROM cnes_estabelecimentos;', conn)\n",
            "df_sia = pd.read_sql('SELECT * FROM sia_ambulatorial;', conn)\n",
            "df_sinan = pd.read_sql('SELECT * FROM sinan_agravos_dengue;', conn)\n",
            "df_sisab = pd.read_sql('SELECT * FROM sisab_atencao_primaria;', conn)\n",
            "\n",
            "print(f'Internacoes Hospitalares (SIH):       {len(df_encounters):,} registros')\n",
            "print(f'Pacientes Unicos (MPI):               {len(df_patients):,} registros')\n",
            "print(f'Procedimentos Ambulatoriais (SIA):   {len(df_sia):,} registros')\n",
            "print(f'Estabelecimentos Hospitalares (CNES): {len(df_cnes):,} registros')\n",
            "print(f'Notificacoes Epidemiologicas (SINAN): {len(df_sinan):,} registros')\n",
            "print(f'Municipios Atencao Primaria (SISAB):  {len(df_sisab):,} registros')"
        ]
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## 2. Inferencia 1: A Piramide Assistencial e o Fluxo Financeiro (SISAB -> SIA -> SIH)"
        ]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "total_aps_consultas = df_sisab['QT_ATENDIMENTOS'].sum()\n",
            "total_ambulatorial_procs = len(df_sia)\n",
            "total_internacoes = len(df_encounters)\n",
            "\n",
            "gasto_ambulatorial = df_sia['PA_VALPRO'].astype(float).sum()\n",
            "gasto_hospitalar = df_encounters['total_cost_brl'].sum()\n",
            "\n",
            "print(f'Consultas na Atencao Basica (APS):    {total_aps_consultas:,}')\n",
            "print(f'Procedimentos de Media/Alta (SIA):     {total_ambulatorial_procs:,} (R$ {gasto_ambulatorial:,.2f})')\n",
            "print(f'Internacoes Hospitalares (SIH):        {total_internacoes:,} (R$ {gasto_hospitalar:,.2f})')\n",
            "\n",
            "# Grafico de Comparacao de Custos\n",
            "fig, ax = plt.subplots(1, 2, figsize=(14, 5))\n",
            "\n",
            "ax[0].bar(['Atencao Basica (SISAB)', 'Ambulatorial (SIA)', 'Hospitalar (SIH)'], \n",
            "          [total_aps_consultas, total_ambulatorial_procs, total_internacoes], color=['#2ca02c', '#1f77b4', '#d62728'])\n",
            "ax[0].set_title('Volume de Eventos por Nivel de Assistencia')\n",
            "ax[0].set_ylabel('Quantidade de Procedimentos/Atendimentos')\n",
            "\n",
            "ax[1].pie([gasto_ambulatorial, gasto_hospitalar], labels=['Ambulatorial SIA (82.2%)', 'Hospitalar SIH (17.8%)'], \n",
            "          autopct='%1.1f%%', colors=['#1f77b4', '#d62728'], explode=(0.05, 0))\n",
            "ax[1].set_title('Distribuicao de Recursos Faturados ao SUS')\n",
            "\n",
            "plt.tight_layout()\n",
            "plt.show()"
        ]
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## 3. Inferencia 2: Taxa de Ocupacao e Sobrecarga de Leitos Hospitalares (SIH x CNES)"
        ]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "# Agregar dias de leito ocupados no SIH por hospital\n",
            "hosp_usage = df_encounters.groupby('organization_id').agg(\n",
            "    internacoes=('encounter_id', 'count'),\n",
            "    dias_leito=('length_of_stay_days', 'sum'),\n",
            "    obitos=('discharge_disposition', lambda x: (x == 'expired').sum())\n",
            ").reset_index()\n",
            "\n",
            "# Unir com CNES para calcular capacidade\n",
            "df_cnes_clean = df_cnes[['CNES', 'NOME_FANTASIA', 'LEITOS']].copy()\n",
            "df_cnes_clean['organization_id'] = 'org_cnes_' + df_cnes_clean['CNES'].astype(str)\n",
            "df_hosp_report = hosp_usage.merge(df_cnes_clean, on='organization_id', how='inner')\n",
            "\n",
            "dias_mes = 31\n",
            "df_hosp_report['taxa_ocupacao_pct'] = (df_hosp_report['dias_leito'] / (df_hosp_report['LEITOS'].astype(int) * dias_mes)) * 100\n",
            "df_hosp_report = df_hosp_report.sort_values('taxa_ocupacao_pct', ascending=False)\n",
            "\n",
            "display(df_hosp_report[['NOME_FANTASIA', 'LEITOS', 'internacoes', 'dias_leito', 'taxa_ocupacao_pct', 'obitos']])\n",
            "\n",
            "# Grafico da Taxa de Ocupacao\n",
            "plt.figure(figsize=(12, 6))\n",
            "sns.barplot(data=df_hosp_report, x='taxa_ocupacao_pct', y='NOME_FANTASIA', palette='Reds_r')\n",
            "plt.axvline(100, color='red', linestyle='--', label='Capacidade Maxima Nominal (100%)')\n",
            "plt.title('Taxa de Ocupacao Real de Leitos por Hospital (Janeiro/2025)')\n",
            "plt.xlabel('Taxa de Ocupacao (%)')\n",
            "plt.legend()\n",
            "plt.show()"
        ]
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## 4. Inferencia 3: Carga de Doenca, Mortalidade e Custos (CID-10)"
        ]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "# Analise de Letalidade e Custos por Diagnostico Principal\n",
            "diag_analysis = df_encounters.groupby(['primary_diagnosis_code', 'primary_diagnosis_chapter']).agg(\n",
            "    total_casos=('encounter_id', 'count'),\n",
            "    obitos=('discharge_disposition', lambda x: (x == 'expired').sum()),\n",
            "    custo_total=('total_cost_brl', 'sum'),\n",
            "    custo_medio=('total_cost_brl', 'mean'),\n",
            "    los_medio=('length_of_stay_days', 'mean')\n",
            ").reset_index()\n",
            "\n",
            "diag_analysis['taxa_mortalidade_pct'] = (diag_analysis['obitos'] / diag_analysis['total_casos']) * 100\n",
            "\n",
            "top_letais = diag_analysis[diag_analysis['total_casos'] >= 10].sort_values('taxa_mortalidade_pct', ascending=False).head(8)\n",
            "top_custo = diag_analysis.sort_values('custo_total', ascending=False).head(8)\n",
            "\n",
            "print('--- Top Diagnosticos com Maior Taxa de Mortalidade (Min. 10 Casos) ---')\n",
            "display(top_letais[['primary_diagnosis_code', 'primary_diagnosis_chapter', 'total_casos', 'obitos', 'taxa_mortalidade_pct', 'los_medio']])\n",
            "\n",
            "print('--- Top Diagnosticos com Maior Gasto Acumulado ao SUS ---')\n",
            "display(top_custo[['primary_diagnosis_code', 'primary_diagnosis_chapter', 'total_casos', 'custo_total', 'custo_medio']])"
        ]
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## 5. Inferencia 4: Readmissoes e Jornadas Longitudinais (Master Patient Index)"
        ]
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "# Identificar pacientes com multiplas internacoes no mesmo mes via MPI\n",
            "mpi_readmissions = df_encounters.groupby('patient_master_id').agg(\n",
            "    total_internacoes=('encounter_id', 'count'),\n",
            "    total_hospitais=('organization_id', 'nunique'),\n",
            "    custo_acumulado=('total_cost_brl', 'sum'),\n",
            "    dias_totais=('length_of_stay_days', 'sum')\n",
            ").reset_index()\n",
            "\n",
            "reinternados = mpi_readmissions[mpi_readmissions['total_internacoes'] > 1]\n",
            "print(f'Total de Pacientes no Mes:           {len(mpi_readmissions):,}')\n",
            "print(f'Pacientes com Readmissao (<30 dias): {len(reinternados):,} ({len(reinternados)/len(mpi_readmissions)*100:.2f}%)')\n",
            "\n",
            "# Exemplo de Paciente Superutilizador\n",
            "top_patient_id = reinternados.sort_values('total_internacoes', ascending=False).iloc[0]['patient_master_id']\n",
            "display(df_encounters[df_encounters['patient_master_id'] == top_patient_id][\n",
            "    ['encounter_id', 'organization_id', 'period_start', 'period_end', 'length_of_stay_days', 'primary_diagnosis_code', 'discharge_disposition', 'total_cost_brl']\n",
            "].sort_values('period_start'))"
        ]
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "## 6. Conclusao e Proximos Passos (QIMED AutoML & Quantum Optimization)\n",
            "As bases consolidadas neste banco SQLite fornecem os alvos exatos de modelagem:\n",
            "1. **AutoML**: Predicao de risco de readmissao hospitalar e tempo de permanencia (`length_of_stay_days`).\n",
            "2. **Otimizacao QUBO**: Redistribuicao de leitos e fluxo inter-hospitalar baseando-se nas taxas de ocupacao de `dim_organizations`.\n",
            "3. **Grafos Relacionais**: Rastreabilidade completa da jornada do paciente da Atencao Basica ao Hospital."
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

print(f"Jupyter Notebook gerado com sucesso em: {notebook_file}")
