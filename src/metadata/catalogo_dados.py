"""
Catálogo de Dados de Saúde - QIMED Lakehouse V3.
Documenta a semântica, origem técnica e regras de cada entidade e atributo.
"""

CATALOGO_ENTIDADES = {
    "dim_tempo": "Dimensão de datas, anos, meses, trimestres, semestres e indicador de dias úteis.",
    "dim_paciente": "Cadastro pseudonimizado de pacientes com chaves MPI e atributos demográficos.",
    "dim_operadoras_saude": "Cadastro oficial de operadoras ativas de planos de saúde da ANS (Cadop).",
    "dim_estabelecimento": "Cadastro Nacional de Estabelecimentos de Saúde (CNES), mantenedoras e leitos.",
    "dim_municipio": "Tabela de referência de municípios do IBGE com códigos de 6 e 7 dígitos.",
    "dim_procedimento": "Tabela unificada de procedimentos e ações de saúde SIGTAP/SUS.",
    "dim_diagnostico": "Classificação Estatística Internacional de Doenças e Problemas Relacionados à Saúde (CID-10).",
    "fct_internacao": "Fatos de internações hospitalares do SUS (SIH/RD) com diagnósticos, permanência e valores.",
    "fct_atendimentos_ambulatoriais": "Fatos da produção ambulatorial do SUS (SIA/PA) com procedimentos e valores.",
    "fct_ressarcimento_sus": "Fatos de cobranças de ressarcimento ao SUS das operadoras privadas (ABI/ANS).",
    "fct_glosas_hospitalares": "Fatos de glosas e recusas de AIHs do SIH-RJ e relatórios de críticas do SIH-ER.",
    "aud_alertas_anomalias": "Tabela de auditoria clínica forense e gestão operacional da Central de Anomalias.",
    "agg_internacoes_uf": "Data Mart analítico de internações, dias de leito, mortalidade e custos por UF.",
    "agg_procedimentos_uf": "Data Mart analítico de procedimentos ambulatoriais produzidos e aprovados por UF.",
    "agg_perfil_epidemiologico": "Data Mart analítico de prevalência de internações e óbitos por capítulo de CID-10.",
    "lakehouse_system_manifest": "Tabela Delta da Camada Sistema com histórico transacional de cada arquivo e UF.",
    "lakehouse_system_lineage": "Tabela Delta da Camada Sistema com a árvore genealógica de cada transformação.",
    "lakehouse_system_metrics": "Tabela Delta da Camada Sistema com telemetria contínua de hardware (RAM/CPU/Throughput).",
}
