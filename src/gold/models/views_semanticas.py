"""
Módulo de Views Semânticas & Contratos Analíticos e Preditivos - QIMED Lakehouse V3.
Implementa a camada semântica com:
1. vw_internacoes_consolidadas (Granularidade 1:1 por numero_aih para BI e ANS)
2. vw_ressarcimento_sus_consolidado_por_aih (Pré-agregação 1:1 de ressarcimento por AIH para evitar explosão de cardinalidade)
3. vw_ml_features_admissao (Features puras de admissão - zero leakage para inferência)
4. vw_ml_targets_internacao (Alvos de desfecho pós-alta isolados)
5. vw_ml_treinamento_admissao (Dataset para treino offline via JOIN controlado)
"""
import duckdb
from src.utils.logging_config import setup_logger

logger = setup_logger(__name__)


def registrar_views_semanticas(conn: duckdb.DuckDBPyConnection):
    """
    Registra as views semânticas para consumo de BI, Analytics e Treinamento de ML.
    """
    logger.info("Registrando views semânticas analíticas e preditivas no DuckDB...")

    # 1. [TASK 3.1] View de Consolidação de AIHs (Granularidade 1:1 por numero_aih)
    conn.execute("""
    CREATE OR REPLACE VIEW vw_internacoes_consolidadas AS
    SELECT
        numero_aih,
        ANY_VALUE(id_episodio_internacao) AS id_episodio_internacao,
        ANY_VALUE(pseudonimo_paciente) AS pseudonimo_paciente,
        ANY_VALUE(codigo_estabelecimento_cnes) AS codigo_estabelecimento_cnes,
        ANY_VALUE(codigo_municipio_hospital) AS codigo_municipio_hospital,
        ANY_VALUE(uf_residencia_paciente) AS uf_residencia_paciente,
        ANY_VALUE(data_nascimento_paciente) AS data_nascimento_paciente,
        ANY_VALUE(sexo_biologico) AS sexo_biologico,
        
        -- Datas consolidadas do episódio
        MIN(data_internacao) AS data_internacao_inicial,
        MAX(data_alta) AS data_alta_final,
        
        -- Diagnósticos e Procedimentos
        ANY_VALUE(codigo_cid10_principal) AS codigo_cid10_principal,
        ANY_VALUE(codigo_procedimento_realizado) AS codigo_procedimento_realizado,
        
        -- Métricas financeiras e de permanência acumuladas
        COUNT(*) AS total_faturamentos_competencia,
        SUM(dias_permanencia_faturados_mes) AS dias_permanencia_total_faturados,
        MAX(dias_duracao_acumulada_episodio) AS dias_duracao_total_episodio,
        ROUND(SUM(valor_total_brl), 2) AS valor_total_acumulado_brl,
        ROUND(SUM(valor_servicos_hospitalares_brl), 2) AS valor_sh_acumulado_brl,
        ROUND(SUM(valor_servicos_profissionais_brl), 2) AS valor_sp_acumulado_brl,
        ROUND(SUM(valor_uti_brl), 2) AS valor_uti_acumulado_brl,
        
        -- Desfecho final do episódio
        BOOL_OR(indicador_obito) AS indicador_obito_consolidado,
        ANY_VALUE(uf) AS uf,
        MAX(ano) AS ano_fechamento,
        MAX(mes) AS mes_fechamento
    FROM fct_internacao
    GROUP BY numero_aih;
    """)
    logger.info("  ✓ View vw_internacoes_consolidadas registrada com sucesso.")

    # 2. [TASK 3.1b] View de Pré-Agregação ANS Ressarcimento ao SUS por AIH (Garante 1:1 sem explosão de cardinalidade)
    tables = [t[0] for t in conn.execute("SHOW TABLES;").fetchall()]
    if "fct_ressarcimento_sus" in tables:
        conn.execute("""
        CREATE OR REPLACE VIEW vw_ressarcimento_sus_consolidado_por_aih AS
        SELECT
            numero_aih,
            COUNT(DISTINCT identificador_cobranca_abi) AS total_notificacoes_abi,
            ANY_VALUE(codigo_registro_ans) AS codigo_registro_ans,
            ANY_VALUE(razao_social_operadora) AS razao_social_operadora,
            ANY_VALUE(modalidade_operadora) AS modalidade_operadora,
            ROUND(SUM(valor_notificado_brl), 2) AS valor_total_notificado_brl,
            ROUND(SUM(valor_recolhido_brl), 2) AS valor_total_recolhido_brl,
            BOOL_OR(situacao_cobranca LIKE '%IMPUGNAD%' OR situacao_cobranca LIKE '%RECURSO%') AS tem_impugnacao_ou_recurso,
            ANY_VALUE(situacao_cobranca) AS situacao_cobranca_predominante,
            BOOL_OR(
                UPPER(TRIM(CAST(situacao_cobranca AS VARCHAR))) LIKE '%PAGO%'
                AND COALESCE(TRY_CAST(valor_recolhido_brl AS DOUBLE), 0.0) = 0.0
            ) AS flag_anomalia_contabil_ans
        FROM fct_ressarcimento_sus
        WHERE numero_aih IS NOT NULL
        GROUP BY numero_aih;
        """)
        logger.info("  ✓ View vw_ressarcimento_sus_consolidado_por_aih registrada com sucesso.")

    # 3. [TASK 3.2a] View de Features Puras de Admissão (Zero Leakage para Inferência Online)
    conn.execute("""
    CREATE OR REPLACE VIEW vw_ml_features_admissao AS
    SELECT
        id_internacao_hospitalar,
        numero_aih,
        pseudonimo_paciente,
        codigo_estabelecimento_cnes,
        codigo_municipio_hospital,
        uf_residencia_paciente,
        sexo_biologico,
        
        -- Feature Demográfica (Idade na Admissão)
        CASE 
            WHEN LENGTH(TRIM(CAST(data_nascimento_paciente AS VARCHAR))) = 8 
                 AND LENGTH(TRIM(CAST(data_internacao AS VARCHAR))) = 8
            THEN DATEDIFF('year', STRPTIME(CAST(data_nascimento_paciente AS VARCHAR), '%Y%m%d'), STRPTIME(CAST(data_internacao AS VARCHAR), '%Y%m%d'))
            ELSE NULL 
        END AS idade_admissao_anos,
        
        codigo_cid10_principal,
        SUBSTRING(codigo_cid10_principal, 1, 3) AS cid10_categoria_3digitos,
        codigo_procedimento_solicitado,
        
        -- Sazonalidade da Admissão (Features Temporais de Entrada)
        STRPTIME(CAST(data_internacao AS VARCHAR), '%Y%m%d') AS data_internacao_dt,
        DAYOFWEEK(STRPTIME(CAST(data_internacao AS VARCHAR), '%Y%m%d')) AS dia_semana_admissao,
        MONTH(STRPTIME(CAST(data_internacao AS VARCHAR), '%Y%m%d')) AS mes_admissao
    FROM fct_internacao
    WHERE tipo_identificacao_aih = '1' -- Exclusão de AIH 5 (Evita viés de sobrevivência)
      AND data_nascimento_paciente IS NOT NULL 
      AND data_internacao IS NOT NULL;
    """)
    logger.info("  ✓ View vw_ml_features_admissao registrada com sucesso.")

    # 4. [TASK 3.2b] View de Targets de Desfecho da Internação (Pós-Alta)
    conn.execute("""
    CREATE OR REPLACE VIEW vw_ml_targets_internacao AS
    SELECT
        id_internacao_hospitalar,
        numero_aih,
        indicador_obito AS target_obito_hospitalar,
        CASE WHEN dias_permanencia_real > 11 THEN 1 ELSE 0 END AS target_longa_permanencia_11d,
        CASE WHEN valor_total_brl > 4274.27 THEN 1 ELSE 0 END AS target_alto_custo_p90
    FROM fct_internacao
    WHERE tipo_identificacao_aih = '1'
      AND data_nascimento_paciente IS NOT NULL 
      AND data_internacao IS NOT NULL;
    """)
    logger.info("  ✓ View vw_ml_targets_internacao registrada com sucesso.")

    # 5. [TASK 3.2c] View Composta para Treinamento Offline de Modelos (Anti-Leakage)
    # NOTA DE GOVERNANÇA: O JOIN entre features e targets é exclusivo para scripts de treinamento offline.
    # Em tempo de inferência/produção, consuma estritamente a view vw_ml_features_admissao.
    conn.execute("""
    CREATE OR REPLACE VIEW vw_ml_treinamento_admissao AS
    SELECT
        f.*,
        t.target_obito_hospitalar,
        t.target_longa_permanencia_11d,
        t.target_alto_custo_p90
    FROM vw_ml_features_admissao f
    INNER JOIN vw_ml_targets_internacao t USING (id_internacao_hospitalar);
    """)
    logger.info("  ✓ View vw_ml_treinamento_admissao registrada com sucesso.")
