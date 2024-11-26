-- Criação da CTE `hsat_inicial`, que serve como uma base de dados inicial para análises posteriores
WITH hsat_inicial AS (
    SELECT
        -- Converte `local_start_time` para o tipo DATETIME
        CAST(local_start_time AS DATETIME) AS local_start_time,
        -- Trunca `local_start_time` ao primeiro dia do mês e converte para o tipo DATE
        CAST(DATE_TRUNC(local_start_time, MONTH) AS DATE) AS local_start_month,
        agent,  -- Seleciona o agente
        id__agent,  -- Seleciona o ID do agente
        squad_do_agent,  -- Seleciona o squad do agente
        hsat_rating_value,  -- Seleciona o valor de avaliação do HSAT
        status,  -- Seleciona o status do atendimento
        operacao_do_agente,  -- Seleciona a operação do agente
        tipo_de_atendimento,  -- Seleciona o tipo de atendimento realizado
        id_participacao_de_pesquisa_hsat,  -- Seleciona o ID de participação na pesquisa HSAT
        -- Mapeia `operacao_do_agente` para um nome fantasia, usando um CASE para simplificar o valor
        CASE
            WHEN operacao_do_agente = "empresa1" THEN "nomefantasia1"
            WHEN operacao_do_agente = "empresa2" THEN "nomefantasia2"
            WHEN operacao_do_agente = "empresa3" THEN "nomefantasia3"
            WHEN operacao_do_agente = "empresa4" THEN "nomefantasia4"
            ELSE "outros"
        END AS operacao,
        -- Converte `pesquisa_avaliacao_suporte` para o tipo INT64, garantindo segurança com SAFE_CAST
        SAFE_CAST(pesquisa_avaliacao_suporte AS INT64) AS pesquisa_avaliacao_suporte,
        -- Converte `pesquisa_nps` para o tipo INT64, também usando SAFE_CAST para evitar erros de conversão. Em situações onde os dados são limpos e o tipo é garantido, CAST simples pode ser utilizado para um desempenho ligeiramente melhor.
        SAFE_CAST(pesquisa_nps AS INT64) AS pesquisa_nps
    FROM `dataset.atividades_operacionais`  -- Origem dos dados
),

-- Criação da CTE `nps_final`, que agrega dados de NPS e adiciona informações temporais para análise
nps_final AS ( 
    SELECT
        -- Extrai apenas a data de `local_start_time`
        DATE(local_start_time) AS local_start_date,
        -- Trunca `local_start_time` à semana que começa no domingo
        CAST(DATE_TRUNC(local_start_time, WEEK(SUNDAY)) AS DATE) AS week,
        local_start_month,  -- Mantém o mês de início
        operacao_do_agente,  -- Mantém a operação do agente
        tipo_de_atendimento,  -- Mantém o tipo de atendimento
        status,  -- Mantém o status
        pesquisa_nps,  -- Mantém o NPS da pesquisa
        hsat_rating_value,  -- Mantém o valor da avaliação HSAT
        agent,  -- Mantém o agente
        pesquisa_avaliacao_suporte,  -- Mantém a avaliação do suporte
        operacao,  -- Mantém a operação mapeada para o nome fantasia
        squad_do_agent,  -- Mantém o squad do agente
        -- Calcula valor de promotoras (1 para NPS 9 ou 10, caso contrário 0)
        CASE
            WHEN pesquisa_nps IN (9, 10) THEN 1
            ELSE 0
        END AS nps_value_promotoras,
        -- Calcula valor de detratoras (1 para NPS entre 0 e 6, caso contrário 0)
        CASE
            WHEN pesquisa_nps IN (0, 1, 2, 3, 4, 5, 6) THEN 1
            ELSE 0
        END AS nps_value_detratoras,
        -- Calcula a diferença em meses entre a data atual e `local_start_time`
        DATE_DIFF(CURRENT_DATE, DATE(local_start_time), MONTH) AS M_DIFF,
        -- A estrutura de diferencial de tempo (d_diff, m_diff e w_diff) pode servir imensamente na manutenção do código e seleção simplificada de informações, não sendo necessário filtrar um range de data a cada mês que vira, por exemplo, e adotando sempre o M_diff igual a zero, para trazer dados do mês presente.
        -- Calcula a diferença em semanas entre a data atual e `local_start_time`
        DATE_DIFF(CURRENT_DATE, DATE(local_start_time), WEEK(SUNDAY)) AS W_DIFF,
        -- Calcula a diferença em dias entre a data atual e `local_start_time`
        DATE_DIFF(CURRENT_DATE, DATE(local_start_time), DAY) AS D_DIFF
    FROM hsat_inicial  -- Utiliza os dados processados na CTE `hsat_inicial`
)

-- Consulta final que gera o resultado desejado
SELECT
    local_start_month,  -- Agrupa por mês de início
    agent,  -- Agrupa por agente
    COUNT(*) AS total_avaliacoes,  -- Conta o total de avaliações
    SUM(nps_value_promotoras) AS total_promotoras,  -- Soma o total de promotoras
    SUM(nps_value_detratoras) AS total_detratoras,  -- Soma o total de detratoras
    -- Calcula o NPS total com base nos promotores e detratores
    (SUM(nps_value_promotoras) - SUM(nps_value_detratoras)) / COUNT(*) * 100.0 AS t_nps
FROM nps_final  -- Utiliza os dados processados na CTE `nps_final`
WHERE 
    squad_do_agent = "squad1"  -- Filtra pelo squad específico
    AND pesquisa_nps IS NOT NULL  -- Filtra para excluir NPS nulos
    AND tipo_de_atendimento <> "ligacoes"  -- Exclui tipos de atendimento que sejam "ligações"
    AND hsat_rating_value IS NULL  -- Filtra por avaliações HSAT nulas
    AND local_start_month >= "2024-04-01"  -- Filtra para incluir apenas dados a partir de abril de 2024
    AND operacao_do_agente IN ("empresa1", "empresa2", "empresa3")  -- Filtra para incluir apenas operações específicas
GROUP BY 
    local_start_month,  -- Agrupa por mês de início
    agent  -- Agrupa por agente
ORDER BY 
    local_start_month,  -- Ordena pelo mês de início
    agent;  -- Ordena pelo agente
