-- Criação de um CTE chamado AUX para organizar e modularizar a consulta principal
WITH table.aux AS (
  -- Seleção e transformação dos dados necessários
  SELECT
    local_todo_date AS date,  -- Renomeia a data de atendimento, utilizando alias 'date'
    tipo_atendimento,  -- Tipo de atendimento realizado
    -- Extrai o nome do agente usando uma expressão regular, ignorando possíveis dados adicionais
    REGEXP_EXTRACT(agent, r"^[a-zA-Z0-9_.+-]+") AS agent,  
    tempo_de_casa,  -- Tempo de casa do agente
    fila_de_atendimento,  -- Fila de atendimento em que o agente estava alocado
    -- Mapeiando as operações de call center para nomes fantasia, facilitando a padronização dos dados
    CASE 
      WHEN operacao_call_center = 'empresa1' THEN 'NomeFantasia1'
      WHEN operacao_call_center = 'empresa2' THEN 'NomeFantasia2'
      WHEN operacao_call_center = 'empresa3' THEN 'NomeFantasia3'
      WHEN operacao_call_center = 'empresa4' THEN 'NomeFantasia4'
      WHEN operacao_call_center = 'empresa5' THEN 'NomeFantasia5'  
      ELSE 'others'  -- Qualquer outro valor é categorizado como 'others'
    END AS operacao_call_center,
    -- Trunca a data para obter o início da semana
    DATE_TRUNC(local_todo_date, WEEK) AS Date_Week,  
    -- Trunca a data para obter o início do mês
    DATE_TRUNC(local_todo_date, month) AS Date_Month,  
    -- Calcula a diferença em meses entre a data atual e a data do atendimento
    DATE_DIFF(current_date, local_todo_date, MONTH) AS M_DIFF,
    -- Calcula a diferença em semanas entre a data atual e a data do atendimento
    DATE_DIFF(current_date, local_todo_date, WEEK) AS W_DIFF,
    -- Calcula a diferença em dias entre a data atual e a data do atendimento
    DATE_DIFF(current_date, local_todo_date, DAY) AS D_DIFF,
    -- A estrutura de diferencial de tempo (d_diff, m_diff e w_diff) pode servir imensamente na manutenção do código e seleção simplificada de informações, não sendo necessário filtrar um range de data a cada mês que vira, por exemplo, e adotando sempre o M_diff igual a zero, para trazer dados do mês presente.
    -- Conta o número de atendimentos finalizados (status = 'finished')
    COUNT(DISTINCT CASE WHEN status = 'finished' THEN dist_key ELSE NULL END) AS atendimentos_finalizados,
    -- Conta o número de atendimentos pulados pela 'squad1'
    COUNT(DISTINCT CASE WHEN skip AND squad_atendente = 'squad1' THEN dist_key ELSE NULL END) AS atendimentos_pulados,
    -- Conta o número de atendimentos transferidos para outra squad diferente de 'squad2'
    COUNT(DISTINCT CASE WHEN skip AND squad_atendente <> 'squad2' THEN dist_key ELSE NULL END) AS atendimentos_transferidos,
    -- Conta o número total de atendimentos realizados
    COUNT(DISTINCT dist_key) AS atendimentos_feitos

  FROM dataset.atividades_operacionais  -- Define a tabela de onde os dados serão extraídos

  -- Define as condições de filtragem dos dados
  WHERE (local_todo_date BETWEEN DATE_TRUNC(DATE_SUB(current_date, INTERVAL 3 MONTH), MONTH) AND current_date)  -- Considera apenas os últimos 3 meses
    AND squad_atendente = 'squad1'  -- Filtra para a squad específica
    AND tipo_atendimento NOT IN ('ligacoes')  -- Exclui atendimentos do tipo 'ligações'

  -- Agrupa os dados pelas colunas selecionadas, essencial para as agregações
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
  -- Ordena os resultados pela data (decrescente), tipo de atendimento e agente
  ORDER BY 1 DESC, 2, 3
)

-- Seleciona todos os dados do CTE AUX, com uma agregação adicional
SELECT *,
  -- Calcula o somatório cumulativo de atendimentos feitos, particionando por mês, tipo de atendimento e squad. Se o PARTITION BY fosse removido, o SUM seria acumulado em toda a tabela, independentemente do mês, tipo de atendimento ou equipe, o que não seria útil se você deseja ver o progresso acumulado dentro de cada grupo específico.
  SUM(atendimentos_feitos) OVER (PARTITION BY Date_Month, tipo_atendimento, squad_atendente ORDER BY date) AS total_acumulado
FROM table.aux  -- Consulta o CTE table.aux
-- Ordena os resultados finais pela data (decrescente) e tipo de atendimento
ORDER BY 1 DESC, 2;
