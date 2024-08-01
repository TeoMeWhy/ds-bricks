--  Distribuição de horário/período de atividade do usuário;               -- DONE
--  Quantidade de dias de iteração;                                        -- DONE
--  Tempo semanal de iteração;                                             -- DONE
--  Tempo de atividade nas lives (primeira vs última iteração no dia);     -- DONE
--  Data do MAU, dia do mês, semana do mês, mês, ano;                      -- DONE
--  Quantidade de lives com iteração na semana (média)                     -- DONE

WITH tb_transacao AS (

  select t1.idCliente,
         t1.idTransacao,
         t1.dtTransacao - INTERVAL 3 HOUR as dtTransacao

  FROM silver.upsell.transacoes AS t1

  WHERE t1.dtTransacao < '{dt_ref}'
  AND t1.dtTransacao >= '{dt_ref}' - INTERVAL 28 DAY  
),

tb_periodo AS (

  SELECT t1.idCliente,
        count(distinct date(t1.dtTransacao)) AS nrQtdeDias,
        count(distinct case when hour(t1.dtTransacao) BETWEEN 0 AND 12 THEN date(t1.dtTransacao) END) AS nrQtdeDiasManha,
        count(distinct case when hour(t1.dtTransacao) BETWEEN 13 AND 18 THEN date(t1.dtTransacao) END) AS nrQtdeDiasTarde,
        count(distinct case when hour(t1.dtTransacao) BETWEEN 19 AND 23 THEN date(t1.dtTransacao) END) AS nrQtdeDiasNoite,
        count(distinct case when hour(t1.dtTransacao) BETWEEN 0 AND 12 THEN date(t1.dtTransacao) END) / COUNT(DISTINCT DATE(t1.dtTransacao)) AS nrPctDiasManha,
        count(distinct case when hour(t1.dtTransacao) BETWEEN 13 AND 18 THEN date(t1.dtTransacao) END) / COUNT(DISTINCT DATE(t1.dtTransacao)) AS nrPctDiasTarde,
        count(distinct case when hour(t1.dtTransacao) BETWEEN 19 AND 23 THEN date(t1.dtTransacao) END) / COUNT(DISTINCT DATE(t1.dtTransacao)) AS nrPctDiasNoite

  FROM tb_transacao AS t1
  GROUP BY t1.idCliente
),

tb_dia_minuto AS (

    SELECT t1.idCliente,
          date(dtTransacao) AS dtTransacao,
          (max(float(to_timestamp(dtTransacao))) - min(float(to_timestamp(dtTransacao)))) / 60.0 AS nrMinutos
    FROM tb_transacao aS t1
    GROUP BY ALL
),

tb_tempo AS (

    SELECT idCliente,
          sum(nrMinutos) AS nrQtdeMinutos,
          avg(nrMinutos) AS nrAvgMinutosDia,
          sum(nrMinutos) / 4 AS nrAvgMinutosSemana,
          sum(nrMinutos) / count(distinct weekofyear(dtTransacao)) AS nrAvgMinutosSemanaAtiva,
          count(distinct dtTransacao) / count(distinct weekofyear(dtTransacao)) AS nrQtdeLivesSemanal

    FROM tb_dia_minuto
    GROUP BY ALL

)

SELECT 
      '{dt_ref}' AS dtRef,
       dayofweek('{dt_ref}') AS nrDiaSemana,
       dayofmonth('{dt_ref}') AS nrDiaMes,
       weekofyear('{dt_ref}') AS nrSemanaAno,
       month('{dt_ref}') AS nrMes,
       year('{dt_ref}') AS nrAno,
       t1.*,
       t2.nrQtdeMinutos,
       t2.nrAvgMinutosDia,
       t2.nrAvgMinutosSemana,
       t2.nrAvgMinutosSemanaAtiva,
       t2.nrQtdeLivesSemanal

FROM tb_periodo As t1

LEFT JOIN tb_tempo AS t2
ON t1.idCliente = t2.idCliente