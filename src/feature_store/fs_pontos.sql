--  Quantidade de pontos acumulados, D7, D28, D56; Vida;  --DONE
--  Média de pontos por dia;                              --DONE
--  Pontos / Transação;                                   --DONE
--  Quantidade de pontos por produto (absoluto);          --DONE

WITH tb_transacao AS (
  SELECT t1.idCliente,
         t1.idTransacao,
         t1.dtTransacao - INTERVAL 3 HOUR AS dtTransacao,
         t1.nrPontosTransacao

  FROM silver.upsell.transacoes AS t1
  WHERE t1.dtTransacao - INTERVAL 3 HOUR < '{dt_ref}'
),

tb_ds (

    SELECT idCliente,
          sum(CASE WHEN dtTransacao > '{dt_ref}' - INTERVAL 7 DAY THEN nrPontosTransacao END) AS nrQtdePontosD7,
          sum(CASE WHEN dtTransacao > '{dt_ref}' - INTERVAL 7 DAY AND nrPontosTransacao > 0 THEN nrPontosTransacao END) AS nrQtdePontosPosD7,
          sum(CASE WHEN dtTransacao > '{dt_ref}' - INTERVAL 28 DAY AND nrPontosTransacao < 0 THEN ABS(nrPontosTransacao) END) AS nrQtdePontosNegD7,
          
          sum(CASE WHEN dtTransacao > '{dt_ref}' - INTERVAL 28 DAY THEN nrPontosTransacao END) AS nrQtdePontosD28,
          sum(CASE WHEN dtTransacao > '{dt_ref}' - INTERVAL 28 DAY AND nrPontosTransacao > 0 THEN nrPontosTransacao END) AS nrQtdePontosPosD28,
          sum(CASE WHEN dtTransacao > '{dt_ref}' - INTERVAL 28 DAY AND nrPontosTransacao < 0 THEN ABS(nrPontosTransacao) END) AS nrQtdePontosNegD28,

          sum(CASE WHEN dtTransacao > '{dt_ref}' - INTERVAL 56 DAY THEN nrPontosTransacao END) AS nrQtdePontosD56,
          sum(CASE WHEN dtTransacao > '{dt_ref}' - INTERVAL 56 DAY AND nrPontosTransacao > 0 THEN nrPontosTransacao END) AS nrQtdePontosPosD56,
          sum(CASE WHEN dtTransacao > '{dt_ref}' - INTERVAL 56 DAY AND nrPontosTransacao < 0 THEN ABS(nrPontosTransacao) END) AS nrQtdePontosNegD56,

          sum(nrPontosTransacao) AS nrQtdePontosVida,
          sum(CASE WHEN nrPontosTransacao > 0 THEN nrPontosTransacao END) AS nrQtdePontosPosVida,
          sum(CASE WHEN nrPontosTransacao < 0 THEN ABS(nrPontosTransacao) END) AS nrQtdePontosNegVida

    FROM tb_transacao
    GROUP BY idCliente

),

tb_produtos AS (

    SELECT 
        idCliente,

        SUM(CASE WHEN descNomeProduto = 'Airflow Lover' THEN ABS(nrPontosTransacao) END) AS nrQtdePontosAirflowLover,
        SUM(CASE WHEN descNomeProduto = 'ChatMessage' THEN ABS(nrPontosTransacao) END) AS nrQtdePontosChatMessage,
        SUM(CASE WHEN descNomeProduto = 'Churn_10pp' THEN ABS(nrPontosTransacao) END) AS nrQtdePontosChurn10pp,
        SUM(CASE WHEN descNomeProduto = 'Churn_2pp' THEN ABS(nrPontosTransacao) END) AS nrQtdePontosChurn2pp,
        SUM(CASE WHEN descNomeProduto = 'Churn_5pp' THEN ABS(nrPontosTransacao) END) AS nrQtdePontosChurn5pp,
        SUM(CASE WHEN descNomeProduto = 'Daily Loot' THEN ABS(nrPontosTransacao) END) AS nrQtdePontosDailyLoot,
        SUM(CASE WHEN descNomeProduto = 'Lista de presença' THEN ABS(nrPontosTransacao) END) AS nrQtdePontosListaPresenca,
        SUM(CASE WHEN descNomeProduto = 'Presença Streak' THEN ABS(nrPontosTransacao) END) AS nrQtdePontosPresencaStreak,
        SUM(CASE WHEN descNomeProduto = 'R Lover' THEN ABS(nrPontosTransacao) END) AS nrQtdePontosRLover,
        SUM(CASE WHEN descNomeProduto = 'Resgatar Ponei' THEN ABS(nrPontosTransacao) END) AS nrQtdePontosResgatarPonei,
        SUM(CASE WHEN descNomeProduto = 'Troca de Pontos StreamElements' THEN ABS(nrPontosTransacao) END) AS nrQtdePontosTrocaStreamElements,
        SUM(CASE WHEN descNomeProduto like '%Venda de Item: %' THEN ABS(nrPontosTransacao) END) AS nrQtdePontosVendaItemRPG,


        SUM(CASE WHEN descNomeProduto = 'Airflow Lover' THEN ABS(nrPontosTransacao) END) / sum(abs(nrPontosTransacao)) AS nrPctPontosAirflowLover,
        SUM(CASE WHEN descNomeProduto = 'ChatMessage' THEN ABS(nrPontosTransacao) END) / sum(abs(nrPontosTransacao)) AS nrPctPontosChatMessage,
        SUM(CASE WHEN descNomeProduto = 'Churn_10pp' THEN ABS(nrPontosTransacao) END) / sum(abs(nrPontosTransacao)) AS nrPctPontosChurn10pp,
        SUM(CASE WHEN descNomeProduto = 'Churn_2pp' THEN ABS(nrPontosTransacao) END) / sum(abs(nrPontosTransacao)) AS nrPctPontosChurn2pp,
        SUM(CASE WHEN descNomeProduto = 'Churn_5pp' THEN ABS(nrPontosTransacao) END) / sum(abs(nrPontosTransacao)) AS nrPctPontosChurn5pp,
        SUM(CASE WHEN descNomeProduto = 'Daily Loot' THEN ABS(nrPontosTransacao) END) / sum(abs(nrPontosTransacao)) AS nrPctPontosDailyLoot,
        SUM(CASE WHEN descNomeProduto = 'Lista de presença' THEN ABS(nrPontosTransacao) END) / sum(abs(nrPontosTransacao)) AS nrPctPontosListaPresenca,
        SUM(CASE WHEN descNomeProduto = 'Presença Streak' THEN ABS(nrPontosTransacao) END) / sum(abs(nrPontosTransacao)) AS nrPctPontosPresencaStreak,
        SUM(CASE WHEN descNomeProduto = 'R Lover' THEN ABS(nrPontosTransacao) END) / sum(abs(nrPontosTransacao)) AS nrPctPontosRLover,
        SUM(CASE WHEN descNomeProduto = 'Resgatar Ponei' THEN ABS(nrPontosTransacao) END) / sum(abs(nrPontosTransacao)) AS nrPctPontosResgatarPonei,
        SUM(CASE WHEN descNomeProduto = 'Troca de Pontos StreamElements' THEN ABS(nrPontosTransacao) END) / sum(abs(nrPontosTransacao)) AS nrPctPontosTrocaStreamElements,
        SUM(CASE WHEN descNomeProduto like '%Venda de Item: %' THEN ABS(nrPontosTransacao) END) / sum(abs(nrPontosTransacao)) AS nrPctPontosVendaItemRPG,

        sum(nrPontosTransacao) / count(distinct date(dtTransacao)) AS nrQtdePontosDia,
        sum(abs(nrPontosTransacao)) / count(distinct date(dtTransacao)) AS nrQtdePontosGeralDia,
        sum(CASE WHEN nrPontosTransacao > 0 THEN nrPontosTransacao END) / count(distinct CASE WHEN nrPontosTransacao > 0 THEN date(dtTransacao) END) AS nrQtdePontosPosDia,
        sum(CASE WHEN nrPontosTransacao < 0 THEN abs(nrPontosTransacao) END) / count(distinct CASE WHEN nrPontosTransacao < 0 THEN date(dtTransacao) END) AS nrQtdePontosNegDia,

        sum(nrPontosTransacao) / count(distinct t1.idTransacao) AS nrQtdePontosTransacao,
        sum(abs(nrPontosTransacao)) / count(distinct t1.idTransacao) AS nrQtdePontosGeralTransacao,
        sum(CASE WHEN nrPontosTransacao > 0 THEN nrPontosTransacao END) / count(distinct CASE WHEN nrPontosTransacao > 0 THEN t1.idTransacao END) AS nrQtdePontosPosTransacao,
        sum(CASE WHEN nrPontosTransacao < 0 THEN abs(nrPontosTransacao) END) / count(distinct CASE WHEN nrPontosTransacao < 0 THEN t1.idTransacao END) AS nrQtdePontosNegTransacao


    FROM tb_transacao AS t1

    LEFT JOIN silver.upsell.transacao_produto AS t2
    ON t1.idTransacao = t2.idTransacao

    WHERE t1.dtTransacao >= '{dt_ref}' - INTERVAL 28 DAY

    GROUP BY idCliente

)

SELECT  '{dt_ref}' AS dtRef,
        t1.*,
        t2.nrQtdePontosD7,
        t2.nrQtdePontosPosD7,
        t2.nrQtdePontosNegD7,
        t2.nrQtdePontosD28,
        t2.nrQtdePontosPosD28,
        t2.nrQtdePontosNegD28,
        t2.nrQtdePontosD56,
        t2.nrQtdePontosPosD56,
        t2.nrQtdePontosNegD56,
        t2.nrQtdePontosVida,
        t2.nrQtdePontosPosVida,
        t2.nrQtdePontosNegVida

FROM tb_produtos AS t1

LEFT JOIN tb_ds AS t2
ON t1.idCliente = t2.idCliente
