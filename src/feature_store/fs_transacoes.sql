--  Quantidade de transações por produto: D28;                  -- DONE
--  Quantidade de transações: D7, D28, D56, Vida;               -- DONE
--  Total transações / quantidade de dias: D7, D28, D56, Vida;  -- DONE

WITH tb_transacao AS (
  SELECT t1.idCliente,
         t1.idTransacao,
         t1.dtTransacao - INTERVAL 3 HOUR AS dtTransacao

  FROM silver.upsell.transacoes AS t1
  WHERE t1.dtTransacao - INTERVAL 3 HOUR < '{dt_ref}'
),

tb_ds AS (

    SELECT t1.idCliente,
          count(distinct CASE WHEN t1.dtTransacao > '{dt_ref}' - INTERVAL 7 DAY THEN t1.idTransacao END) AS nrQtdeTransacaoD7,
          count(distinct CASE WHEN t1.dtTransacao > '{dt_ref}' - INTERVAL 28 DAY THEN t1.idTransacao END) AS nrQtdeTransacaoD28,
          count(distinct CASE WHEN t1.dtTransacao > '{dt_ref}' - INTERVAL 56 DAY THEN t1.idTransacao END) AS nrQtdeTransacaoD56,
          count(distinct t1.idTransacao) AS nrQtdeTransacaoVida,
          count(distinct CASE WHEN t1.dtTransacao > '{dt_ref}' - INTERVAL 7 DAY THEN t1.idTransacao END) / count(distinct CASE WHEN t1.dtTransacao > '{dt_ref}' - INTERVAL 7 DAY THEN date(t1.dtTransacao) END) AS nrQtdeTransacaoDiaD7,
          count(distinct CASE WHEN t1.dtTransacao > '{dt_ref}' - INTERVAL 28 DAY THEN t1.idTransacao END) / count(distinct CASE WHEN t1.dtTransacao > '{dt_ref}' - INTERVAL 28 DAY THEN date(t1.dtTransacao) END) AS nrQtdeTransacaoDiaD28,
          count(distinct CASE WHEN t1.dtTransacao > '{dt_ref}' - INTERVAL 56 DAY THEN t1.idTransacao END) / count(distinct CASE WHEN t1.dtTransacao > '{dt_ref}' - INTERVAL 56 DAY THEN date(t1.dtTransacao) END) AS nrQtdeTransacaoDiaD56, 
          count(distinct t1.idTransacao) / count(distinct date(t1.dtTransacao)) AS nrQtdeTransacaoDiaVida

    FROM tb_transacao AS t1
    GROUP BY t1.idCliente

),

tb_produtos AS (

      SELECT t1.idCliente,
            count(DISTINCT CASE WHEN t2.descNomeProduto = 'Airflow Lover'  THEN t1.idTransacao END) AS nrQtdTransacaoAirflowLover,
            count(DISTINCT CASE WHEN t2.descNomeProduto = 'ChatMessage'  THEN t1.idTransacao END) AS nrQtdTransacaoChatMessage,
            count(DISTINCT CASE WHEN t2.descNomeProduto = 'Churn_10pp'  THEN t1.idTransacao END) AS nrQtdTransacaoChurn10pp,
            count(DISTINCT CASE WHEN t2.descNomeProduto = 'Churn_2pp'  THEN t1.idTransacao END) AS nrQtdTransacaoChurn2pp,
            count(DISTINCT CASE WHEN t2.descNomeProduto = 'Churn_5pp'  THEN t1.idTransacao END) AS nrQtdTransacaoChurn5pp,
            count(DISTINCT CASE WHEN t2.descNomeProduto = 'Daily Loot'  THEN t1.idTransacao END) AS nrQtdTransacaoDailyLoot,
            count(DISTINCT CASE WHEN t2.descNomeProduto = 'Lista de presença'  THEN t1.idTransacao END) AS nrQtdTransacaoListapresenca,
            count(DISTINCT CASE WHEN t2.descNomeProduto = 'Presença Streak'  THEN t1.idTransacao END) AS nrQtdTransacaoPresencaStreak,
            count(DISTINCT CASE WHEN t2.descNomeProduto = 'R Lover'  THEN t1.idTransacao END) AS nrQtdTransacaoRLover,
            count(DISTINCT CASE WHEN t2.descNomeProduto = 'Resgatar Ponei'  THEN t1.idTransacao END) AS nrQtdTransacaoResgatarPonei,
            count(DISTINCT CASE WHEN t2.descNomeProduto = 'Troca de Pontos StreamElements'  THEN t1.idTransacao END) AS nrQtdTransacaoTrocaStreamElements,
            count(DISTINCT CASE WHEN t2.descNomeProduto like '%Venda de Item: %'  THEN t1.idTransacao END) AS nrQtdTransacaoVendaItemRPG,

            count(DISTINCT CASE WHEN t2.descNomeProduto = 'Airflow Lover' THEN t1.idTransacao END) / count(distinct t1.idTransacao) AS nrPctTransacaoAirflowLover,
            count(DISTINCT CASE WHEN t2.descNomeProduto = 'ChatMessage' THEN t1.idTransacao END) / count(distinct t1.idTransacao) AS nrPctTransacaoChatMessage,
            count(DISTINCT CASE WHEN t2.descNomeProduto = 'Churn_10pp' THEN t1.idTransacao END) / count(distinct t1.idTransacao) AS nrPctTransacaoChurn10pp,
            count(DISTINCT CASE WHEN t2.descNomeProduto = 'Churn_2pp' THEN t1.idTransacao END) / count(distinct t1.idTransacao) AS nrPctTransacaoChurn2pp,
            count(DISTINCT CASE WHEN t2.descNomeProduto = 'Churn_5pp' THEN t1.idTransacao END) / count(distinct t1.idTransacao) AS nrPctTransacaoChurn5pp,
            count(DISTINCT CASE WHEN t2.descNomeProduto = 'Daily Loot' THEN t1.idTransacao END) / count(distinct t1.idTransacao) AS nrPctTransacaoDailyLoot,
            count(DISTINCT CASE WHEN t2.descNomeProduto = 'Lista de presença' THEN t1.idTransacao END) / count(distinct t1.idTransacao) AS nrPctTransacaoListapresenca,
            count(DISTINCT CASE WHEN t2.descNomeProduto = 'Presença Streak' THEN t1.idTransacao END) / count(distinct t1.idTransacao) AS nrPctTransacaoPresencaStreak,
            count(DISTINCT CASE WHEN t2.descNomeProduto = 'R Lover' THEN t1.idTransacao END) / count(distinct t1.idTransacao) AS nrPctTransacaoRLover,
            count(DISTINCT CASE WHEN t2.descNomeProduto = 'Resgatar Ponei' THEN t1.idTransacao END) / count(distinct t1.idTransacao) AS nrPctTransacaoResgatarPonei,
            count(DISTINCT CASE WHEN t2.descNomeProduto = 'Troca de Pontos StreamElements' THEN t1.idTransacao END) / count(distinct t1.idTransacao) AS nrPctTransacaoTrocaStreamElements,
            count(DISTINCT CASE WHEN t2.descNomeProduto = '%Venda de Item: %'THEN t1.idTransacao END) / count(distinct t1.idTransacao) AS nrPctTransacaoVendaItemRPG


      FROM tb_transacao AS t1
      LEFT JOIN silver.upsell.transacao_produto AS t2
      ON t1.idTransacao = t2.idTransacao

      WHERE t1.dtTransacao >= '{dt_ref}' - INTERVAL 28 DAY

      GROUP BY t1.idCliente

)

SELECT 
       '{dt_ref}' AS dtRef,
       t1.*,
       t2.nrQtdeTransacaoD7,
       t2.nrQtdeTransacaoD28,
       t2.nrQtdeTransacaoD56,
       t2.nrQtdeTransacaoVida,
       t2.nrQtdeTransacaoDiaD7,
       t2.nrQtdeTransacaoDiaD28,
       t2.nrQtdeTransacaoDiaD56, 
       t2.nrQtdeTransacaoDiaVida

FROM tb_produtos AS t1

LEFT JOIN tb_ds AS t2
ON t1.idCliente = t2.idCliente