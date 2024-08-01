WITH tb_daily AS (
  SELECT distinct
        idCLiente,
        date(dtTransacao) AS dtDia

  FROM silver.upsell.transacoes
  ORDER BY idCliente, dtDia
),

tb_ref (
    SELECT dtRef,
          idCliente
    FROM feature_store.upsell.fs_geral
    WHERE day(dtRef) = 1
),

tb_churn AS (

    SELECT 
          t1.dtRef,
          t1.idCliente,
          max(case when t2.idCliente IS NULL THEN 1 ELSE 0 END) AS flChurn
          
    FROM tb_ref AS t1

    LEFT JOIN tb_daily AS t2
    ON t1.idCliente = t2.idCliente
    AND t1.dtRef <= t2.dtDia
    AND t1.dtRef > t2.dtDia - INTERVAL 28 DAY

    GROUP BY ALL
    ORDER BY t1.idCliente, t1.dtref
)


SELECT *
FROM tb_churn
QUALIFY row_number() OVER (PARTITION BY idCliente ORDER BY rand()) <= 2