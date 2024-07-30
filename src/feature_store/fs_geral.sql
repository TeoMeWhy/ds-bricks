-- #  Recencia (qtde dias desde última iteração) - DONE
-- #  Saldo de pontos atual                      - DONE
-- #  Idade na base                              - DONE
-- #  Marcação se tem email ativo                - DONE

WITH tb_ativa AS (

  SELECT *

  FROM silver.upsell.transacoes

  WHERE dtTransacao - INTERVAL 3 HOUR < '{dt_ref}'
  AND dtTransacao - INTERVAL 3 HOUR >= '{dt_ref}' - INTERVAL 28 DAY

  ORDER BY dtTransacao DESC

),

tb_recencia AS (

    SELECT idCliente,
          MIN(date_diff('{dt_ref}', dtTransacao - interval 3 hour)) AS nrRecencia

    FROM tb_ativa
    GROUP BY idCliente
    ORDER BY idCliente
),

tb_vida AS (

    SELECT idCliente,
           SUM(nrPontosTransacao) AS nrSaldoPontos,
           MAX(datediff('{dt_ref}', dtTransacao - INTERVAL 3 HOUR)) AS nrIdadeBase

    FROM silver.upsell.transacoes

    WHERE dtTransacao - INTERVAL 3 HOUR < '{dt_ref}'
    AND idCliente IN (SELECT idCliente FROM tb_recencia)

    GROUP BY idCliente
),

tb_final AS (

    SELECT t1.*,
            t2.nrSaldoPontos,
            t2.nrIdadeBase,
            t3.flEmailCliente As flEmail

    FROM tb_recencia AS t1

    LEFT JOIN tb_vida AS t2
    ON t1.idCliente = t2.idCliente

    LEFT JOIN silver.upsell.cliente AS t3
    ON t1.idCliente = t3.idCliente
)

SELECT '{dt_ref}' AS dtRef,
       *

FROM tb_final