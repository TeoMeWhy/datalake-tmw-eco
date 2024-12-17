WITH tb_dia_ativo AS (

  SELECT DISTINCT
        idCliente,
        date(dtCriacao) AS dtAtivacao
  FROM silver.points.transacoes

),

tb_referencia AS (

  SELECT id_cliente,
        dt_ref
  FROM feature_store.points.user_life
  WHERE dt_ref <= '2024-11-15'
  -- AND dayofmonth(dt_ref) = 1

),

tb_group_join AS (

  SELECT t1.dt_ref,
         t1.id_cliente,
         count(t2.idCliente) AS qtde_ativacao

  FROM tb_referencia AS t1

  LEFT JOIN tb_dia_ativo AS t2
  ON t1.id_cliente = t2.idCliente
  AND t1.dt_ref <= t2.dtAtivacao
  AND t2.dtAtivacao < (t1.dt_ref + interval 28 day)

  GROUP BY ALL
),

tb_target AS (

  SELECT *,
        CASE WHEN qtde_ativacao = 0 THEN 1 ELSE 0 END AS flag_churn
  FROM tb_group_join

),

tb_unique_start AS (
  SELECT *
  FROM tb_target
  QUALIFY row_number() OVER (PARTITION BY id_cliente ORDER BY dt_ref ASC) = 1
),

tb_unique_after AS (
  SELECT t2.*
  FROM tb_unique_start AS t1
  LEFT JOIN tb_target AS t2
  ON t1.id_cliente = t2.id_cliente
  AND datediff(t2.dt_ref, t1.dt_ref) % 28 = 0
),

tb_all AS (
  SELECT *
  FROM tb_unique_after
  ORDER BY id_cliente, dt_ref
)

SELECT *
FROM tb_all