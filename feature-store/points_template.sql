WITH tb_base_ativa AS (
  SELECT DISTINCT t1.idCliente
  FROM silver.points.transacoes AS t1
  WHERE t1.dtCriacao < '{dt_ref}'
  AND t1.dtCriacao >= '{dt_ref}' - INTERVAL 28 DAY
),

tb_join(

    SELECT t1.idCLiente,
           t2.idTransacao,
           t2.dtCriacao,
           t2.vlPontosTransacao,
           t4.descNomeProduto

    FROM tb_base_ativa AS t1

    LEFT JOIN silver.points.transacoes AS t2
    ON t1.idCliente = t2.idCliente

    LEFT JOIN silver.points.transacao_produto AS t3
    ON t2.idTransacao = t3.idTransacao

    LEFT JOIN silver.points.produtos AS t4
    ON t3.idProduto = t4.idProduto

    WHERE t2.dtCriacao < '{dt_ref}'
    AND t2.dtCriacao > '{dt_ref}' - INTERVAL {days} DAY
),

tb_vida AS (
    SELECT
          idCliente AS id_cliente,
          max(datediff('{dt_ref}', dtCriacao)) AS dias_primeira_iteracao,
          min(datediff('{dt_ref}', dtCriacao)) AS dias_ultima_iteracao,
          count(DISTINCT idTransacao) AS qtd_iteracoes,
          sum(vlPontosTransacao) AS saldo_atual,
          sum(CASE WHEN vlPontosTransacao > 0 THEN vlPontosTransacao ELSE 0 END) AS pontos_acum,
          sum(CASE WHEN vlPontosTransacao < 0 THEN vlPontosTransacao ELSE 0 END) AS pontos_neg,
          count(distinct date(dtCriacao)) AS frequencia,
          sum(CASE WHEN vlPontosTransacao > 0 THEN vlPontosTransacao ELSE 0 END) /  count(distinct date(dtCriacao)) AS pontos_acum_dia,
          count(DISTINCT CASE WHEN descNomeProduto = 'ChatMessage' THEN idTransacao END) as qtd_mensagens,
          count(DISTINCT CASE WHEN descNomeProduto = 'Resgatar Ponei' THEN idTransacao END) as qtd_resgate_poneis,
          count(DISTINCT CASE WHEN descNomeProduto = 'ChatMessage' THEN idTransacao END) / count(distinct date(dtCriacao)) as mensagens_dia,

          count(DISTINCT CASE WHEN dayofweek(dtCriacao) = 1 THEN idTransacao END) / count(distinct idTransacao) AS pct_transacao_dia01,
          count(DISTINCT CASE WHEN dayofweek(dtCriacao) = 2 THEN idTransacao END) / count(distinct idTransacao) AS pct_transacao_dia02,
          count(DISTINCT CASE WHEN dayofweek(dtCriacao) = 3 THEN idTransacao END) / count(distinct idTransacao) AS pct_transacao_dia03,
          count(DISTINCT CASE WHEN dayofweek(dtCriacao) = 4 THEN idTransacao END) / count(distinct idTransacao) AS pct_transacao_dia04,
          count(DISTINCT CASE WHEN dayofweek(dtCriacao) = 5 THEN idTransacao END) / count(distinct idTransacao) AS pct_transacao_dia05,
          count(DISTINCT CASE WHEN dayofweek(dtCriacao) = 6 THEN idTransacao END) / count(distinct idTransacao) AS pct_transacao_dia06,
          count(DISTINCT CASE WHEN dayofweek(dtCriacao) = 7 THEN idTransacao END) / count(distinct idTransacao) AS pct_transacao_dia07,

          COUNT(DISTINCT CASE WHEN hour(from_utc_timestamp(dtCriacao, 'America/Sao_Paulo')) BETWEEN 7 AND 12 THEN idTransacao END) / count(distinct idTransacao) AS pct_transacao_manha,
          COUNT(DISTINCT CASE WHEN hour(from_utc_timestamp(dtCriacao, 'America/Sao_Paulo')) BETWEEN 12 AND 18 THEN idTransacao END) / count(distinct idTransacao) AS pct_transacao_tarde,
          COUNT(DISTINCT CASE WHEN hour(from_utc_timestamp(dtCriacao, 'America/Sao_Paulo')) BETWEEN 19 AND 6 THEN idTransacao END) / count(distinct idTransacao) AS pct_transacao_noite

    FROM tb_join AS t1
    GROUP BY ALL
),

tb_daily AS (
  SELECT DISTINCT
        idCliente,
        date(dtCriacao)
  FROM tb_join
),

tb_lag_dt AS (
  SELECT idCliente,
        dtCriacao,
        lag(dtCriacao) OVER (PARTITION BY idCliente ORDER BY dtCriacao) AS lagDtCriacao

  FROM tb_daily
),

tb_recorrencia AS (
  SELECT idCliente,
        avg(datediff(dtCriacao,lagDtCriacao)) AS media_dias_recorrencia,
        median(datediff(dtCriacao,lagDtCriacao)) AS mediana_dias_recorrencia

  FROM tb_lag_dt
  WHERE lagDtCriacao IS NOT NULL
  GROUP BY ALL
)

SELECT 
        '{dt_ref}' AS dt_ref,
        t1.*,
        t2.media_dias_recorrencia,
        t2.mediana_dias_recorrencia

FROM tb_vida AS t1

LEFT JOIN tb_recorrencia AS t2
ON t1.id_cliente = t2.idCLiente
