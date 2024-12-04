WITH tb_transacao AS (
    SELECT t1.idCliente,
          t1.idTransacao,
          t1.dtCriacao,
          t1.vlPontosTransacao,
          t3.descCategoriaProduto,
          t3.descDescricaoProduto,
          t3.descNomeProduto

    FROM silver.points.transacoes AS t1

    LEFT JOIN silver.points.transacao_produto AS t2
    ON t1.idTransacao = t2.idTransacao

    LEFT JOIN silver.points.produtos AS t3
    ON t2.idProduto = t3.idProduto

    WHERE year(dtCriacao) = 2024
),

tb_summary AS (
    SELECT
        t1.idCliente,
        count(distinct date(t1.dtCriacao)) AS qtDias,
        sum(CASE WHEN t1.vlPontosTransacao > 0 THEN t1.vlPontosTransacao ELSE 0 END) AS qtPontosAcumulados,
        sum(CASE WHEN t1.vlPontosTransacao < 0 THEN t1.vlPontosTransacao ELSE 0 END) AS qtPontosGastos,
        count(DISTINCT CASE WHEN t1.descNomeProduto = 'ChatMessage' THEN t1.idTransacao ELSE NULL END) AS qtChatMessages,
        count(DISTINCT CASE WHEN t1.descNomeProduto = 'Lista de presença' THEN t1.idTransacao ELSE NULL END) AS qtPresente,
        min(date(t1.dtCriacao)) AS dtPrimeiraTransacao,
        max(datediff(NOW(), date(t1.dtCriacao))) AS qtDiasPrimtransacao

    FROM tb_transacao AS t1

    GROUP BY ALL
    ORDER BY 2 DESC
),

tb_live_time AS (
    SELECT idCliente,
          date(dtCriacao) AS dtLive,
          bigint(max(dtCriacao) - min(dtCriacao)) AS diffLive
    FROM tb_transacao
    GROUP BY 1,2
),

tb_timeall AS (
    SELECT 
        idCliente,
        round(sum(diffLive)/3600,1) AS qtTempoTotalHoras

    FROM tb_live_time
    GROUP BY ALL
),

tb_summary_join AS (

    SELECT 
          t1.*,
          
          CASE
              WHEN t2.qtTempoTotalHoras / t1.qtDias > 5 THEN (t2.qtTempoTotalHoras / 2) * t1.qtDias
              ELSE t2.qtTempoTotalHoras
          END AS qtTempoTotalHoras,
          
          CASE
              WHEN t2.qtTempoTotalHoras / t1.qtDias > 5 THEN (t2.qtTempoTotalHoras / 2) / t1.qtDias
              ELSE t2.qtTempoTotalHoras / t1.qtDias
          END AS qtHorasDia

    FROM tb_summary AS t1

    LEFT JOIN tb_timeall AS t2
    ON t1.idCliente = t2.idCliente

    LEFT JOIN bronze.points.customers AS t3
    ON t1.idCliente = t3.uuid

    WHERE t3.desc_customer_name NOT LIKE '%teomewhy%'
    AND t3.desc_customer_name NOT LIKE '%teomebot%'

    ORDER BY t2.qtTempoTotalHoras DESC
),

tb_rank AS (

  SELECT *,
        row_number() OVER (ORDER BY qtPontosAcumulados DESC) AS rankPontos,
        row_number() OVER (ORDER BY qtDiasPrimtransacao DESC, qtPontosAcumulados DESC) AS rankAntigo

  FROM tb_summary_join

)

SELECT *,
       rankPontos / (SELECT COUNT(*) FROM tb_rank) AS pctRankPontos,
       rankAntigo / (SELECT COUNT(*) FROM tb_rank) AS pctRankAntigo

FROM tb_rank