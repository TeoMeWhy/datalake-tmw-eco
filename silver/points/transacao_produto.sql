SELECT 
      uuid AS idTransacaoProduto,
      id_transaction AS idTransacao,
      cod_product AS idProduto,
      qtde_product AS nrQuantidadeProduto,
      vl_product AS vlPontosProduto

FROM bronze.points.transaction_products