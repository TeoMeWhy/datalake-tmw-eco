SELECT
    uuid AS idTransacao,
    id_customer As idCliente,
    created_at AS dtCriacao,
    vl_points AS vlPontosTransacao,
    desc_sys_origin AS DescSistemaOrigem

FROM bronze.points.transactions