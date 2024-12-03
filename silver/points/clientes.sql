SELECT 
      uuid AS idCustomer,
      CASE WHEN desc_email IS NOT NULL THEN 1 else 0 END AS flEmail,
      CASE WHEN id_twitch IS NOT NULL THEN 1 else 0 END AS flTwitch,
      CASE WHEN id_you_tube IS NOT NULL THEN 1 else 0 END AS flYouTube,
      CASE WHEN id_blue_sky IS NOT NULL THEN 1 else 0 END AS flBlueSky,
      CASE WHEN id_instagram IS NOT NULL THEN 1 else 0 END AS flInstagram,
      nr_points AS nrSaldoPontos,
      created_at AS dtCriacao,
      updated_at AS dtAtualizacao

FROM bronze.points.customers
