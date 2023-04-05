-- Databricks notebook source
WITH tb_join AS
(
  SELECT
    tb_pag_ped.*,
    tb_item_ped.idVendedor
  FROM
    silver.olist.pedido AS tb_pedido
  LEFT JOIN
    silver.olist.pagamento_pedido AS tb_pag_ped
  ON
    tb_pedido.idPedido = tb_pag_ped.idPedido
  LEFT JOIN
    silver.olist.item_pedido AS tb_item_ped
  ON
    tb_pedido.idPedido = tb_item_ped.idPedido
  WHERE
    tb_pedido.dtPedido < "2018-01-01"
    AND tb_pedido.dtPedido >= add_months("2018-01-01", -6)
    AND tb_item_ped.idVendedor IS NOT NULL
),

tb_group AS 
(
  SELECT 
    idVendedor,
    descTipoPagamento,
    COUNT(DISTINCT idPedido) AS qtdePedidoMeioPagamento,
    SUM(vlPagamento) AS vlPedidoMeioPagamento
  FROM 
    tb_join
  GROUP BY
    idVendedor,
    descTipoPagamento
  ORDER BY
    idVendedor,
    descTipoPagamento
)

SELECT
  idVendedor,
  SUM(CASE WHEN descTipoPagamento = 'boleto' THEN qtdePedidoMeioPagamento ELSE 0 END) AS qtde_boleto_pedido,
  SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN qtdePedidoMeioPagamento ELSE 0 END) AS qtde_credit_card_pedido,
  SUM(CASE WHEN descTipoPagamento = 'voucher' THEN qtdePedidoMeioPagamento ELSE 0 END) AS qtde_voucher_pedido,
  SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN qtdePedidoMeioPagamento ELSE 0 END) AS qtde_debit_card_pedido,
  
  SUM(CASE WHEN descTipoPagamento = 'boleto' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_boleto_pedido,
  SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_credit_card_pedido,
  SUM(CASE WHEN descTipoPagamento = 'voucher' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_voucher_pedido,
  SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN vlPedidoMeioPagamento ELSE 0 END) AS valor_debit_card_pedido,
  
  SUM(CASE WHEN descTipoPagamento = 'boleto' THEN qtdePedidoMeioPagamento ELSE 0 END)/SUM(qtdePedidoMeioPagamento) AS pct_qtde_boleto_pedido,
  SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN qtdePedidoMeioPagamento ELSE 0 END)/SUM(qtdePedidoMeioPagamento) AS pct_qtde_credit_card_pedido,
  SUM(CASE WHEN descTipoPagamento = 'voucher' THEN qtdePedidoMeioPagamento ELSE 0 END)/SUM(qtdePedidoMeioPagamento) AS pct_qtde_voucher_pedido,
  SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN qtdePedidoMeioPagamento ELSE 0 END)/SUM(qtdePedidoMeioPagamento) AS pct_qtde_debit_card_pedido,
  
  SUM(CASE WHEN descTipoPagamento = 'boleto' THEN vlPedidoMeioPagamento ELSE 0 END)/SUM(vlPedidoMeioPagamento) AS pct_valor_boleto_pedido,
  SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN vlPedidoMeioPagamento ELSE 0 END)/SUM(vlPedidoMeioPagamento) AS pct_valor_credit_card_pedido,
  SUM(CASE WHEN descTipoPagamento = 'voucher' THEN vlPedidoMeioPagamento ELSE 0 END)/SUM(vlPedidoMeioPagamento) AS pct_valor_voucher_pedido,
  SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN vlPedidoMeioPagamento ELSE 0 END)/SUM(vlPedidoMeioPagamento) AS pct_valor_debit_card_pedido
FROM
  tb_group
GROUP BY
  idVendedor

-- COMMAND ----------


