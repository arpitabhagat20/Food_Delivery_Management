SELECT 
  JSON_OBJECT(
    'customerId', c.customerid,
    'firstName', c.first_name,
    'lastName', c.last_name,
    'orders', IFNULL(
      (
        SELECT JSON_ARRAYAGG(
          JSON_OBJECT(
            'orderId', o.orderid,
            'restaurantName', r.restaurantname,
            'item', i.item,
            'price', i.price,
            'orderStatus', s.orderstatus
          )
        )
        FROM Orders o 
        JOIN Restaurant r ON o.restaurantid = r.restaurantid 
        JOIN Item i ON o.itemid = i.itemid 
        JOIN Status s ON o.orderstatusid = s.orderstatusid 
        WHERE c.customerid = o.customerid
        GROUP BY o.orderid
      ),
      JSON_ARRAY()
    )
  ) AS customer_orders
FROM 
  Customer c 
ORDER BY 
  c.customerid
;
