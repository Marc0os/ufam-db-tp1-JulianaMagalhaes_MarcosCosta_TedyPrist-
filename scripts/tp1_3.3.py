
import psycopg2 as db

conn = db.connect(dbname="tp1", user="postgres", password="postgres")

cur = conn.cursor()

print("(a) Dado um produto, listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação")
cur.execute("(SELECT c.id, c.date, c.rating, c.votes, c.helpful FROM comment c WHERE c.id_product = 1 ORDER BY c.helpful DESC, c.rating DESC LIMIT 5) UNION ALL (SELECT c.id, c.date, c.rating, c.votes, c.helpful FROM comment c WHERE c.id_product = 1 ORDER BY c.helpful DESC, c.rating ASC LIMIT 5);")
resultado = cur.fetchall()
for row in resultado:
        print(row)

print("\n(b) Dado um produto, listar os produtos similares com maiores vendas do que ele")
cur.execute("SELECT a.asin, a.title, a.salesrank FROM product JOIN similar_products ON product.asin = similar_products.asin_product_2 JOIN product a ON similar_products.id_product_1 = a.id WHERE product.asin = '0827229534' AND a.salesrank < product.salesrank ORDER BY a.salesrank ASC LIMIT 10;")
resultado = cur.fetchall()
for row in resultado:
        print(row)

print("\n(c) Dado um produto, mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada")
cur.execute("SELECT date_trunc('day', comment.date) AS data,AVG(comment.rating) AS media_avaliacao FROM comment JOIN product ON comment.id_product = product.id WHERE product.asin = '0827229534' AND comment.date BETWEEN (SELECT MIN(date) FROM comment WHERE id_product = product.id) AND (SELECT MAX(date) FROM comment WHERE id_product = product.id) GROUP BY date_trunc('day', comment.date) ORDER BY data;")
resultado = cur.fetchall()
for row in resultado:
        print(row)


print("\n(d) Listar os 10 produtos líderes de venda em cada grupo de produtos")
cur.execute("SELECT category, title AS produto, salesrank AS classificacao_vendas FROM (SELECT product.category, product.title, product.salesrank, RANK() OVER (PARTITION BY product.category ORDER BY product.salesrank ASC) AS rank FROM product WHERE (SELECT COUNT(*)FROM comment WHERE comment.id_product = product.id AND comment.rating > 0 ) >= 10) AS subquery WHERE rank <= 10 ORDER BY  category,salesrank;")
resultado = cur.fetchall()
for row in resultado:
        print(row)


print("\n(e) Listar os 10 produtos com a maior média de avaliações úteis positivas por produto")
cur.execute("SELECT product.asin, product.title AS produto,AVG(comment.helpful) AS media_avaliacoes_uteis FROM product JOIN  comment  ON product.id = comment.id_product WHERE comment.helpful > 0 GROUP BY product.asin,    product.title ORDER BY media_avaliacoes_uteis DESC LIMIT 10;")
resultado = cur.fetchall()
for row in resultado:
        print(row)


print("\n(f) Listar a 5 categorias de produto com a maior média de avaliações úteis positivas por produto")
cur.execute("SELECT product.category, AVG(comment.helpful) AS media_avaliacoes_uteis FROM  product JOIN comment ON product.id = comment.id_product WHERE comment.helpful > 0 GROUP BY product.category ORDER BY media_avaliacoes_uteis DESC LIMIT 5;")
resultado = cur.fetchall()
for row in resultado:
        print(row)


print("\n(g) Listar os 10 clientes que mais fizeram comentários por grupo de produto")
cur.execute("SELECT  category, id_client, total_comentarios FROM (  SELECT product.category, comment.id_client,  COUNT(comment.id) AS total_comentarios, ROW_NUMBER() OVER (PARTITION BY product.category ORDER BY COUNT(comment.id) DESC) AS row_num FROM   product  JOIN comment ON product.id = comment.id_product  GROUP BY  product.category,   comment.id_client) AS subquery WHERE  row_num <= 10;")
resultado = cur.fetchall()
for row in resultado:
        print(row)