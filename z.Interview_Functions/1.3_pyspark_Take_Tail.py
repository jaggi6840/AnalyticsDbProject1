# Databricks notebook source
# MAGIC %md 
# MAGIC ###### How to use explode Function

# COMMAND ----------

simpleData = [(6991,"6/9/2014 18:26","engagement","home_page","United States","iphone 5"), \
(18851,"8/29/2014 13:18","signup_flow","enter_info","Russia","asus chromebook"), \
(14998,"7/1/2014 12:47","engagement","login","France","hp pavilion desktop"), \
(8186,"5/23/2014 10:44","engagement","home_page","Italy","macbook pro"), \
(9626,"7/31/2014 17:15","engagement","login","Russia","nexus 7"), \
(16460,"7/24/2014 18:43","signup_flow","create_user","United States","samsung galaxy note"), \
(10101,"8/27/2014 5:54","engagement","home_page","Singapore","dell inspiron notebook"), \
(2670,"5/10/2014 10:03","engagement","like_message","United States","nexus 7"), \
(8708,"5/26/2014 10:42","engagement","send_message","Australia","macbook pro"), \
(167,"7/30/2014 19:39","engagement","view_inbox","United Arab Emirates","lenovo thinkpad"), \
(12725,"6/17/2014 8:03","engagement","home_page","Brazil","macbook pro"), \
(14014,"7/6/2014 16:24","engagement","view_inbox","Japan","macbook pro"), \
(13536,"6/18/2014 9:34","engagement","send_message","United States","nexus 5"), \
(2663,"6/13/2014 7:45","engagement","home_page","United States","nokia lumia 635"), \
(8149,"6/6/2014 15:35","engagement","like_message","Australia","samsung galaxy s4"), \
(3152,"5/20/2014 17:57","engagement","home_page","Venezuela","iphone 5"), \
(11496,"8/28/2014 7:45","engagement","view_inbox","United States","lenovo thinkpad"), \
(2755,"5/5/2014 14:14","engagement","view_inbox","France","samsung galaxy s4"), \
(3422,"7/3/2014 14:09","engagement","send_message","United States","kindle fire"), \
(2360,"6/2/2014 19:53","engagement","search_click_result_9","United States","iphone 4s"), \
(10061,"8/18/2014 9:32","engagement","view_inbox","Spain","macbook pro"), \
(12740,"5/21/2014 0:57","signup_flow","enter_email","United Kingdom","iphone 4s"), \
(7849,"6/16/2014 15:53","engagement","home_page","Japan","nexus 7"), \
(1002,"5/16/2014 9:33","engagement","view_inbox","Canada","iphone 5s"), \
(11836,"8/3/2014 16:12","engagement","send_message","United States","iphone 4s"), \
(6836,"8/1/2014 14:58","engagement","view_inbox","United Kingdom","ipad air"), \
(6663,"6/16/2014 12:04","engagement","like_message","Australia","asus chromebook"), \
(14653,"7/2/2014 13:40","engagement","login","Australia","macbook pro"), \
(2719,"8/17/2014 15:19","engagement","view_inbox","India","iphone 4s"), \
(2670,"5/11/2014 13:58","engagement","home_page","United States","iphone 5s"), \
(3067,"7/29/2014 7:24","engagement","home_page","Canada","iphone 5s"), \
(5108,"5/16/2014 14:41","engagement","like_message","Canada","samsung galaxy s4"), \
(5874,"7/31/2014 14:55","engagement","search_run","Russia","iphone 5"), \
(2267,"7/18/2014 9:57","engagement","search_autocomplete","Brazil","macbook pro"), \
(3526,"5/9/2014 12:49","engagement","home_page","United States","hp pavilion desktop"), \
(10323,"5/19/2014 17:08","engagement","view_inbox","Switzerland","hp pavilion desktop"), \
(11824,"5/20/2014 7:22","engagement","home_page","Saudi Arabia","ipad mini"), \
(12396,"5/14/2014 18:10","engagement","view_inbox","United Kingdom","hp pavilion desktop"), \
(13056,"8/12/2014 13:45","engagement","home_page","Germany","ipad air"), \
(6493,"5/4/2014 10:33","engagement","search_run","United States","iphone 5"), \
(12047,"5/25/2014 18:56","engagement","search_autocomplete","Austria","macbook air"), \
(10516,"7/22/2014 17:35","engagement","send_message","Singapore","dell inspiron notebook"), \
(3076,"8/8/2014 12:03","engagement","send_message","United States","dell inspiron notebook"), \
(9863,"7/11/2014 8:59","engagement","view_inbox","Philippines","acer aspire desktop"), \
(9915,"5/1/2014 9:34","engagement","view_inbox","Indonesia","macbook pro"), \
(175,"7/23/2014 9:32","engagement","home_page","Russia","iphone 4s"), \
(10350,"6/16/2014 15:28","engagement","home_page","United States","iphone 5"), \
(9283,"6/25/2014 13:47","engagement","search_click_result_3","Canada","dell inspiron notebook"), \
(10394,"5/27/2014 6:50","engagement","home_page","Japan","macbook pro"), \
(13397,"8/18/2014 15:04","engagement","view_inbox","Japan","macbook air"), \
(1078,"6/9/2014 17:46","engagement","home_page","Germany","ipad mini"), \
(7393,"8/8/2014 12:32","engagement","search_autocomplete","Japan","acer aspire desktop"), \
(4765,"8/29/2014 11:09","engagement","search_autocomplete","United States","macbook pro"), \
(17067,"8/4/2014 19:01","signup_flow","complete_signup","United States","lenovo thinkpad"), \
(13597,"6/5/2014 20:28","signup_flow","enter_email","Netherlands","nexus 5"), \
(8258,"5/8/2014 9:34","engagement","home_page","United States","samsung galaxy note"), \
(10523,"7/2/2014 9:16","engagement","like_message","Canada","samsumg galaxy tablet"), \
(2242,"8/3/2014 18:49","engagement","home_page","United Kingdom","iphone 5"), \
(11595,"7/15/2014 17:18","engagement","view_inbox","Mexico","hp pavilion desktop"), \
(9985,"7/28/2014 9:34","engagement","like_message","Sweden","ipad air"), \
(15725,"7/12/2014 18:41","engagement","view_inbox","Japan","lenovo thinkpad"), \
(4370,"6/19/2014 17:28","engagement","like_message","Russia","iphone 5"), \
(13466,"6/3/2014 10:52","engagement","search_autocomplete","Hong Kong","macbook air"), \
(8866,"8/5/2014 8:34","engagement","home_page","United States","dell inspiron desktop"), \
(10310,"7/18/2014 11:54","engagement","send_message","Brazil","amazon fire phone"), \
(13067,"7/23/2014 4:16","engagement","like_message","United States","lenovo thinkpad"), \
(2171,"8/26/2014 7:29","engagement","send_message","United States","asus chromebook"), \
(6622,"6/6/2014 10:08","engagement","search_click_result_8","United States","acer aspire notebook"), \
(12548,"6/5/2014 13:46","engagement","login","Germany","nokia lumia 635"), \
(12540,"5/16/2014 14:20","signup_flow","create_user","Russia","lenovo thinkpad"), \
(9876,"7/4/2014 13:22","engagement","search_run","United States","samsung galaxy s4"), \
(8716,"8/26/2014 10:28","engagement","search_autocomplete","France","iphone 5"), \
(8740,"7/26/2014 9:39","engagement","search_autocomplete","United States","nexus 10"), \
(12688,"7/11/2014 18:00","engagement","search_autocomplete","Germany","hp pavilion desktop"), \
(5109,"8/23/2014 6:02","engagement","login","United States","dell inspiron desktop"), \
(6261,"5/19/2014 13:39","engagement","login","India","iphone 5"), \
(11264,"7/17/2014 17:53","engagement","view_inbox","Taiwan","lenovo thinkpad"), \
(5298,"6/7/2014 18:28","engagement","home_page","Brazil","asus chromebook"), \
(14726,"7/15/2014 12:08","engagement","home_page","United Kingdom","macbook pro"), \
(13433,"7/26/2014 6:48","engagement","like_message","United States","dell inspiron notebook"), \
(264,"5/30/2014 19:36","engagement","login","Venezuela","samsung galaxy s4"), \
(108,"7/4/2014 21:48","engagement","login","Mexico","lenovo thinkpad"), \
(264,"6/5/2014 9:23","engagement","login","Venezuela","dell inspiron desktop"), \
(251,"7/29/2014 14:32","engagement","login","Argentina","ipad air"), \
(108,"7/8/2014 7:43","engagement","login","Mexico","hp pavilion desktop"), \
(264,"5/20/2014 17:46","engagement","login","Venezuela","dell inspiron desktop"), \
(251,"8/6/2014 15:24","engagement","login","Argentina","macbook air"), \
(238,"7/16/2014 14:28","engagement","login","Venezuela","samsung galaxy note"), \
(251,"8/2/2014 10:47","engagement","login","Argentina","macbook air"), \
(108,"7/21/2014 17:34","engagement","login","Mexico","hp pavilion desktop"), \
(12103,"6/25/2014 11:10","engagement","like_message","Argentina","macbook pro"), \
(16170,"8/23/2014 18:53","engagement","like_message","Argentina","macbook pro"), \
(12103,"6/19/2014 19:45","engagement","home_page","Argentina","macbook pro"), \
(12103,"6/25/2014 11:08","engagement","like_message","Argentina","macbook pro"), \
(16170,"8/25/2014 13:32","engagement","home_page","Argentina","macbook pro"), \
(16170,"8/22/2014 13:33","engagement","home_page","Argentina","macbook pro"), \
(16170,"8/19/2014 11:07","engagement","login","Argentina","macbook pro"), \
(12103,"6/25/2014 11:07","engagement","login","Argentina","macbook pro"), \
(12103,"6/19/2014 19:47","engagement","home_page","Argentina","macbook pro"), \
(12103,"6/18/2014 16:00","engagement","search_autocomplete","Argentina","macbook pro")
]
columns= ["user_id","occurred_at","event_type","event_name","location","device"]
df=spark.createDataFrame(simpleData,columns)

# COMMAND ----------

df.take(5)

# COMMAND ----------

df.tail(5)
