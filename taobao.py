#_*_ coding:utf-8 _*_
import re
import requests
import MySQLdb
import time
from ProxiesPool import ProxiesPool

def getProductDataByKeyword(keyword,ip):

    conn=MySQLdb.connect(host='127.0.0.1',user='root',passwd='root',port=3306,charset="utf8")
    url = 'https://s.taobao.com/search'
    payload = {'q': keyword,'s': '1','ie':'utf8'}  #字典传递url参数
    cur=conn.cursor()
    conn.select_db('taobao')
    print payload

    for k in range(0,100):        #100次，就是100个页的商品数据

        payload ['s'] = 44*k+1   #此处改变的url参数为s，s为1时第一页，s为45是第二页，89时第三页以此类推
        print "准备代理请求"
        resp = requests.get(url, params = payload,proxies=ip) #使用代理
        resp.encoding = 'utf-8'  #设置编码
        title = re.findall(r'"raw_title":"([^"]+)"',resp.text,re.I)  #正则保存所有raw_title的内容
        price = re.findall(r'"view_price":"([^"]+)"',resp.text,re.I)
        item_loc = re.findall(r'"item_loc":"([^"]+)"',resp.text,re.I)
        nid = re.findall(r'"nid":"([^"]+)"',resp.text,re.I)
        category = re.findall(r'"category":"([^"]+)"',resp.text,re.I)
        pic_url = re.findall(r'"pic_url":"([^"]+)"',resp.text,re.I)
        detail_url = re.findall(r'"detail_url":"([^"]+)"',resp.text,re.I)
        view_fee = re.findall(r'"view_fee":"([^"]+)"',resp.text,re.I)
        view_sales = re.findall(r'"view_sales":"([^"]+)"',resp.text,re.I)
        user_id = re.findall(r'"user_id":"([^"]+)"',resp.text,re.I)
        nick = re.findall(r'"nick":"([^"]+)"',resp.text,re.I)
        comment_url = re.findall(r'"comment_url":"([^"]+)"',resp.text,re.I)
        shopLink = re.findall(r'"shopLink":"([^"]+)"',resp.text,re.I)
        try:
            for i in range(title.__len__()):
                single_meichandise = []
                print detail_url[i]
                single_meichandise.append(category[i])
                single_meichandise.append(title[i])
                single_meichandise.append(price[i])
                single_meichandise.append(nid[i])
                single_meichandise.append(pic_url[i])
                single_meichandise.append(detail_url[i])
                single_meichandise.append(view_fee[i])
                single_meichandise.append(item_loc[i])
                single_meichandise.append(view_sales[i])
                single_meichandise.append(user_id[i])
                single_meichandise.append(nick[i])
                single_meichandise.append(comment_url[i])
                single_meichandise.append(shopLink[i])
                single_meichandise.append(time.time())#时间
                cur.execute('insert into product values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',single_meichandise)
        except:
            print "异常"
    conn.commit()
    cur.close()
    conn.close()

def getDataByCategory():
    print("抓取目录开始")
    url="https://www.taobao.com/tbhome/page/market-list?spm=a21bo.50862.201867-main.1.28689e73VprFO6"
    resp = requests.get(url)
    category = re.findall(r'target="_blank">([^<]+)<', resp.text, re.I)
    conn=MySQLdb.connect(host='127.0.0.1',user='root',passwd='root',port=3306,charset="utf8")
    cur=conn.cursor()
    conn.select_db('taobao')

    for i in range(category.__len__()): #遍历所有类目
        print "正在抓取类目:"
        categoryPart=category[i]
        insert=[]
        insert.append(categoryPart)
        insert.append(1)
        print categoryPart
        cur.execute('insert into product_category values(%s,%s)',insert)
    conn.commit()
    cur.close()
    conn.close()

def executeSql(sql,param):
    conn=MySQLdb.connect(host='127.0.0.1',user='root',passwd='root',port=3306,charset="utf8")
    cur=conn.cursor()
    conn.select_db('taobao')
    cur.execute(sql,param)
    conn.commit()
    cur.close()
    conn.close()

def query(sql):
    conn=MySQLdb.connect(host='127.0.0.1',user='root',passwd='root',port=3306,charset="utf8")
    cur=conn.cursor()
    conn.select_db('taobao')
    count=cur.execute(sql)
    result=cur.fetchmany(count)
    conn.commit()
    cur.close()
    conn.close()
    return result

def getProductData():
    proxiesPool = ProxiesPool() #代理池
    print "开始查询"
    data=query('select * from product_category where paqu_status=1')
    for record in data:
        ip = proxiesPool.find_valued_proxy()  # 每个类目用一个代理
        mulu=record[0]
        print mulu
        try:
            mulu={mulu}
            executeSql('update product_category set paqu_status=0 where category_name=%s', mulu)
            getProductDataByKeyword(mulu,ip)
        except:
            print "抓取产品异常"


#主进程
print("程序开始")
getProductData()




