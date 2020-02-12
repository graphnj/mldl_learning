# -*- coding: utf-8 -*-
"""
Created on Mon Aug 26 12:27:16 2019

@author: 0049003202
"""

# coding: utf-8

import os,csv,codecs
import json,time
from py2neo import Graph,Node

class ZNVKGraph:
    def __init__(self):
        cur_dir = 'data/'
        self.cur_dir = cur_dir
        self.data_path = os.path.join(self.cur_dir, 'film_info.json')
        self.g = Graph(
            host="10.45.154.218", 
            http_port=7474, 
            user="neo4j",  
            password="zjhneo4j")
        
    
    def create_rels_from_csv(self,csv_file_name,start_node,end_node,rel_type,rel_name):
        query = "USING PERIODIC COMMIT 10000 \
                 LOAD CSV WITH HEADERS FROM 'file:///%s' AS line \
                 match (from:%s{baike_id:line.from_name}),(to:%s{name:line.to_name}) \
                 merge (from)-[rel:%s{name:'%s'}]->(to)" %(csv_file_name,start_node,end_node,rel_type,rel_name)
        try:
            self.g.run(query)
            print("Create relationship successfully")
        except Exception as e:
            print(e)        
 
            
    def nodeExist(self, baike_id):
        matcher = NodeMatcher(self.g)
        m = matcher.match("Baike", baike_id = baike_id).first()
        if m is None:
            return False
        else:
            return True
    def create_graphrels(self):
        films,directors,stars,types,company,areas,languages,film_infos,rels_director,rels_star,rels_type,\
            rels_faxing_company,rels_area,rels_language,rels_scriptwriter = handler.get_nodes()        
        #self.create_relationship('Film', 'Film_Director', rels_director, 'director', '导演')
        self.create_relationship('Film', 'Film_Star', rels_star, 'actor', '主演')
        self.create_relationship('Film', 'Film_Type', rels_type, 'type', '类型')
        self.create_relationship('Film', 'Film_Company', rels_faxing_company, 'faxing_company', '发行公司')
        self.create_relationship('Film', 'Film_Area', rels_area, 'area', '地区')
        self.create_relationship('Film', 'Film_Language', rels_language, 'language', '语言')

    def create_relationship(self, start_node, end_node, edges, rel_type, rel_name):
        count = 0
        # 去重处理
        set_edges = []
        for edge in edges:
            set_edges.append('###'.join(edge))
        all = len(set(set_edges))
        for edge in set(set_edges):
            edge = edge.split('###')
            p = edge[0]
            q = edge[1]
            query = "match(p:%s),(q:%s) where p.name='%s'and q.name='%s' merge (p)-[rel:%s{name:'%s'}]->(q)" % (
                start_node, end_node, p, q, rel_type, rel_name)
            try:
                self.g.run(query)
                count += 1
                print(rel_type, count, all)
            except Exception as e:
                print(e)
                continue
        return




    def create_xzqh_node(self,csv_name):
        query = "USING PERIODIC COMMIT 10000 \
                LOAD CSV WITH HEADERS FROM 'file:///%s' AS row \
                merge (n:XZQH{code:row.code})\
                on create set n.name=row.name,n.level=row.level,n.codeabbr=row.codeabbr,n.prov=row.prov,n.city=row.city"%csv_name
        try:
            self.g.run(query)
            
            print("Create Node successfully")
        except Exception as e:
            print(e)   
            
    def create_xzqh_rel(self,csv_file_name,rel_type,rel_name):
        query = "USING PERIODIC COMMIT 10000 \
                 LOAD CSV WITH HEADERS FROM 'file:///%s' AS line \
                 match (from:XZQH{code:line.code}),(to:XZQH{code:line.uplevelcode}) \
                 merge (from)-[rel:%s{name:'%s'}]->(to)" %(csv_file_name,rel_type,rel_name)
        try:
            self.g.run(query)
            print("Create relationship successfully")
        except Exception as e:
            print(e)      
            
    def create_shop_node(self,csv_name):
        query = "USING PERIODIC COMMIT 10000 \
                LOAD CSV WITH HEADERS FROM 'file:///%s' AS row \
                merge (n:Shop{shopid:toInt(row.shopid)})\
                on create set n.name=row.name,n.addr=row.addr,n.regaddr=row.regaddr,n.legalname=row.legalname,\
                n.licensecode=row.licensecode,n.type=row.type,n.score=row.score,n.tele=row.tele,n.gpsx=row.gpsx,n.gpsy=row.gpsy"%csv_name
        try:
            self.g.run(query)
            
            print("Create create_shop_node successfully")
        except Exception as e:
            print(e)  
    def create_resident_node(self,csv_name):
        query = "USING PERIODIC COMMIT 10000 \
                LOAD CSV WITH HEADERS FROM 'file:///%s' AS row \
                merge (n:Resident{gmsfhm:row.gmsfhm})\
                on create set n.xm=row.xm, n.zz=row.zz, n.hjdqh=row.hjdqh, n.hjdmc=row.hjdmc, n.\
                jiguanqh=row.jiguanqh, n.jiguanmc=row.jiguanmc, n.whcd=row.whcd, n.xb=row.xb, n.mz=row.mz, n.tele=row.tele"%csv_name
        try:
            self.g.run(query)
            
            print("Create create_resident_node successfully")
        except Exception as e:
            print(e)               
            
    def create_shopboss_rel(self,csv_file_name,rel_type,rel_name):
        query = "USING PERIODIC COMMIT 10000 \
                 LOAD CSV WITH HEADERS FROM 'file:///%s' AS line \
                 match (from:Shop{shopid:toInt(line.shopid)}),(to:Resident{xm:line.legalname}) \
                 merge (from)-[rel:%s{name:'%s'}]->(to)" %(csv_file_name,rel_type,rel_name)
        print(query)
        try:
            self.g.run(query)
            print("Create create_shopboss_rel successfully")
        except Exception as e:
            print(e) 
            
    def create_shopIllegalVNode_rel(self,csv_file_name,rel_type,rel_name):
        query = "USING PERIODIC COMMIT 10000 \
                 LOAD CSV WITH HEADERS FROM 'file:///%s' AS line \
                 merge (from:Shop{shopid:toInt(line.shopid)}) \
                 merge (to:IllegalVNode{name:line.name}) \
                 merge (from)-[rel:%s{name:'%s'}]->(to)" %(csv_file_name,rel_type,rel_name)
        #print(query)
        try:
            self.g.run(query)
            print("Create create_shopIllegalVNode_rel successfully")
        except Exception as e:
            print(e) 
            
    def create_shopIllegalDetail_rel(self,csv_file_name,rel_type,rel_name):
        query = "USING PERIODIC COMMIT 10000 \
                 LOAD CSV WITH HEADERS FROM 'file:///%s' AS line \
                 merge (from:IllegalVNode{name:line.shopname})\
                 merge (to:IllegalDetail{eventid:line.eventid}) on create \
                 set to.illegaltype=line.illegaltype,to.illegaltime=line.illegaltime,to.imgurl1=line.imgurl1,to.imgurl2=line.imgurl2 \
                 merge (from)-[rel:%s{name:'%s'}]->(to)" %(csv_file_name,rel_type,rel_name)
        #print(query)
        try:
            self.g.run(query)
            print("Create create_shopIllegalDetail_rel successfully")
        except Exception as e:
            print(e) 
            
    def create_shoptype_node(self):
        t=['餐饮店','五金店','烟酒店','水果店','商超','美发店']
        for i in range(6):
            query = "merge(n:ShopType{name:'%s'})" %(t[i])
            print(query)
            try:
                self.g.run(query)
                print("Create create_shoptype_rel successfully")
            except Exception as e:
                print(e)  
            
    def create_shoptype_rel(self,csv_file_name,rel_type,rel_name):
        query = "USING PERIODIC COMMIT 10000 \
                 LOAD CSV WITH HEADERS FROM 'file:///%s' AS line \
                 match (from:Shop{shopid:toInt(line.shopid)}),(to:ShopType{name:line.shoptype}) \
                 merge (from)-[rel:%s{name:'%s'}]->(to)" %(csv_file_name,rel_type,rel_name)
        #print(query)
        try:
            self.g.run(query)
            print("Create create_shoptype_rel successfully")
        except Exception as e:
            print(e)  
    def create_gmjiguan_rel(self,csv_file_name,rel_type,rel_name):
        query = "USING PERIODIC COMMIT 10000 \
                 LOAD CSV WITH HEADERS FROM 'file:///%s' AS line \
                 match (from:Resident{gmsfhm:line.gmsfhm}),(to:XZQH{code:line.jiguanqh}) \
                 merge (from)-[rel:%s{name:'%s'}]->(to)" %(csv_file_name,rel_type,rel_name)
        try:
            self.g.run(query)
            print("Create relationship successfully")
        except Exception as e:
            print(e)  
    def create_gmhuji_rel(self,csv_file_name,rel_type,rel_name):
        query = "USING PERIODIC COMMIT 10000 \
                 LOAD CSV WITH HEADERS FROM 'file:///%s' AS line \
                 match (from:Resident{gmsfhm:line.gmsfhm}),(to:XZQH{code:line.hjdqh}) \
                 merge (from)-[rel:%s{name:'%s'}]->(to)" %(csv_file_name,rel_type,rel_name)
        try:
            self.g.run(query)
            print("Create relationship successfully")
        except Exception as e:
            print(e)               
            
            
if __name__ == '__main__':
    import subprocess
    handler = ZNVKGraph()
    '''handler.create_xzqh_node('xzqhneo4jcsvnode.csv')
    handler.create_xzqh_rel('xzqhneo4jcsvedge.csv','guishu','归属')
    handler.create_shop_node('entity_lujiazuishopnew.csv')
    handler.create_resident_node('entity_resident.csv')
    handler.create_shopboss_rel('rel_shop_boss.csv','boss','店主')
    handler.create_shoptype_node()
    handler.create_shoptype_rel('rel_shop_type.csv','shoptype','店类型')'''
    #handler.create_gmhuji_rel('rel_resident_hjd.csv','hjd','户籍地')
    '''handler.create_gmjiguan_rel('rel_resident_jiguan.csv','jiguan','籍贯')'''
    
    handler.create_shop_node('entity_spinfo.csv')
    handler.create_resident_node('entity_resident.csv')
    handler.create_shopboss_rel('entity_spinfo.csv','legalperson','法人')
    handler.create_shopIllegalVNode_rel('entity_spinfo.csv','illegalvnode','违规')
    handler.create_shopIllegalDetail_rel('entity_illegal.csv','illegalevent','违规事件')
    handler.create_shoptype_node()
    handler.create_shoptype_rel('entity_spinfo.csv','shoptype','店类型')