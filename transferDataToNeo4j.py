#!/usr/bin/env python
# coding: utf-8


from optparse import OptionParser
usage='''Welcome! This .py is used to transfer three data files to the Neo4j graph databases.
Please change the userName and passWord for Neo4j before using this program.
Please use "python transferDataToNeo4j.py -h" to see some helps.
'''
parser = OptionParser(usage) #带参的话会把参数变量的内容作为帮助信息输出
parser.add_option("-e","--entities_file",dest="entities_file",default='entities.csv',action = "store",type="string",help='This is a specific file for Tianyuan project, whose content is .csv and looks like a excel-book.')
parser.add_option("-r","--relations_file",dest="relations_file",default='relations.txt',action = "store",type="string",help='The format is [key1,relation_type,key2{,prop_key:prop_val}*]')
parser.add_option("-p","--properties_file",dest="properties_file",default='properties.txt',action = "store",type="string",help='The format is [key1,key2{,key3}*], which means that the key1 is the main key while others are key1\'s properties.')
parser.add_option("-d","--deleteAllFlag",dest="deleteAllFlag",default = 1, type='int',help='if it is 1(default), the neo4j database will be deleted ')
parser.add_option("-m","--demoFlag",dest="demoFlag",default = 1, type='int',help='If it is 0, it is not demo and will have all entities. If it is 1 or 2 or 10, it would be different demos.')
parser.add_option("-u","--userName",dest="userName",default = 'neo4j', type='string',help='the username for the Neo4j')
parser.add_option("-w","--passWord",dest="passWord",default = '123123', type='string',help='the password for the Neo4j')
(options,args)=parser.parse_args()
option_entities_file = options.entities_file
option_relations_file = options.relations_file
option_properties_file = options.properties_file
option_deleteAllFlag = options.deleteAllFlag
option_demoFlag = options.demoFlag
option_userName = options.userName
option_passWord = options.passWord



# In[3]:


import pandas as pd
import numpy as np
import os
from tqdm import tqdm


# In[4]:


from py2neo import Graph,Node,Relationship
graph = Graph('http://localhost:7474',username=option_userName,password=option_passWord)


# In[5]:


df = pd.read_csv(option_entities_file,sep=';',encoding='utf-16')
# print(df.shape)
# print(df.columns.values)
df.head()


# In[6]:


if option_demoFlag == 1:
    df = df.iloc[[3,4,568,570]]
    tmp = df['机型'][3] 
    df.loc[4,'机型'] = tmp
    df.loc[568,'机型'] = tmp
    df.loc[570,'机型'] = tmp
elif option_demoFlag == 2:
    df = df.iloc[[3,4,580]]
    tmp = df['机型'][3] 
    df.loc[4,'机型'] = tmp
    df.loc[580,'故障原因'] = df.loc[3,'故障原因']
    df.loc[580,'故障现象'] = df.loc[3,'故障现象']
    df.loc[580,'故障代码'] = df.loc[3,'故障代码']
elif option_demoFlag == 10:
    df = df.iloc[545:590]


# In[7]:


df


# In[8]:


def getPropertiesFromFile(file_path=option_properties_file):
    schema_prop = {}
    file = open(file_path)
    for s in file.readlines():
        s = s.replace('，',',') # 支持中英文逗号
        s = s.replace('\n','') # 去掉末尾的换行符
        s_sp = s.split(',')
        if (len(s_sp) < 2):
            raise(ImportError("输入字符串格式不标准，应为'key1,key2'，含义为key2字段是key1字段的属性"))
        properties = []
        for i in range(1,len(s_sp)):
            properties.append(s_sp[i])
        schema_prop[s_sp[0]] = properties
    file.close()
    notGoodKey = []
    for val in schema_prop.values():
        for v in val:
            notGoodKey.append(v)
    return schema_prop, notGoodKey
schema_prop, notGoodKey = getPropertiesFromFile()
# print(notGoodKey)
schema_prop


# In[9]:


def getRelationFromString(s):
    s = s.replace('，',',') # 支持中英文逗号
    s = s.replace('：',':')
    s = s.replace('\n','') # 去掉末尾的换行符
    s_sp = s.split(',')
    if (len(s_sp) < 3):
        raise(ImportError("输入字符串格式不标准，应为'key1,relation_type,key2,property_key1=property1,property_key2=property2'"))
    dic = {}
    for i in range(3, len(s_sp)):
        dic[s_sp[i].split(':')[0]] = s_sp[i].split(':')[1]
    return {'key1':s_sp[0],'relation_type':s_sp[1],'key2':s_sp[2],'properties':dic}
#     return s_sp[0],s_sp[1],s_sp[2],dic
s = "故障原因，对应，故障代码,label:LabelTest1,name:NameTest2"
getRelationFromString(s)


# In[10]:


def getRelationsFromFile(file_path = option_relations_file):
    relations = []
    rela_file = open(file_path)
    # print(rela_file.readlines())
    for line in rela_file.readlines():
#         print(line)
#         print(getRelationFromString(line))
        relations.append(getRelationFromString(line))
    rela_file.close()
    return relations
schema_relations= getRelationsFromFile()

schema_relations


# In[11]:


# aNode = Node("编号",label="实例节点",小时='test')
# graph.create(aNode)


# In[12]:


if option_deleteAllFlag == 1:
    graph.delete_all()

goodKeyNode = {}
for key in tqdm(list(df.columns.values)):
    if key in notGoodKey:
        continue
    goodKeyNode[key] = Node(key,label="概念节点",value=key)
    graph.create(goodKeyNode[key])
    if option_demoFlag >= 1 and option_demoFlag < 10:
        break
for i_index in tqdm(list(df.index)):
#     print(i_index)
    key_to_node = {}
    for key in list(df.columns.values):
#         print(key)
        if key in notGoodKey: # 有些字段是作为其他字段的属性存在的，不需要单独建立实体节点
            continue
#         graph.create(Node(key))
        if df[key][i_index] == df[key][i_index]: # 判断该cell不是NaN
            # 判断是否已经存在该节点，若不存在则创建，存在则直接连接
            nodeBefore = graph.nodes.match(key,value=df[key][i_index]).first() 
            if (nodeBefore is None):
#                 print(df[key][i_index])
                key_to_node[key] = Node( key,label='实例节点',value=df[key][i_index],type=key)
                if key in schema_prop.keys(): # 有些字段是其他字段的属性，如“小时数”是“编号”的属性，为“编号”增加该属性
                    for val in schema_prop[key]:
                        if df[val][i_index] == df[val][i_index]:
                            key_to_node[key][val] = df[val][i_index]
                graph.create(key_to_node[key])
            else:
                key_to_node[key] = nodeBefore

    for relation in schema_relations:
#         print(relation)
        try:
            aRelation = Relationship(key_to_node[relation['key1']],relation['relation_type'],key_to_node[relation['key2']])
            properties = relation['properties']
            for key in properties.keys():
                aRelation[key] = properties[key]
            graph.create(aRelation)
        except(KeyError):
            pass
    
    for key in goodKeyNode.keys():
        if df[key][i_index] == df[key][i_index]: # 判断该cell不是NaN
            graph.create(Relationship(key_to_node[key],'type',goodKeyNode[key],label='type'))

