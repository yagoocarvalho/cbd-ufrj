# -*- coding: utf-8 -*-
"""
Created on Sun Dec  2 16:18:13 2018

@author: BConfessor
"""

#Script de geração de arquivos, cria os seguintes:
#tabelas PK (ordenada e desordenada) (só possuem chave primária e um atributo de enchimento)
#tabelas FK (ordenada e desordenada) (possui chave primária, chave estrangeira (= PK das tabelas PK) e atributo de enchimento)
#registros possuem tamanho fixo e relações são 1:1

import string
from random import choice
from random import shuffle

outputTableName = "Tabela" #nome dos arquivos a serem gerados
outputTableExtension = "txt" #extensão dos arquivos
fieldSeparator = ";" #delimitador de atributos
registrySize = 1024 #tamanho de cada linha(record) em bytes/chars
numberOfRegistries = 10000 #numero de linhas(records) em cada tabela



PKOrderedFileName = outputTableName + "OrdenadaPK." + outputTableExtension
PKUnorderedFileName = outputTableName + "DesordenadaPK." + outputTableExtension
FKOrderedFileName = outputTableName + "OrdenadaFK." + outputTableExtension
FKUnorderedFileName = outputTableName + "DesordenadaFK." + outputTableExtension



print("Gerando chaves...")

#gera chaves primárias para as tabelas PK
PKTableKeyList = list(range(numberOfRegistries)) #converte para lista pois range em Python3 é só iterador

#gera chaves primárias para as tabelas FK
FKTableKeyList = list(range(numberOfRegistries,2*numberOfRegistries)) #intervalo distinto para diferenciar das chaves PK




print("Gerando relações...")

#cria as relações entre registros das tabelas PK e FK
PK_FK_RelationshipDic = {} #cria um dicionario de relações entre chaves primárias das tabelas PK e chaves primárias das tabelas FK
tempList = FKTableKeyList
for k1 in PKTableKeyList: #para cada chave primária de PK...
    PK_FK_RelationshipDic[k1] = tempList.pop(tempList.index(choice(tempList))) #...escolhe aleatoriamente uma chave primária de FK e cria a relação




print("Gerando registros de PK...")

#gera os registros das tabelas PK
PKRecordsList = []
for k1 in PKTableKeyList:
    temp = str(k1) #começa a escrever o registro...
    temp += fieldSeparator #..., separa o atributo...
    temp += ''.join(choice(string.ascii_uppercase + string.digits) for _ in range(registrySize - len(temp))) #... e por fim preenche o resto do registro com enchimento
    PKRecordsList.append(temp)

#mesmo funcionamento para os registros de FK

print("Gerando registros de FK...")

#gera os registros das tabelas FK
FKRecordsList = []
for k1 in PKTableKeyList:
    temp = str(PK_FK_RelationshipDic[k1])
    temp += fieldSeparator
    temp += str(k1)
    temp += fieldSeparator
    temp += ''.join(choice(string.ascii_uppercase + string.digits) for _ in range(registrySize - len(temp)))
    FKRecordsList.append(temp)



#gera a tabela PK ordenada 
print("Criando " + PKOrderedFileName)
with open(PKOrderedFileName, "w") as f:
    for temp in PKRecordsList:
     f.write(temp + "\n")



#embaralhamos os registros de PK para criar a tabela desordenada do mesmo
shuffle(PKRecordsList)

print("Criando " + PKUnorderedFileName)
with open(PKUnorderedFileName, "w") as f:
    for temp in PKRecordsList:
        f.write(temp + "\n")  
    
    
    
    
        
        
#agora, a tabela FK Ordenada
print("Criando " + FKOrderedFileName)
with open(FKOrderedFileName, "w") as f:
    for temp in FKRecordsList:
        f.write(temp + "\n")

#embaralhamos FK para criar sua tabela desordenada
shuffle(FKRecordsList)

print ("Criando " + FKUnorderedFileName)
with open(FKUnorderedFileName, "w") as f:
    for temp in FKRecordsList:
        f.write(temp + "\n")

print("Finalizado.")