# -*- coding: utf-8 -*-
"""
Created on Mon Dec  3 23:18:55 2018

@author: BConfessor
"""

#from BTrees.OOBTree import OOBTree
import datetime as dt
import TableGen as tg
import math as m




#Tamanho de um bloco de memoria (medido em registros)
blockSize = 5







#Realiza Join com loops aninhados
#OrderedPK: Define se a tabela de PK (tabela da esquerda) será ordenada ou não(por default, ordenada)
#OrderedFK: Define se a tabela de FK (tabela da direita) será ordenada ou não(por default, desordenada)
def JoinNestedLoop(orderedPK = True, orderedFK = False):
    start = dt.datetime.now()#Para medirmos tempo de execução //(necessário?)
    #matches = 0
    
    blocksFetched = 0 #Contador de blocos, diz quantos blocos de X registros foram "acessados" da memória
    
    
    #Para a tabela da esquerda, acessaremos TODOS os registros, então já podemos saber quantos blocos de memória acessaremos
    #Simplesmente dividimos seu número de registros pelo tamanho de um bloco
    blocksFetched += m.ceil(float(tg.numberOfRegistries)/blockSize)#Ceiling pois pode sobrar espaço em algum bloco
    
    print("Current blocks fetched in left table: " + str(blocksFetched))
    #Arquivos que usaremos no JOIN
    PKFile = ""
    FKFile = ""
    if(orderedPK):
        PKFile = tg.PKOrderedFileName
    else:
        PKFile = tg.PKUnorderedFileName
    
    if(orderedFK):
        FKFile = tg.FKOrderedFileName
    else:
        FKFile = tg.FKUnorderedFileName
    
    #Abrimos ambas as tabelas 
    with open(FKFile) as rightTable:
        with open(PKFile) as leftTable:
            leftRegistryIndex = 1
            #Para cada registro na tabela PK
            for leftRegistries in leftTable:
                lRegistry = leftRegistries.split(tg.fieldSeparator)
                if (leftRegistryIndex%100==0):
                    print("Looking for match for left record " + str(leftRegistryIndex))
                
                #conta os registros acessados na tabela direita nessa iteração
                rightTableRegistriesCounter = 0
                #Faremos a iteração pela tabela da direita(FK), fixando a linha da tabela da esquerda(PK)
                for rightRegistries in rightTable:
                    rightTableRegistriesCounter+=1
                    #print("Checking right record "+str(rightTableRegistriesCounter))
                    rRegistry = rightRegistries.split(tg.fieldSeparator)
                    if lRegistry[0] == rRegistry[1]: #Encontramos um match entre os registros das tabelas
                        #Adiciona os blocos coletados da memória durante essa iteração por parte da tabela da direita
                        blocksFetched += m.ceil(float(rightTableRegistriesCounter)/blockSize)
                        print("Found match on right registry " + str(rightTableRegistriesCounter))
                        #joined = l1 + l2
                        rightTable.seek(0) #retornamos o ponteiro da tabela da direita para o seu início para uma nova iteração
                        break
                leftRegistryIndex+=1
    end = dt.datetime.now()                    
    
    #print("Total joined registers: " + str(matches))
    print("Tempo de execução: " + str((end-start).total_seconds()) + "s")
    print("Número total de blocos acessados: " + str(blocksFetched))




def MergeJoin():
    
    return 0




def main():
    JoinNestedLoop()