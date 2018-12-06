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
    
    
    #Para a tabela da esquerda, acessaremos TODOS os registros ordenadamente, então já podemos saber quantos blocos de memória acessaremos
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




def MergeJoin(orderedPK = True, orderedFK = True):
    start = dt.datetime.now()
    blocksFetched = 0 #Contador de blocos, diz quantos blocos de X registros foram "acessados" da memória
    leftRegistriesCount = 1
    rightRegistriesCount = 1
    matches = 0
    
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
    with open(FKFile) as rightTable:
        with open(PKFile) as leftTable:
            leftRecord = leftTable.readline().split(tg.fieldSeparator)
            rightRecord = rightTable.readline().split(tg.fieldSeparator)
            while True:
                if leftRecord[0] == rightRecord[1]:
                    matches += 1
                    joined = leftRecord + rightRecord
                if int(leftRecord[0]) < int(rightRecord[1]):
                    ln = leftTable.readline()
                    if ln == "":
                        break
                    leftRegistriesCount += 1
                    leftRecord = ln.split(tg.fieldSeparator)
                else: 
                    ln = rightTable.readline()
                    if ln == "":
                        break
                    rightRegistriesCount += 1
                    rightRecord = ln.split(tg.fieldSeparator)
    end = dt.datetime.now()   

    print ("Número de blocos acessados: " + str(m.ceil(leftRegistriesCount/blockSize) + m.ceil(leftRegistriesCount/blockSize)))
    print ("Total joined registers: " + str(matches))
    print ("Tempo de execução: " + str((end-start).total_seconds()) + "s")







#Tanto PK quanto FK são consideradas falsas por default;
def HashJoin(orderedPK = False, orderedFK = False):
    lineSize = tg.registrySize + 2 #Tamanho de uma linha no arquivo linha, inclui o pulo de linha(funciona com 2 chars em Windows)
    start = dt.datetime.now()
    blocksFetched = 0 #blocos de memória vistos
    hashTable = {} #Tabela de hash a ser usada

    print("Line size: " + str(lineSize))
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
    
    #Para a tabela da esquerda, acessaremos TODOS os registros ordenadamente, então já podemos saber quantos blocos de memória acessaremos
    #Simplesmente dividimos seu número de registros pelo tamanho de um bloco
    blocksFetched += m.ceil(float(tg.numberOfRegistries)/blockSize)#Ceiling pois pode sobrar espaço em algum bloco
    
    
    
    lineCounter = 0 #anota as linhas de cada registro
    with open (FKFile) as hashTableBuilderFile:
        for registries in hashTableBuilderFile:
            # "primary_key|foreign_key|data"
            registry = registries.split(tg.fieldSeparator)
            hashTable[registry[1]] = lineCounter #para cada registro, criamos um dicionário associando sua chave primária à sua linha
            lineCounter += 1 
            
            
    matches = 0
    #abrimos as duas tabelas
    with open(FKFile) as rightTable:
        with open(PKFile) as leftTable:
            #Para cada linha na tabela PK, pegamos sua chave e usamos para encontrar a posição da respectiva linha na tabela FK, a partir da tabela hash
            for line in leftTable:
                #print("\n\nLeft line: " + line) 
                leftRecord = line.split(tg.fieldSeparator)
                position = hashTable[leftRecord[0]] * lineSize #vai para a posição absoluta do início do respectivo registro em FK
                #print("Position to seek: " + str(position))
                rightTable.seek(position, 0)#vai para a posição absoluta do início do respectivo registro em FK
                rightLine = rightTable.read(tg.registrySize)
                #print("Right line: " + rightLine)
                rightRecord = rightLine.split(tg.fieldSeparator)#na teoria, após o cálculo da posição absoluta, ...
                blocksFetched+=1 #... o sistema deve buscar o registro em memória, logo isso ocasiona um bloco a mais coletado de memória(ainda que com um só registro)
                rightTable.seek(0, 0)
                if leftRecord[0] == rightRecord[1]:
                    matches += 1
                    joined = leftRecord + rightRecord
                    
                else:
                    print("ERRO FATAL. SEMPRE DEVERIA HAVER UM MATCH.")
                    return 
    end = dt.datetime.now()   
        
    print("Registros com match: " + str(matches))
    print("Número total de blocos acessados: " + str(blocksFetched))
    print("Tempo de execução: " + str((end-start).total_seconds()) + "s")






def main():
    #JoinNestedLoop()
    #MergeJoin()
    HashJoin()
    
main()