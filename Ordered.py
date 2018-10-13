#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 12 21:10:36 2018

@author: eduardo
"""

import os
import fileinput
import DBHelperFunctions as aux


###################################################################################
################################# AUX FUNCTIONS ###################################
###################################################################################

#colName = Desired column of the query (SEE LISTS ABOVE FOR COL NAMES)
#value = desired value
#SQL Format: Select * from Table WHERE colName = value
#singleRecordSelection = Retorna o PRIMEIRO registro onde 'colName' = à value se True
def LinearSelectRecord(colName, value, singleRecordSelection = False, valueIsArray = False, secondColName = "", secondValue = ""):
    numberOfBlocksUsed = 0 #conta o número de vezes que "acessamos a memória do disco"
    registryFound = False
    endOfFile = False
    
    values = ""
    if valueIsArray:
        for val in value:
            values+= val + ", "
        values = values[:len(values)-2]#tira ultima ', '
    
    if colName not in aux.colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex = aux.colHeadersList.index(colName) #pega o indice referente àquela coluna

    secondValuePresent = False

    secondColumnIndex = -1
    if secondColName != "" and secondValue != "":
        if secondColName not in aux.colHeadersList:
            print("Error: Second column name not found in relation")
            return
        secondColumnIndex = aux.colHeadersList.index(secondColName)
        secondValuePresent = True

    currentRegistry= 0#busca linear, sempre começamos do primeiro
    results = []
    while not (registryFound or endOfFile):
        currentBlock = aux.FetchBlock(aux.OrderedPath, currentRegistry)#pega 5 registros a partir do registro atual
        if currentBlock == []:
            endOfFile = True
            break
        
        #mais um bloco varrido
        numberOfBlocksUsed +=1
                      
        for i in range(len(currentBlock)):
            if (not valueIsArray and ((not secondValuePresent and currentBlock[i][columnIndex] == value) or (secondValuePresent and currentBlock[i][columnIndex]==value and currentBlock[i][secondColumnIndex]==secondValue) ) ) or (valueIsArray and currentBlock[i][columnIndex] in value):
                print("Result found in registry " + str(currentRegistry+i) + "!")
                results += [currentBlock[i]]
                if singleRecordSelection:
                    registryFound = True
                    break
        #se não é EOF e não encontrou registro, repete operação com outro bloco
        currentRegistry +=aux.blockSize
        
    if results == []:
        return -1, numberOfBlocksUsed      
    else:
        return results, numberOfBlocksUsed



#colName = Desired column of the query (SEE LISTS ABOVE FOR COL NAMES)
#value = desired value
#SQL Format: Select * from Table WHERE colName = value
#singleRecordDeletion = Retorna o PRIMEIRO registro onde 'colName' = à value se True
def DeleteRecord(colName, value, singleRecordDeletion = False, valueIsArray = False, secondColName = "", secondValue = ""):
    numberOfBlocksUsed = 0 #conta o número de vezes que "acessamos a memória do disco"
    registryFound = False
    endOfFile = False
    
    indexesToDelete = []
    
    values = ""
    if valueIsArray:
        for val in value:
            values+= val + ", "
        values = values[:len(values)-2]#tira ultima ', '
    
    if colName not in aux.colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex = aux.colHeadersList.index(colName) #pega o indice referente àquela coluna

    secondValuePresent = False


    secondColumnIndex = -1
    if secondColName != "" and secondValue != "":
        if secondColName not in aux.colHeadersList:
            print("Error: Second column name not found in relation")
            return
        secondColumnIndex = aux.colHeadersList.index(secondColName)
        secondValuePresent = True

    currentRegistry= 0#busca linear, sempre começamos do primeiro
    results = [] #retornar os deletados
    while not (registryFound or endOfFile):
        currentBlock = aux.FetchBlock(aux.OrderedPath, currentRegistry)#pega 5 registros a partir do registro atual
        if currentBlock == []:
            endOfFile = True
            break
        
        #mais um bloco varrido
        numberOfBlocksUsed +=1
                      
        for i in range(len(currentBlock)):
            if (not valueIsArray and ((not secondValuePresent and currentBlock[i][columnIndex] == value) or (secondValuePresent and currentBlock[i][columnIndex]==value and currentBlock[i][secondColumnIndex]==secondValue) ) ) or (valueIsArray and currentBlock[i][columnIndex] in value):
                print("Result found in registry " + str(currentRegistry+i) + "!")
                results += [currentBlock[i]]
                #salvar index para deletar posteriormente
                indexesToDelete+=[currentRegistry+i]

                if singleRecordDeletion:
                    aux.DeleteLineFromFile(currentRegistry+i, aux.OrderedPath)
                    registryFound = True
                    break
        #se não é EOF e não encontrou registro, repete operação com outro bloco
        currentRegistry +=aux.blockSize
        
    if results == []:
        return -1, numberOfBlocksUsed
        
    else:
        for reg in reversed(indexesToDelete):
            aux.DeleteLineFromFile(reg, aux.OrderedPath)
    
    print("End of query.")
    print("Number of blocks fetched: " + str(numberOfBlocksUsed))

    #updateHEAD with new number of registries if there were deletions
    if results != []:
        aux.UpdateHEADFile(aux.OrderedHeadPath, "Ordered", aux.GetNumRegistries(aux.OrderedHeadPath, aux.heapHeadSize-1)-len(results))
    
    return results, numberOfBlocksUsed

###################################################################################
################################### ORDERED #######################################
###################################################################################
from dateutil import parser
import math

# Campo nao chave para ordenacao
numColToOrder = 5



#Le o CSV e cria o arquivo do BD Ordenado
def CreateOrderedBD(csvFilePath):
    #Lê do CSV e preenche os registros com enchimento para criar o tamanho fixo
    valuesToLoad = aux.PadRegistries(aux.ReadFromFile(csvFilePath))
    valuesToLoad = sortList(valuesToLoad)
    #apaga o conteúdo existente no momento(se houver)
    if os.path.exists(aux.OrderedPath):
        os.remove(aux.OrderedPath)
    
 
    # Cria Head do bd
    aux.MakeHEAD(aux.OrderedHeadPath, "Ordered", len(valuesToLoad))

    #preenche os valores direto no arquivo
    file = open(aux.OrderedPath, "w+")
    for row in valuesToLoad:
        #insere o CPF com seu proprio padding
        file.write(aux.FillCPF(row[0]))
        #assumindo que estão na ordem correta já
        for i in range(1, len(row)):
            file.write(aux.PadString(row[i], aux.maxColSizesList[i]))
        #por fim pulamos uma linha para o próximo registro
        file.write("\n")

    
    file.close()


# Funcao para auxiliar na ordenacao
def sortComparison(elem):
    # Se o campo for de data, converte-lo para este formato
    if(numColToOrder == 7):
        return parser.parse(elem[numColToOrder])
    #Outros campos
    else:
        return elem[numColToOrder]

# Ordena um array de registros
def sortList(values):
    return sorted(values, key=sortComparison)


###################################################################################
########################## ORDERED - SELECT FUNCTIONS #############################
###################################################################################


#########################  OrderedSelectSingleRecord ##############################
# TODO consertar busca em data
# Retorna o bloco se o achar, ou -1, caso contrario; e o numero de blocos utilizados
def binarySearch(columnIndex, value, maxNumBlocks, singleRecordSelection = False):
    #conta o número de vezes que "acessamos a memória do disco"
    numberOfBlocksUsed = []
    
    # blocos encontrados na query    
    foundedBlocks=[]
    
    # blocos acessados durante a query
    accessedBlocks=[]
    
    # intervalo de procura dos blocos
    l = 0
    r = maxNumBlocks
    while l <= r: 
        # Pega o numero do bloco do meio
        mid = math.ceil(l + (r - l)/2); 
                
        # 0-based
        # Busca o registro
        blockRegistries = aux.FetchBlock(aux.OrderedPath, (mid-1)*5)
        
        getNearBlocks(mid, foundedBlocks,columnIndex, value, accessedBlocks,
                      maxNumBlocks, numberOfBlocksUsed, singleRecordSelection)
#                    
        # Check if x is present at mid 
        if (foundedBlocks):         
            return foundedBlocks, len(numberOfBlocksUsed)
        else:
            # Se o valor é maior que o ultimo elemento, ignora a metade esquerda  
            if(value > blockRegistries[-1][columnIndex]):
                l = mid + 1
            # Se o valor é menor, ignorar metade direita
            else:
                r = mid - 1
                
    # Retorna -1 se não achar
    return -1, len(numberOfBlocksUsed)


def getNearBlocks(numberBlock, foundedBlocks,columnIndex, value, accessedBlocks,
                  maxNumBlocks, numberOfBlocksUsed, singleRecordSelection = False):
    
    if(numberBlock not in accessedBlocks):
        accessedBlocks.append(numberBlock)
        numberOfBlocksUsed.append('1')
        
        # array de indices de blocos encontrados
        indexesFoundedBlocks = []
        
        # recupera o bloco de registros
        blockRegistries = aux.FetchBlock(aux.OrderedPath, (numberBlock-1)*5)
        
        for idx, block in enumerate(blockRegistries):
            if value in block[columnIndex]:
                if(singleRecordSelection):
                    foundedBlocks.append(block)
                    return
                indexesFoundedBlocks.append(idx)
                foundedBlocks.append(block)
            
        # se contem o indice 0 é possivel que tenha mais registros no bloco anterior
        if( (0 in indexesFoundedBlocks) and numberBlock>1):
            getNearBlocks(numberBlock-1, foundedBlocks,
                columnIndex, value, accessedBlocks,maxNumBlocks, numberOfBlocksUsed)
    
        # se contem o indice 4 é possivel que tenha mais registros no proximo bloco
        if((4 in indexesFoundedBlocks) and numberBlock<maxNumBlocks):
            getNearBlocks(numberBlock+1, foundedBlocks, 
                    columnIndex, value, accessedBlocks, maxNumBlocks, numberOfBlocksUsed)

    


#colName = Desired column of the query (SEE LISTS ABOVE FOR COL NAMES)
#value = desired value
#SQL Format: Select * from OrderedTable WHERE colName = value
#Retorna o PRIMEIRO registro onde o colName tenha o valor igual à value
def OrderedSelectSingleRecord(colName, value, singleRecordSelection = True):
    numberOfBlocksUsed = 0 #conta o número de vezes que "acessamos a memória do disco"
    
    if colName not in aux.colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex = aux.colHeadersList.index(colName) #pega o indice referente àquela coluna

    print("Running query: ")
    print("SELECT * FROM TB_ORDERED WHERE " + colName + " = " + value + " limit 1;")

    # Obtem o numero de blocos do BD
    numBlocks = math.ceil(aux.GetNumRegistries(aux.OrderedHeadPath, aux.heapHeadSize-1)/aux.blockSize)
    
    # Verifica se o campo procurado eh equivalente ao campo pelo qual o banco foi ordenado
    # Caso seja, utilizar busca binaria
    if(columnIndex == numColToOrder):   
        blockFounded,numberOfBlocksUsed = binarySearch(columnIndex, value,
                                                       numBlocks, singleRecordSelection)
    # Senao realizar select linear
    else:
        blockFounded,numberOfBlocksUsed = LinearSelectRecord(colName, value, singleRecordSelection) #fazer select normal

    if(blockFounded):
        print("Registro encontrado: ")
        print(blockFounded)
    else:
        print("Registro não encontrado")

    print("Fim da busca.")
    print("Número de blocos varridos: " + str(numberOfBlocksUsed))
    

####################### SELECT - WHERE CAMPO IN (conjunto de valores) #############

#colName = Desired column of the query (SEE LISTS ABOVE FOR COL NAMES)
#values = multiple values
#SQL Format: Select * from OrderedTable WHERE colName in [value1,value2...]
#Retorna lita de registros em que colName tenha um dos valores da lista values
def OrderedSelectWithMultipleValues(colName, values):
    numberOfBlocksUsed = 0 #conta o número de vezes que "acessamos a memória do disco"
    totalBlocksFounded = []
    totalNumberOfBlocksUsed = 0 
    if colName not in aux.colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex = aux.colHeadersList.index(colName) #pega o indice referente àquela coluna

    print("Running query: ")
    print("SELECT * FROM TB_ORDERED WHERE " + colName + " in " + str(values) + ";")

    # Obtem o numero de blocos do BD
    numBlocks = math.ceil(aux.GetNumRegistries(aux.OrderedHeadPath, aux.heapHeadSize-1)/aux.blockSize)

    # Verifica se o campo procurado eh equivalente ao campo pelo qual o banco foi ordenado
    # Caso seja, utilizar busca binaria
    if(columnIndex == numColToOrder):
        for value in values:
            blocksFounded,numberOfBlocksUsed = binarySearch(columnIndex, value,
                                                       numBlocks, singleRecordSelection = False )
            totalBlocksFounded.append(blocksFounded)
            totalNumberOfBlocksUsed += numberOfBlocksUsed
    # Senao realizar select linear
    else:
        blocksFounded, numberOfBlocksUsed = LinearSelectRecord(aux.OrderedPath,colName, values, valueIsArray = True) #fazer select normal
        totalBlocksFounded.append(blocksFounded)
        totalNumberOfBlocksUsed += numberOfBlocksUsed

    if(len(totalBlocksFounded)):
        print("Registro(s) encontrado(s): ")
        print(totalBlocksFounded)
    else:
        print("Registro não encontrado")

    print("Fim da busca.")
    print("Número de blocos varridos: " + str(totalNumberOfBlocksUsed))


####################### SELECT…WHERE colName1=value1 AND colName2=value2 #############


# TODO consertar busca em data
# Retorna o bloco se o achar, ou -1, caso contrario; e o numero de blocos utilizados
def binarySearchWithTwoFields(columnIndex, value, maxNumBlocks, 
                              secondColIndex = "", secondValue = ""):
    #conta o número de vezes que "acessamos a memória do disco"
    numberOfBlocksUsed = []
    
    # blocos encontrados na query    
    foundedBlocks=[]
    
    # blocos acessados durante a query
    accessedBlocks=[]
    
    # intervalo de procura dos blocos
    l = 0
    r = maxNumBlocks
    while l <= r: 
        # Pega o numero do bloco do meio
        mid = math.ceil(l + (r - l)/2); 
                
        # 0-based
        # Busca o registro
        blockRegistries = aux.FetchBlock(aux.OrderedPath, (mid-1)*5)
        
        getNearBlocksWithTwoFields(mid, foundedBlocks,columnIndex, value, accessedBlocks,
                      maxNumBlocks, numberOfBlocksUsed,
                      secondColIndex,secondValue)
#                    
        # Check if x is present at mid 
        if (foundedBlocks):         
            return foundedBlocks, len(numberOfBlocksUsed)
        else:
            # Se o valor é maior que o ultimo elemento, ignora a metade esquerda  
            if(value > blockRegistries[-1][columnIndex]):
                l = mid + 1
            # Se o valor é menor, ignorar metade direita
            else:
                r = mid - 1
                
    # Retorna -1 se não achar
    return -1, len(numberOfBlocksUsed)


def getNearBlocksWithTwoFields(numberBlock, foundedBlocks,columnIndex, value, accessedBlocks,
                  maxNumBlocks, numberOfBlocksUsed,secondColIndex,
                  secondValue):
    
    if(numberBlock not in accessedBlocks):
        accessedBlocks.append(numberBlock)
        numberOfBlocksUsed.append('1')
        
        # array de indices de blocos encontrados
        indexesFoundedBlocks = []
        
        # recupera o bloco de registros
        blockRegistries = aux.FetchBlock(aux.OrderedPath, (numberBlock-1)*5)
        
        for idx, block in enumerate(blockRegistries):
            if value in block[columnIndex]:
                if(secondValue in block[secondColIndex]):
                    indexesFoundedBlocks.append(idx)
                    foundedBlocks.append(block)
            
        # se contem o indice 0 é possivel que tenha mais registros no bloco anterior
        if( (0 in indexesFoundedBlocks) and numberBlock>1):
            getNearBlocksWithTwoFields(numberBlock-1, foundedBlocks,
                columnIndex, value, accessedBlocks,maxNumBlocks, 
                numberOfBlocksUsed, secondColIndex, secondValue)
    
        # se contem o indice 4 é possivel que tenha mais registros no proximo bloco
        if((4 in indexesFoundedBlocks) and numberBlock<maxNumBlocks):
            getNearBlocksWithTwoFields(numberBlock+1, foundedBlocks, 
                    columnIndex, value, accessedBlocks, maxNumBlocks, 
                    numberOfBlocksUsed, secondColIndex, secondValue)

    


#colName = Desired column of the query (SEE LISTS ABOVE FOR COL NAMES)
#values = multiple values
#SQL Format: Select * from OrderedTable WHERE colName = value1 AND 
# secondColName = value2;
#Retorna lita de registros em que possuam os colNames e values correspondentes
def OrderedSelectWithTwoFields(colName, value, secondColName = "", secondValue = ""):
    numberOfBlocksUsed = 0 #conta o número de vezes que "acessamos a memória do disco"
    
    if colName not in aux.colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex1 = aux.colHeadersList.index(colName) #pega o indice referente àquela coluna

    if secondColName not in aux.colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex2 = aux.colHeadersList.index(secondColName) #pega o indice referente àquela coluna

    print("Running query: ")
    print("SELECT * FROM TB_ORDERED WHERE " + colName + " = " + str(value) + 
          " AND " + str(secondColName) +" = " + str(secondValue) +  ";")

    # Obtem o numero de blocos do BD
    numBlocks = math.ceil(aux.GetNumRegistries(aux.OrderedHeadPath, aux.heapHeadSize-1)/aux.blockSize)

    # Verifica se o campo procurado eh equivalente ao campo pelo qual o banco foi ordenado
    # Caso seja, utilizar busca binaria
    if(columnIndex1 == numColToOrder):
        blocksFounded,numberOfBlocksUsed = binarySearchWithTwoFields(columnIndex1, value,
              numBlocks, columnIndex2, secondValue)
    elif(columnIndex2 == numColToOrder):
        blocksFounded,numberOfBlocksUsed = binarySearchWithTwoFields(columnIndex2, secondValue,
              numBlocks, columnIndex1, value)
        
    # Senao realizar select linear
    else:
        blocksFounded, numberOfBlocksUsed = LinearSelectRecord(
                colName, value, False, False, secondColName, secondValue) #fazer select normal

    if(blocksFounded and blocksFounded != -1):
        print("Registro(s) encontrado(s): ")
        print(blocksFounded)
    else:
        print("Registro não encontrado")

    print("Fim da busca.")
    print("Número de blocos varridos: " + str(numberOfBlocksUsed))



###################################################################################
################### SELECT - WHERE CAMPO Between v_inicial and v_final ############
###################################################################################

# Retorna o bloco se o achar, ou -1, caso contrario; e o numero de blocos utilizados
def binarySearchBetween(columnIndex, firstValue, maxNumBlocks, secondValue = ""):
    #conta o número de vezes que "acessamos a memória do disco"
    numberOfBlocksUsed = []
    
    # blocos encontrados na query    
    foundedBlocks=[]
    
    # blocos acessados durante a query
    accessedBlocks=[]
    
    # intervalo de procura dos blocos
    l = 0
    r = maxNumBlocks
    while l <= r: 
        # Pega o numero do bloco do meio
        mid = math.ceil(l + (r - l)/2); 
                
        # 0-based
        # Busca o registro
        blockRegistries = aux.FetchBlock(aux.OrderedPath, (mid-1)*5)
        
        getNearBlocksBetween(mid, foundedBlocks,columnIndex, firstValue, accessedBlocks,
                      maxNumBlocks, numberOfBlocksUsed,
                      secondValue)
#                    
        # Check if x is present at mid 
        if (foundedBlocks):         
            return foundedBlocks, len(numberOfBlocksUsed)
        else:
            # Se o valor menor é maior que o ultimo elemento, ignora a metade esquerda  
            if(firstValue > blockRegistries[-1][columnIndex]):
                l = mid + 1
            # Se o valor maior é menor que o ultimo elemento, ignorar metade direita
            elif (secondValue < blockRegistries[0][columnIndex]):
                r = mid - 1
            else:
                break
                
    # Retorna -1 se não achar
    return -1, len(numberOfBlocksUsed)


def getNearBlocksBetween(numberBlock, foundedBlocks,columnIndex, value, accessedBlocks,
                  maxNumBlocks, numberOfBlocksUsed,secondValue):
    
    if(numberBlock not in accessedBlocks):
        accessedBlocks.append(numberBlock)
        numberOfBlocksUsed.append('1')
        
        # array de indices de blocos encontrados
        indexesFoundedBlocks = []
        
        # recupera o bloco de registros
        blockRegistries = aux.FetchBlock(aux.OrderedPath, (numberBlock-1)*5)
        
        for idx, block in enumerate(blockRegistries):
            if block[columnIndex] >= value :
                if(block[columnIndex]) <= secondValue:
                    indexesFoundedBlocks.append(idx)
                    foundedBlocks.append(block)
            
        # se contem o indice 0 é possivel que tenha mais registros no bloco anterior
        if( (0 in indexesFoundedBlocks) and numberBlock>1):
            getNearBlocksBetween(numberBlock-1, foundedBlocks,
                columnIndex, value, accessedBlocks,maxNumBlocks, numberOfBlocksUsed,
                secondValue)
    
        # se contem o indice 4 é possivel que tenha mais registros no proximo bloco
        if((4 in indexesFoundedBlocks) and numberBlock<maxNumBlocks):
            getNearBlocksBetween(numberBlock+1, foundedBlocks, 
                    columnIndex, value, accessedBlocks, maxNumBlocks, numberOfBlocksUsed,
                    secondValue)

    


#colName = Desired column of the query (SEE LISTS ABOVE FOR COL NAMES)
#values = multiple values
#SQL Format: Select * from OrderedTable WHERE colName in [value1,value2...]
#Retorna lita de registros em que colName tenha um dos valores da lista values
def OrderedSelectBetweenTwoValues(colName, firstValue, secondValue):
    numberOfBlocksUsed = 0 #conta o número de vezes que "acessamos a memória do disco"
    
    if colName not in aux.colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex1 = aux.colHeadersList.index(colName) #pega o indice referente àquela coluna


    print("Running query: ")
    print("SELECT * FROM TB_ORDERED WHERE " + colName + " BETWEEN " + str(firstValue) + 
          " AND " + str(secondValue) +  ";")

    # Obtem o numero de blocos do BD
    numBlocks = math.ceil(aux.GetNumRegistries(aux.OrderedHeadPath, aux.heapHeadSize-1)/aux.blockSize)

    # Verifica se o campo procurado eh equivalente ao campo pelo qual o banco foi ordenado
    # Caso seja, utilizar busca binaria
    if(columnIndex1 == numColToOrder):
        blocksFounded,numberOfBlocksUsed = binarySearchBetween(columnIndex1, firstValue,
              numBlocks, secondValue)
        
    # Senao realizar select linear
    else:
        #FALTANDO implementação na heap
        blocksFounded = None

    if(blocksFounded and blocksFounded != -1):
        print("Registro(s) encontrado(s): ")
        print(blocksFounded)
    else:
        print("Registro não encontrado")

    print("Fim da busca.")
    print("Número de blocos varridos: " + str(numberOfBlocksUsed))


###################################################################################
########################## ORDERED - INSERT FUNCTIONS #############################
###################################################################################




###################################################################################
########################## ORDERED - DELETE FUNCTIONS #############################
###################################################################################
# Method to insert a given record into the file. The record will be inserted at the position/line specified(0-based)
def InsertLineIntoFile(record, location, filepath):
    # Open the file
    for line in fileinput.input(filepath, inplace=1):
        # Check line number
        linenum = fileinput.lineno()
        # If we are in our desired location, append the new record to the current one. Else, just remove the line-ending character
        if linenum == location:
            line = line + record
        else:
            line = line.rstrip()
        # write line in the output file
        print(line)


# Method to delete a record from the file. (0-based)
def DeleteLineFromFile(registry, filepath):
    # Open the file
    for line in fileinput.FileInput(filepath, inplace=1):
        # Check line number
        registryLine = [aux.CleanRegistry(line)]
        # If we are in our desired location, append the new record to the current one. Else, just remove the line-ending character
        if registry == registryLine:
            continue
        else:
            line = line.rstrip()
            # write line in the output file
            print(line)
            

#singleRecordDeletion = Retorna o PRIMEIRO registro onde 'colName' = à value se True
def OrderdDeleteSingleRecord(colName, value, singleRecordSelection = True):
    if colName not in aux.colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex = aux.colHeadersList.index(colName) #pega o indice referente àquela coluna

    print("Running query: ")
    print("Delete * FROM TB_ORDERED WHERE " + colName + " = " + str(value) + " limit 1;")

    # Obtem o numero de blocos do BD
    numBlocks = math.ceil(aux.GetNumRegistries(aux.OrderedHeadPath, aux.heapHeadSize-1)/aux.blockSize)

    # Verifica se o campo procurado eh equivalente ao campo pelo qual o banco foi ordenado
    # Caso seja, utilizar busca binaria
    if(columnIndex == numColToOrder):   
        blockFounded,numberOfBlocksUsed = binarySearch(columnIndex, value,
                                                       numBlocks, singleRecordSelection)
    # Senao realizar select linear
    else:
        blockFounded,numberOfBlocksUsed = DeleteRecord(colName, value, singleRecordSelection) #fazer select normal

    if(blockFounded):
        print("Registro encontrado: ")
        print(blockFounded)
        DeleteLineFromFile(blockFounded, aux.OrderedPath)
        print("Registro Deletado")
        
    else:
        print("Registro não encontrado")
        
    print("Número de blocos varridos: " + str(numberOfBlocksUsed))



###################################################################################
################################### MAIN ##########################################
###################################################################################


CreateOrderedBD(aux.RJPath)
#numColToOrder = 5

#print('-----')

#blocks = binarySearch(1)

#query = ["86551337791","01093643765"]
#query = ["RJ","SP"]
query = "CELIASANCHES134@GMAIL.COM"
#p = [block if "09650185712" in block[numColToOrder] else None for block in blocks ]
#q = next((block for block in blocks if query in block[numColToOrder]), None)

#print(q)

#0, len(arr)-1,
#result = binarySearch(numColToOrder, query, 4)
#print (result)

OrderedSelectSingleRecord('NM_EMAIL', query)
#OrderedSelectWithTwoFields(aux.colHeadersList[5], "GPM.GUIMARAES.ADV@GMAIL.COM", 
#                         aux.colHeadersList[0], "07833694762")
#OrderedSelectWithTwoFields(aux.colHeadersList[5], "GPM.GUIMARAES.ADV@GMAIL.COM", 
#                         aux.colHeadersList[4], "VINICIUS DE FREITAS DOS SANTOS")

#OrderedSelectBetweenTwoValues(aux.colHeadersList[numColToOrder],"01093643765","07833694762" )
#OrderedSelectBetweenTwoValues(aux.colHeadersList[numColToOrder],"ANDERSONMJFILHOS@HOTMAIL.COM",
#                            "CELIASANCHES134@GMAIL.COM" )

#OrderdDeleteSingleRecord(aux.colHeadersList[5], "PHS@GMAIL.COM")

#print("----")
#print(aux.FetchBlock(aux.OrderedPath,0 ))
#DeleteLineFromFile(2, OrderedPath)
