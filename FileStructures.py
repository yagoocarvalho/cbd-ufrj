# -*- coding: utf-8 -*-
"""
Created on Sun Oct  7 14:48:57 2018

@author: BRC
"""

import os
import DBHelperFunctions as aux



###################################################################################
######################## DB INITIALIZATION FUNCTIONS ##############################
###################################################################################


#Le o CSV e cria o arquivo do BD de Heap
def CreateHeapBD(csvFilePath):
    #Lê do CSV e preenche os registros com enchimento para criar o tamanho fixo
    valuesToLoad = aux.PadRegistries(aux.ReadFromFile(csvFilePath))
    
    #apaga o conteúdo existente no momento(se houver)
    if os.path.exists(aux.HeapPath):
        os.remove(aux.HeapPath)
    
    #make HEAD File
    aux.MakeHEAD(aux.HeapHeadPath, "Heap", 0)
    #preenche os valores direto no arquivo
    #file = open(aux.HeapPath, "w+")
    #file.write(aux.MakeHEADString("HEAP"))
    #file.close()
    
    registryCounter = 0
    #inserimos valor a valor com a função de inserção da Heap
    for row in valuesToLoad:
        HeapInsertSingleRecord(row)
        registryCounter +=1
    
    aux.UpdateHEADFile(aux.HeapHeadPath, "HEAP", registryCounter)
    






###################################################################################
##################################### HEAP ########################################
###################################################################################

###################################################################################
############################ HEAP - SELECT FUNCTIONS ##############################
###################################################################################

#colName = Desired column of the query (SEE LISTS ABOVE FOR COL NAMES)
#value = desired value
#SQL Format: Select * from HeapTable WHERE colName = value
#singleRecordSelection = Retorna o PRIMEIRO registro onde 'colName' = à value se True
def HeapSelectRecord(colName, value, singleRecordSelection = False, valueIsArray = False, secondColName = "", secondValue = ""):
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

    print("\nRunning query: ")
    if singleRecordSelection:
        if valueIsArray:
            print("\nSELECT * FROM TB_HEAP WHERE " + colName + " in (" + values + ") LIMIT 1;\n\n")
        else:
            if secondValuePresent:
                print("\nSELECT * FROM TB_HEAP WHERE " + colName + " = " + value + " AND " + secondColName + "=" + secondValue + " LIMIT 1;\n\n")
            else:
                print("\nSELECT * FROM TB_HEAP WHERE " + colName + " = " + value + " LIMIT 1;\n\n")
    else:
        if valueIsArray:
            print("\nSELECT * FROM TB_HEAP WHERE " + colName + " in (" + values + ");\n\n")
        else:
            if secondValuePresent:
                print("\nSELECT * FROM TB_HEAP WHERE " + colName + " = " + value + " AND " + secondColName + "=" + secondValue + ";\n\n")
            else:
                print("\nSELECT * FROM TB_HEAP WHERE " + colName + " = " + value + ";\n\n")

    currentRegistry= 0#busca linear, sempre começamos do primeiro
    results = []
    while not (registryFound or endOfFile):
        currentBlock = aux.FetchBlock(aux.HeapPath, currentRegistry)#pega 5 registros a partir do registro atual
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
        if valueIsArray:
            print("Não foi encontrado registro com "+colName+ " in (" + values +")")
        else:
            print("Não foi encontrado registro com valor " +colName+ " = " + value)
        
    else:
        print("Results found: \n")
        for result in results:
            print(result)
            print("\n")
        
    print("End of search.")
    print("Number of blocks fetched: " + str(numberOfBlocksUsed))







#DONE
###################################################################################
############################ HEAP - INSERT FUNCTIONS ##############################
###################################################################################

#insere um valor novo na Heap(ou seja, no final dela)
def HeapInsertSingleRecord(listOfValues):
    if len(listOfValues) != len(aux.maxColSizesList):
        print("Erro: lista de valores recebidos não tem a mesma quantidade de campos da relação")
        return
    with open(aux.HeapPath, 'a') as file:
        #insere o CPF com seu proprio padding
        file.write(aux.FillCPF(listOfValues[0]))
        #assumindo que estão na ordem correta já
        for i in range(1, len(listOfValues)):
            file.write(aux.PadString(listOfValues[i], aux.maxColSizesList[i]))
        #por fim pulamos uma linha para o próximo registro
        file.write("\n")
    aux.UpdateHEADFile(aux.HeapHeadPath, "Heap", aux.GetNumRegistries(aux.HeapHeadPath, aux.heapHeadSize)+1)


def HeapMassInsertCSV(csvFilePath):
    #Lê do CSV e preenche os registros com enchimento para criar o tamanho fixo
    valuesToLoad = aux.PadRegistries(aux.ReadFromFile(csvFilePath))
    
    registryCounter = aux.GetNumRegistries(aux.HeapHeadPath, aux.heapHeadSize)
    #inserimos valor a valor com a função de inserção da Heap
    for row in valuesToLoad:
        HeapInsertSingleRecord(row)
        registryCounter +=1
    
    aux.UpdateHEADFile(aux.HeapHeadPath, "HEAP", registryCounter)


###################################################################################
############################ HEAP - DELETE FUNCTIONS ##############################
###################################################################################

#colName = Desired column of the query (SEE LISTS ABOVE FOR COL NAMES)
#value = desired value
#SQL Format: Select * from HeapTable WHERE colName = value
#singleRecordDeletion = Retorna o PRIMEIRO registro onde 'colName' = à value se True
def HeapDeleteRecord(colName, value, singleRecordDeletion = False, valueIsArray = False, secondColName = "", secondValue = ""):
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

    print("\nRunning query: ")
    if singleRecordDeletion:
        if valueIsArray:
            print("\nDELETE FROM TB_HEAP WHERE " + colName + " in (" + values + ") LIMIT 1;\n\n")
        else:
            if secondValuePresent:
                print("\nDELETE FROM TB_HEAP WHERE " + colName + " = " + value + " AND " + secondColName + "=" + secondValue + " LIMIT 1;\n\n")
            else:
                print("\nDELETE FROM TB_HEAP WHERE " + colName + " = " + value + " LIMIT 1;\n\n")
    else:
        if valueIsArray:
            print("\nDELETE FROM TB_HEAP WHERE " + colName + " in (" + values + ");\n\n")
        else:
            if secondValuePresent:
                print("\nDELETE FROM TB_HEAP WHERE " + colName + " = " + value + " AND " + secondColName + "=" + secondValue + ";\n\n")
            else:
                print("\nDELETE FROM TB_HEAP WHERE " + colName + " = " + value + ";\n\n")

    currentRegistry= 0#busca linear, sempre começamos do primeiro
    results = [] #retornar os deletados
    while not (registryFound or endOfFile):
        currentBlock = aux.FetchBlock(aux.HeapPath, currentRegistry)#pega 5 registros a partir do registro atual
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
                    aux.DeleteLineFromFile(currentRegistry+i, aux.HeapPath)
                    registryFound = True
                    break
        #se não é EOF e não encontrou registro, repete operação com outro bloco
        currentRegistry +=aux.blockSize
        
    if results == []:
        if valueIsArray:
            print("Não foi encontrado registro com "+colName+ " in (" + values +")")
        else:
            print("Não foi encontrado registro com valor " +colName+ " = " + value)
        
    else:
        print(indexesToDelete)
        
        for reg in reversed(indexesToDelete):
            aux.DeleteLineFromFile(reg, aux.HeapPath)
        print("\n\nRegistries deleted: \n")
        for result in results:
            print(result)
            print("\n")
    
    print("End of query.")
    print("Number of blocks fetched: " + str(numberOfBlocksUsed))

    #updateHEAD with new number of registries if there were deletions
    if results != []:
        aux.UpdateHEADFile(aux.HeapHeadPath, "Heap", aux.GetNumRegistries(aux.HeapHeadPath, aux.heapHeadSize)-len(results))
    





###################################################################################
################################### ORDERED #######################################
###################################################################################
from dateutil import parser
import math

# Campo nao chave para ordenacao
numColToOrder = 0
isOrderedByPrimaryKey = True


#Le o CSV e cria o arquivo do BD Ordenado
def CreateOrderedBD(csvFilePath, isOrderedByPrimaryKey):
    #Lê do CSV e preenche os registros com enchimento para criar o tamanho fixo
    valuesToLoad = aux.PadRegistries(aux.ReadFromFile(csvFilePath))
    valuesToLoad = sortList(valuesToLoad, isOrderedByPrimaryKey)
    #apaga o conteúdo existente no momento(se houver)
    if os.path.exists(aux.OrderedPath):
        os.remove(aux.OrderedPath)
    
    #preenche os valores direto no arquivo
    file = open(aux.OrderedPath, "w+")
    file.write(aux.MakeHEADString("Ordered", len(valuesToLoad)))
    for row in valuesToLoad:
        for cols in row:
            file.write(cols)
    
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
def sortList(values, isPrimaryKey):
    if (isPrimaryKey is False):
        return sorted(values, key=sortComparison)
    else:
        return sorted(values)


###################################################################################
########################## ORDERED - SELECT FUNCTIONS #############################
###################################################################################

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
        mid = math.ceil(l + (r - l)/2)
                
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
        
        # Varre cada registro do bloco procurando o valor
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
def OrderedSelectSingleRecord(colName, value):
    numberOfBlocksUsed = 0 #conta o número de vezes que "acessamos a memória do disco"
    
    if colName not in aux.colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex = aux.colHeadersList.index(colName) #pega o indice referente àquela coluna

    print("Running query: ")
    print("SELECT * FROM TB_ORDERED WHERE " + colName + " = " + value + ";")

    # Obtem o numero de blocos do BD
    numBlocks = math.ceil(aux.GetNumRegistries(aux.OrderedPath, 0)/aux.blockSize)
    
    # Verifica se o campo procurado eh equivalente ao campo pelo qual o banco foi ordenado
    # Caso seja, utilizar busca binaria
    if(columnIndex == numColToOrder):   
        blockFounded,numberOfBlocksUsed = binarySearch(numColToOrder, value,
                                                       numBlocks, True)
    # Senao realizar select linear
    else:
        blockFounded = None #fazer select normal
        
    if(blockFounded):
        print("Registro encontrado: ")
        print(blockFounded)
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


###################################################################################
##################################### HASH ########################################
###################################################################################

#Le o CSV e cria o arquivo do BD de Hash
def CreateHashBD(csvFilePath):

    #Reads the csv file and create the records to be inserted, with fixed length
    valuesToLoad = aux.PadRegistries(aux.ReadFromFile(csvFilePath))
    
    # Delete previous database
    if os.path.exists(aux.HashPath):
        os.remove(aux.HashPath)
    
    # Create empty file to reserve disk space
    with open(aux.HashPath, 'wb') as hashFile:
        hashFile.seek((aux.bucketSize * aux.numberOfBuckets * aux.blockSize * (aux.registrySize -1)) - 1)
        hashFile.write(b'\0')
    
    # Create HEAD to File
    aux.MakeHEAD(aux.HashHeadPath, "Hash", 0)
    
    registryCounter = 0
    #inserimos valor a valor com a função de inserção do Hash
    for row in valuesToLoad:
        registry = Registry(row, False)
        HashInsertRecord(registry)
        registryCounter +=1


def MassHashInsert(csvFilePath):
    #Reads the csv file and create the records to be inserted, with fixed length
    valuesToLoad = aux.PadRegistries(aux.ReadFromFile(csvFilePath))

    registryCounter = 0
    #inserimos valor a valor com a função de inserção do Hash
    for row in valuesToLoad:
        registry = Registry(row, False)
        HashInsertRecord(registry)
        registryCounter +=1

def CalculateHashKey(key):
    return int(key)

def CalculateHashAddress(hashKey):
    return hashKey % aux.numberOfBuckets
    
def FetchBlockBytes(hashFile, startOffset):
    hashFile.seek(startOffset)
    return hashFile.read((aux.blockSize * (aux.registrySize -1)))

class Bucket:
    def __init__(self, hashFile, startOffset):
        self.blocksList = []
        for i in range(startOffset, startOffset + aux.bucketSize * aux.blockSize * (aux.registrySize -1) - 1, aux.blockSize * (aux.registrySize -1)):
            self.blocksList += [Block(FetchBlockBytes(hashFile, i))]
        self.firstBlockWithEmptyRecordIndex = self.__FirstBlockWithEmptyRecordIndex()

    def __FirstBlockWithEmptyRecordIndex(self):
        for i in range(len(self.blocksList)):
            if (self.blocksList[i].firstEmptyRecordIndex != -1):
                return i
        
        return -1

class Block:

    def __init__(self, registriesBytes):
        self.registriesList = []
        #iterate over registries bytes
        for b in range(0, len(registriesBytes), (aux.registrySize -1)):
            self.registriesList += [Registry(registriesBytes[b : b + (aux.registrySize -1)], True)]

        self.firstEmptyRecordIndex = self.__FirstEmptyRecordIndex()

    def SizeInBytes(self):
        sizeInBytes = 0
        for registry in self.registriesList:
            sizeInBytes += registry.sizeInBytes

        return sizeInBytes

    def __FirstEmptyRecordIndex(self):
        for i in range(len(self.registriesList)):
            try:
                if (self.registriesList[i].docNumber.index('\x00') >= 0):
                    return i
            except:
                pass
        return -1

    def __str__(self):
        str_block = ""
        for registry in self.registriesList:
            str_block += str(registry)
        
        return str_block

class Registry:

    def __init__(self, listOfValues, dataInBytes):
        if (not dataInBytes):
            self.docNumber        = listOfValues[0]
            self.state            = listOfValues[1]
            self.jobType          = listOfValues[2]
            self.candidateNumber  = listOfValues[3]
            self.candidateName    = listOfValues[4]
            self.candidateEmail   = listOfValues[5]
            self.partyNumber      = listOfValues[6]
            self.birthDate        = listOfValues[7]
            self.gender           = listOfValues[8]
            self.instructionLevel = listOfValues[9]
            self.maritalStatus    = listOfValues[10]
            self.colorRace        = listOfValues[11]
            self.ocupation        = listOfValues[12]
        else:
            listOfValues = listOfValues.decode("utf-8")
            self.docNumber        = listOfValues[0:11]
            self.state            = listOfValues[11:13]
            self.jobType          = listOfValues[13:15]
            self.candidateNumber  = listOfValues[15:20]
            self.candidateName    = listOfValues[20:90]
            self.candidateEmail   = listOfValues[90:133]
            self.partyNumber      = listOfValues[133:135]
            self.birthDate        = listOfValues[135:145]
            self.gender           = listOfValues[145:146]
            self.instructionLevel = listOfValues[146:147]
            self.maritalStatus    = listOfValues[147:148]
            self.colorRace        = listOfValues[148:150]
            self.ocupation        = listOfValues[150:153]

        self.sizeInBytes = len(str(self))
    
    def __str__(self):
        return self.docNumber + self.state + self.jobType + self.candidateNumber + self.candidateName + self.candidateEmail + self.partyNumber + self.birthDate + self.gender + self.instructionLevel + self.maritalStatus + self.colorRace + self.ocupation

    def Clear(self):
        self.docNumber        = '\x00' * 11
        self.state            = '\x00' * 2
        self.jobType          = '\x00' * 2
        self.candidateNumber  = '\x00' * 5
        self.candidateName    = '\x00' * 70
        self.candidateEmail   = '\x00' * 43
        self.partyNumber      = '\x00' * 2
        self.birthDate        = '\x00' * 10
        self.gender           = '\x00' * 1
        self.instructionLevel = '\x00' * 1
        self.maritalStatus    = '\x00' * 1
        self.colorRace        = '\x00' * 2
        self.ocupation        = '\x00' * 3

        self.sizeInBytes = len(str(self))


###################################################################################
############################ HASH - SELECT FUNCTIONS ##############################
###################################################################################

def HashSelectRecord(searchKeys, goodSearchKeys):
    registryList = []
    for searchKey in searchKeys:
        freeBlockIndex = -1
        blocksVisitedCount = 0
        #calculate hash key and address
        hashKey     = CalculateHashKey(searchKey)
        hashAddress = CalculateHashAddress(hashKey)

        # Init the start offset
        startingOffset = hashAddress * aux.bucketSize * aux.blockSize * (aux.registrySize - 1)

        # Place the record the first block with enough space starting from the file
        with open(aux.HashPath, 'r+b') as hashFile:
            while freeBlockIndex == -1:
                # Load the bucket
                currentBucket = Bucket(hashFile, startingOffset)
                freeBlockIndex = currentBucket.firstBlockWithEmptyRecordIndex
                foundRegistry = False
                # Search for the key in the registries in the bucket
                for block in currentBucket.blocksList:
                    blocksVisitedCount += 1
                    for registry in block.registriesList:
                        if (registry.docNumber == searchKey):
                            registryList += [registry]
                            foundRegistry = True
                            print("Blocks visited for key {}: {}".format(searchKey, blocksVisitedCount))
                            if (freeBlockIndex == -1):
                                freeBlockIndex = 0
                            break
                    
                    if (foundRegistry):
                        break

                if (not foundRegistry):
                    # if registry was not found and the bucket is full, it may have occured overflow, so we search in the next bucket
                    if (freeBlockIndex == -1):
                        startingOffset += aux.bucketSize * aux.blockSize * (aux.registrySize - 1)
                        pass
                    # else, print an error and continue
                    else:
                        print("Record {} not found".format(searchKey))
                        print("Blocks visited for key {}: {}".format(searchKey, blocksVisitedCount))
                        pass

    return registryList

    






#colName = Desired column of the query (SEE LISTS ABOVE FOR COL NAMES)
#value = desired value
#SQL Format: Select * from HeapTable WHERE colName = value
#singleRecordSelection = Retorna o PRIMEIRO registro onde 'colName' = à value se True
def HashLinearSelectRecord(colName, value, customRegistrySize, singleRecordSelection = False, valueIsArray = False, secondColName = "", secondValue = ""):
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

    print("\nRunning query: ")
    if singleRecordSelection:
        if valueIsArray:
            print("\nSELECT * FROM TB_HEAP WHERE " + colName + " in (" + values + ") LIMIT 1;\n\n")
        else:
            if secondValuePresent:
                print("\nSELECT * FROM TB_HEAP WHERE " + colName + " = " + value + " AND " + secondColName + "=" + secondValue + " LIMIT 1;\n\n")
            else:
                print("\nSELECT * FROM TB_HEAP WHERE " + colName + " = " + value + " LIMIT 1;\n\n")
    else:
        if valueIsArray:
            print("\nSELECT * FROM TB_HEAP WHERE " + colName + " in (" + values + ");\n\n")
        else:
            if secondValuePresent:
                print("\nSELECT * FROM TB_HEAP WHERE " + colName + " = " + value + " AND " + secondColName + "=" + secondValue + ";\n\n")
            else:
                print("\nSELECT * FROM TB_HEAP WHERE " + colName + " = " + value + ";\n\n")

    currentRegistry= 0#busca linear, sempre começamos do primeiro
    results = []
    while not (registryFound or endOfFile):
        currentBlock = aux.FetchBlock2(aux.HashPath, currentRegistry, customRegistrySize)#pega 5 registros a partir do registro atual
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
        if valueIsArray:
            print("Não foi encontrado registro com "+colName+ " in (" + values +")")
        else:
            print("Não foi encontrado registro com valor " +colName+ " = " + value)
        
    else:
        print("Results found: \n")
        for result in results:
            print(result)
            print("\n")
        
    print("End of search.")
    print("Number of blocks fetched: " + str(numberOfBlocksUsed))
    return results

###################################################################################
############################ HASH - INSERT FUNCTIONS ##############################
###################################################################################

def HashInsertRecord(registry):
    freeBlockIndex = -1
    freeSpaceIndex = -1

    #calculate hash key and address
    hashKey     = CalculateHashKey(registry.docNumber)
    hashAddress = CalculateHashAddress(hashKey)

    # Init the start offset
    startingOffset = hashAddress * aux.bucketSize * aux.blockSize * (aux.registrySize - 1)

    # Place the record the first block with enough space starting from the file
    with open(aux.HashPath, 'r+b') as hashFile:
        while freeBlockIndex == -1:
            # Load the bucket
            currentBucket = Bucket(hashFile, startingOffset)
            freeBlockIndex = currentBucket.firstBlockWithEmptyRecordIndex
            # Check if there is a collision
            if (freeBlockIndex == -1):
                #If the collision happened a lot and the bucket is full, load the next bucket
                startingOffset += aux.bucketSize * aux.blockSize * (aux.registrySize - 1)
                pass
            else:
                # Select block
                currentBlock = currentBucket.blocksList[freeBlockIndex]

                # Set registry to rigth block
                freeSpaceIndex = currentBlock.firstEmptyRecordIndex
                currentBlock.registriesList[freeSpaceIndex] = registry
        
        # Re-write block to the file
        hashFile.seek(startingOffset + (freeBlockIndex * aux.blockSize * (aux.registrySize - 1)))
        hashFile.write(str(currentBlock).encode("utf-8"))
        
        
###################################################################################
############################ HASH - DELETE FUNCTIONS ##############################
###################################################################################

def HashDeleteRecord(searchKeys, goodSearchKeys):
    for searchKey in searchKeys:
        freeBlockIndex = -1
        blocksVisitedCount = 0
        #calculate hash key and address
        hashKey     = CalculateHashKey(searchKey)
        hashAddress = CalculateHashAddress(hashKey)

        # Init the start offset
        startingOffset = hashAddress * aux.bucketSize * aux.blockSize * (aux.registrySize - 1)

        # Place the record the first block with enough space starting from the file
        with open(aux.HashPath, 'r+b') as hashFile:
            while freeBlockIndex == -1:
                # Load the bucket
                currentBucket = Bucket(hashFile, startingOffset)
                freeBlockIndex = currentBucket.firstBlockWithEmptyRecordIndex
                foundRegistry = False
                # Search for the key in the registries in the bucket
                for i in range(len(currentBucket.blocksList)):
                    block = currentBucket.blocksList[i]
                    blocksVisitedCount += 1
                    for registry in block.registriesList:
                        if (registry.docNumber == searchKey):
                            registry.Clear()
                            foundRegistry = True
                            # Re-write block to the file
                            hashFile.seek(startingOffset + (i * aux.blockSize * (aux.registrySize - 1)))
                            hashFile.write(str(block).encode("utf-8"))
                            print("Blocks visited for key {}: {}".format(searchKey, blocksVisitedCount))
                            if (freeBlockIndex == -1):
                                freeBlockIndex = 0
                            break
                    
                    if (foundRegistry):
                        break

                if (not foundRegistry):
                    # if registry was not found and the bucket is full, it may have occured overflow, so we search in the next bucket
                    if (freeBlockIndex == -1):
                        startingOffset += aux.bucketSize * aux.blockSize * (aux.registrySize - 1)
                        pass
                    # else, print an error and continue
                    else:
                        print("Record {} not found".format(searchKey))
                        print("Blocks visited for key {}: {}".format(searchKey, blocksVisitedCount))
                        pass

###################################################################################
################################### MAIN ##########################################
###################################################################################

CreateHashBD(aux.RJPath)
MassHashInsert(aux.MGPath)
MassHashInsert(aux.SPPath)




#CreateOrderedBD(aux.RJPath, False)

#print('-----')

#blocks = binarySearch(1)

#query = "86551337791"
#p = [block if "09650185712" in block[numColToOrder] else None for block in blocks ]
#q = next((block for block in blocks if query in block[numColToOrder]), None)

#print(q)

#0, len(arr)-1,
#result = binarySearch(numColToOrder, query, 4)
#print (result)

#OrderedSelectSingleRecord('NM_EMAIL', query)
#OrderedSelectSingleRecord(aux.colHeadersList[numColToOrder], query)

