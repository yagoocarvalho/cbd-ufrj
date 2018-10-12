# -*- coding: utf-8 -*-
"""
Created on Sun Oct  7 14:48:57 2018

@author: BRC
"""

import os
import csv
import time
import datetime
import fileinput


###################################################################################
####################### AUXILIARY FUNCTIONS AND VARIABLES #########################
###################################################################################


#Caminho dos arquivos de candidatos
CandidatesFilePath = "data/consulta_cand_2018_"
SPPath = CandidatesFilePath + "SP.csv"
RJPath = CandidatesFilePath + "RJ.csv"
MGPath = CandidatesFilePath + "MG.csv"

#caminho dos arquivos 
BDFilePath = "BD/"
HeapPath = BDFilePath + "HeapBD.txt"
HeapHeadPath = BDFilePath + "HeapHEAD.txt"
OrderedPath = BDFilePath + "OrderedBD.txt"
OrderedHeadPath = BDFilePath + "OrderedHEAD.txt"
HashPath = BDFilePath + "HashBD.txt"
HashHeadPath = BDFilePath+ "HashHEAD.txt"

#caracter usado como enchimento de valores nao-cheios de um registro
paddingCharacter = "#"
#tamanho de um registro(medido em caracteres)
registrySize = 153+1 #153 chars + escape key


#Tamanho de um bloco de memoria (medido em registros)
blockSize = 5

#Tamanho do head do heap(em linhas)
heapHeadSize = 5

#Tamanho do head da lista ordenada(em linhas)
orderedHeadSize = 5

#Tamanho do head do hash(em linhas)
hashHeadSize = 5



#Tamanhos maximos de cada atributo(for reference mostly)
dicColunaTamanhoMax = {
	"K": 2,
	"N": 2,
	"Q": 5,
	"R": 70,
	"U": 11, #CPF, PK
	"V": 43,
	"AB": 2,
	"AM": 10,
	"AP": 1,
	"AR": 1,
	"AT": 1,
	"AV": 2, #vem com um 0 antes, aparentemente
	"AX": 3
}

dicColHeaderType = {
        "CPF": "INTEGER(11)",
        "SG_UF": "VARCHAR(2)",
        "CD_CARGO": "INTEGER(2)",
        'NR_CANDIDATO': "INTEGER(5)", 
        'NM_CANDIDATO': "VARCHAR(70)", 
        'NM_EMAIL': "VARCHAR(43)",
        'NR_PARTIDO': "INTEGER(2)", 
        'DT_NASCIMENTO': "DATE", 
        'CD_GENERO': "INTEGER(1)", 
        'CD_GRAU_INSTRUCAO': "INTEGER(1)", 
        'CD_ESTADO_CIVIL': "INTEGER(1)", 
        'CD_COR_RACA': "INTEGER(2)",
        'CD_OCUPACAO': "VARCHAR(3)"
}



#Baseado no dic acima(CPF JOGADO PARA A PRIMEIRA POSICAO)
maxColSizesList = [11,2,2,5,70,43,2,10,1,1,1,2,3]

#Baseado no dic acima(e na ordem da lista acima, com CPF no início)
colHeadersList = ["CPF", "SG_UF", "CD_CARGO", 'NR_CANDIDATO', 'NM_CANDIDATO', 'NM_EMAIL', 'NR_PARTIDO', 'DT_NASCIMENTO', 'CD_GENERO', 'CD_GRAU_INSTRUCAO', 'CD_ESTADO_CIVIL', 'CD_COR_RACA', 'CD_OCUPACAO']

#Baseado nos indices acima
relevantColsList = [10, 13, 16, 17, 20, 21, 27, 38, 41, 43, 45, 47, 49]

#retorna se e uma coluna relevante dentro do Excel(baseado nas colunas escolhidas acima)
def isRelevantRow(rowNumber):
    #
    return rowNumber in relevantColsList

#calcula o tamanho do registro novamente, caso necessario
def calculateRegistrySize():
    sum = 0
    for key, value in dicColunaTamanhoMax:
        sum+=value
    return sum


#Preenche com 0's a esquerda CPFs que nao possuem seu tamanho totalmente preenchido
def fillCPF(cpf):
    return cpf.zfill(maxColSizesList[0])#tamanho de CPF e fixo

#Completa a string com caracter escolhido para padding(p/ manter tamanho fixo)
def padString(stringToPad, totalSizeOfField):
    tmp = stringToPad
    for i in range (totalSizeOfField - len(stringToPad)):
        tmp+=paddingCharacter
    return tmp        


#Lê o arquivo desejado e retorna uma lista com todos os registros relevantes do mesmo
#lista retornada sera usada para construir nossos proprios arquivos
def readFromFile(csvFilePath):
    lineCount = 0
    registros = []
    with open(csvFilePath, 'r') as file:
        rows = csv.reader(file, delimiter = ";")
        for row in rows:
            if lineCount == 0 :#headers
                lineCount+=1
            else:
                finalRow = []
                
                for i in range(len(row)):
                    if isRelevantRow(i):
                        #Se for a coluna do CPF, coloca o mesmo no inicio da lista
                        if i == relevantColsList[4]:
                            finalRow.insert(0, fillCPF(row[i]))
                        else:
                            finalRow += [row[i]]
                print(finalRow)
                registros +=[finalRow]
                lineCount+=1
                if lineCount == 15: return registros #limita tamanho p/ testes
    return registros

#pega uma lista de registros(matriz bidimensional) e para cada elemento, preenche os espaços faltantes
def padRegistries(listOfRegistries):
    for i in range(len(listOfRegistries)):
        for j in range(len(listOfRegistries[i])):
            listOfRegistries[i][j] = padString(listOfRegistries[i][j], maxColSizesList[j])
    return listOfRegistries

#Retira o padding dos campos de um registro, e retorna o registro em formato de lista
#registryString = registro a ser limpo, em formato de string
def CleanRegistry(registryString):
    newRegistry = []
    offset = 0
    for i in range(len(maxColSizesList)):
        #print(registryString[offset:offset+maxColSizesList[i]])
        newRegistry += [registryString[offset:offset+maxColSizesList[i]].replace(paddingCharacter, "").replace("\n", "")]
        
        offset+=maxColSizesList[i]
    return newRegistry

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
def DeleteLineFromFile(location, filepath):
    # Open the file
    for line in fileinput.input(filepath, inplace=1):
        # Check line number
        linenum = fileinput.lineno()
        # If we are in our desired location, append the new record to the current one. Else, just remove the line-ending character
        if linenum == location+1:
            continue
        else:
            line = line.rstrip()
            # write line in the output file
            print(line)


def MakeHEAD(headType, numRegistries):
    string = "File structure: " + headType + "\n"
    string += "Creation: " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + "\n"
    string += "Last modification: " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + "\n"
    string += "Schema: "
    for key, value in dicColHeaderType.items():
        string += key + "-" + value + "|"
    string += "Number of registries: " + str(numRegistries) + "\n"
    
    return string


def MakeHEAD2(headPath, headType, numRegistries):
    if os.path.exists(headPath):
        os.remove(headPath)
    file = open(headPath, 'a')
    string = "File structure: " + headType + "\n"
    string += "Creation: " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + "\n"
    string += "Last modification: " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + "\n"
    string += "Schema: "
    for key, value in dicColHeaderType.items():
        string += key + "-" + value + "|"
    string += "Number of registries: " + str(numRegistries) + "\n"
    file.write(string)
    #return string


#Updates de HEAD File with new timestamp and current number of Registries
def UpdateHEADFile(headPath, headType, numRegistries):
    if os.path.exists(headPath):
        file = open(headPath, 'r')
    
        headContent = file.readlines()
        #print(headContent)
        headContent
        file.close()
        os.remove(headPath)
        
        #recria ela com as alteracoes
        file = open(headPath, 'a')
        file.write(headContent[0])
        file.write(headContent[1])
        file.write("Last modification: " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + "\n")
        file.write(headContent[3])
        file.write("Number of registries: " + str(numRegistries) + "\n")
    else:
        #Doesn't exist, create it
        MakeHEAD2(headPath, headType, numRegistries)


#Gets number of registries from HEAD file
def GetNumRegistries(DBHeadFilePath, headSize):
    #posição de início de leitura dos dados
    #cursorBegin = startingR
    with open(DBHeadFilePath, 'r') as file:
        for i in range(headSize-1):
            file.readline()
        return (int(file.readline().split("Number of registries: ")[1]))


#StartingRegistry = index do registro inicial a ser buscado (0-based)
def FetchBlock(DBFilePath, startingRegistry):
    #posicao de inicio de leitura dos dados
    #TODO
    #cursorBegin = startingR
    block = []
    with open(DBFilePath, 'r') as file:
        #Pula o HEAD(UPDATE: HEAD is in another cast....file)
        #for i in range(heapHeadSize):
        #    file.readline()#HEAD possui tamanho variável, então pulamos a linha inteira
            #Em termos de BD, seria o análogo à buscar o separador de registros, nesse caso, '\n'
        
        #Em seguida, move o ponteiro do arquivo para a posição correta(offset)
        for i in range(registrySize*startingRegistry):
            c = file.read(1) #vamos de 1 em 1 char para não jogar tudo de uma vez na memória
        
        #Após isso, faz um seek no número de blocos até preencher o bloco(ou acabar o arquivo)
        
        for i in range(blockSize):
            registry = ""
            for j in range(registrySize):
                c = file.read(1)
                #print(c)
                if c == "": 
                    #print("FIM DO ARQUIVO")
                    return block
                registry+=c
            #print("Current registry: "+registry)
            block += [CleanRegistry(registry)]
    return block




###################################################################################
######################## DB INITIALIZATION FUNCTIONS ##############################
###################################################################################


#Le o CSV e cria o arquivo do BD de Heap
def CreateHeapBD(csvFilePath):
    #Lê do CSV e preenche os registros com enchimento para criar o tamanho fixo
    valuesToLoad = padRegistries(readFromFile(csvFilePath))
    
    #apaga o conteúdo existente no momento(se houver)
    if os.path.exists(HeapPath):
        os.remove(HeapPath)
    
    #make HEAD File
    MakeHEAD2(HeapHeadPath, "Heap", 0)
    #preenche os valores direto no arquivo
    #file = open(HeapPath, "w+")
    #file.write(MakeHEAD("HEAP"))
    #file.close()
    
    registryCounter = 0
    #inserimos valor a valor com a função de inserção da Heap
    for row in valuesToLoad:
        HeapInsertSingleRecord(row)
        registryCounter +=1
    
    UpdateHEADFile(HeapHeadPath, "HEAP", registryCounter)





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
    
    if colName not in colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex = colHeadersList.index(colName) #pega o indice referente àquela coluna

    secondValuePresent = False


    secondColumnIndex = -1
    if secondColName != "" and secondValue != "":
        if secondColName not in colHeadersList:
            print("Error: Second column name not found in relation")
            return
        secondColumnIndex = colHeadersList.index(secondColName)
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
        currentBlock = FetchBlock(HeapPath, currentRegistry)#pega 5 registros a partir do registro atual
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
        currentRegistry +=blockSize
        
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
    if len(listOfValues) != len(maxColSizesList):
        print("Erro: lista de valores recebidos não tem a mesma quantidade de campos da relação")
        return
    with open(HeapPath, 'a') as file:
        #insere o CPF com seu proprio padding
        file.write(fillCPF(listOfValues[0]))
        #assumindo que estão na ordem correta já
        for i in range(1, len(listOfValues)):
            file.write(padString(listOfValues[i], maxColSizesList[i]))
        #por fim pulamos uma linha para o próximo registro
        file.write("\n")
    UpdateHEADFile(HeapHeadPath, "Heap", GetNumRegistries(HeapHeadPath, heapHeadSize)+1)




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
    
    if colName not in colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex = colHeadersList.index(colName) #pega o indice referente àquela coluna

    secondValuePresent = False


    secondColumnIndex = -1
    if secondColName != "" and secondValue != "":
        if secondColName not in colHeadersList:
            print("Error: Second column name not found in relation")
            return
        secondColumnIndex = colHeadersList.index(secondColName)
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
        currentBlock = FetchBlock(HeapPath, currentRegistry)#pega 5 registros a partir do registro atual
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
                    DeleteLineFromFile(currentRegistry+i, HeapPath)
                    registryFound = True
                    break
        #se não é EOF e não encontrou registro, repete operação com outro bloco
        currentRegistry +=blockSize
        
    if results == []:
        if valueIsArray:
            print("Não foi encontrado registro com "+colName+ " in (" + values +")")
        else:
            print("Não foi encontrado registro com valor " +colName+ " = " + value)
        
    else:
        print(indexesToDelete)
        
        for reg in reversed(indexesToDelete):
            DeleteLineFromFile(reg, HeapPath)
        print("\n\nRegistries deleted: \n")
        for result in results:
            print(result)
            print("\n")
    
    print("End of query.")
    print("Number of blocks fetched: " + str(numberOfBlocksUsed))

    #updateHEAD with new number of registries if there were deletions
    if results != []:
        UpdateHEADFile(HeapHeadPath, "Heap", GetNumRegistries(HeapHeadPath, heapHeadSize)-len(results))
    





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
    valuesToLoad = padRegistries(readFromFile(csvFilePath))
    valuesToLoad = sortList(valuesToLoad, isOrderedByPrimaryKey)
    #apaga o conteúdo existente no momento(se houver)
    if os.path.exists(OrderedPath):
        os.remove(OrderedPath)
    
    #preenche os valores direto no arquivo
    file = open(OrderedPath, "w+")
    file.write(MakeHEAD("Ordered", len(valuesToLoad)))
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
        blockRegistries = FetchBlock(OrderedPath, (mid-1)*5)
        
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
        blockRegistries = FetchBlock(OrderedPath, (numberBlock-1)*5)
        
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
    
    if colName not in colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex = colHeadersList.index(colName) #pega o indice referente àquela coluna

    print("Running query: ")
    print("SELECT * FROM TB_ORDERED WHERE " + colName + " = " + value + ";")

    # Obtem o numero de blocos do BD
    numBlocks = math.ceil(GetNumRegistries(OrderedPath, 0)/blockSize)
    
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
# Bucket size in blocks 
bucketSize      = 3 #10
# Number of needed buckets
numberOfBuckets = 2 #220

hashTablePath = "BD/HashTable.txt"

#Le o CSV e cria o arquivo do BD de Hash
def CreateHashBD(csvFilePath):

    #Reads the csv file and create the records to be inserted, with fixed length
    valuesToLoad = padRegistries(readFromFile(csvFilePath))
    
    # Delete previous database
    if os.path.exists(HashPath):
        os.remove(HashPath)
    
    # Create empty file to reserve disk space
    with open(HashPath, 'wb') as hashFile:
        hashFile.seek((bucketSize * numberOfBuckets * blockSize * 153) - 1)
        hashFile.write(b'\0')
    
    # Create HEAD to File
    MakeHEAD2(HashHeadPath, "Hash", 0)
    
    registryCounter = 0
    #inserimos valor a valor com a função de inserção do Hash
    for row in valuesToLoad:
        registry = Registry(row, False)
        HashInsertRecord(registry)
        registryCounter +=1

def CalculateHashKey(registry):
    return int(registry.docNumber)

def CalculateHashAddress(hashKey):
    return hashKey % numberOfBuckets
    
class Block:

    def __init__(self, registriesBytes):
        self.listOfRegistries = []
        #iterate over registries bytes
        for b in range(0, len(registriesBytes), 153):
            print(registriesBytes[b])
            self.listOfRegistries += [Registry(registriesBytes[b : b + 153], True)]

    def SizeInBytes(self):
        sizeInBytes = 0
        for registry in self.listOfRegistries:
            sizeInBytes += registry.sizeInBytes

        return sizeInBytes

    def FirstEmptyRecordIndex(self):
        for i in range(len(self.listOfRegistries)):
            try:
                if (self.listOfRegistries[i].docNumber.index('\x00') >= 0):
                    return i
            except:
                pass
        return -1

    def __str__(self):
        str_block = ""
        for registry in self.listOfRegistries:
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

###################################################################################
############################ HASH - SELECT FUNCTIONS ##############################
###################################################################################

def HashSelectRecord():
    pass

###################################################################################
############################ HASH - INSERT FUNCTIONS ##############################
###################################################################################

def HashInsertRecord(registry):
    freeSpaceIndex = -1

    #calculate hash key and address
    hashKey     = CalculateHashKey(registry)
    hashAddress = CalculateHashAddress(hashKey)

    # Init the start offset
    startingOffset = bucketSize * hashAddress

    # Place the record the first block with enough space starting from the file
    with open(HashPath, 'r+b') as hashFile:
        # Search for the right position
        hashFile.seek(startingOffset)
        # Check if there is a colision
        currentBlock = Block(hashFile.read((blockSize * 153)))
        
        # Search for the first free space in the block
        freeSpaceIndex = currentBlock.FirstEmptyRecordIndex()

        if (freeSpaceIndex >= 0):
            # Set the cursor to the beginig of the block again
            hashFile.seek(startingOffset)
            # Place the new record in the right place in the block
            currentBlock.listOfRegistries[freeSpaceIndex] = registry
        else:
            #If we have bucket overflow, change the startting offset to the next bucket and try again in the next bucket
            startingOffset += (blockSize * 153)
        
        # Re-write block to the file
        writtenSize = hashFile.write(str(currentBlock).encode('utf-8'))
        print(writtenSize)



###################################################################################
############################ HASH - DELETE FUNCTIONS ##############################
###################################################################################

def HashDeleteRecord(record):
    pass

###################################################################################
################################### MAIN ##########################################
###################################################################################

CreateHashBD(RJPath)
#HashSelectRecord()
#CreateOrderedBD(RJPath, False)

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
#OrderedSelectSingleRecord(colHeadersList[numColToOrder], query)

