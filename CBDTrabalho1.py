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
CandidatesFilePath = "./data/consulta_cand_2018_"
SPPath = CandidatesFilePath + "SP.csv"
RJPath = CandidatesFilePath + "RJ.csv"
MGPath = CandidatesFilePath + "MG.csv"

#caminho dos arquivos 
BDFilePath = "./BD/"
HeapPath = BDFilePath + "HeapBD.txt"
HeapHeadPath = BDFilePath + "HeapHEAD.txt"
OrderedPath = BDFilePath + "OrderedBD.txt"
OrderedHeadPath = BDFilePath + "OrderedHEAD.txt"
HashPath = BDFilePath + "HashBD.txt"
HashHeadPath = BDFilePath+ "HashHEAD.txt"

#caracter usado como enchimento de valores nao-cheios de um registro
paddingCharacter = "#"
#tamanho de um registro(medido em caracteres)
registrySize = 153


#Tamanho de um bloco de memoria (medido em registros)
blockSize = 5

#Tamanho do head(em linhas)
heapHeadSize = 5

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
    with open(csvFilePath, 'r', encoding="ISO-8859-1") as file:
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
                if lineCount == 21: return registros #limita tamanho p/ testes
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
        newRegistry += [registryString[offset:offset+maxColSizesList[i]].replace(paddingCharacter, "")]
        
        offset+=maxColSizesList[i]
    return newRegistry

# Method to insert a given record into the file. The record will be inserted immediately after the location.
def insertLineIntoFile(record, location, filepath):
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

# Method to delete a record from the file.
def deleteLineFromFile(location, filepath):
    # Open the file
    for line in fileinput.input(filepath, inplace=1):
        # Check line number
        linenum = fileinput.lineno()
        # If we are in our desired location, append the new record to the current one. Else, just remove the line-ending character
        if linenum == location:
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
    string += "\nNumber of registries: " + str(numRegistries) + "\n"
    
    return string


def MakeHEAD2(headPath, headType, numRegistries):
    file = open(headPath, 'a')
    string = "File structure: " + headType + "\n"
    string += "Creation: " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + "\n"
    string += "Last modification: " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + "\n"
    string += "Schema: "
    for key, value in dicColHeaderType.items():
        string += key + "-" + value + "|"
    string += "\nNumber of registries: " + str(numRegistries) + "\n"
    file.write(string)
    #return string



def UpdateHEADFile(headPath, numRegistries):
    file = open(headPath, 'r')
    
    headContent = file.readlines()
    print(headContent)
    headContent
    file.close()
    os.remove(headPath)
    
    #recria ela com as alteracoes
    file = open(headPath, 'a')
    file.write(headContent[0])
    file.write(headContent[1])
    file.write("Last modification: " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + "\n")
    file.write(headContent[3])
    file.write("\nNumber of registries: " + str(numRegistries) + "\n")


#Gets number of registries from HEAD file
def getNumRegistries(DBHeadFilePath, headSize):
    #posiÃ§Ã£o de inÃ­cio de leitura dos dados
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
def CreateHeapBD(csvFilePath, heapFilePath):
    #Lê do CSV e preenche os registros com enchimento para criar o tamanho fixo
    valuesToLoad = padRegistries(readFromFile(csvFilePath))
    
    #apaga o conteúdo existente no momento(se houver)
    if os.path.exists(heapFilePath):
        os.remove(heapFilePath)
    
    
    #preenche os valores direto no arquivo
    file = open(HeapPath, "w+")
    file.write(MakeHEAD("HEAP"))
    file.close()
    
    registryCounter = 0
    #inserimos valor a valor com a função de inserção da Heap
    for row in valuesToLoad:
        HeapInsertSingleRecord(row)
        registryCounter +=1
    
    #for row in valuesToLoad:
    #    for cols in row:
    #        file.write(cols) #Não chamamos





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
def HeapSelectRecord(filePath, colName, value, singleRecordSelection = False, valueIsArray = False, secondColName = "", secondValue = ""):
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
    #TODO: Ver como fazer para pegar blocos de registros
    while not (registryFound or endOfFile):
        currentBlock = FetchBlock(filePath, currentRegistry)#pega 5 registros a partir do registro atual
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
        
    print("Fim da busca.")
    print("Número de blocos varridos: " + str(numberOfBlocksUsed))

    return results, numberOfBlocksUsed





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




###################################################################################
############################ HEAP - DELETE FUNCTIONS ##############################
###################################################################################


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
    
 
    # Cria Head do bd
    MakeHEAD2(OrderedHeadPath, "Ordered", len(valuesToLoad))

    #preenche os valores direto no arquivo
    file = open(OrderedPath, "w+")
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
def OrderedSelectSingleRecord(colName, value, singleRecordSelection = False):
    numberOfBlocksUsed = 0 #conta o número de vezes que "acessamos a memória do disco"
    
    if colName not in colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex = colHeadersList.index(colName) #pega o indice referente àquela coluna

    print("Running query: ")
    print("SELECT * FROM TB_ORDERED WHERE " + colName + " = " + value + " limit 1;")

    # Obtem o numero de blocos do BD
    numBlocks = math.ceil(getNumRegistries(OrderedHeadPath, heapHeadSize)/blockSize)
    
    # Verifica se o campo procurado eh equivalente ao campo pelo qual o banco foi ordenado
    # Caso seja, utilizar busca binaria
    if(columnIndex == numColToOrder):   
        blockFounded,numberOfBlocksUsed = binarySearch(columnIndex, value,
                                                       numBlocks, singleRecordSelection)
    # Senao realizar select linear
    else:
        blockFounded = HeapSelectRecord(OrderedPath,colName, value, singleRecordSelection) #fazer select normal
        
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
    if colName not in colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex = colHeadersList.index(colName) #pega o indice referente àquela coluna

    print("Running query: ")
    print("SELECT * FROM TB_ORDERED WHERE " + colName + " in " + str(values) + ";")

    # Obtem o numero de blocos do BD
    numBlocks = math.ceil(getNumRegistries(OrderedHeadPath, heapHeadSize)/blockSize)

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
        blocksFounded, numberOfBlocksUsed = HeapSelectRecord(OrderedPath,colName, values, valueIsArray = True) #fazer select normal
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
        blockRegistries = FetchBlock(OrderedPath, (mid-1)*5)
        
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
        blockRegistries = FetchBlock(OrderedPath, (numberBlock-1)*5)
        
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
    totalBlocksFounded = []
    totalNumberOfBlocksUsed = 0 
    
    if colName not in colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex1 = colHeadersList.index(colName) #pega o indice referente àquela coluna

    if secondColName not in colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex2 = colHeadersList.index(secondColName) #pega o indice referente àquela coluna

    print("Running query: ")
    print("SELECT * FROM TB_ORDERED WHERE " + colName + " = " + str(value) + 
          " AND " + str(secondColName) +" = " + str(secondValue) +  ";")

    # Obtem o numero de blocos do BD
    numBlocks = math.ceil(getNumRegistries(OrderedHeadPath, heapHeadSize)/blockSize)

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
        blocksFounded, numberOfBlocksUsed = HeapSelectRecord(OrderedPath,
                colName, value, False, secondColName, secondValue) #fazer select normal
        totalBlocksFounded.append(blocksFounded)
        totalNumberOfBlocksUsed += numberOfBlocksUsed

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
        blockRegistries = FetchBlock(OrderedPath, (mid-1)*5)
        
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
        blockRegistries = FetchBlock(OrderedPath, (numberBlock-1)*5)
        
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
    totalNumberOfBlocksUsed = 0 
    
    if colName not in colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex1 = colHeadersList.index(colName) #pega o indice referente àquela coluna


    print("Running query: ")
    print("SELECT * FROM TB_ORDERED WHERE " + colName + " BETWEEN " + str(firstValue) + 
          " AND " + str(secondValue) +  ";")

    # Obtem o numero de blocos do BD
    numBlocks = math.ceil(getNumRegistries(OrderedHeadPath, heapHeadSize)/blockSize)

    # Verifica se o campo procurado eh equivalente ao campo pelo qual o banco foi ordenado
    # Caso seja, utilizar busca binaria
    if(columnIndex1 == numColToOrder):
        blocksFounded,numberOfBlocksUsed = binarySearchBetween(columnIndex1, firstValue,
              numBlocks, secondValue)
        
    # Senao realizar select linear
    else:
        #VER ISSO
        blocksFounded, numberOfBlocksUsed = HeapSelectRecord(OrderedPath,
                colName, firstValue, False, secondValue) #fazer select normal
        totalNumberOfBlocksUsed += numberOfBlocksUsed

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



###################################################################################
################################### MAIN ##########################################
###################################################################################


CreateOrderedBD(RJPath, False)

#print('-----')

#blocks = binarySearch(1)

#query = ["86551337791","01093643765"]
query = ["RJ","SP"]
#p = [block if "09650185712" in block[numColToOrder] else None for block in blocks ]
#q = next((block for block in blocks if query in block[numColToOrder]), None)

#print(q)

#0, len(arr)-1,
#result = binarySearch(numColToOrder, query, 4)
#print (result)

#OrderedSelectSingleRecord('NM_EMAIL', query)
OrderedSelectWithTwoFields(colHeadersList[5], "GPM.GUIMARAES.ADV@GMAIL.COM", 
                           colHeadersList[numColToOrder], "07833694762")
#OrderedSelectBetweenTwoValues(colHeadersList[numColToOrder],"01093643765","07833694762" )
#OrderedSelectBetweenTwoValues(colHeadersList[numColToOrder],"ANDERSONMJFILHOS@HOTMAIL.COM",
#                              "CELIASANCHES134@GMAIL.COM" )