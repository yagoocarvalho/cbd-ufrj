# -*- coding: utf-8 -*-
"""
Created on Fri Oct 12 19:03:55 2018

@author: BRC
"""

import os
import csv
import time
import datetime
import fileinput
import unidecode




###################################################################################
############################ PATH AND DATASET VARIABLES ###########################
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

#Tamanho do bucket do hash (em blocos)
bucketSize = 10

#Quantidade máxima de buckets
numberOfBuckets = 220

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
def IsRelevantRow(rowNumber):
    #
    return rowNumber in relevantColsList

#calcula o tamanho do registro novamente, caso necessario
def CalculateRegistrySize():
    sum = 0
    for key, value in dicColunaTamanhoMax:
        sum+=value
    return sum


#Preenche com 0's a esquerda CPFs que nao possuem seu tamanho totalmente preenchido
def FillCPF(cpf):
    return cpf.zfill(maxColSizesList[0])#tamanho de CPF e fixo

#Completa a string com caracter escolhido para padding(p/ manter tamanho fixo)
def PadString(stringToPad, totalSizeOfField):
    tmp = stringToPad
    for i in range (totalSizeOfField - len(stringToPad)):
        tmp+=paddingCharacter
    return tmp        


#Lê o arquivo desejado e retorna uma lista com todos os registros relevantes do mesmo
#lista retornada sera usada para construir nossos proprios arquivos
def ReadFromFile(csvFilePath):
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
                    if IsRelevantRow(i):
                        #Se for a coluna do CPF, coloca o mesmo no inicio da lista
                        if i == relevantColsList[4]:
                            finalRow.insert(0, FillCPF(row[i]))
                        else:
                            finalRow += [unidecode.unidecode(row[i])]
                print(finalRow)
                registros +=[finalRow]
                lineCount+=1
                if lineCount == 5000: return registros #limita tamanho p/ testes
    return registros

#pega uma lista de registros(matriz bidimensional) e para cada elemento, preenche os espaços faltantes
def PadRegistries(listOfRegistries):
    for i in range(len(listOfRegistries)):
        for j in range(len(listOfRegistries[i])):
            listOfRegistries[i][j] = PadString(listOfRegistries[i][j], maxColSizesList[j])
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


def MakeHEADString(headType, numRegistries):
    string = "File structure: " + headType + "\n"
    string += "Creation: " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + "\n"
    string += "Last modification: " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') + "\n"
    string += "Schema: "
    for key, value in dicColHeaderType.items():
        string += key + "-" + value + "|"
    string += "Number of registries: " + str(numRegistries) + "\n"
    
    return string


def MakeHEAD(headPath, headType, numRegistries):
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
        MakeHEAD(headPath, headType, numRegistries)


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


#StartingRegistry = index do registro inicial a ser buscado (0-based)
def FetchBlock2(DBFilePath, startingRegistry, registryCustomSize):
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
        for i in range(registryCustomSize*startingRegistry):
            c = file.read(1) #vamos de 1 em 1 char para não jogar tudo de uma vez na memória
        
        #Após isso, faz um seek no número de blocos até preencher o bloco(ou acabar o arquivo)
        
        for i in range(blockSize):
            registry = ""
            for j in range(registryCustomSize):
                c = file.read(1)
                #print(c)
                if c == "": 
                    #print("FIM DO ARQUIVO")
                    return block
                registry+=c
            #print("Current registry: "+registry)
            block += [CleanRegistry(registry)]
    return block
