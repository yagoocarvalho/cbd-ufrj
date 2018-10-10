# -*- coding: utf-8 -*-
"""
Created on Sun Oct  7 14:48:57 2018

@author: BRC
"""

import os
import csv


###################################################################################
####################### AUXILIARY FUNCTIONS AND VARIABLES #########################
###################################################################################


#Caminho dos arquivos de candidatos
CandidatesFilePath = "C:/Users/BRC/Downloads/consulta_cand_2018/consulta_cand_2018_"
SPPath = CandidatesFilePath + "SP.csv"
RJPath = CandidatesFilePath + "RJ.csv"
MGPath = CandidatesFilePath + "MG.csv"

#caminho dos arquivos 
BDFilePath = "C:/Users/BRC/Downloads/consulta_cand_2018/BD/"
HeapPath = BDFilePath + "HeapBD.txt"
OrderedPath = BDFilePath + "OrderedBD.txt"
HashPath = BDFilePath + "HashBD.txt"

#caracter usado como enchimento de valores não-cheios de um registro
paddingCharacter = "#"
#tamanho de um registro(medido em caracteres)
registrySize = 152

#Tamanho de um bloco de memória (medido em registros)
blockSize = 5

#Tamanhos máximos de cada atributo(for reference mostly)
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

#Baseado no dic acima(CPF JOGADO PARA A PRIMEIRA POSICAO)
maxColSizesList = [11,2,2,5,70,43,2,10,1,1,1,2,3]

#Baseado no dic acima(e na ordem da lista acima, com CPF no início)
colHeadersList = ["CPF", "SG_UF", "CD_CARGO", 'NR_CANDIDATO', 'NM_CANDIDATO', 'NM_EMAIL', 'NR_PARTIDO', 'DT_NASCIMENTO', 'CD_GENERO', 'CD_GRAU_INSTRUCAO', 'CD_ESTADO_CIVIL', 'CD_COR_RACA', 'CD_OCUPACAO']

#Baseado nos indices acima
relevantColsList = [10, 13, 16, 17, 20, 21, 27, 38, 41, 43, 45, 47, 49]

#é uma coluna relevante dentro do Excel(baseado nas colunas escolhidas acima)
def isRelevantRow(rowNumber):
    #
    return rowNumber in relevantColsList

#calcula o tamanho do registro novamente, caso necessario
def calculateRegistrySize():
    sum = 0
    for key, value in dicColunaTamanhoMax:
        sum+=value
    return sum


#Preenche com 0's à esquerda CPFs que não possuem seu tamanho totalmente preenchido
def fillCPF(cpf):
    return cpf.zfill(maxColSizesList[0])#tamanho de CPF é fixo

#Completa a string com caracter escolhido para padding(p/ manter tamanho fixo)
def padString(stringToPad, totalSizeOfField):
    tmp = stringToPad
    for i in range (totalSizeOfField - len(stringToPad)):
        tmp+=paddingCharacter
    return tmp        


#Lê o arquivo desejado e retorna uma lista com todos os registros relevantes do mesmo
#lista retornada será usada para construir nossos proprios arquivos
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
                        #Se é a coluna do CPF, coloca o mesmo no inicio da lista
                        if i == relevantColsList[4]:
                            finalRow.insert(0, fillCPF(row[i]))
                        else:
                            finalRow += [row[i]]
                print(finalRow)
                registros +=[finalRow]
                lineCount+=1
                if lineCount == 5: return registros #limita tamanho p/ testes
    return registros

#pega uma lista de registros(matriz bidimensional) e para cada elemento, preenche os espaços faltantes
def padRegistries(listOfRegistries):
    for i in range(len(listOfRegistries)):
        for j in range(len(listOfRegistries[i])):
            listOfRegistries[i][j] = padString(listOfRegistries[i][j], maxColSizesList[j])
    return listOfRegistries


#Le o CSV e cria o arquivo do BD de Heap
def CreateHeapBD(csvFilePath, heapFilePath):
    #Lê do CSV e preenche os registros com enchimento para criar o tamanho fixo
    valuesToLoad = padRegistries(readFromFile(csvFilePath))
    
    #apaga o conteúdo existente no momento(se houver)
    if os.path.exists(heapFilePath):
        os.remove(heapFilePath)
    
    
    #preenche os valores direto no arquivo
    file = open(HeapPath, "w+")
    for row in valuesToLoad:
        for cols in row:
            file.write(cols)





###################################################################################
##################################### HEAP ########################################
###################################################################################

###################################################################################
############################ HEAP - SELECT FUNCTIONS ##############################
###################################################################################

#colName = Desired column of the query (SEE LISTS ABOVE FOR COL NAMES)
#value = desired value
#SQL Format: Select * from HeapTable WHERE colName = value
#Retorna o PRIMEIRO registro onde o colName tenha o valor igual à value
def HeapSelectSingleRecord(colName, value):
    numberOfBlocksUsed = 0 #conta o número de vezes que "acessamos a memória do disco"
    #TODO: Finish
    
    if colName not in colHeadersList:
        print("Error: Column name not found in relation.")
        return
    columnIndex = colHeadersList.index(colName) #pega o indice referente àquela coluna

    print("Running query: ")
    print("SELECT * FROM TB_HEAP WHERE " + colName + " = " + value + ";")

    offset = 0
    for i in range(len(maxColSizesList)):
        #calcula offset
        if i < columnIndex:
            #coluna veio antes da procurada, adicionar seu tamanho ao offset
            offset +=maxColSizesList[i]
    print("Offset = " + offset)

    #TODO: usa o offset pra ir olhando a coluna relevante no arquivo
    #TODO: Ver como fazer para pegar blocos de registros


    print("Fim da busca.")
    print("Número de blocos varridos: " + numberOfBlocksUsed)


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

###################################################################################
########################## ORDERED - SELECT FUNCTIONS #############################
###################################################################################

###################################################################################
########################## ORDERED - INSERT FUNCTIONS #############################
###################################################################################

###################################################################################
########################## ORDERED - DELETE FUNCTIONS #############################
###################################################################################