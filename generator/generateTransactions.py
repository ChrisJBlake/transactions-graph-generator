import csv
import multiprocessing as mp
import os
from models.Transaction import Transaction
from .utils import writeBatch, log

transactionHeaders = ['source', 'target', 'date', 'time', 'amount', 'currency']

def __generateTransactions(edges, transactionsFile, batchSize, label):
    try:
        os.remove(transactionsFile)
    except OSError:
        pass

    totalNumberOfTransactions = 0

    with open(transactionsFile, 'a') as transactions:
        batch = []
        sourceNodesCount = 0
        # transactions.write("|".join(transactionHeaders) + "\n")  # Can't have the header if we take transactions from every file

        for sourceNode, targets in edges.items():
            sourceNodesCount += 1
            if sourceNodesCount % batchSize == 1: log(label + ": generating transactions for source node " + str(sourceNodesCount) + ", transaction count: " + str(totalNumberOfTransactions))
            
            targetNodesCount = 0
            for targetNode, transactionsCount in targets.items():
                targetNodesCount += 1

                for _ in range(0, transactionsCount):
                    t = Transaction(sourceNode, targetNode)

                    batch.append(t.toRow(transactionHeaders))
                    totalNumberOfTransactions += 1

                if len(batch) > batchSize:
                    writeBatch(transactions, batch)
                    batch = []

        if len(batch) != 0:
            writeBatch(transactions, batch)

        log(label + ": TOTAL: generating transactions for source node " + str(sourceNodesCount) + ", transaction count: " + str(totalNumberOfTransactions))

def generateTransactions(files, batchSize):
    print("Reading nodes in memory")
    clientEdges = {}
    companyEdges = {}

    with open(files['clients-clients-edges'], 'r') as file:
        reader = csv.reader(file, delimiter="|")
        for row in reader:
            if not row[0] in clientEdges:
                clientEdges[row[0]] = {}
            clientEdges[row[0]].update(eval(row[1]))

    with open(files['clients-companies-edges'], 'r') as file:
        reader = csv.reader(file, delimiter="|")
        for row in reader:
            if not row[0] in clientEdges:
                clientEdges[row[0]] = {}
            clientEdges[row[0]].update(eval(row[1]))

    with open(files['companies-clients-edges'], 'r') as file:
        reader = csv.reader(file, delimiter="|")
        for row in reader:
            if not row[0] in clientEdges:
                companyEdges[row[0]] = {}
            companyEdges[row[0]].update(eval(row[1]))


    clientSourcingTransactions = mp.Process(target=__generateTransactions, args=(
        clientEdges,
        files['clients-sourcing-transactions'],
        batchSize,
        'transaction(client->*)'
    ))

    companyClientTransactions = mp.Process(target=__generateTransactions, args=(
        companyEdges,
        files['companies-sourcing-transactions'],
        batchSize,
        'transaction(company->client)'
    ))

    clientSourcingTransactions.start()
    companyClientTransactions.start()

    clientSourcingTransactions.join()
    companyClientTransactions.join()

