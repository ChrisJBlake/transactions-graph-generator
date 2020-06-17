import csv
import multiprocessing as mp
import os
from math import ceil
import random
from .utils import writeBatch, log
from models.Patterns import generateFlowPattern, generateCircularPattern, generateTimePattern

transactionHeaders = ['source', 'target', 'date', 'time', 'amount', 'currency']

def __generatePatterns(nodes, counts, transactionsFile, batchSize, patternsGenerator, label, numPatterns: int):
	try:
		os.remove(transactionsFile)
	except OSError:
		pass

	totalNumberOfPatterns = 0
	totalNumberOfTransactions = 0

	numberOfPatterns = numPatterns

	log(label + ': Number of patterns to be generated: ' + str(numberOfPatterns))

	with open(transactionsFile, 'a') as transactions:
		batch = []

		# transactions.write('|'.join(transactionHeaders) + '\n')
		for i in range(numberOfPatterns):
			batch += patternsGenerator(nodes)

			totalNumberOfPatterns += 3
			totalNumberOfTransactions += len(batch)

			if len(batch) > batchSize:
				writeBatch(transactions, batch)
				batch = []
				log(label + ': Generated ' + str(totalNumberOfPatterns) + ' patterns with ' + str(totalNumberOfTransactions) + ' transactions in total')

		if len(batch) != 0:
			writeBatch(transactions, batch)

		log(label + ': TOTAL Generated ' + str(totalNumberOfPatterns) + ' patterns with ' + str(totalNumberOfTransactions) + ' transactions in total')


def generatePatterns(files, counts: int, batchSize):
	print("Reading nodes in memory")
	nodes = []

	with open(files['client'], 'r') as f:
		reader = csv.reader(f, delimiter="|")
		next(reader)
		log("Loading clients...")
		for row in reader:
			nodes.append(row[0])

	with open(files['company'], 'r') as f:
		reader = csv.reader(f, delimiter="|")
		next(reader)
		log("Loading companies...")
		for row in reader:
			nodes.append(row[0])

	# Determine how many of each pattern we're going to make
	numFlow = random.randint(0, counts)
	counts -= numFlow
	numCircular = random.randint(0, counts)
	counts -= numCircular
	numTime = counts  # Have the remainder of patterns be Time

	flow = mp.Process(target=__generatePatterns, args=(
		nodes,
		counts,
		files['flow-pattern-transactions'],
		batchSize,
		generateFlowPattern,
		'Flow patterns',
		numFlow
	))

	circular = mp.Process(target=__generatePatterns, args=(
		nodes,
		counts,
		files['circular-pattern-transactions'],
		batchSize,
		generateCircularPattern,
		'Circular patterns',
		numCircular
	))

	time = mp.Process(target=__generatePatterns, args=(
		nodes,
		counts,
		files['time-pattern-transactions'],
		batchSize,
		generateTimePattern,
		'Time patterns',
		numTime
	))

	flow.start()
	circular.start()
	time.start()

	flow.join()
	circular.join()
	time.join()